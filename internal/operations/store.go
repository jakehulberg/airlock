package operations

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrations embed.FS

type Store struct{ DB *pgxpool.Pool }

func Open(ctx context.Context, url string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("invalid database configuration")
	}
	if caPath := os.Getenv("AIRLOCK_DATABASE_CA"); caPath != "" {
		pem, err := os.ReadFile(caPath)
		if err != nil {
			return nil, fmt.Errorf("read database CA: %w", err)
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("invalid database CA")
		}
		cfg.ConnConfig.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: cfg.ConnConfig.Host}
		cfg.ConnConfig.Fallbacks = nil
	}
	cfg.MaxConns = 12
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	db, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	s := &Store{db}
	if err = s.Migrate(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}
func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(837201); CREATE TABLE IF NOT EXISTS airlock_migrations(version text PRIMARY KEY, checksum text NOT NULL)`); err != nil {
		return err
	}
	files, err := migrations.ReadDir("migrations")
	if err != nil {
		return err
	}
	var count int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM airlock_migrations`).Scan(&count); err != nil {
		return err
	}
	if count > len(files) {
		return fmt.Errorf("database schema is newer than this application")
	}
	for _, file := range files {
		b, err := migrations.ReadFile("migrations/" + file.Name())
		if err != nil {
			return err
		}
		hash := sha256.Sum256(b)
		checksum := hex.EncodeToString(hash[:])
		var stored string
		err = tx.QueryRow(ctx, `SELECT checksum FROM airlock_migrations WHERE version=$1`, file.Name()).Scan(&stored)
		if err == nil {
			if stored != checksum {
				return fmt.Errorf("migration checksum mismatch: %s", file.Name())
			}
			continue
		}
		if err != pgx.ErrNoRows {
			return err
		}
		if _, err = tx.Exec(ctx, string(b)); err != nil {
			return fmt.Errorf("migration %s: %w", file.Name(), err)
		}
		if _, err = tx.Exec(ctx, `INSERT INTO airlock_migrations VALUES($1,$2)`, file.Name(), checksum); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
func audit(ctx context.Context, tx pgx.Tx, owner, kind, ref string) error {
	_, err := tx.Exec(ctx, `INSERT INTO activity(owner,kind,reference) VALUES($1,$2,$3)`, owner, kind, ref)
	return err
}
func (s *Store) Transaction(ctx context.Context, f func(pgx.Tx) error) error {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = f(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func jsonBytes(v any) []byte { b, _ := json.Marshal(v); return b }
func (s *Store) SaveAgent(ctx context.Context, owner string, a Agent) (Agent, error) {
	if strings.TrimSpace(a.Name) == "" || a.Pool == "" || len(a.Instructions) > 65536 {
		return a, fmt.Errorf("%w: agent needs name, pool and bounded instructions", ErrInvalid)
	}
	if a.ID == "" {
		a.ID = ID()
	}
	tag, err := s.DB.Exec(ctx, `INSERT INTO agents(id,owner,definition) VALUES($1,$2,$3) ON CONFLICT(id) DO UPDATE SET definition=excluded.definition,updated_at=now() WHERE agents.owner=excluded.owner`, a.ID, owner, jsonBytes(a))
	if err == nil && tag.RowsAffected() == 0 {
		err = ErrNotFound
	}
	return a, err
}
func (s *Store) SaveWorkflow(ctx context.Context, owner string, w Workflow) (Workflow, error) {
	if err := w.Validate(); err != nil {
		return w, err
	}
	if w.ID == "" {
		w.ID = ID()
	}
	err := s.Transaction(ctx, func(tx pgx.Tx) error {
		for _, step := range w.Steps {
			var id string
			if err := tx.QueryRow(ctx, `SELECT id FROM agents WHERE id=$1 AND owner=$2`, step.AgentID, owner).Scan(&id); err != nil {
				return fmt.Errorf("%w: unknown agent", ErrInvalid)
			}
		}
		tag, err := tx.Exec(ctx, `INSERT INTO workflows(id,owner,definition) VALUES($1,$2,$3) ON CONFLICT(id) DO UPDATE SET definition=excluded.definition,updated_at=now() WHERE workflows.owner=excluded.owner`, w.ID, owner, jsonBytes(w))
		if err == nil && tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		return err
	})
	return w, err
}
func insertTask(ctx context.Context, tx pgx.Tx, owner string, t Task) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,17))`, owner); err != nil {
		return err
	}
	var pending int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE owner=$1 AND state NOT IN ('completed','failed','cancelled')`, owner).Scan(&pending); err != nil {
		return err
	}
	if pending >= 1000 {
		return fmt.Errorf("%w: pending task limit reached", ErrConflict)
	}
	if t.ID == "" || strings.TrimSpace(t.Title) == "" || strings.TrimSpace(t.Prompt) == "" || len(t.Prompt) > 65536 || t.Priority < -10 || t.Priority > 10 || t.MaxAttempts < 1 || t.MaxAttempts > 10 || t.TimeoutSeconds < 1 || t.TimeoutSeconds > 14400 {
		return fmt.Errorf("%w: invalid task fields", ErrInvalid)
	}
	if t.DependencyPolicy == "" {
		t.DependencyPolicy = "fail"
	}
	if t.DependencyPolicy != "fail" && t.DependencyPolicy != "block" {
		return fmt.Errorf("%w: dependency policy", ErrInvalid)
	}
	var agent string
	var agentApproval bool
	if err := tx.QueryRow(ctx, `SELECT id,COALESCE((definition->>'approvalRequired')::boolean,false) FROM agents WHERE id=$1 AND owner=$2 AND (definition->>'enabled')::boolean`, t.AgentID, owner).Scan(&agent, &agentApproval); err != nil {
		return fmt.Errorf("%w: agent unavailable", ErrInvalid)
	}
	t.ApprovalRequired = t.ApprovalRequired || agentApproval
	if t.State == "" {
		t.State = "queued"
	}
	if t.State != "queued" && t.State != "draft" {
		return fmt.Errorf("%w: initial state", ErrInvalid)
	}
	if t.ApprovalRequired && t.State != "draft" {
		t.State = "awaiting_approval"
	}
	_, err := tx.Exec(ctx, `INSERT INTO tasks(id,owner,title,prompt,agent_id,run_id,state,priority,max_attempts,timeout_seconds,approval_required,idempotency_key,dependency_policy) VALUES($1,$2,$3,$4,$5,NULLIF($6,''),$7,$8,$9,$10,$11,NULLIF($12,''),$13)`, t.ID, owner, t.Title, t.Prompt, t.AgentID, t.RunID, t.State, t.Priority, t.MaxAttempts, t.TimeoutSeconds, t.ApprovalRequired, t.IdempotencyKey, t.DependencyPolicy)
	if err != nil {
		return err
	}
	for _, dep := range t.Dependencies {
		var id string
		if err := tx.QueryRow(ctx, `SELECT id FROM tasks WHERE id=$1 AND owner=$2`, dep, owner).Scan(&id); err != nil {
			return fmt.Errorf("%w: unknown dependency", ErrInvalid)
		}
		if _, err = tx.Exec(ctx, `INSERT INTO dependencies VALUES($1,$2)`, t.ID, dep); err != nil {
			return err
		}
	}
	if err = audit(ctx, tx, owner, "task.created", t.ID); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `SELECT pg_notify('airlock_queue','')`)
	return err
}
func (s *Store) CreateTask(ctx context.Context, owner string, t Task) (Task, error) {
	t.ID = ID()
	t.RunID = "" // Workflow membership is assigned only by RunWorkflow.
	if t.MaxAttempts == 0 {
		t.MaxAttempts = 3
	}
	if t.TimeoutSeconds == 0 {
		t.TimeoutSeconds = 1800
	}
	err := s.Transaction(ctx, func(tx pgx.Tx) error {
		if t.IdempotencyKey != "" {
			if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, owner+":"+t.IdempotencyKey); err != nil {
				return err
			}
			var b []byte
			err := tx.QueryRow(ctx, `SELECT to_jsonb(t) FROM tasks t WHERE owner=$1 AND idempotency_key=$2`, owner, t.IdempotencyKey).Scan(&b)
			if err == nil {
				var prior struct{ ID string }
				_ = json.Unmarshal(b, &prior)
				t.ID = prior.ID
				return nil
			}
			if err != pgx.ErrNoRows {
				return err
			}
		}
		return insertTask(ctx, tx, owner, t)
	})
	return t, err
}
func (s *Store) Action(ctx context.Context, owner, id, action, reason string) error {
	return s.Transaction(ctx, func(tx pgx.Tx) error {
		var state string
		var count, limit int
		var approval, approved bool
		if err := tx.QueryRow(ctx, `SELECT state,attempt_count,max_attempts,approval_required,approved FROM tasks WHERE id=$1 AND owner=$2 FOR UPDATE`, id, owner).Scan(&state, &count, &limit, &approval, &approved); err != nil {
			if err == pgx.ErrNoRows {
				return ErrNotFound
			}
			return err
		}
		if action == "cancel" && (state == "running" || state == "uncertain") {
			_, err := tx.Exec(ctx, `UPDATE tasks SET cancel_requested=true WHERE id=$1`, id)
			if err != nil {
				return err
			}
			return audit(ctx, tx, owner, "task.cancel_requested", id)
		}
		next, err := Transition(state, action)
		if err != nil {
			return err
		}
		if next == "queued" && approval && !approved && action != "approve" {
			next = "awaiting_approval"
		}
		if action == "retry" && count >= limit {
			return fmt.Errorf("%w: maximum attempts reached", ErrConflict)
		}
		if action == "approve" || action == "reject" {
			decision := "approved"
			if action == "reject" {
				decision = "rejected"
			}
			if _, err = tx.Exec(ctx, `INSERT INTO approvals(id,task_id,owner,decision,reason) VALUES($1,$2,$3,$4,$5)`, ID(), id, owner, decision, reason); err != nil {
				return err
			}
		}
		_, err = tx.Exec(ctx, `UPDATE tasks SET state=$2,approved=approved OR $3,available_at=now(),finished_at=CASE WHEN $2='cancelled' THEN now() ELSE NULL END WHERE id=$1`, id, next, action == "approve")
		if err != nil {
			return err
		}
		return audit(ctx, tx, owner, "task."+action, id)
	})
}

// Snapshot returns bounded, owner-scoped operational data, including released sandbox history.
func (s *Store) Snapshot(ctx context.Context, owner string) (map[string]any, error) {
	out := map[string]any{}
	queries := map[string]string{
		"agents":          `SELECT definition FROM agents WHERE owner=$1 ORDER BY updated_at DESC LIMIT 200`,
		"workflows":       `SELECT definition FROM workflows WHERE owner=$1 ORDER BY updated_at DESC LIMIT 200`,
		"tasks":           `SELECT to_jsonb(t)-'worker'-'prompt' AS data FROM tasks t WHERE owner=$1 ORDER BY created_at DESC LIMIT 500`,
		"runs":            `SELECT to_jsonb(r) FROM workflow_runs r WHERE owner=$1 ORDER BY created_at DESC LIMIT 200`,
		"schedules":       `SELECT definition || jsonb_build_object('next',next_run,'enabled',enabled) FROM schedules WHERE owner=$1 ORDER BY updated_at DESC LIMIT 200`,
		"activity":        `SELECT to_jsonb(a) FROM activity a WHERE owner=$1 ORDER BY id DESC LIMIT 200`,
		"approvals":       `SELECT to_jsonb(t)-'worker' FROM tasks t WHERE owner=$1 AND state='awaiting_approval' ORDER BY created_at LIMIT 200`,
		"scheduleHistory": `SELECT to_jsonb(f) FROM schedule_fires f JOIN schedules s ON s.id=f.schedule_id WHERE s.owner=$1 ORDER BY due_at DESC LIMIT 200`,
		"interactive":     `SELECT data-'output'-'prompt' FROM interactive_tasks WHERE owner=$1 ORDER BY updated_at DESC LIMIT 100`,
	}
	for key, q := range queries {
		rows, err := s.DB.Query(ctx, q, owner)
		if err != nil {
			return nil, err
		}
		items := []json.RawMessage{}
		for rows.Next() {
			var b []byte
			if err = rows.Scan(&b); err != nil {
				rows.Close()
				return nil, err
			}
			items = append(items, b)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
		out[key] = items
	}
	rows, err := s.DB.Query(ctx, `SELECT state,count(*) FROM tasks WHERE owner=$1 GROUP BY state`, owner)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := map[string]int{}
	for rows.Next() {
		var state string
		var n int
		if err = rows.Scan(&state, &n); err != nil {
			return nil, err
		}
		counts[state] = n
	}
	out["counts"] = counts
	return out, rows.Err()
}
func (s *Store) Detail(ctx context.Context, owner, id string) (map[string]any, error) {
	var b []byte
	err := s.DB.QueryRow(ctx, `SELECT to_jsonb(t)-'worker' FROM tasks t WHERE owner=$1 AND id=$2`, owner, id).Scan(&b)
	if err == pgx.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	out := map[string]any{"task": json.RawMessage(b)}
	for key, q := range map[string]string{"attempts": `SELECT to_jsonb(a)-'sandbox_owner' FROM attempts a WHERE task_id=$1 ORDER BY number`, "dependencies": `SELECT to_jsonb(d) FROM dependencies d WHERE task_id=$1`, "messages": `SELECT to_jsonb(m) FROM messages m WHERE task_id=$1 ORDER BY created_at LIMIT 200`} {
		rows, err := s.DB.Query(ctx, q, id)
		if err != nil {
			return nil, err
		}
		items := []json.RawMessage{}
		for rows.Next() {
			var b []byte
			if err = rows.Scan(&b); err != nil {
				rows.Close()
				return nil, err
			}
			items = append(items, b)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
		out[key] = items
	}
	return out, nil
}

// Assign changes only work that has never started and invalidates an old approval.
func (s *Store) Assign(ctx context.Context, owner, id, agent string, priority int) error {
	if priority < -10 || priority > 10 {
		return fmt.Errorf("%w: priority", ErrInvalid)
	}
	return s.Transaction(ctx, func(tx pgx.Tx) error {
		var state string
		var attempts int
		var required bool
		if err := tx.QueryRow(ctx, `SELECT state,attempt_count,approval_required FROM tasks WHERE id=$1 AND owner=$2 FOR UPDATE`, id, owner).Scan(&state, &attempts, &required); err != nil {
			return ErrNotFound
		}
		if attempts != 0 || state == "cancelled" || state == "completed" || state == "failed" {
			return fmt.Errorf("%w: started tasks cannot be reassigned", ErrConflict)
		}
		var agentApproval bool
		if err := tx.QueryRow(ctx, `SELECT COALESCE((definition->>'approvalRequired')::boolean,false) FROM agents WHERE id=$1 AND owner=$2 AND (definition->>'enabled')::boolean`, agent, owner).Scan(&agentApproval); err != nil {
			return ErrNotFound
		}
		required = required || agentApproval
		if required && state != "draft" {
			state = "awaiting_approval"
		}
		if _, err := tx.Exec(ctx, `UPDATE tasks SET agent_id=$2,priority=$3,approved=false,approval_required=$4,state=$5 WHERE id=$1`, id, agent, priority, required, state); err != nil {
			return err
		}
		return audit(ctx, tx, owner, "task.assigned", id)
	})
}

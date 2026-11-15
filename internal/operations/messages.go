package operations

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
)

type Principal struct{ Attempt, Task, Owner, Agent, Run string }

func tokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
func (s *Store) IssueToken(ctx context.Context, w *Work) (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	token := hex.EncodeToString(b)
	_, err := s.DB.Exec(ctx, `UPDATE attempts SET token_hash=$2,token_expires=now()+make_interval(secs=>$3) WHERE id=$1`, w.Attempt, tokenHash(token), w.Task.TimeoutSeconds)
	return token, err
}
func (s *Store) AuthenticateAgent(ctx context.Context, token string) (Principal, error) {
	var p Principal
	if len(token) != 64 {
		return p, ErrNotFound
	}
	err := s.DB.QueryRow(ctx, `SELECT a.id,t.id,t.owner,t.agent_id,COALESCE(t.run_id,'') FROM attempts a JOIN tasks t ON t.id=a.task_id WHERE a.token_hash=$1 AND a.token_expires>now() AND a.state='running' AND t.state='running' AND t.lease_until>now() AND NOT t.cancel_requested AND a.number=t.attempt_count AND a.pod_uid<>''`, tokenHash(token)).Scan(&p.Attempt, &p.Task, &p.Owner, &p.Agent, &p.Run)
	return p, err
}

type Message struct {
	ID            string          `json:"id"`
	Recipient     string          `json:"recipient"`
	Type          string          `json:"type"`
	Payload       json.RawMessage `json:"payload"`
	CorrelationID string          `json:"correlationId"`
}

func (s *Store) SendMessage(ctx context.Context, p Principal, m Message) (Message, error) {
	if m.Type == "" || len(m.Type) > 64 || len(m.Payload) == 0 || len(m.Payload) > 32768 || !json.Valid(m.Payload) {
		return m, fmt.Errorf("%w: message requires a type and bounded JSON payload", ErrInvalid)
	}
	m.ID = ID()
	if m.CorrelationID == "" {
		m.CorrelationID = ID()
	}
	err := s.Transaction(ctx, func(tx pgx.Tx) error {
		var allowed bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM tasks WHERE owner=$1 AND agent_id=$2 AND (run_id=NULLIF($3,'') OR parent_id=$4 OR id=(SELECT parent_id FROM tasks WHERE id=$4)))`, p.Owner, m.Recipient, p.Run, p.Task).Scan(&allowed); err != nil {
			return err
		}
		if !allowed {
			return fmt.Errorf("%w: recipient must participate in this workflow or delegation", ErrInvalid)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO messages(id,owner,sender,recipient,task_id,run_id,type,payload,correlation_id) VALUES($1,$2,$3,$4,$5,NULLIF($6,''),$7,$8,$9)`, m.ID, p.Owner, p.Agent, m.Recipient, p.Task, p.Run, m.Type, m.Payload, m.CorrelationID); err != nil {
			return err
		}
		return audit(ctx, tx, p.Owner, "message.sent", p.Task)
	})
	return m, err
}
func (s *Store) Inbox(ctx context.Context, p Principal) ([]json.RawMessage, error) {
	rows, err := s.DB.Query(ctx, `SELECT to_jsonb(m) FROM messages m WHERE owner=$1 AND recipient=$2 AND (run_id=NULLIF($3,'') OR task_id IN (SELECT id FROM tasks WHERE parent_id=$4) OR task_id=(SELECT parent_id FROM tasks WHERE id=$4)) ORDER BY created_at LIMIT 100`, p.Owner, p.Agent, p.Run, p.Task)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []json.RawMessage{}
	for rows.Next() {
		var b []byte
		if err = rows.Scan(&b); err != nil {
			return nil, err
		}
		items = append(items, b)
	}
	return items, rows.Err()
}
func (s *Store) Delegate(ctx context.Context, p Principal, t Task) (Task, error) {
	t.ID = ID()
	t.RunID = p.Run
	t.MaxAttempts = 1
	t.IdempotencyKey = ""
	t.State = "queued"
	t.Dependencies = nil
	err := s.Transaction(ctx, func(tx pgx.Tx) error {
		var root string
		var depth, timeout int
		var definition []byte
		if err := tx.QueryRow(ctx, `SELECT COALESCE(t.root_id,t.id),t.depth,t.timeout_seconds,a.definition FROM tasks t JOIN agents a ON a.id=t.agent_id WHERE t.id=$1 FOR UPDATE OF t`, p.Task).Scan(&root, &depth, &timeout, &definition); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, root); err != nil {
			return err
		}
		var agent Agent
		if err := json.Unmarshal(definition, &agent); err != nil {
			return err
		}
		if !slices.Contains(agent.Delegates, t.AgentID) || depth >= 3 {
			return fmt.Errorf("%w: delegation target or depth not permitted", ErrInvalid)
		}
		var count int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE root_id=$1`, root).Scan(&count); err != nil {
			return err
		}
		if count >= 8 {
			return fmt.Errorf("%w: delegation budget exhausted", ErrConflict)
		}
		t.TimeoutSeconds = min(timeout, 1800)
		t.ApprovalRequired = true // Delegated work always requires a human decision.
		if err := insertTask(ctx, tx, p.Owner, t); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE tasks SET parent_id=$2,root_id=$3,depth=$4 WHERE id=$1`, t.ID, p.Task, root, depth+1)
		return err
	})
	return t, err
}

func (s *Store) Result(ctx context.Context, p Principal, result json.RawMessage) error {
	if len(result) > 65536 || !json.Valid(result) {
		return fmt.Errorf("%w: result must be valid JSON up to 64 KiB", ErrInvalid)
	}
	_, err := s.DB.Exec(ctx, `UPDATE attempts SET result=$2 WHERE id=$1 AND state='running'`, p.Attempt, result)
	return err
}
func (s *Store) Acknowledge(ctx context.Context, p Principal, id string) error {
	tag, err := s.DB.Exec(ctx, `UPDATE messages SET status='processed' WHERE id=$1 AND owner=$2 AND recipient=$3 AND (run_id=NULLIF($4,'') OR task_id IN (SELECT id FROM tasks WHERE parent_id=$5) OR task_id=(SELECT parent_id FROM tasks WHERE id=$5))`, id, p.Owner, p.Agent, p.Run, p.Task)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

// Listen complements periodic queue scans. A dropped connection is retried without
// losing work, because notifications contain no authoritative state.
func (s *Store) Listen(ctx context.Context, wake chan<- struct{}) {
	for ctx.Err() == nil {
		conn, err := s.DB.Acquire(ctx)
		if err == nil {
			_, err = conn.Exec(ctx, `LISTEN airlock_queue`)
			for err == nil {
				_, err = conn.Conn().WaitForNotification(ctx)
				if err == nil {
					select {
					case wake <- struct{}{}:
					default:
					}
				}
			}
			// A listening connection must not return to the general pool.
			raw := conn.Hijack()
			_ = raw.Close(context.Background())
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

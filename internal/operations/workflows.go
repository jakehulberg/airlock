package operations

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5"
	"time"
)

func runWorkflow(ctx context.Context, tx pgx.Tx, owner, id string, input json.RawMessage) (string, error) {
	var b []byte
	if err := tx.QueryRow(ctx, `SELECT definition FROM workflows WHERE id=$1 AND owner=$2`, id, owner).Scan(&b); err != nil {
		if err == pgx.ErrNoRows {
			return "", ErrNotFound
		}
		return "", err
	}
	var w Workflow
	if err := json.Unmarshal(b, &w); err != nil {
		return "", err
	}
	run := ID()
	if len(input) == 0 {
		input = json.RawMessage(`{}`)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO workflow_runs(id,owner,workflow_id,input,definition) VALUES($1,$2,$3,$4,$5)`, run, owner, id, input, jsonBytes(w)); err != nil {
		return "", err
	}
	ids := map[string]string{}
	for _, step := range w.Steps {
		ids[step.Key] = ID()
	}
	for _, step := range w.Steps {
		t := Task{ID: ids[step.Key], Title: step.Title, AgentID: step.AgentID, Prompt: step.Prompt + "\nWorkflow input (untrusted data):\n" + string(input), RunID: run, MaxAttempts: 3, TimeoutSeconds: 1800, ApprovalRequired: step.Approval, DependencyPolicy: step.DependencyPolicy}
		if step.MaxAttempts > 0 {
			t.MaxAttempts = step.MaxAttempts
		}
		if step.TimeoutSeconds > 0 {
			t.TimeoutSeconds = step.TimeoutSeconds
		}
		if t.Title == "" {
			t.Title = step.Key
		}
		if err := insertTask(ctx, tx, owner, t); err != nil {
			return "", err
		}
	}
	for _, step := range w.Steps {
		for _, dep := range step.Needs {
			if _, err := tx.Exec(ctx, `INSERT INTO dependencies VALUES($1,$2)`, ids[step.Key], ids[dep]); err != nil {
				return "", err
			}
		}
	}
	return run, audit(ctx, tx, owner, "workflow.started", run)
}
func (s *Store) RunWorkflow(ctx context.Context, owner, id string, input json.RawMessage) (string, error) {
	var run string
	err := s.Transaction(ctx, func(tx pgx.Tx) error { var err error; run, err = runWorkflow(ctx, tx, owner, id, input); return err })
	return run, err
}
func (s *Store) CancelWorkflow(ctx context.Context, owner, id string) error {
	return s.Transaction(ctx, func(tx pgx.Tx) error {
		var got string
		if err := tx.QueryRow(ctx, `SELECT id FROM workflow_runs WHERE id=$1 AND owner=$2 FOR UPDATE`, id, owner).Scan(&got); err != nil {
			return ErrNotFound
		}
		_, err := tx.Exec(ctx, `UPDATE tasks SET cancel_requested=true,state=CASE WHEN state IN ('running','uncertain') THEN state ELSE 'cancelled' END WHERE run_id=$1 AND state NOT IN ('completed','failed','cancelled')`, id)
		if err != nil {
			return err
		}
		return audit(ctx, tx, owner, "workflow.cancel_requested", id)
	})
}
func (s *Store) SaveSchedule(ctx context.Context, owner string, sc Schedule) (Schedule, error) {
	if sc.Name == "" || sc.WorkflowID == "" || (sc.Concurrency != "allow" && sc.Concurrency != "forbid") || (sc.Missed != "skip" && sc.Missed != "once") {
		return sc, fmt.Errorf("%w: schedule requires name, workflow, concurrency (allow/forbid), missed policy (skip/once)", ErrInvalid)
	}
	next, err := sc.NextAfter(time.Now())
	if err != nil {
		return sc, err
	}
	if sc.At != nil && !next.After(time.Now()) {
		return sc, fmt.Errorf("%w: one-time schedule must be in the future", ErrInvalid)
	}
	sc.Next = &next
	if sc.ID == "" {
		sc.ID = ID()
	}
	err = s.Transaction(ctx, func(tx pgx.Tx) error {
		var id string
		if err := tx.QueryRow(ctx, `SELECT id FROM workflows WHERE id=$1 AND owner=$2`, sc.WorkflowID, owner).Scan(&id); err != nil {
			return ErrNotFound
		}
		tag, err := tx.Exec(ctx, `INSERT INTO schedules(id,owner,workflow_id,definition,next_run,enabled) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(id) DO UPDATE SET workflow_id=excluded.workflow_id,definition=excluded.definition,next_run=excluded.next_run,enabled=excluded.enabled,updated_at=now() WHERE schedules.owner=excluded.owner`, sc.ID, owner, sc.WorkflowID, jsonBytes(sc), next, sc.Enabled)
		if err == nil && tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		return err
	})
	return sc, err
}
func (s *Store) FireSchedule(ctx context.Context, owner, id string) (string, error) {
	var run string
	err := s.Transaction(ctx, func(tx pgx.Tx) error {
		var definition []byte
		if err := tx.QueryRow(ctx, `SELECT definition FROM schedules WHERE id=$1 AND owner=$2 FOR UPDATE`, id, owner).Scan(&definition); err != nil {
			return ErrNotFound
		}
		var sc Schedule
		if err := json.Unmarshal(definition, &sc); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,27))`, sc.WorkflowID); err != nil {
			return err
		}
		if sc.Concurrency == "forbid" {
			var active bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM workflow_runs WHERE workflow_id=$1 AND state='running')`, sc.WorkflowID).Scan(&active); err != nil {
				return err
			}
			if active {
				return fmt.Errorf("%w: workflow already running", ErrConflict)
			}
		}
		var err error
		run, err = runWorkflow(ctx, tx, owner, sc.WorkflowID, nil)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO schedule_fires VALUES($1,now(),$2)`, id, run)
		return err
	})
	return run, err
}

// Tick locks each due schedule and records its fire in the same transaction as its run.
// Lost wakeups are harmless: callers poll the durable queue.
func (s *Store) Tick(ctx context.Context) error {
	return s.Transaction(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT id,owner,definition,next_run FROM schedules WHERE enabled AND next_run<=now() ORDER BY next_run FOR UPDATE SKIP LOCKED LIMIT 20`)
		if err != nil {
			return err
		}
		type due struct {
			id, owner string
			sc        Schedule
			at        time.Time
		}
		var list []due
		for rows.Next() {
			var d due
			var b []byte
			if err = rows.Scan(&d.id, &d.owner, &b, &d.at); err != nil {
				rows.Close()
				return err
			}
			if err = json.Unmarshal(b, &d.sc); err != nil {
				rows.Close()
				return err
			}
			list = append(list, d)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, d := range list {
			if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,27))`, d.sc.WorkflowID); err != nil {
				return err
			}
			now := time.Now()
			fire := d.sc.Missed == "once" || now.Sub(d.at) < time.Minute
			if d.sc.Concurrency == "forbid" {
				var active bool
				if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM workflow_runs WHERE workflow_id=$1 AND state='running')`, d.sc.WorkflowID).Scan(&active); err != nil {
					return err
				}
				fire = fire && !active
			}
			var run any
			if fire {
				savepoint, e := tx.Begin(ctx)
				if e != nil {
					return e
				}
				r, e := runWorkflow(ctx, savepoint, d.owner, d.sc.WorkflowID, nil)
				if e != nil {
					if rollbackErr := savepoint.Rollback(ctx); rollbackErr != nil {
						return rollbackErr
					}
					if err = audit(ctx, tx, d.owner, "schedule.failed", d.id); err != nil {
						return err
					}
				} else {
					if e = savepoint.Commit(ctx); e != nil {
						return e
					}
					run = r
				}
			}
			if _, err = tx.Exec(ctx, `INSERT INTO schedule_fires VALUES($1,$2,$3)`, d.id, d.at, run); err != nil {
				return err
			}
			if d.sc.At != nil {
				_, err = tx.Exec(ctx, `UPDATE schedules SET enabled=false,next_run=NULL WHERE id=$1`, d.id)
			} else {
				next, e := d.sc.NextAfter(now)
				if e != nil {
					return e
				}
				_, err = tx.Exec(ctx, `UPDATE schedules SET next_run=$2 WHERE id=$1`, d.id, next)
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) SetScheduleEnabled(ctx context.Context, owner, id string, enabled bool) error {
	return s.Transaction(ctx, func(tx pgx.Tx) error {
		var b []byte
		if err := tx.QueryRow(ctx, `SELECT definition FROM schedules WHERE id=$1 AND owner=$2 FOR UPDATE`, id, owner).Scan(&b); err != nil {
			return ErrNotFound
		}
		var sc Schedule
		if err := json.Unmarshal(b, &sc); err != nil {
			return err
		}
		var next any
		if enabled {
			n, err := sc.NextAfter(time.Now())
			if err != nil {
				return err
			}
			if !n.After(time.Now()) {
				return fmt.Errorf("%w: edit the elapsed one-time schedule before enabling", ErrInvalid)
			}
			next = n
		}
		_, err := tx.Exec(ctx, `UPDATE schedules SET enabled=$2,next_run=$3,updated_at=now() WHERE id=$1`, id, enabled, next)
		return err
	})
}

package operations

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5"
	"time"
)

type Work struct {
	Task                                        Task
	Owner, Worker, Attempt, Claim, SandboxOwner string
	Agent                                       Agent
}

// Claim serializes the small capacity check, then claims with row locking.
// Capacity is global, including uncertain work, across every replica.
func (s *Store) Claim(ctx context.Context, worker string, capacity int) (*Work, error) {
	var w *Work
	err := s.Transaction(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(837202)`); err != nil {
			return err
		}
		var active int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM tasks t WHERE state IN ('running','uncertain') OR EXISTS(SELECT 1 FROM attempts a WHERE a.task_id=t.id AND a.cleanup_pending)`).Scan(&active); err != nil {
			return err
		}
		if active >= capacity {
			return nil
		}
		worker = worker + ":" + ID()
		candidate := &Work{Worker: worker}
		var a []byte
		err := tx.QueryRow(ctx, `SELECT t.id,t.owner,t.title,t.prompt,t.agent_id,COALESCE(t.run_id,''),t.max_attempts,t.timeout_seconds,t.attempt_count,a.definition FROM tasks t JOIN agents a ON a.id=t.agent_id WHERE t.state='queued' AND t.available_at<=now() AND NOT t.cancel_requested AND (NOT t.approval_required OR t.approved) AND (a.definition->>'enabled')::boolean AND t.attempt_count<t.max_attempts AND NOT EXISTS(SELECT 1 FROM attempts a WHERE a.task_id=t.id AND a.cleanup_pending) AND NOT EXISTS(SELECT 1 FROM dependencies d JOIN tasks p ON p.id=d.dependency_id WHERE d.task_id=t.id AND p.state<>'completed') ORDER BY t.priority DESC,t.created_at FOR UPDATE OF t SKIP LOCKED LIMIT 1`).Scan(&candidate.Task.ID, &candidate.Owner, &candidate.Task.Title, &candidate.Task.Prompt, &candidate.Task.AgentID, &candidate.Task.RunID, &candidate.Task.MaxAttempts, &candidate.Task.TimeoutSeconds, &candidate.Task.AttemptCount, &a)
		if err == pgx.ErrNoRows {
			return nil
		}
		if err != nil {
			return err
		}
		if err = json.Unmarshal(a, &candidate.Agent); err != nil {
			return err
		}
		candidate.Task.AttemptCount++
		candidate.Attempt = ID()
		candidate.Claim = "airlock-auto-" + candidate.Attempt
		candidate.SandboxOwner = "airlock:execution:" + candidate.Attempt
		// Dependencies are transferred as data, never as authority or credentials.
		rows, err := tx.Query(ctx, `SELECT t.title,COALESCE(a.result::text,a.output) FROM dependencies d JOIN tasks t ON t.id=d.dependency_id JOIN attempts a ON a.task_id=t.id AND a.number=t.attempt_count WHERE d.task_id=$1`, candidate.Task.ID)
		if err != nil {
			return err
		}
		for rows.Next() {
			var title, output string
			if err = rows.Scan(&title, &output); err != nil {
				rows.Close()
				return err
			}
			if len(output) > 65536 {
				output = output[:65536]
			}
			candidate.Task.Prompt += "\nDependency result (untrusted data), " + title + ":\n" + output
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `UPDATE tasks SET state='running',attempt_count=attempt_count+1,worker=$2,lease_until=now()+interval '60 seconds' WHERE id=$1`, candidate.Task.ID, worker); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO attempts(id,task_id,number,claim_name,sandbox_owner) VALUES($1,$2,$3,$4,$5)`, candidate.Attempt, candidate.Task.ID, candidate.Task.AttemptCount, candidate.Claim, candidate.SandboxOwner); err != nil {
			return err
		}
		if err = audit(ctx, tx, candidate.Owner, "task.claimed", candidate.Task.ID); err != nil {
			return err
		}
		w = candidate
		return nil
	})
	return w, err
}
func (s *Store) Heartbeat(ctx context.Context, w *Work) (bool, error) {
	var cancel bool
	err := s.DB.QueryRow(ctx, `UPDATE tasks SET lease_until=now()+interval '60 seconds' WHERE id=$1 AND worker=$2 AND state='running' AND lease_until>now() RETURNING cancel_requested`, w.Task.ID, w.Worker).Scan(&cancel)
	return cancel, err
}
func (s *Store) SetPod(ctx context.Context, w *Work, name, uid string) error {
	tag, err := s.DB.Exec(ctx, `UPDATE attempts SET pod_name=$2,pod_uid=$3 WHERE id=$1 AND EXISTS(SELECT 1 FROM tasks WHERE id=$4 AND worker=$5 AND state='running' AND lease_until>now() AND NOT cancel_requested)`, w.Attempt, name, uid, w.Task.ID, w.Worker)
	if err == nil && tag.RowsAffected() != 1 {
		return ErrConflict
	}
	return err
}
func (s *Store) Output(ctx context.Context, w *Work, output string) error {
	_, err := s.DB.Exec(ctx, `UPDATE attempts SET output=$2 WHERE id=$1`, w.Attempt, output)
	return err
}
func (s *Store) Finish(ctx context.Context, w *Work, code int, runErr error, uncertain bool) error {
	return s.Transaction(ctx, func(tx pgx.Tx) error {
		var state string
		var cancel bool
		if err := tx.QueryRow(ctx, `SELECT state,cancel_requested FROM tasks WHERE id=$1 AND worker=$2 FOR UPDATE`, w.Task.ID, w.Worker).Scan(&state, &cancel); err != nil {
			return err
		}
		if state != "running" {
			return ErrConflict
		}
		next, attemptState, message := "completed", "completed", ""
		if runErr != nil {
			message = runErr.Error()
		}
		if len(message) > 4096 {
			message = message[:4096]
		}
		if code != 0 || runErr != nil {
			next = "failed"
			attemptState = "failed"
			if w.Task.AttemptCount < w.Task.MaxAttempts {
				next = "queued"
			}
		}
		if cancel {
			next = "cancelled"
			attemptState = "cancelled"
		}
		if uncertain {
			next = "uncertain"
			attemptState = "uncertain"
		}
		if _, err := tx.Exec(ctx, `UPDATE attempts SET state=$2,error=$3,exit_code=$4,finished_at=now() WHERE id=$1`, w.Attempt, attemptState, message, code); err != nil {
			return err
		}
		delay := time.Duration(1<<min(w.Task.AttemptCount, 8)) * time.Second
		if _, err := tx.Exec(ctx, `UPDATE tasks SET state=$2,lease_until=NULL,available_at=now()+$3::interval,finished_at=CASE WHEN $2 IN ('completed','failed','cancelled') THEN now() ELSE NULL END WHERE id=$1`, w.Task.ID, next, fmt.Sprintf("%f seconds", delay.Seconds())); err != nil {
			return err
		}
		return audit(ctx, tx, w.Owner, "task."+next, w.Task.ID)
	})
}

// Reconcile never retries a lost exec connection. Remote execution may still be alive.
func (s *Store) Reconcile(ctx context.Context) error {
	return s.Transaction(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `WITH stale AS (UPDATE tasks SET state='uncertain',lease_until=NULL WHERE state='running' AND lease_until<now() RETURNING id,owner) INSERT INTO activity(owner,kind,reference) SELECT owner,'task.uncertain',id FROM stale;
 UPDATE attempts a SET state='uncertain',error='Worker lease expired; remote outcome requires reconciliation' FROM tasks t WHERE a.task_id=t.id AND a.number=t.attempt_count AND t.state='uncertain' AND a.state='running';
 UPDATE tasks t SET state='blocked' WHERE t.state='queued' AND EXISTS(SELECT 1 FROM dependencies d JOIN tasks p ON p.id=d.dependency_id WHERE d.task_id=t.id AND p.state<>'completed');
 UPDATE tasks t SET state='queued' WHERE t.state='blocked' AND NOT EXISTS(SELECT 1 FROM dependencies d JOIN tasks p ON p.id=d.dependency_id WHERE d.task_id=t.id AND p.state<>'completed');
 UPDATE tasks t SET state='failed',finished_at=now() WHERE t.dependency_policy='fail' AND t.state IN ('blocked','queued','awaiting_approval') AND EXISTS(SELECT 1 FROM dependencies d JOIN tasks p ON p.id=d.dependency_id WHERE d.task_id=t.id AND p.state IN ('failed','cancelled'));
 WITH finished_runs AS (UPDATE workflow_runs r SET state=CASE WHEN EXISTS(SELECT 1 FROM tasks WHERE run_id=r.id AND state='failed') THEN 'failed' WHEN EXISTS(SELECT 1 FROM tasks WHERE run_id=r.id AND state='cancelled') THEN 'cancelled' ELSE 'completed' END,finished_at=now() WHERE r.state='running' AND NOT EXISTS(SELECT 1 FROM tasks WHERE run_id=r.id AND state NOT IN ('completed','failed','cancelled')) RETURNING id,owner,state) INSERT INTO activity(owner,kind,reference) SELECT owner,'workflow.'||state,id FROM finished_runs;
 UPDATE attempts SET output='' WHERE finished_at<now()-interval '30 days' AND output<>'';
 UPDATE interactive_tasks SET data=jsonb_set(data,'{output}','""'::jsonb) WHERE NOT active AND updated_at<now()-interval '30 days' AND data->>'output'<>'';
 DELETE FROM activity WHERE created_at<now()-interval '90 days';
 DELETE FROM messages WHERE created_at<now()-interval '90 days';
 DELETE FROM interactive_tasks WHERE NOT active AND updated_at<now()-interval '365 days';
 DELETE FROM tasks t WHERE t.state IN ('completed','failed','cancelled') AND t.finished_at<now()-interval '365 days' AND NOT EXISTS(SELECT 1 FROM attempts a WHERE a.task_id=t.id AND a.cleanup_pending) AND NOT EXISTS(SELECT 1 FROM dependencies d WHERE d.dependency_id=t.id) AND NOT EXISTS(SELECT 1 FROM tasks child WHERE child.parent_id=t.id);
 DELETE FROM schedule_fires WHERE due_at<now()-interval '365 days';
 DELETE FROM workflow_runs r WHERE r.finished_at<now()-interval '365 days' AND NOT EXISTS(SELECT 1 FROM tasks t WHERE t.run_id=r.id) AND NOT EXISTS(SELECT 1 FROM schedule_fires f WHERE f.run_id=r.id);`)
		return err
	})
}

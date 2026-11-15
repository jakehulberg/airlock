package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jakehulberg/airlock/internal/api"
)

// Restore recovers interactive history. An interrupted exec keeps its claim lock:
// releasing the sandbox is required before another execution can start there.
func (s *Tasks) Restore(ctx context.Context) error {
	rows, err := s.Store.DB.Query(ctx, `SELECT owner,data,active FROM interactive_tasks ORDER BY updated_at DESC LIMIT 500`)
	if err != nil {
		return err
	}
	defer rows.Close()
	s.tasks = map[string]*task{}
	for rows.Next() {
		var owner string
		var b []byte
		var active bool
		if err = rows.Scan(&owner, &b, &active); err != nil {
			return err
		}
		var v api.Task
		if err = json.Unmarshal(b, &v); err != nil {
			return err
		}
		if active {
			v.Status = api.TaskFailed
			v.Error = "Execution outcome is uncertain after restart. Release this sandbox before starting new work."
		}
		t := &task{owner: owner, limit: s.MaxOutput, t: v, out: []byte(v.Output), changed: make(chan struct{})}
		t.t.Output = ""
		s.tasks[v.ID] = t
	}
	return rows.Err()
}
func (s *Tasks) persistInteractive(t *task, insert bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	v := t.t
	v.Output = string(t.out)
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if insert {
		_, err = s.Store.DB.Exec(ctx, `INSERT INTO interactive_tasks(id,owner,claim,data,active) VALUES($1,$2,$3,$4,true)`, v.ID, t.owner, v.Claim, b)
		if err != nil {
			return fmt.Errorf("%w: sandbox has active or uncertain work, or history could not be persisted", ErrConflict)
		}
		return nil
	}
	// A transport error cannot prove the remote process has stopped.
	_, err = s.Store.DB.Exec(ctx, `UPDATE interactive_tasks SET data=$2,active=$3,updated_at=now() WHERE id=$1`, v.ID, b, v.Status == api.TaskRunning || v.Error != "")
	return err
}

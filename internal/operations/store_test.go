package operations

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("AIRLOCK_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set AIRLOCK_TEST_DATABASE_URL for PostgreSQL integration tests")
	}
	ctx := context.Background()
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := "test_" + strings.ReplaceAll(ID(), "-", "")
	if _, err = db.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	s, err := Open(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.DB.Close(); _, _ = db.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`); db.Close() })
	return s
}
func seedAgent(t *testing.T, s *Store) Agent {
	t.Helper()
	a, err := s.SaveAgent(context.Background(), "owner", Agent{Name: "Research", Pool: "claude", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	return a
}
func seedTask(t *testing.T, s *Store, a Agent) Task {
	t.Helper()
	v, err := s.CreateTask(context.Background(), "owner", Task{Title: "Research", Prompt: "Do research", AgentID: a.ID})
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func TestConcurrentClaimsAndLeases(t *testing.T) {
	s := testStore(t)
	a := seedAgent(t, s)
	for i := 0; i < 12; i++ {
		seedTask(t, s, a)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := map[string]*Work{}
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			w, err := s.Claim(context.Background(), fmt.Sprint(i), 4)
			if err != nil {
				t.Error(err)
				return
			}
			if w != nil {
				mu.Lock()
				defer mu.Unlock()
				if seen[w.Task.ID] != nil {
					t.Error("duplicate claim")
				}
				seen[w.Task.ID] = w
			}
		}(i)
	}
	wg.Wait()
	if len(seen) != 4 {
		t.Fatalf("global capacity: got %d", len(seen))
	}
	for _, w := range seen {
		cancel, err := s.Heartbeat(context.Background(), w)
		if err != nil || cancel {
			t.Fatalf("heartbeat: %v %v", cancel, err)
		}
	}
	_, err := s.DB.Exec(context.Background(), `UPDATE tasks SET lease_until=now()-interval '1 second' WHERE state='running'`)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, w := range seen {
		var state string
		s.DB.QueryRow(context.Background(), `SELECT state FROM tasks WHERE id=$1`, w.Task.ID).Scan(&state)
		if state != "uncertain" {
			t.Fatal(state)
		}
		if _, err = s.Heartbeat(context.Background(), w); err == nil {
			t.Fatal("stale heartbeat revived lease")
		}
		if err = s.Action(context.Background(), "owner", w.Task.ID, "retry", ""); err == nil {
			t.Fatal("uncertain retry accepted")
		}
	}
	if w, err := s.Claim(context.Background(), "extra", 4); err != nil || w != nil {
		t.Fatalf("uncertain work must consume capacity: %v %v", w, err)
	}
}
func TestWorkflowDependenciesApprovalsAndIsolation(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	a := seedAgent(t, s)
	w, err := s.SaveWorkflow(ctx, "owner", Workflow{Name: "Daily research", Steps: []Step{{Key: "research", AgentID: a.ID, Prompt: "research"}, {Key: "report", AgentID: a.ID, Prompt: "report", Needs: []string{"research"}, Approval: true}}})
	if err != nil {
		t.Fatal(err)
	}
	run, err := s.RunWorkflow(ctx, "owner", w.ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.Claim(ctx, "worker", 4)
	if err != nil || first == nil {
		t.Fatalf("%v %v", first, err)
	}
	if first.Task.Title != "research" {
		t.Fatal(first.Task)
	}
	if err = s.Output(ctx, first, "research findings"); err != nil {
		t.Fatal(err)
	}
	if err = s.Finish(ctx, first, 0, nil, false); err != nil {
		t.Fatal(err)
	}
	var report string
	if err = s.DB.QueryRow(ctx, `SELECT id FROM tasks WHERE run_id=$1 AND title='report'`, run).Scan(&report); err != nil {
		t.Fatal(err)
	}
	if err = s.Action(ctx, "other", report, "approve", ""); err != ErrNotFound {
		t.Fatal(err)
	}
	if _, err = s.Detail(ctx, "other", report); err != ErrNotFound {
		t.Fatal(err)
	}
	if work, err := s.Claim(ctx, "worker", 4); err != nil || work != nil {
		t.Fatalf("approval bypassed: %v %v", work, err)
	}
	if err = s.Action(ctx, "owner", report, "approve", "reviewed"); err != nil {
		t.Fatal(err)
	}
	second, err := s.Claim(ctx, "worker", 4)
	if err != nil || second == nil {
		t.Fatalf("%v %v", second, err)
	}
	if !strings.Contains(second.Task.Prompt, "research findings") {
		t.Fatal("dependency output not transferred")
	}
	if err = s.Finish(ctx, second, 0, nil, false); err != nil {
		t.Fatal(err)
	}
	if err = s.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	var status string
	s.DB.QueryRow(ctx, `SELECT state FROM workflow_runs WHERE id=$1`, run).Scan(&status)
	if status != "completed" {
		t.Fatal(status)
	}
}
func TestRetryCancellationAndIdempotency(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	a := seedAgent(t, s)
	input := Task{Title: "one", Prompt: "one", AgentID: a.ID, IdempotencyKey: "same", MaxAttempts: 2}
	first, err := s.CreateTask(ctx, "owner", input)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.CreateTask(ctx, "owner", input)
	if err != nil || first.ID != second.ID {
		t.Fatalf("idempotency %v %v", second, err)
	}
	work, err := s.Claim(ctx, "worker", 4)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Finish(ctx, work, 1, nil, false); err != nil {
		t.Fatal(err)
	}
	if w, e := s.Claim(ctx, "worker", 4); e != nil || w != nil {
		t.Fatal("backoff ignored")
	}
	s.DB.Exec(ctx, `UPDATE tasks SET available_at=now(); UPDATE attempts SET cleanup_pending=false`)
	work, err = s.Claim(ctx, "worker", 4)
	if err != nil || work == nil {
		t.Fatalf("retry: %v %v", work, err)
	}
	if work.Task.AttemptCount != 2 {
		t.Fatal("attempt count")
	}
	if err = s.Action(ctx, "owner", work.Task.ID, "cancel", ""); err != nil {
		t.Fatal(err)
	}
	if cancel, e := s.Heartbeat(ctx, work); e != nil || !cancel {
		t.Fatal("cancel missing")
	}
	if err = s.Finish(ctx, work, 0, nil, false); err != nil {
		t.Fatal(err)
	}
	if err = s.Action(ctx, "owner", work.Task.ID, "retry", ""); err == nil {
		t.Fatal("cancelled retry")
	}
}
func TestConcurrentScheduleAndMigrations(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	a := seedAgent(t, s)
	w, err := s.SaveWorkflow(ctx, "owner", Workflow{Name: "daily", Steps: []Step{{Key: "a", AgentID: a.ID, Prompt: "a"}}})
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().Add(time.Hour)
	sc, err := s.SaveSchedule(ctx, "owner", Schedule{Name: "once", WorkflowID: w.ID, At: &at, Enabled: true, Concurrency: "forbid", Missed: "once"})
	if err != nil {
		t.Fatal(err)
	}
	s.DB.Exec(ctx, `UPDATE schedules SET next_run=now()-interval '5 minutes' WHERE id=$1`, sc.ID)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.Migrate(ctx); err != nil {
				t.Error(err)
			}
			if err := s.Tick(ctx); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	var n int
	s.DB.QueryRow(ctx, `SELECT count(*) FROM schedule_fires`).Scan(&n)
	if n != 1 {
		t.Fatalf("fires %d", n)
	}
	s.DB.QueryRow(ctx, `SELECT count(*) FROM workflow_runs`).Scan(&n)
	if n != 1 {
		t.Fatalf("runs %d", n)
	}
}
func TestAgentCredentialsAndDelegation(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	a := seedAgent(t, s)
	b := seedAgent(t, s)
	a.Delegates = []string{b.ID}
	if _, err := s.SaveAgent(ctx, "owner", a); err != nil {
		t.Fatal(err)
	}
	seedTask(t, s, a)
	w, err := s.Claim(ctx, "worker", 4)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.SetPod(ctx, w, "pod", "uid"); err != nil {
		t.Fatal(err)
	}
	token, err := s.IssueToken(ctx, w)
	if err != nil {
		t.Fatal(err)
	}
	p, err := s.AuthenticateAgent(ctx, token)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.AuthenticateAgent(ctx, "wrong"); err == nil {
		t.Fatal("bad credential accepted")
	}
	child, err := s.Delegate(ctx, p, Task{Title: "child", Prompt: "child", AgentID: b.ID})
	if err != nil {
		t.Fatal(err)
	}
	var state string
	s.DB.QueryRow(ctx, `SELECT state FROM tasks WHERE id=$1`, child.ID).Scan(&state)
	if state != "awaiting_approval" {
		t.Fatal("delegation bypassed approval")
	}
	if _, err = s.Delegate(ctx, p, Task{Title: "bad", Prompt: "bad", AgentID: a.ID}); err == nil {
		t.Fatal("target bypass")
	}
	_, err = s.SendMessage(ctx, p, Message{Recipient: b.ID, Type: "findings", Payload: json.RawMessage(`{"finding":"test"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.SendMessage(ctx, p, Message{Recipient: "other", Type: "findings", Payload: json.RawMessage(`{}`)}); err == nil {
		t.Fatal("recipient bypass")
	}
	if err = s.Finish(ctx, w, 0, nil, false); err != nil {
		t.Fatal(err)
	}
	if _, err = s.AuthenticateAgent(ctx, token); err == nil {
		t.Fatal("finished credential accepted")
	}
}

func TestAssignmentInvalidatesApproval(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	a := seedAgent(t, s)
	b := seedAgent(t, s)
	task, err := s.CreateTask(ctx, "owner", Task{Title: "approval", Prompt: "approval", AgentID: a.ID, ApprovalRequired: true})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Action(ctx, "owner", task.ID, "approve", ""); err != nil {
		t.Fatal(err)
	}
	if err = s.Assign(ctx, "owner", task.ID, b.ID, 5); err != nil {
		t.Fatal(err)
	}
	var state string
	var approved bool
	if err = s.DB.QueryRow(ctx, `SELECT state,approved FROM tasks WHERE id=$1`, task.ID).Scan(&state, &approved); err != nil {
		t.Fatal(err)
	}
	if state != "awaiting_approval" || approved {
		t.Fatal("assignment retained approval")
	}
}
func TestRetentionPreservesActiveDependencies(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	a := seedAgent(t, s)
	parent := seedTask(t, s, a)
	_, err := s.CreateTask(ctx, "owner", Task{Title: "child", Prompt: "child", AgentID: a.ID, Dependencies: []string{parent.ID}})
	if err != nil {
		t.Fatal(err)
	}
	s.DB.Exec(ctx, `UPDATE tasks SET state='completed',finished_at=now()-interval '400 days' WHERE id=$1`, parent.ID)
	if err = s.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	var n int
	s.DB.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE id=$1`, parent.ID).Scan(&n)
	if n != 1 {
		t.Fatal("required history removed")
	}
}
func TestScheduleRunNowHonorsConcurrency(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	a := seedAgent(t, s)
	w, err := s.SaveWorkflow(ctx, "owner", Workflow{Name: "workflow", Steps: []Step{{Key: "a", AgentID: a.ID, Prompt: "a"}}})
	if err != nil {
		t.Fatal(err)
	}
	sc, err := s.SaveSchedule(ctx, "owner", Schedule{Name: "daily", WorkflowID: w.ID, Cron: "0 8 * * *", Timezone: "UTC", Concurrency: "forbid", Missed: "once", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.FireSchedule(ctx, "owner", sc.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.FireSchedule(ctx, "owner", sc.ID); err == nil {
		t.Fatal("overlap allowed")
	}
	if err = s.SetScheduleEnabled(ctx, "other", sc.ID, false); err != ErrNotFound {
		t.Fatal(err)
	}
	if err = s.SetScheduleEnabled(ctx, "owner", sc.ID, false); err != nil {
		t.Fatal(err)
	}
}

func TestStaleAttemptCannotFinishRetry(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	a := seedAgent(t, s)
	seedTask(t, s, a)
	old, err := s.Claim(ctx, "same-process", 4)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Finish(ctx, old, 1, nil, false); err != nil {
		t.Fatal(err)
	}
	s.DB.Exec(ctx, `UPDATE attempts SET cleanup_pending=false; UPDATE tasks SET available_at=now()`)
	current, err := s.Claim(ctx, "same-process", 4)
	if err != nil || current == nil {
		t.Fatalf("%v %v", current, err)
	}
	if err = s.Finish(ctx, old, 0, nil, false); err == nil {
		t.Fatal("stale attempt finalized retry")
	}
	var state string
	s.DB.QueryRow(ctx, `SELECT state FROM tasks WHERE id=$1`, current.Task.ID).Scan(&state)
	if state != "running" {
		t.Fatal(state)
	}
}
func TestMigrationRejectsChangedChecksum(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	if _, err := s.DB.Exec(ctx, `UPDATE airlock_migrations SET checksum='modified' WHERE version='001_initial.sql'`); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("%v", err)
	}
}
func TestFailedDependencyPolicies(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	a := seedAgent(t, s)
	parent := seedTask(t, s, a)
	failed, err := s.CreateTask(ctx, "owner", Task{Title: "fail", Prompt: "fail", AgentID: a.ID, Dependencies: []string{parent.ID}})
	if err != nil {
		t.Fatal(err)
	}
	blocked, err := s.CreateTask(ctx, "owner", Task{Title: "block", Prompt: "block", AgentID: a.ID, Dependencies: []string{parent.ID}, DependencyPolicy: "block"})
	if err != nil {
		t.Fatal(err)
	}
	s.DB.Exec(ctx, `UPDATE tasks SET state='failed',finished_at=now() WHERE id=$1`, parent.ID)
	if err = s.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	for id, want := range map[string]string{failed.ID: "failed", blocked.ID: "blocked"} {
		var state string
		if err = s.DB.QueryRow(ctx, `SELECT state FROM tasks WHERE id=$1`, id).Scan(&state); err != nil || state != want {
			t.Fatalf("%s: %s %v", id, state, err)
		}
	}
}

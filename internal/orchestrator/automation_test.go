package orchestrator

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jakehulberg/airlock/internal/api"
	"github.com/jakehulberg/airlock/internal/operations"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"
)

func operationStore(t *testing.T) *operations.Store {
	t.Helper()
	dsn := os.Getenv("AIRLOCK_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires AIRLOCK_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	db, e := pgxpool.New(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	schema := "test_" + strings.ReplaceAll(operations.ID(), "-", "")
	if _, e = db.Exec(ctx, `CREATE SCHEMA `+schema); e != nil {
		t.Fatal(e)
	}
	u, _ := url.Parse(dsn)
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	s, e := operations.Open(ctx, u.String())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.DB.Close(); db.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`); db.Close() })
	return s
}

type automationExecutor struct{ fail bool }

func (e automationExecutor) Exec(ctx context.Context, pod *corev1.Pod, container string, cmd []string, in io.Reader, out io.Writer) (int, error) {
	b, _ := io.ReadAll(in)
	token, _, _ := strings.Cut(string(b), "\n")
	fmt.Fprint(out, "report ", token)
	if e.fail {
		return -1, fmt.Errorf("connection lost")
	}
	return 0, nil
}
func TestAutomatedExecutionAndUncertainCleanup(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			ctx := context.Background()
			store := operationStore(t)
			sb, dyn, kube := newSandboxes(t, pool("claude", nil))
			sb.Now = nil
			// Fake Agent Sandbox controller provisions a ready sandbox after claim creation.
			dyn.PrependReactor("create", "sandboxclaims", func(action ktesting.Action) (bool, runtime.Object, error) {
				u := action.(ktesting.CreateAction).GetObject().(*unstructured.Unstructured)
				u.SetUID("claim-uid")
				_ = unstructured.SetNestedField(u.Object, u.GetName()+"-sb", "status", "sandbox", "name")
				_ = unstructured.SetNestedSlice(u.Object, []any{map[string]any{"type": "Ready", "status": "True"}}, "status", "conditions")
				sandbox := &unstructured.Unstructured{Object: map[string]any{"apiVersion": SandboxGVR.GroupVersion().String(), "kind": "Sandbox", "metadata": map[string]any{"name": u.GetName() + "-sb", "namespace": ns}}}
				dyn.Tracker().Create(SandboxGVR, sandbox, ns)
				kube.CoreV1().Pods(ns).Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: u.GetName() + "-sb", Namespace: ns, UID: "pod-uid"}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}, metav1.CreateOptions{})
				return false, nil, nil
			})
			agent, err := store.SaveAgent(ctx, "owner", operations.Agent{Name: "agent", Pool: "claude", Enabled: true})
			if err != nil {
				t.Fatal(err)
			}
			task, err := store.CreateTask(ctx, "owner", operations.Task{Title: "research", Prompt: "research", AgentID: agent.ID})
			if err != nil {
				t.Fatal(err)
			}
			work, err := store.Claim(ctx, "worker", 4)
			if err != nil {
				t.Fatal(err)
			}
			a := &Automation{Store: store, Tasks: &Tasks{Sandboxes: sb, Executor: automationExecutor{fail}, Command: []string{"sh", "-c", "agent"}}, Log: slog.New(slog.DiscardHandler), Capacity: 4}
			a.execute(ctx, work)
			var state, output string
			err = store.DB.QueryRow(ctx, `SELECT t.state,a.output FROM tasks t JOIN attempts a ON a.task_id=t.id WHERE t.id=$1`, task.ID).Scan(&state, &output)
			if err != nil {
				t.Fatal(err)
			}
			want := "completed"
			if fail {
				want = "uncertain"
			}
			if state != want {
				t.Fatalf("state %s", state)
			}
			if output != "report [redacted]" {
				t.Fatalf("output: %q", output)
			}
			if err = a.cleanup(ctx); err != nil {
				t.Fatal(err)
			}
			if err = a.cleanup(ctx); err != nil {
				t.Fatal(err)
			}
			var pending bool
			store.DB.QueryRow(ctx, `SELECT cleanup_pending FROM attempts WHERE id=$1`, work.Attempt).Scan(&pending)
			if !pending {
				t.Fatal("cleanup completed while pod exists")
			}
			kube.CoreV1().Pods(ns).Delete(ctx, work.Claim+"-sb", metav1.DeleteOptions{})
			if err = a.cleanup(ctx); err != nil {
				t.Fatal(err)
			}
			store.DB.QueryRow(ctx, `SELECT state FROM tasks WHERE id=$1`, task.ID).Scan(&state)
			if fail && state != "failed" {
				t.Fatalf("uncertain outcome not fenced before resolution: %s", state)
			}
		})
	}
}
func TestInteractiveRestartPreservesHistoryAndLock(t *testing.T) {
	store := operationStore(t)
	ctx := context.Background()
	sb, dyn, kube := newSandboxes(t, pool("claude", nil))
	_, err := sb.Create(ctx, "owner", "owner", api.CreateClaim{Name: "interactive", Pool: "claude"})
	if err != nil {
		t.Fatal(err)
	}
	markReady(t, sb, dyn, kube, "interactive")
	tasks := &Tasks{Store: store, MaxOutput: 1024}
	record := &task{owner: "owner", t: api.Task{ID: "history", Claim: "interactive", Status: api.TaskRunning, Started: time.Now(), Prompt: "prompt"}, out: []byte("persisted output")}
	if err = tasks.persistInteractive(record, true); err != nil {
		t.Fatal(err)
	}
	restored := &Tasks{Store: store, Sandboxes: sb, MaxOutput: 1024}
	if err = restored.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := restored.Get("owner", "interactive", "history")
	if err != nil || got.Output != "persisted output" {
		t.Fatalf("%+v %v", got, err)
	}
	if _, err = restored.Start(ctx, "owner", "interactive", "follow-up"); err == nil {
		t.Fatal("interrupted sandbox reused")
	}
	if _, err = restored.Get("other", "interactive", "history"); err == nil {
		t.Fatal("history authorization bypass")
	}
}

package orchestrator

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"

	"github.com/jakehulberg/airlock/internal/api"
)

const ns = "agents"

var now = time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)

func pool(name string, lbls map[string]string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "extensions.agents.x-k8s.io/v1beta1",
		"kind":       "SandboxWarmPool",
		"metadata":   map[string]any{"name": name, "namespace": ns},
		"spec":       map[string]any{"replicas": int64(0), "sandboxTemplateRef": map[string]any{"name": name}},
	}}
	u.SetLabels(lbls)
	return u
}

func newSandboxes(t *testing.T, objs ...runtime.Object) (*Sandboxes, *dynfake.FakeDynamicClient, *kubefake.Clientset) {
	t.Helper()
	dyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		ClaimGVR:   "SandboxClaimList",
		PoolGVR:    "SandboxWarmPoolList",
		SandboxGVR: "SandboxList",
	}, objs...)
	kube := kubefake.NewClientset()
	return &Sandboxes{
		Dynamic:    dyn,
		Kube:       kube,
		Namespace:  ns,
		DefaultTTL: time.Hour,
		MaxTTL:     8 * time.Hour,
		Now:        func() time.Time { return now },
	}, dyn, kube
}

// markReady does what the agent-sandbox controller does once a claim is bound.
func markReady(t *testing.T, s *Sandboxes, dyn *dynfake.FakeDynamicClient, kube *kubefake.Clientset, claim string) {
	t.Helper()
	ctx := context.Background()
	u, err := dyn.Resource(ClaimGVR).Namespace(ns).Get(ctx, claim, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_ = unstructured.SetNestedField(u.Object, claim+"-sb", "status", "sandbox", "name")
	_ = unstructured.SetNestedSlice(u.Object, []any{map[string]any{"type": "Ready", "status": "True", "reason": "SandboxReady"}}, "status", "conditions")
	if _, err := dyn.Resource(ClaimGVR).Namespace(ns).Update(ctx, u, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	sb := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "agents.x-k8s.io/v1beta1",
		"kind":       "Sandbox",
		"metadata":   map[string]any{"name": claim + "-sb", "namespace": ns},
		"status":     map[string]any{"selector": "sandbox=" + claim},
	}}
	if _, err := dyn.Resource(SandboxGVR).Namespace(ns).Create(ctx, sb, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "warm-pod-abc", Namespace: ns, Labels: map[string]string{"sandbox": claim}},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	if _, err := kube.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestCreateClaim(t *testing.T) {
	s, dyn, _ := newSandboxes(t, pool("coding-agent", nil))
	ctx := context.Background()

	c, err := s.Create(ctx, "alice", "alice@example.com", api.CreateClaim{Pool: "coding-agent", Name: "fix-test", TTLMinutes: 30})
	if err != nil {
		t.Fatal(err)
	}
	if c.Name != "fix-test" || c.Pool != "coding-agent" || c.Owner != "alice@example.com" {
		t.Errorf("claim = %+v", c)
	}
	if want := now.Add(30 * time.Minute); !c.ShutdownTime.Equal(want) {
		t.Errorf("shutdownTime = %s, want %s", c.ShutdownTime, want)
	}

	u, _ := dyn.Resource(ClaimGVR).Namespace(ns).Get(ctx, "fix-test", metav1.GetOptions{})
	if p, _, _ := unstructured.NestedString(u.Object, "spec", "lifecycle", "shutdownPolicy"); p != "Delete" {
		t.Errorf("shutdownPolicy = %q, want Delete", p)
	}
	if u.GetLabels()[LabelOwner] != ownerKey("alice") {
		t.Errorf("owner label = %q", u.GetLabels()[LabelOwner])
	}
}

func TestCreateClaimRejects(t *testing.T) {
	s, _, _ := newSandboxes(t, pool("coding-agent", nil), pool("hidden", map[string]string{"tier": "admin"}))
	s.PoolSelector = "tier!=admin"
	ctx := context.Background()
	if _, err := s.Create(ctx, "alice", "alice", api.CreateClaim{Pool: "coding-agent", Name: "taken"}); err != nil {
		t.Fatal(err)
	}

	for name, tc := range map[string]struct {
		req  api.CreateClaim
		want error
	}{
		"no pool":       {api.CreateClaim{}, ErrBadRequest},
		"unknown pool":  {api.CreateClaim{Pool: "nope"}, ErrBadRequest},
		"filtered pool": {api.CreateClaim{Pool: "hidden"}, ErrBadRequest},
		"bad name":      {api.CreateClaim{Pool: "coding-agent", Name: "Not_Valid"}, ErrBadRequest},
		"ttl over max":  {api.CreateClaim{Pool: "coding-agent", TTLMinutes: 9 * 60}, ErrBadRequest},
		"name taken":    {api.CreateClaim{Pool: "coding-agent", Name: "taken"}, ErrConflict},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := s.Create(ctx, "alice", "alice", tc.req); !errors.Is(err, tc.want) {
				t.Errorf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestPoolsHonorSelector(t *testing.T) {
	s, _, _ := newSandboxes(t, pool("b", map[string]string{"airlock": "true"}), pool("a", map[string]string{"airlock": "true"}), pool("c", nil))
	s.PoolSelector = "airlock=true"
	pools, err := s.Pools(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(pools) != 2 || pools[0].Name != "a" || pools[1].Name != "b" {
		t.Errorf("pools = %+v", pools)
	}
}

func TestClaimsAreScopedToOwner(t *testing.T) {
	s, _, _ := newSandboxes(t, pool("coding-agent", nil))
	ctx := context.Background()
	if _, err := s.Create(ctx, "alice", "alice", api.CreateClaim{Pool: "coding-agent", Name: "alices"}); err != nil {
		t.Fatal(err)
	}

	if list, _ := s.List(ctx, "bob"); len(list) != 0 {
		t.Errorf("bob sees %d claims", len(list))
	}
	if _, err := s.Get(ctx, "bob", "alices"); !errors.Is(err, ErrNotFound) {
		t.Errorf("bob Get err = %v, want not found", err)
	}
	if err := s.Delete(ctx, "bob", "alices"); !errors.Is(err, ErrNotFound) {
		t.Errorf("bob Delete err = %v, want not found", err)
	}
	if list, _ := s.List(ctx, "alice"); len(list) != 1 {
		t.Errorf("alice sees %d claims, want 1", len(list))
	}
	if err := s.Delete(ctx, "alice", "alices"); err != nil {
		t.Errorf("alice Delete: %v", err)
	}
}

func TestPodFollowsSandboxSelector(t *testing.T) {
	s, dyn, kube := newSandboxes(t, pool("coding-agent", nil))
	ctx := context.Background()
	if _, err := s.Create(ctx, "alice", "alice", api.CreateClaim{Pool: "coding-agent", Name: "c1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pod(ctx, "alice", "c1"); !errors.Is(err, ErrNotReady) {
		t.Fatalf("before ready: err = %v, want not ready", err)
	}
	markReady(t, s, dyn, kube, "c1")
	pod, err := s.Pod(ctx, "alice", "c1")
	if err != nil {
		t.Fatal(err)
	}
	if pod.Name != "warm-pod-abc" {
		t.Errorf("pod = %s, want the adopted warm pod", pod.Name)
	}
}

type fakeExec struct {
	mu      sync.Mutex
	release chan struct{}
	gotCmd  []string
	gotIn   string
	gotPod  string
	code    int
}

func (f *fakeExec) Exec(ctx context.Context, pod *corev1.Pod, container string, cmd []string, stdin io.Reader, out io.Writer) (int, error) {
	in, _ := io.ReadAll(stdin)
	f.mu.Lock()
	f.gotCmd, f.gotIn, f.gotPod = cmd, string(in), pod.Name+"/"+container
	f.mu.Unlock()
	_, _ = io.WriteString(out, "working\n")
	if f.release != nil {
		select {
		case <-f.release:
		case <-ctx.Done():
			return -1, ctx.Err()
		}
	}
	_, _ = io.WriteString(out, "done\n")
	return f.code, nil
}

func newTasks(t *testing.T, exec Executor) (*Tasks, *Sandboxes) {
	t.Helper()
	s, dyn, kube := newSandboxes(t, pool("coding-agent", nil))
	if _, err := s.Create(context.Background(), "alice", "alice", api.CreateClaim{Pool: "coding-agent", Name: "c1"}); err != nil {
		t.Fatal(err)
	}
	markReady(t, s, dyn, kube, "c1")
	return &Tasks{
		Sandboxes: s,
		Executor:  exec,
		Container: "agent",
		Command:   []string{"sh", "-c", "claude -p"},
		Timeout:   time.Minute,
		MaxOutput: 1 << 20,
		MaxTasks:  10,
	}, s
}

func TestTaskRuns(t *testing.T) {
	exec := &fakeExec{release: make(chan struct{})}
	tasks, _ := newTasks(t, exec)
	ctx := context.Background()

	task, err := tasks.Start(ctx, "alice", "c1", "  fix the test  ")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tasks.Start(ctx, "alice", "c1", "another"); !errors.Is(err, ErrConflict) {
		t.Errorf("second task err = %v, want conflict", err)
	}
	if _, err := tasks.Get("bob", "c1", task.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("bob Get err = %v, want not found", err)
	}

	var streamed strings.Builder
	done := make(chan api.Task)
	go func() {
		final, _ := tasks.Follow(ctx, "alice", "c1", task.ID, func(b []byte) error { streamed.Write(b); return nil })
		done <- final
	}()
	close(exec.release)
	final := <-done

	if final.Status != api.TaskSucceeded || final.ExitCode == nil || *final.ExitCode != 0 {
		t.Errorf("final = %+v", final)
	}
	if streamed.String() != "working\ndone\n" {
		t.Errorf("streamed = %q", streamed.String())
	}
	if exec.gotIn != "fix the test\n" || exec.gotPod != "warm-pod-abc/agent" {
		t.Errorf("exec got stdin %q on %s", exec.gotIn, exec.gotPod)
	}
	if got, _ := tasks.Get("alice", "c1", task.ID); got.Output != "working\ndone\n" {
		t.Errorf("output = %q", got.Output)
	}
}

func TestTaskFailsOnNonZeroExit(t *testing.T) {
	tasks, _ := newTasks(t, &fakeExec{code: 2})
	task, err := tasks.Start(context.Background(), "alice", "c1", "x")
	if err != nil {
		t.Fatal(err)
	}
	tasks.Wait()
	got, _ := tasks.Get("alice", "c1", task.ID)
	if got.Status != api.TaskFailed || *got.ExitCode != 2 {
		t.Errorf("task = %+v", got)
	}
}

func TestTaskTimesOut(t *testing.T) {
	tasks, _ := newTasks(t, &fakeExec{release: make(chan struct{})})
	tasks.Timeout = 10 * time.Millisecond
	task, _ := tasks.Start(context.Background(), "alice", "c1", "x")
	tasks.Wait()
	got, _ := tasks.Get("alice", "c1", task.ID)
	if got.Status != api.TaskFailed || !strings.Contains(got.Error, "timed out") {
		t.Errorf("task = %+v", got)
	}
}

func TestTaskOutputIsCapped(t *testing.T) {
	tk := &task{limit: 8, changed: make(chan struct{})}
	_, _ = tk.Write([]byte("12345"))
	_, _ = tk.Write([]byte("67890"))
	_, _ = tk.Write([]byte("more"))
	if got := string(tk.out); got != "12345678\n[airlock: output truncated]\n" {
		t.Errorf("out = %q", got)
	}
}

func TestServer(t *testing.T) {
	tasks, s := newTasks(t, &fakeExec{})
	srv := httptest.NewServer((&Server{Sandboxes: s, Tasks: tasks, Token: "secret", Log: slog.New(slog.DiscardHandler)}).Handler())
	defer srv.Close()

	call := func(method, path, token, user, body string) *http.Response {
		req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		if user != "" {
			req.Header.Set(api.HeaderUser, user)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { resp.Body.Close() })
		return resp
	}

	if r := call("GET", "/healthz", "", "", ""); r.StatusCode != 204 {
		t.Errorf("healthz = %d", r.StatusCode)
	}
	if r := call("GET", "/v1/claims", "wrong", "alice", ""); r.StatusCode != 401 {
		t.Errorf("bad token = %d", r.StatusCode)
	}
	if r := call("GET", "/v1/claims", "secret", "", ""); r.StatusCode != 400 {
		t.Errorf("no user = %d", r.StatusCode)
	}
	if r := call("GET", "/v1/claims/c1", "secret", "bob", ""); r.StatusCode != 404 {
		t.Errorf("other user's claim = %d", r.StatusCode)
	}
	if r := call("POST", "/v1/claims", "secret", "alice", `{"pool":"coding-agent","name":"c2"}`); r.StatusCode != 201 || r.Header.Get("Content-Type") != "application/json" {
		t.Errorf("create = %d %s", r.StatusCode, r.Header.Get("Content-Type"))
	}
	if r := call("POST", "/v1/claims/c2/tasks", "secret", "alice", `{"prompt":"x"}`); r.StatusCode != 409 {
		t.Errorf("task on unready claim = %d", r.StatusCode)
	}

	r := call("POST", "/v1/claims/c1/tasks", "secret", "alice", `{"prompt":"go"}`)
	if r.StatusCode != 201 {
		t.Fatalf("start task = %d", r.StatusCode)
	}
	var task api.Task
	_ = json.NewDecoder(r.Body).Decode(&task)

	stream := call("GET", "/v1/claims/c1/tasks/"+task.ID+"/stream", "secret", "alice", "")
	var events []string
	sc := bufio.NewScanner(stream.Body)
	for sc.Scan() {
		if e, ok := strings.CutPrefix(sc.Text(), "event: "); ok {
			events = append(events, e)
		}
	}
	if len(events) == 0 || events[len(events)-1] != "done" {
		t.Errorf("events = %v, want output then done", events)
	}

	if r := call("DELETE", "/v1/claims/c1", "secret", "alice", ""); r.StatusCode != 204 {
		t.Errorf("delete = %d", r.StatusCode)
	}
	if got := tasks.List("alice", "c1"); len(got) != 0 {
		t.Errorf("tasks kept after release: %d", len(got))
	}
}

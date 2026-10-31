package orchestrator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/utils/exec"

	"github.com/jakehulberg/airlock/internal/api"
)

// Executor runs a command in a container and returns its exit code.
type Executor interface {
	Exec(ctx context.Context, pod *corev1.Pod, container string, cmd []string, stdin io.Reader, out io.Writer) (int, error)
}

// KubeExecutor runs commands through the pods/exec subresource.
type KubeExecutor struct {
	Config *rest.Config
	Kube   kubernetes.Interface
}

func (k *KubeExecutor) Exec(ctx context.Context, pod *corev1.Pod, container string, cmd []string, stdin io.Reader, out io.Writer) (int, error) {
	req := k.Kube.CoreV1().RESTClient().Post().
		Resource("pods").Namespace(pod.Namespace).Name(pod.Name).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   cmd,
			Stdin:     stdin != nil,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	ws, err := remotecommand.NewWebSocketExecutor(k.Config, "GET", req.URL().String())
	if err != nil {
		return -1, err
	}
	spdy, err := remotecommand.NewSPDYExecutor(k.Config, "POST", req.URL())
	if err != nil {
		return -1, err
	}
	executor, err := remotecommand.NewFallbackExecutor(ws, spdy, httpstream.IsUpgradeFailure)
	if err != nil {
		return -1, err
	}
	err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdin: stdin, Stdout: out, Stderr: out})
	var exitErr exec.CodeExitError
	if errors.As(err, &exitErr) {
		return exitErr.Code, nil
	}
	if err != nil {
		return -1, err
	}
	return 0, nil
}

// Tasks runs agent tasks in claimed sandboxes and keeps their output in memory.
// Output is lost when the orchestrator restarts; the sandbox is the source of
// truth for anything the agent produced.
type Tasks struct {
	Sandboxes *Sandboxes
	Executor  Executor
	Container string        // the agent container in the sandbox pod, unless the pool says otherwise
	Command   []string      // run with the prompt on stdin, unless the pool says otherwise
	Timeout   time.Duration // per task
	MaxOutput int           // bytes kept per task
	MaxTasks  int           // tasks kept in total, oldest finished dropped first

	mu    sync.Mutex
	tasks map[string]*task
	wg    sync.WaitGroup
}

type task struct {
	owner string
	limit int // bytes of output kept

	mu        sync.Mutex
	t         api.Task
	out       []byte
	truncated bool
	changed   chan struct{} // closed and replaced on every update
}

// Write takes the agent's stdout and stderr. It keeps the first limit bytes.
func (t *task) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.truncated {
		return len(p), nil
	}
	if room := t.limit - len(t.out); room < len(p) {
		t.out = append(t.out, p[:max(room, 0)]...)
		t.out = append(t.out, "\n[airlock: output truncated]\n"...)
		t.truncated = true
	} else {
		t.out = append(t.out, p...)
	}
	close(t.changed)
	t.changed = make(chan struct{})
	return len(p), nil
}

func (t *task) finish(code int, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	t.t.Finished = &now
	if err != nil {
		t.t.Status = api.TaskFailed
		t.t.Error = err.Error()
	} else {
		t.t.ExitCode = &code
		if code == 0 {
			t.t.Status = api.TaskSucceeded
		} else {
			t.t.Status = api.TaskFailed
		}
	}
	close(t.changed)
	t.changed = make(chan struct{})
}

// snapshot returns the task, its output from offset on, and a channel that is
// closed on the next change.
func (t *task) snapshot(offset int) (api.Task, []byte, <-chan struct{}) {
	t.mu.Lock()
	defer t.mu.Unlock()
	var chunk []byte
	if offset < len(t.out) {
		chunk = append([]byte(nil), t.out[offset:]...)
	}
	return t.t, chunk, t.changed
}

func newID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Start runs prompt in the user's claimed sandbox. One task runs per claim at a time.
func (s *Tasks) Start(ctx context.Context, user, claim, prompt string) (api.Task, error) {
	prompt = strings.TrimSpace(prompt)
	if prompt == "" {
		return api.Task{}, fmt.Errorf("%w: prompt is required", ErrBadRequest)
	}
	pod, err := s.Sandboxes.Pod(ctx, user, claim)
	if err != nil {
		return api.Task{}, err
	}
	container, command, err := s.agent(ctx, user, claim)
	if err != nil {
		return api.Task{}, err
	}

	s.mu.Lock()
	if s.tasks == nil {
		s.tasks = map[string]*task{}
	}
	for _, t := range s.tasks {
		if t.owner == user && t.t.Claim == claim && t.status() == api.TaskRunning {
			s.mu.Unlock()
			return api.Task{}, fmt.Errorf("%w: a task is already running in %s", ErrConflict, claim)
		}
	}
	t := &task{
		owner:   user,
		limit:   s.MaxOutput,
		changed: make(chan struct{}),
		t: api.Task{
			ID:      newID(),
			Claim:   claim,
			Prompt:  prompt,
			Status:  api.TaskRunning,
			Started: time.Now(),
		},
	}
	s.tasks[t.t.ID] = t
	s.prune()
	s.mu.Unlock()
	started := t.t

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		// Not the request's context: the task outlives the request.
		runCtx, cancel := context.WithTimeout(context.Background(), s.Timeout)
		defer cancel()
		code, err := s.Executor.Exec(runCtx, pod, container, command, strings.NewReader(prompt+"\n"), t)
		if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
			err = fmt.Errorf("timed out after %s", s.Timeout)
		}
		t.finish(code, err)
	}()
	return started, nil
}

// agent picks the container and command for a claim: its pool's annotations,
// else the defaults.
func (s *Tasks) agent(ctx context.Context, user, claim string) (string, []string, error) {
	c, err := s.Sandboxes.Get(ctx, user, claim)
	if err != nil {
		return "", nil, err
	}
	a, err := s.Sandboxes.PoolAgent(ctx, c.Pool)
	if err != nil {
		return "", nil, err
	}
	container, command := s.Container, s.Command
	if a.Container != "" {
		container = a.Container
	}
	if a.Command != "" {
		command = []string{"sh", "-c", a.Command}
	}
	return container, command, nil
}

// Wait blocks until every running task has finished. For tests and shutdown.
func (s *Tasks) Wait() { s.wg.Wait() }

// ID, owner, claim and start time never change, so they are read without t.mu.
func (t *task) status() api.TaskStatus { return t.meta().Status }

func (t *task) meta() api.Task {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.t
}

// prune must be called with s.mu held.
func (s *Tasks) prune() {
	if s.MaxTasks <= 0 || len(s.tasks) <= s.MaxTasks {
		return
	}
	var done []*task
	for _, t := range s.tasks {
		if t.status() != api.TaskRunning {
			done = append(done, t)
		}
	}
	sort.Slice(done, func(i, j int) bool { return done[i].t.Started.Before(done[j].t.Started) })
	for _, t := range done {
		if len(s.tasks) <= s.MaxTasks {
			return
		}
		delete(s.tasks, t.t.ID)
	}
}

func (s *Tasks) lookup(user, claim, id string) (*task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok || t.owner != user || t.t.Claim != claim {
		return nil, ErrNotFound
	}
	return t, nil
}

// List returns the user's tasks for a claim, newest first, with their output so far.
func (s *Tasks) List(user, claim string) []api.Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []api.Task{}
	for _, t := range s.tasks {
		if t.owner == user && t.t.Claim == claim {
			snap, o, _ := t.snapshot(0)
			snap.Output = string(o)
			out = append(out, snap)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Started.After(out[j].Started) })
	return out
}

// Get returns one task with all of its output so far.
func (s *Tasks) Get(user, claim, id string) (api.Task, error) {
	t, err := s.lookup(user, claim, id)
	if err != nil {
		return api.Task{}, err
	}
	snap, out, _ := t.snapshot(0)
	snap.Output = string(out)
	return snap, nil
}

// Follow calls onOutput with the task's output as it arrives, from the start,
// and returns the task once it has finished or ctx is done.
func (s *Tasks) Follow(ctx context.Context, user, claim, id string, onOutput func([]byte) error) (api.Task, error) {
	t, err := s.lookup(user, claim, id)
	if err != nil {
		return api.Task{}, err
	}
	offset := 0
	for {
		snap, chunk, changed := t.snapshot(offset)
		if len(chunk) > 0 {
			offset += len(chunk)
			if err := onOutput(chunk); err != nil {
				return snap, err
			}
		}
		if snap.Status != api.TaskRunning {
			return snap, nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return snap, ctx.Err()
		}
	}
}

// Forget drops a claim's tasks, after the claim is released.
func (s *Tasks) Forget(user, claim string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, t := range s.tasks {
		if t.owner == user && t.t.Claim == claim {
			delete(s.tasks, id)
		}
	}
}

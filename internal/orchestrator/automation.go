package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/jakehulberg/airlock/internal/api"
	"github.com/jakehulberg/airlock/internal/operations"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type Automation struct {
	Store    *operations.Store
	Tasks    *Tasks
	Log      *slog.Logger
	Capacity int
}

func (a *Automation) Run(ctx context.Context) {
	worker := operations.ID()
	wake := make(chan struct{}, 1)
	go a.Store.Listen(ctx, wake)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-wake:
		}
		{
			tickCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := a.Store.Reconcile(tickCtx)
			if err == nil {
				err = a.Store.Tick(tickCtx)
			}
			if err == nil {
				err = a.cleanup(tickCtx)
			}
			if err != nil {
				a.Log.Error("automation reconciliation failed", "err", err)
				cancel()
				continue
			}
			_, err = a.Store.DB.Exec(tickCtx, `INSERT INTO engine_health VALUES('scheduler',now()) ON CONFLICT(component) DO UPDATE SET last_success=excluded.last_success`)
			if err != nil {
				cancel()
				continue
			}
			w, err := a.Store.Claim(tickCtx, worker, a.Capacity)
			cancel()
			if err != nil {
				a.Log.Error("queue claim failed", "err", err)
				continue
			}
			if w != nil {
				wg.Add(1)
				go func() { defer wg.Done(); a.execute(ctx, w) }()
			}
		}
	}
}

type durableOutput struct {
	mu     sync.Mutex
	buf    []byte
	secret string
}

func (o *durableOutput) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	room := (1 << 20) - len(o.buf)
	o.buf = append(o.buf, p[:min(room, len(p))]...)
	return len(p), nil
}
func (o *durableOutput) String() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	value := strings.ToValidUTF8(string(o.buf), "?")
	if o.secret != "" {
		value = strings.ReplaceAll(value, o.secret, "[redacted]")
	}
	return value
}
func (a *Automation) execute(parent context.Context, w *operations.Work) {
	ctx, cancel := context.WithTimeout(parent, time.Duration(w.Task.TimeoutSeconds)*time.Second)
	defer cancel()
	output := &durableOutput{}
	heartbeatDone := make(chan struct{})
	heartbeatStopped := make(chan struct{})
	go func() {
		defer close(heartbeatStopped)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatDone:
				return
			case <-ticker.C:
				dbCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
				requested, err := a.Store.Heartbeat(dbCtx, w)
				if err == nil {
					err = a.Store.Output(dbCtx, w, output.String())
				}
				stop()
				if err != nil || requested {
					cancel()
					return
				}
			}
		}
	}()
	code := -1
	var runErr error
	started := false
	defer func() {
		close(heartbeatDone)
		<-heartbeatStopped
		dbCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err := a.Store.Output(dbCtx, w, output.String()); err != nil {
			a.Log.Error("persist execution output", "attempt", w.Attempt, "err", err)
		}
		// Any transport error after dispatch is uncertain, including cancellation.
		if err := a.Store.Finish(dbCtx, w, code, runErr, started && runErr != nil); err != nil {
			a.Log.Error("persist execution result", "attempt", w.Attempt, "err", err)
		}
	}()
	// Reserve five minutes for provisioning; TTL bounds orphaned remote processes.
	ttl := min(a.Tasks.Sandboxes.MaxTTL, time.Duration(w.Task.TimeoutSeconds)*time.Second+5*time.Minute)
	if ttl < time.Duration(w.Task.TimeoutSeconds)*time.Second {
		runErr = fmt.Errorf("task timeout exceeds maximum sandbox TTL")
		return
	}
	_, runErr = a.Tasks.Sandboxes.Create(ctx, w.SandboxOwner, "Automation · "+w.Owner, api.CreateClaim{Name: w.Claim, Pool: w.Agent.Pool, TTLMinutes: int(ttl.Minutes())})
	if runErr != nil {
		return
	}
	readyCtx, stop := context.WithTimeout(ctx, 5*time.Minute)
	defer stop()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		pod, err := a.Tasks.Sandboxes.Pod(readyCtx, w.SandboxOwner, w.Claim)
		if err == nil {
			container, command, e := a.Tasks.agent(readyCtx, w.SandboxOwner, w.Claim)
			if e != nil {
				runErr = e
				return
			}
			if runErr = a.Store.SetPod(ctx, w, pod.Name, string(pod.UID)); runErr != nil {
				return
			}
			token, tokenErr := a.Store.IssueToken(ctx, w)
			if tokenErr != nil {
				runErr = tokenErr
				return
			}
			output.mu.Lock()
			output.secret = token
			output.mu.Unlock()
			// Credentials arrive over stdin, never command arguments or persisted prompts.
			quoted := make([]string, len(command))
			for i, arg := range command {
				quoted[i] = "'" + strings.ReplaceAll(arg, "'", "'\"'\"'") + "'"
			}
			command = []string{"sh", "-c", "IFS= read -r AIRLOCK_AGENT_TOKEN; export AIRLOCK_AGENT_TOKEN; export AIRLOCK_AGENT_URL=http://airlock-orchestrator.airlock.svc.cluster.local:8081/v1/agent; export NO_PROXY=airlock-orchestrator.airlock.svc.cluster.local; export no_proxy=$NO_PROXY; exec " + strings.Join(quoted, " ")}
			prompt := token + "\n" + w.Agent.Instructions + "\n\n" + w.Task.Prompt + "\nAgent coordination is available through AIRLOCK_AGENT_URL with Authorization: Bearer $AIRLOCK_AGENT_TOKEN. Never print, persist, or forward this credential. Use curl --noproxy airlock-orchestrator.airlock.svc.cluster.local. GET/POST /messages and POST /delegate are scoped to this execution.\n"
			started = true
			code, runErr = a.Tasks.Executor.Exec(ctx, pod, container, command, strings.NewReader(prompt), output)
			return
		}
		if !errors.Is(err, ErrNotReady) {
			runErr = err
			return
		}
		select {
		case <-readyCtx.Done():
			runErr = fmt.Errorf("sandbox provisioning: %w", readyCtx.Err())
			return
		case <-tick.C:
		}
	}
}

// Cleanup remains pending until the claim and recorded pod are both absent.
// Uncertain attempts never become retryable automatically, even after deletion.
func (a *Automation) cleanup(ctx context.Context) error {
	rows, err := a.Store.DB.Query(ctx, `SELECT a.id,a.claim_name,a.sandbox_owner,a.pod_name,a.pod_uid,t.id,t.state,t.cancel_requested FROM attempts a JOIN tasks t ON t.id=a.task_id WHERE a.cleanup_pending AND (a.state<>'running' OR t.state='uncertain') LIMIT 50`)
	if err != nil {
		return err
	}
	type item struct {
		id, claim, owner, pod, uid, task, state string
		cancel                                  bool
	}
	var items []item
	for rows.Next() {
		var i item
		if err = rows.Scan(&i.id, &i.claim, &i.owner, &i.pod, &i.uid, &i.task, &i.state, &i.cancel); err != nil {
			rows.Close()
			return err
		}
		items = append(items, i)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, i := range items {
		err := a.Tasks.Sandboxes.Delete(ctx, i.owner, i.claim)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return err
		}
		if err == nil {
			continue
		}
		if i.pod != "" {
			pod, e := a.Tasks.Sandboxes.Kube.CoreV1().Pods(a.Tasks.Sandboxes.Namespace).Get(ctx, i.pod, metav1.GetOptions{})
			if e != nil && !apierrors.IsNotFound(e) {
				return e
			}
			if e == nil && string(pod.UID) == i.uid {
				continue
			}
		}
		if _, err = a.Store.DB.Exec(ctx, `UPDATE attempts SET cleanup_pending=false WHERE id=$1`, i.id); err != nil {
			return err
		}
		if i.state == "uncertain" {
			state := "failed"
			if i.cancel {
				state = "cancelled"
			}
			if _, err = a.Store.DB.Exec(ctx, `UPDATE tasks SET state=$2,finished_at=now() WHERE id=$1 AND state='uncertain'`, i.task, state); err != nil {
				return err
			}
		}
	}
	return nil
}

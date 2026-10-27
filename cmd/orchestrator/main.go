// Command orchestrator claims and releases agent sandboxes and runs agent
// tasks in them. It talks to the Kubernetes API; only the web app talks to it.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/jakehulberg/airlock/internal/env"
	"github.com/jakehulberg/airlock/internal/orchestrator"
)

// The agent container gets its Agent Vault proxy settings from /session/env,
// written by the session sidecar. The prompt arrives on stdin.
const defaultTaskCommand = `. /session/env 2>/dev/null; exec claude -p`

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("exiting", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	var req env.Required
	token := req.String("AIRLOCK_API_TOKEN")
	if err := req.Err(); err != nil {
		return err
	}
	defaultTTL, err := env.Duration("AIRLOCK_DEFAULT_TTL", time.Hour)
	if err != nil {
		return err
	}
	maxTTL, err := env.Duration("AIRLOCK_MAX_TTL", 8*time.Hour)
	if err != nil {
		return err
	}
	taskTimeout, err := env.Duration("AIRLOCK_TASK_TIMEOUT", 30*time.Minute)
	if err != nil {
		return err
	}

	cfg, err := kubeConfig()
	if err != nil {
		return err
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return err
	}
	kube, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}

	sandboxes := &orchestrator.Sandboxes{
		Dynamic:      dyn,
		Kube:         kube,
		Namespace:    env.String("AIRLOCK_NAMESPACE", "agents"),
		PoolSelector: env.String("AIRLOCK_POOL_SELECTOR", ""),
		DefaultTTL:   defaultTTL,
		MaxTTL:       maxTTL,
	}
	tasks := &orchestrator.Tasks{
		Sandboxes: sandboxes,
		Executor:  &orchestrator.KubeExecutor{Config: cfg, Kube: kube},
		Container: env.String("AIRLOCK_AGENT_CONTAINER", "agent"),
		Command:   []string{"sh", "-c", env.String("AIRLOCK_TASK_COMMAND", defaultTaskCommand)},
		Timeout:   taskTimeout,
		MaxOutput: 1 << 20,
		MaxTasks:  500,
	}
	srv := &http.Server{
		Addr:              env.String("AIRLOCK_LISTEN", ":8081"),
		Handler:           (&orchestrator.Server{Sandboxes: sandboxes, Tasks: tasks, Token: token, Log: log}).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	log.Info("orchestrator listening", "addr", srv.Addr, "namespace", sandboxes.Namespace)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// kubeConfig uses the in-cluster ServiceAccount, or KUBECONFIG when run locally.
func kubeConfig() (*rest.Config, error) {
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{},
	).ClientConfig()
}

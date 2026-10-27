// Command web serves the Airlock UI. It signs users in with OIDC and calls the
// orchestrator on their behalf; it never talks to the Kubernetes API.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jakehulberg/airlock/internal/env"
	"github.com/jakehulberg/airlock/internal/web"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("exiting", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	var req env.Required
	publicURL := strings.TrimRight(req.String("AIRLOCK_PUBLIC_URL"), "/")
	issuer := req.String("OIDC_ISSUER_URL")
	clientID := req.String("OIDC_CLIENT_ID")
	clientSecret := req.String("OIDC_CLIENT_SECRET")
	sessionKey := req.String("AIRLOCK_SESSION_KEY")
	orchestratorURL := strings.TrimRight(req.String("AIRLOCK_ORCHESTRATOR_URL"), "/")
	token := req.String("AIRLOCK_API_TOKEN")
	if err := req.Err(); err != nil {
		return err
	}
	public, err := url.Parse(publicURL)
	if err != nil || public.Host == "" {
		return errors.New("AIRLOCK_PUBLIC_URL must be an absolute URL")
	}
	sessionTTL, err := env.Duration("AIRLOCK_SESSION_TTL", 12*time.Hour)
	if err != nil {
		return err
	}
	defaultTTL, err := env.Duration("AIRLOCK_DEFAULT_TTL", time.Hour)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cookies, err := web.NewCookies(sessionKey, public.Scheme == "https")
	if err != nil {
		return err
	}
	auth, err := web.NewAuth(ctx, web.AuthConfig{
		IssuerURL:     issuer,
		ClientID:      clientID,
		ClientSecret:  clientSecret,
		RedirectURL:   publicURL + "/callback",
		AllowedGroups: env.List("OIDC_ALLOWED_GROUPS"),
		SessionTTL:    sessionTTL,
	}, cookies)
	if err != nil {
		return err
	}
	handler, err := (&web.Server{
		Auth:         auth,
		Orchestrator: &web.Client{BaseURL: orchestratorURL, Token: token, HTTP: &http.Client{}},
		PublicOrigin: public.Scheme + "://" + public.Host,
		DefaultTTL:   defaultTTL,
		Log:          log,
	}).Handler()
	if err != nil {
		return err
	}

	srv := &http.Server{
		Addr:              env.String("AIRLOCK_LISTEN", ":8080"),
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	log.Info("web listening", "addr", srv.Addr, "public", publicURL, "orchestrator", orchestratorURL)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

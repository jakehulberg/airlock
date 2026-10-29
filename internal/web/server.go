package web

import (
	"embed"
	"errors"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jakehulberg/airlock/internal/api"
)

//go:embed templates/*.html
var templateFS embed.FS

//go:embed static
var staticFS embed.FS

// Server is the Airlock web UI.
type Server struct {
	Auth         *Auth
	Orchestrator *Client
	PublicOrigin string // scheme://host the UI is served on, for the CSRF check
	DefaultTTL   time.Duration
	Log          *slog.Logger

	pages map[string]*template.Template
}

func (s *Server) Handler() (http.Handler, error) {
	if err := s.parseTemplates(); err != nil {
		return nil, err
	}
	static, _ := fs.Sub(staticFS, "static")

	app := http.NewServeMux()
	app.HandleFunc("GET /{$}", s.index)
	app.HandleFunc("POST /claims", s.createClaim)
	app.HandleFunc("GET /claims/{name}", s.claim)
	app.HandleFunc("POST /claims/{name}/release", s.release)
	app.HandleFunc("POST /claims/{name}/tasks", s.startTask)
	app.HandleFunc("GET /claims/{name}/tasks/{id}", s.task)
	app.HandleFunc("GET /claims/{name}/tasks/{id}/stream", s.streamTask)
	app.HandleFunc("POST /logout", s.Auth.Logout)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServerFS(static)))
	mux.HandleFunc("GET /login", s.Auth.Login)
	mux.HandleFunc("GET /callback", s.Auth.Callback)
	mux.Handle("/", s.Auth.Require(app))
	return securityHeaders(s.sameOrigin(mux)), nil
}

func (s *Server) parseTemplates() error {
	funcs := template.FuncMap{
		"until": func(t time.Time) string { return humanDuration(time.Until(t)) },
		"ago":   func(t time.Time) string { return humanDuration(time.Since(t)) + " ago" },
		"clock": func(t time.Time) string { return t.Local().Format("Jan 2 15:04") },
	}
	s.pages = map[string]*template.Template{}
	for _, page := range []string{"index", "claim", "error"} {
		t, err := template.New("layout.html").Funcs(funcs).ParseFS(templateFS, "templates/layout.html", "templates/"+page+".html")
		if err != nil {
			return err
		}
		s.pages[page] = t
	}
	return nil
}

func humanDuration(d time.Duration) string {
	if d < 0 {
		return "expired"
	}
	d = d.Round(time.Minute)
	if d < time.Minute {
		return "less than a minute"
	}
	h, m := int(d.Hours()), int(d.Minutes())%60
	switch {
	case h == 0:
		return fmt.Sprintf("%dm", m)
	case m == 0:
		return fmt.Sprintf("%dh", h)
	default:
		return fmt.Sprintf("%dh %dm", h, m)
	}
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", "default-src 'self'; frame-ancestors 'none'; form-action 'self'")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "same-origin")
		next.ServeHTTP(w, r)
	})
}

// sameOrigin rejects cross-site form posts. The session cookie is SameSite=Lax,
// which already keeps it off them; this is the second lock.
func (s *Server) sameOrigin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			if o := r.Header.Get("Origin"); o != "" && o != s.PublicOrigin {
				http.Error(w, "cross-origin request refused", http.StatusForbidden)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

type page struct {
	User  User
	Title string
	Data  any
}

func (s *Server) render(w http.ResponseWriter, r *http.Request, status int, name, title string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	if err := s.pages[name].Execute(w, page{User: userFrom(r.Context()), Title: title, Data: data}); err != nil {
		s.Log.Error("render", "page", name, "err", err)
	}
}

// fail shows an orchestrator error as a page with the same status.
func (s *Server) fail(w http.ResponseWriter, r *http.Request, err error) {
	status, msg := http.StatusBadGateway, "Airlock's orchestrator could not be reached."
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		status, msg = apiErr.Status, apiErr.Message
		if status >= 500 {
			msg = "Something went wrong in the orchestrator."
		}
	}
	if status >= 500 {
		s.Log.Error("orchestrator", "path", r.URL.Path, "err", err)
	}
	s.render(w, r, status, "error", "Error", msg)
}

func (s *Server) index(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r.Context())
	claims, err := s.Orchestrator.Claims(r.Context(), u)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	pools, err := s.Orchestrator.Pools(r.Context(), u)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	pending := false
	for _, c := range claims {
		if !c.Ready {
			pending = true
		}
	}
	s.render(w, r, http.StatusOK, "index", "Sandboxes", map[string]any{
		"Claims":     claims,
		"Pools":      pools,
		"Pending":    pending,
		"DefaultTTL": int(s.DefaultTTL.Minutes()),
	})
}

func (s *Server) createClaim(w http.ResponseWriter, r *http.Request) {
	ttl, _ := strconv.Atoi(r.FormValue("ttl"))
	claim, err := s.Orchestrator.CreateClaim(r.Context(), userFrom(r.Context()), api.CreateClaim{
		Pool:       r.FormValue("pool"),
		Name:       strings.TrimSpace(r.FormValue("name")),
		TTLMinutes: ttl,
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	http.Redirect(w, r, "/claims/"+esc(claim.Name), http.StatusSeeOther)
}

func (s *Server) claim(w http.ResponseWriter, r *http.Request) {
	u, name := userFrom(r.Context()), r.PathValue("name")
	claim, err := s.Orchestrator.Claim(r.Context(), u, name)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	tasks, err := s.Orchestrator.Tasks(r.Context(), u, name)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	// The page is the conversation, oldest first. A running task's output comes in
	// over its stream, which replays it from the start.
	slices.Reverse(tasks)
	running := false
	for i := range tasks {
		if tasks[i].Status == api.TaskRunning {
			tasks[i].Output = ""
			running = true
		}
	}
	s.render(w, r, http.StatusOK, "claim", claim.Name, map[string]any{"Claim": claim, "Tasks": tasks, "Running": running})
}

func (s *Server) release(w http.ResponseWriter, r *http.Request) {
	if err := s.Orchestrator.ReleaseClaim(r.Context(), userFrom(r.Context()), r.PathValue("name")); err != nil {
		s.fail(w, r, err)
		return
	}
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (s *Server) startTask(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	task, err := s.Orchestrator.StartTask(r.Context(), userFrom(r.Context()), name, r.FormValue("prompt"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	http.Redirect(w, r, taskURL(name, task.ID), http.StatusSeeOther)
}

// task sends old links to the task's place in the conversation.
func (s *Server) task(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, taskURL(r.PathValue("name"), r.PathValue("id")), http.StatusSeeOther)
}

func taskURL(claim, id string) string { return "/claims/" + esc(claim) + "#task-" + esc(id) }

// streamTask relays the orchestrator's event stream to the browser.
func (s *Server) streamTask(w http.ResponseWriter, r *http.Request) {
	body, err := s.Orchestrator.StreamTask(r.Context(), userFrom(r.Context()), r.PathValue("name"), r.PathValue("id"))
	if err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) {
			http.Error(w, apiErr.Message, apiErr.Status)
		} else {
			http.Error(w, "orchestrator unreachable", http.StatusBadGateway)
		}
		return
	}
	defer body.Close()
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	buf := make([]byte, 32<<10)
	for {
		n, err := body.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) && r.Context().Err() == nil {
				s.Log.Warn("task stream ended", "err", err)
			}
			return
		}
	}
}

package orchestrator

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/jakehulberg/airlock/internal/api"
)

// Server is the orchestrator's HTTP API. It is only meant to be reached by the
// web app, which authenticates users and passes their identity in headers.
type Server struct {
	Sandboxes *Sandboxes
	Tasks     *Tasks
	Token     string // shared with the web app
	Log       *slog.Logger
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })

	api := http.NewServeMux()
	api.HandleFunc("GET /v1/pools", s.pools)
	api.HandleFunc("GET /v1/claims", s.listClaims)
	api.HandleFunc("POST /v1/claims", s.createClaim)
	api.HandleFunc("GET /v1/claims/{name}", s.getClaim)
	api.HandleFunc("DELETE /v1/claims/{name}", s.deleteClaim)
	api.HandleFunc("GET /v1/claims/{name}/tasks", s.listTasks)
	api.HandleFunc("POST /v1/claims/{name}/tasks", s.createTask)
	api.HandleFunc("GET /v1/claims/{name}/tasks/{id}", s.getTask)
	api.HandleFunc("GET /v1/claims/{name}/tasks/{id}/stream", s.streamTask)
	mux.Handle("/v1/", s.authenticate(api))
	return mux
}

type userKey struct{}

func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || subtle.ConstantTimeCompare([]byte(token), []byte(s.Token)) != 1 {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		if r.Header.Get(api.HeaderUser) == "" {
			writeError(w, http.StatusBadRequest, "missing "+api.HeaderUser)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func user(r *http.Request) string { return r.Header.Get(api.HeaderUser) }

func (s *Server) pools(w http.ResponseWriter, r *http.Request) {
	pools, err := s.Sandboxes.Pools(r.Context())
	s.respond(w, r, pools, err)
}

func (s *Server) listClaims(w http.ResponseWriter, r *http.Request) {
	claims, err := s.Sandboxes.List(r.Context(), user(r))
	s.respond(w, r, claims, err)
}

func (s *Server) createClaim(w http.ResponseWriter, r *http.Request) {
	var req api.CreateClaim
	if !decode(w, r, &req) {
		return
	}
	name := r.Header.Get(api.HeaderUserName)
	if name == "" {
		name = user(r)
	}
	claim, err := s.Sandboxes.Create(r.Context(), user(r), name, req)
	if err == nil {
		s.Log.Info("claim created", "claim", claim.Name, "pool", claim.Pool, "user", name, "shutdownTime", claim.ShutdownTime)
	}
	s.respondCode(w, r, http.StatusCreated, claim, err)
}

func (s *Server) getClaim(w http.ResponseWriter, r *http.Request) {
	claim, err := s.Sandboxes.Get(r.Context(), user(r), r.PathValue("name"))
	s.respond(w, r, claim, err)
}

func (s *Server) deleteClaim(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := s.Sandboxes.Delete(r.Context(), user(r), name); err != nil {
		s.respond(w, r, nil, err)
		return
	}
	s.Tasks.Forget(user(r), name)
	s.Log.Info("claim released", "claim", name, "user", r.Header.Get(api.HeaderUserName))
	w.WriteHeader(http.StatusNoContent)
}

// claimExists checks the claim belongs to the user, so task routes 404 the
// same way claim routes do.
func (s *Server) claimExists(w http.ResponseWriter, r *http.Request) bool {
	if _, err := s.Sandboxes.Get(r.Context(), user(r), r.PathValue("name")); err != nil {
		s.respond(w, r, nil, err)
		return false
	}
	return true
}

func (s *Server) listTasks(w http.ResponseWriter, r *http.Request) {
	if !s.claimExists(w, r) {
		return
	}
	s.respond(w, r, s.Tasks.List(user(r), r.PathValue("name")), nil)
}

func (s *Server) createTask(w http.ResponseWriter, r *http.Request) {
	var req api.CreateTask
	if !decode(w, r, &req) {
		return
	}
	task, err := s.Tasks.Start(r.Context(), user(r), r.PathValue("name"), req.Prompt)
	if err == nil {
		s.Log.Info("task started", "claim", task.Claim, "task", task.ID, "user", r.Header.Get(api.HeaderUserName))
	}
	s.respondCode(w, r, http.StatusCreated, task, err)
}

func (s *Server) getTask(w http.ResponseWriter, r *http.Request) {
	task, err := s.Tasks.Get(user(r), r.PathValue("name"), r.PathValue("id"))
	s.respond(w, r, task, err)
}

func (s *Server) streamTask(w http.ResponseWriter, r *http.Request) {
	name, id := r.PathValue("name"), r.PathValue("id")
	if _, err := s.Tasks.Get(user(r), name, id); err != nil {
		s.respond(w, r, nil, err)
		return
	}
	flusher, _ := w.(http.Flusher)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	send := func(event string, v any) error {
		b, _ := json.Marshal(v)
		if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, b); err != nil {
			return err
		}
		if flusher != nil {
			flusher.Flush()
		}
		return nil
	}
	task, err := s.Tasks.Follow(r.Context(), user(r), name, id, func(chunk []byte) error {
		return send("output", api.OutputEvent{Chunk: string(chunk)})
	})
	if err == nil {
		_ = send("done", task)
	}
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 256<<10)
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return false
	}
	return true
}

func (s *Server) respond(w http.ResponseWriter, r *http.Request, v any, err error) {
	s.respondCode(w, r, http.StatusOK, v, err)
}

func (s *Server) respondCode(w http.ResponseWriter, r *http.Request, code int, v any, err error) {
	switch {
	case err == nil:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(v)
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, "not found")
	case errors.Is(err, ErrBadRequest):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, ErrConflict), errors.Is(err, ErrNotReady):
		writeError(w, http.StatusConflict, err.Error())
	default:
		s.Log.Error("request failed", "method", r.Method, "path", r.URL.Path, "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(api.Error{Error: msg})
}

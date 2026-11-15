package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/jakehulberg/airlock/internal/operations"
)

func (s *Server) operationsRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/operations/interactive/{id}", func(w http.ResponseWriter, r *http.Request) {
		var b []byte
		err := s.Store.DB.QueryRow(r.Context(), `SELECT data FROM interactive_tasks WHERE id=$1 AND owner=$2`, r.PathValue("id"), user(r)).Scan(&b)
		if err != nil {
			writeError(w, 404, "not found")
			return
		}
		s.operationResponse(w, r, json.RawMessage(b), nil)
	})
	mux.HandleFunc("GET /v1/operations", s.operationsSnapshot)
	mux.HandleFunc("GET /v1/operations/tasks/{id}", func(w http.ResponseWriter, r *http.Request) {
		v, e := s.Store.Detail(r.Context(), user(r), r.PathValue("id"))
		s.operationResponse(w, r, v, e)
	})
	mux.HandleFunc("POST /v1/operations/agents", func(w http.ResponseWriter, r *http.Request) {
		var v operations.Agent
		if !decode(w, r, &v) {
			return
		}
		if e := s.Sandboxes.pool(r.Context(), v.Pool); e != nil {
			s.respond(w, r, nil, e)
			return
		}
		v, e := s.Store.SaveAgent(r.Context(), user(r), v)
		s.operationResponse(w, r, v, e)
	})
	mux.HandleFunc("POST /v1/operations/tasks", func(w http.ResponseWriter, r *http.Request) {
		var v operations.Task
		if !decode(w, r, &v) {
			return
		}
		v, e := s.Store.CreateTask(r.Context(), user(r), v)
		s.operationResponse(w, r, v, e)
	})
	mux.HandleFunc("POST /v1/operations/tasks/{id}/assign", func(w http.ResponseWriter, r *http.Request) {
		var v struct {
			AgentID  string `json:"agentId"`
			Priority int    `json:"priority"`
		}
		if !decode(w, r, &v) {
			return
		}
		e := s.Store.Assign(r.Context(), user(r), r.PathValue("id"), v.AgentID, v.Priority)
		s.operationResponse(w, r, map[string]bool{"ok": e == nil}, e)
	})
	mux.HandleFunc("POST /v1/operations/tasks/{id}/{action}", func(w http.ResponseWriter, r *http.Request) {
		var v struct {
			Reason string `json:"reason"`
		}
		if !decode(w, r, &v) {
			return
		}
		e := s.Store.Action(r.Context(), user(r), r.PathValue("id"), r.PathValue("action"), v.Reason)
		s.operationResponse(w, r, map[string]bool{"ok": e == nil}, e)
	})
	mux.HandleFunc("POST /v1/operations/workflows", func(w http.ResponseWriter, r *http.Request) {
		var v operations.Workflow
		if !decode(w, r, &v) {
			return
		}
		v, e := s.Store.SaveWorkflow(r.Context(), user(r), v)
		s.operationResponse(w, r, v, e)
	})
	mux.HandleFunc("POST /v1/operations/workflows/{id}/run", func(w http.ResponseWriter, r *http.Request) {
		var v json.RawMessage
		if !decode(w, r, &v) {
			return
		}
		id, e := s.Store.RunWorkflow(r.Context(), user(r), r.PathValue("id"), v)
		s.operationResponse(w, r, map[string]string{"id": id}, e)
	})
	mux.HandleFunc("POST /v1/operations/runs/{id}/cancel", func(w http.ResponseWriter, r *http.Request) {
		e := s.Store.CancelWorkflow(r.Context(), user(r), r.PathValue("id"))
		s.operationResponse(w, r, map[string]bool{"ok": e == nil}, e)
	})
	mux.HandleFunc("POST /v1/operations/schedules", func(w http.ResponseWriter, r *http.Request) {
		var v operations.Schedule
		if !decode(w, r, &v) {
			return
		}
		v, e := s.Store.SaveSchedule(r.Context(), user(r), v)
		s.operationResponse(w, r, v, e)
	})
	mux.HandleFunc("POST /v1/operations/schedules/{id}/enabled", func(w http.ResponseWriter, r *http.Request) {
		var v struct {
			Enabled bool `json:"enabled"`
		}
		if !decode(w, r, &v) {
			return
		}
		e := s.Store.SetScheduleEnabled(r.Context(), user(r), r.PathValue("id"), v.Enabled)
		s.operationResponse(w, r, map[string]bool{"ok": e == nil}, e)
	})
	mux.HandleFunc("POST /v1/operations/schedules/{id}/run", func(w http.ResponseWriter, r *http.Request) {
		id, e := s.Store.FireSchedule(r.Context(), user(r), r.PathValue("id"))
		s.operationResponse(w, r, map[string]string{"id": id}, e)
	})
	mux.HandleFunc("GET /v1/operations/stream", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Accel-Buffering", "no")
		tick := time.NewTicker(3 * time.Second)
		defer tick.Stop()
		for {
			v, err := s.Store.Snapshot(r.Context(), user(r))
			if err != nil {
				return
			}
			b, _ := json.Marshal(v)
			if _, err = fmt.Fprintf(w, "event: state\ndata: %s\n\n", b); err != nil {
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			select {
			case <-r.Context().Done():
				return
			case <-tick.C:
			}
		}
	})
}
func (s *Server) operationResponse(w http.ResponseWriter, r *http.Request, v any, err error) {
	switch {
	case errors.Is(err, operations.ErrInvalid):
		writeError(w, 400, err.Error())
	case errors.Is(err, operations.ErrConflict):
		writeError(w, 409, err.Error())
	case errors.Is(err, operations.ErrNotFound):
		writeError(w, 404, "not found")
	default:
		s.respond(w, r, v, err)
	}
}
func (s *Server) operationsSnapshot(w http.ResponseWriter, r *http.Request) {
	v, e := s.Store.Snapshot(r.Context(), user(r))
	s.operationResponse(w, r, v, e)
}
func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	if s.Store != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if s.Store.DB.Ping(ctx) != nil {
			writeError(w, 503, "Database unavailable")
			return
		}
	}
	w.WriteHeader(204)
}
func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	if s.Store == nil {
		w.WriteHeader(503)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	rows, err := s.Store.DB.Query(ctx, `SELECT state,count(*) FROM tasks GROUP BY state`)
	if err != nil {
		w.WriteHeader(503)
		return
	}
	defer rows.Close()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintln(w, "# TYPE airlock_tasks gauge")
	for rows.Next() {
		var state string
		var count int
		if rows.Scan(&state, &count) != nil {
			return
		}
		fmt.Fprintf(w, "airlock_tasks{state=%q} %d\n", state, count)
	}
	rows.Close()
	var retries, attempts, failed int
	var seconds float64
	err = s.Store.DB.QueryRow(ctx, `SELECT count(*),count(*) FILTER(WHERE number>1),count(*) FILTER(WHERE state='failed'),COALESCE(sum(extract(epoch FROM finished_at-started_at)),0) FROM attempts WHERE finished_at IS NOT NULL`).Scan(&attempts, &retries, &failed, &seconds)
	if err == nil {
		fmt.Fprintf(w, "airlock_attempts_total %d\nairlock_retries_total %d\nairlock_failed_attempts_total %d\nairlock_execution_seconds_sum %f\nairlock_execution_seconds_count %d\n", attempts, retries, failed, seconds, attempts)
	}
	var age float64
	if s.Store.DB.QueryRow(ctx, `SELECT extract(epoch FROM now()-last_success) FROM engine_health WHERE component='scheduler'`).Scan(&age) == nil {
		fmt.Fprintf(w, "airlock_scheduler_last_success_age_seconds %f\n", age)
	}
	var duration float64
	var runs int
	if s.Store.DB.QueryRow(ctx, `SELECT count(*),COALESCE(sum(extract(epoch FROM finished_at-created_at)),0) FROM workflow_runs WHERE finished_at IS NOT NULL`).Scan(&runs, &duration) == nil {
		fmt.Fprintf(w, "airlock_workflow_seconds_sum %f\nairlock_workflow_seconds_count %d\n", duration, runs)
	}
	var provisioning int
	if s.Store.DB.QueryRow(ctx, `SELECT count(*) FROM attempts WHERE finished_at IS NOT NULL AND pod_name='' AND state='failed'`).Scan(&provisioning) == nil {
		fmt.Fprintf(w, "airlock_provisioning_failures_total %d\n", provisioning)
	}
}

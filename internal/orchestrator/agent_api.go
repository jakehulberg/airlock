package orchestrator

import (
	"encoding/json"
	"github.com/jakehulberg/airlock/internal/operations"
	"net/http"
	"strings"
)

func (s *Server) agentAPI(w http.ResponseWriter, r *http.Request) {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		writeError(w, 401, "unauthorized")
		return
	}
	p, err := s.Store.AuthenticateAgent(r.Context(), token)
	if err != nil {
		writeError(w, 401, "unauthorized")
		return
	}
	switch {
	case r.Method == "POST" && r.URL.Path == "/v1/agent/result":
		var v json.RawMessage
		if !decode(w, r, &v) {
			return
		}
		if strings.Contains(string(v), token) {
			writeError(w, 400, "Credentials cannot be stored in results")
			return
		}
		e := s.Store.Result(r.Context(), p, v)
		s.operationResponse(w, r, map[string]bool{"ok": e == nil}, e)
	case r.Method == "POST" && r.URL.Path == "/v1/agent/ack":
		var v struct {
			ID string `json:"id"`
		}
		if !decode(w, r, &v) {
			return
		}
		e := s.Store.Acknowledge(r.Context(), p, v.ID)
		s.operationResponse(w, r, map[string]bool{"ok": e == nil}, e)

	case r.Method == "GET" && r.URL.Path == "/v1/agent/messages":
		v, e := s.Store.Inbox(r.Context(), p)
		s.operationResponse(w, r, v, e)
	case r.Method == "POST" && r.URL.Path == "/v1/agent/messages":
		var m operations.Message
		if !decode(w, r, &m) {
			return
		}
		if strings.Contains(string(m.Payload), token) {
			writeError(w, 400, "Credentials cannot be stored in messages")
			return
		}
		v, e := s.Store.SendMessage(r.Context(), p, m)
		s.operationResponse(w, r, v, e)
	case r.Method == "POST" && r.URL.Path == "/v1/agent/delegate":
		var t operations.Task
		if !decode(w, r, &t) {
			return
		}
		if strings.Contains(t.Prompt, token) {
			writeError(w, 400, "Credentials cannot be stored in tasks")
			return
		}
		v, e := s.Store.Delegate(r.Context(), p, t)
		s.operationResponse(w, r, v, e)
	default:
		writeError(w, 404, "not found")
	}
}

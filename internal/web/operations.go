package web

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

func (s *Server) operations(w http.ResponseWriter, r *http.Request) {
	s.render(w, r, 200, "operations", "Operations", nil)
}
func (s *Server) operationsAPI(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/operations/api")
	if strings.Contains(path, "..") {
		http.Error(w, "Invalid path", 400)
		return
	}
	var body any
	if r.Method == http.MethodPost {
		r.Body = http.MaxBytesReader(w, r.Body, 256<<10)
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "Invalid request", 400)
			return
		}
	}
	resp, err := s.Orchestrator.request(r.Context(), userFrom(r.Context()), r.Method, "/v1/operations"+path, body)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	if path == "/stream" {
		buf := make([]byte, 32<<10)
		for {
			n, e := resp.Body.Read(buf)
			if n > 0 {
				if _, err = w.Write(buf[:n]); err != nil {
					return
				}
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
			}
			if e != nil {
				return
			}
		}
	}
	_, _ = io.Copy(w, resp.Body)
}

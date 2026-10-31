package web

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jakehulberg/airlock/internal/api"
)

const testKey = "0123456789abcdef0123456789abcdef"

func TestCookieRoundTrip(t *testing.T) {
	c, err := NewCookies(testKey, true)
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	if err := setCookie(c, rec, sessionCookie, User{Subject: "s", Name: "n"}, time.Hour); err != nil {
		t.Fatal(err)
	}
	ck := rec.Result().Cookies()[0]
	if !ck.HttpOnly || !ck.Secure || ck.SameSite != http.SameSiteLaxMode {
		t.Errorf("cookie flags = %+v", ck)
	}

	req := httptest.NewRequest("GET", "/", nil)
	req.AddCookie(ck)
	if u, ok := getCookie[User](c, req, sessionCookie); !ok || u.Subject != "s" {
		t.Errorf("got %+v %v", u, ok)
	}

	// The same value under another cookie name doesn't open.
	moved := httptest.NewRequest("GET", "/", nil)
	moved.AddCookie(&http.Cookie{Name: loginCookie, Value: ck.Value})
	if _, ok := getCookie[User](c, moved, loginCookie); ok {
		t.Error("cookie value opened under a different name")
	}

	// Another key doesn't open it.
	other, _ := NewCookies(strings.Repeat("x", 32), true)
	if _, ok := getCookie[User](other, req, sessionCookie); ok {
		t.Error("cookie opened with the wrong key")
	}

	if _, err := NewCookies("short", true); err == nil {
		t.Error("short key accepted")
	}
}

func TestCookieExpires(t *testing.T) {
	c, _ := NewCookies(testKey, false)
	rec := httptest.NewRecorder()
	_ = setCookie(c, rec, sessionCookie, User{Subject: "s"}, -time.Second)
	req := httptest.NewRequest("GET", "/", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookie, Value: rec.Result().Cookies()[0].Value})
	if _, ok := getCookie[User](c, req, sessionCookie); ok {
		t.Error("expired cookie accepted")
	}
}

func TestSafeNext(t *testing.T) {
	for in, want := range map[string]string{
		"/claims/x?a=1":     "/claims/x?a=1",
		"":                  "/",
		"https://evil.test": "/",
		"//evil.test":       "/",
		"/\\evil.test":      "/",
		"claims":            "/",
	} {
		if got := safeNext(in); got != want {
			t.Errorf("safeNext(%q) = %q, want %q", in, got, want)
		}
	}
}

// fakeOrchestrator answers like the orchestrator and records the user headers.
func fakeOrchestrator(t *testing.T) (*httptest.Server, *[]string) {
	var users []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" {
			w.WriteHeader(401)
			return
		}
		users = append(users, r.Header.Get(api.HeaderUser))
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == "GET" && r.URL.Path == "/v1/claims":
			_ = json.NewEncoder(w).Encode([]api.Claim{{Name: "c1", Pool: "coding-agent", Ready: true, ShutdownTime: time.Now().Add(time.Hour)}})
		case r.Method == "GET" && r.URL.Path == "/v1/pools":
			_ = json.NewEncoder(w).Encode([]api.Pool{{Name: "coding-agent", Description: "Claude Code"}})
		case r.Method == "POST" && r.URL.Path == "/v1/claims":
			var req api.CreateClaim
			_ = json.NewDecoder(r.Body).Decode(&req)
			if req.Pool == "" {
				w.WriteHeader(400)
				_ = json.NewEncoder(w).Encode(api.Error{Error: "bad request: pool is required"})
				return
			}
			w.WriteHeader(201)
			_ = json.NewEncoder(w).Encode(api.Claim{Name: "airlock-x1"})
		case r.Method == "GET" && (r.URL.Path == "/v1/claims/c1" || r.URL.Path == "/v1/claims/c2"):
			_ = json.NewEncoder(w).Encode(api.Claim{Name: strings.TrimPrefix(r.URL.Path, "/v1/claims/"), Ready: true, ShutdownTime: time.Now().Add(time.Hour)})
		case r.Method == "GET" && r.URL.Path == "/v1/claims/c1/tasks": // newest first
			_ = json.NewEncoder(w).Encode([]api.Task{
				{ID: "t2", Claim: "c1", Prompt: "second", Status: api.TaskRunning, Output: "partial"},
				{ID: "t1", Claim: "c1", Prompt: "first", Status: api.TaskSucceeded, Output: "answer"},
			})
		case r.Method == "GET" && r.URL.Path == "/v1/claims/c2/tasks":
			_ = json.NewEncoder(w).Encode([]api.Task{{ID: "t3", Claim: "c2", Prompt: "only", Status: api.TaskSucceeded}})
		case r.URL.Path == "/v1/claims/c1/tasks/t1/stream":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "event: output\ndata: {\"chunk\":\"hi\"}\n\nevent: done\ndata: {}\n\n")
		default:
			w.WriteHeader(404)
			_ = json.NewEncoder(w).Encode(api.Error{Error: "not found"})
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &users
}

func newTestServer(t *testing.T) (http.Handler, *Cookies, *[]string) {
	t.Helper()
	orch, users := fakeOrchestrator(t)
	c, _ := NewCookies(testKey, false)
	h, err := (&Server{
		Auth:         &Auth{cookies: c, sessionTTL: time.Hour},
		Orchestrator: &Client{BaseURL: orch.URL, Token: "tok", HTTP: orch.Client()},
		PublicOrigin: "https://airlock.test",
		DefaultTTL:   time.Hour,
		Log:          slog.New(slog.DiscardHandler),
	}).Handler()
	if err != nil {
		t.Fatal(err)
	}
	return h, c, users
}

func signedIn(t *testing.T, c *Cookies, req *http.Request) *http.Request {
	rec := httptest.NewRecorder()
	_ = setCookie(c, rec, sessionCookie, User{Subject: "iss#alice", Name: "alice@example.com"}, time.Hour)
	req.AddCookie(rec.Result().Cookies()[0])
	return req
}

func TestSignInRequired(t *testing.T) {
	h, _, _ := newTestServer(t)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/claims/c1", nil))
	if rec.Code != 302 || rec.Header().Get("Location") != "/login?next="+url.QueryEscape("/claims/c1") {
		t.Errorf("page = %d %s", rec.Code, rec.Header().Get("Location"))
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/claims", nil))
	if rec.Code != 401 {
		t.Errorf("post = %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/healthz", nil))
	if rec.Code != 204 {
		t.Errorf("healthz = %d", rec.Code)
	}
}

func TestIndex(t *testing.T) {
	h, c, users := newTestServer(t)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, signedIn(t, c, httptest.NewRequest("GET", "/", nil)))
	body := rec.Body.String()
	if rec.Code != 200 {
		t.Fatalf("index = %d: %s", rec.Code, body)
	}
	for _, want := range []string{"alice@example.com", `href="/claims/c1"`, "Claude Code", "Ready"} {
		if !strings.Contains(body, want) {
			t.Errorf("index missing %q", want)
		}
	}
	if len(*users) == 0 || (*users)[0] != "iss#alice" {
		t.Errorf("orchestrator saw users %v", *users)
	}
	if rec.Header().Get("Content-Security-Policy") == "" {
		t.Error("no CSP header")
	}
}

func TestCreateClaim(t *testing.T) {
	h, c, _ := newTestServer(t)
	post := func(form url.Values, origin string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("POST", "/claims", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, signedIn(t, c, req))
		return rec
	}

	if rec := post(url.Values{"pool": {"coding-agent"}, "ttl": {"30"}}, "https://airlock.test"); rec.Code != 303 || rec.Header().Get("Location") != "/claims/airlock-x1" {
		t.Errorf("create = %d %s", rec.Code, rec.Header().Get("Location"))
	}
	if rec := post(url.Values{"pool": {"coding-agent"}}, "https://evil.test"); rec.Code != 403 {
		t.Errorf("cross-origin create = %d", rec.Code)
	}
	if rec := post(url.Values{}, ""); rec.Code != 400 || !strings.Contains(rec.Body.String(), "pool is required") {
		t.Errorf("bad create = %d %s", rec.Code, rec.Body.String())
	}
}

func TestConversation(t *testing.T) {
	h, c, _ := newTestServer(t)
	get := func(path string) string {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, signedIn(t, c, httptest.NewRequest("GET", path, nil)))
		if rec.Code != 200 {
			t.Fatalf("%s = %d: %s", path, rec.Code, rec.Body)
		}
		return rec.Body.String()
	}

	body := get("/claims/c1")
	if i, j := strings.Index(body, "first"), strings.Index(body, "second"); i < 0 || j < i {
		t.Errorf("turns not oldest first: first at %d, second at %d", i, j)
	}
	if !strings.Contains(body, "answer") {
		t.Error("finished task's output missing")
	}
	if strings.Contains(body, "partial") {
		t.Error("running task's output rendered; it should come from the stream")
	}
	if !strings.Contains(body, `data-stream="/claims/c1/tasks/t2/stream"`) {
		t.Error("running task not streamed")
	}
	if !strings.Contains(body, `action="/claims/c1/tasks" hidden`) {
		t.Error("reply form shown while a task runs")
	}

	body = get("/claims/c2")
	if !strings.Contains(body, `action="/claims/c2/tasks">`) {
		t.Error("reply form hidden with nothing running")
	}
}

func TestTaskLinksToConversation(t *testing.T) {
	h, c, _ := newTestServer(t)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, signedIn(t, c, httptest.NewRequest("GET", "/claims/c1/tasks/t1", nil)))
	if rec.Code != 303 || rec.Header().Get("Location") != "/claims/c1#task-t1" {
		t.Errorf("task page = %d %q", rec.Code, rec.Header().Get("Location"))
	}
}

func TestStreamRelays(t *testing.T) {
	h, c, _ := newTestServer(t)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, signedIn(t, c, httptest.NewRequest("GET", "/claims/c1/tasks/t1/stream", nil)))
	if rec.Header().Get("Content-Type") != "text/event-stream" || !strings.Contains(rec.Body.String(), "event: done") {
		t.Errorf("stream = %d %q", rec.Code, rec.Body.String())
	}
}

func TestHumanDuration(t *testing.T) {
	for d, want := range map[time.Duration]string{
		-time.Second:                "expired",
		20 * time.Second:            "less than a minute",
		45 * time.Minute:            "45m",
		2 * time.Hour:               "2h",
		2*time.Hour + 5*time.Minute: "2h 5m",
	} {
		if got := humanDuration(d); got != want {
			t.Errorf("humanDuration(%s) = %q, want %q", d, got, want)
		}
	}
}

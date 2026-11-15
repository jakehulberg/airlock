package web

// This opt-in harness uses an isolated PostgreSQL schema and fake Kubernetes.
// It is compiled only into the test binary, never either production executable.
import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jakehulberg/airlock/internal/operations"
	"github.com/jakehulberg/airlock/internal/orchestrator"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

func previewHandler(t *testing.T) http.Handler {
	t.Helper()
	if os.Getenv("AIRLOCK_TEST_DATABASE_URL") == "" {
		t.Skip("requires AIRLOCK_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	dsn := os.Getenv("AIRLOCK_TEST_DATABASE_URL")
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schemaName := "visual_" + strings.ReplaceAll(operations.ID(), "-", "")
	if _, err = db.Exec(ctx, `CREATE SCHEMA `+schemaName); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Exec(ctx, `DROP SCHEMA `+schemaName+` CASCADE`); db.Close() })
	u, _ := url.Parse(dsn)
	q := u.Query()
	q.Set("search_path", schemaName)
	u.RawQuery = q.Encode()
	store, err := operations.Open(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.DB.Close)
	pool := &unstructured.Unstructured{Object: map[string]any{"apiVersion": orchestrator.PoolGVR.GroupVersion().String(), "kind": "SandboxWarmPool", "metadata": map[string]any{"name": "claude", "namespace": "agents"}, "spec": map[string]any{"sandboxTemplateRef": map[string]any{"name": "claude"}}}}
	dyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{orchestrator.PoolGVR: "SandboxWarmPoolList", orchestrator.ClaimGVR: "SandboxClaimList"}, pool)
	sb := &orchestrator.Sandboxes{Dynamic: dyn, Kube: kubefake.NewClientset(), Namespace: "agents", DefaultTTL: time.Hour, MaxTTL: 8 * time.Hour}
	log := slog.New(slog.DiscardHandler)
	orch := httptest.NewServer((&orchestrator.Server{Store: store, Sandboxes: sb, Tasks: &orchestrator.Tasks{}, Token: "preview-only", Log: log}).Handler())
	t.Cleanup(orch.Close)
	cookies, _ := NewCookies(testKey, false)
	h, err := (&Server{Auth: &Auth{cookies: cookies, sessionTTL: time.Hour}, Orchestrator: &Client{BaseURL: orch.URL, Token: "preview-only", HTTP: orch.Client()}, PublicOrigin: "http://127.0.0.1:18080", DefaultTTL: time.Hour, Log: log}).Handler()
	if err != nil {
		t.Fatal(err)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		signed := httptest.NewRecorder()
		_ = setCookie(cookies, signed, sessionCookie, User{Subject: "preview-user", Name: "Browser verification"}, time.Hour)
		r.AddCookie(signed.Result().Cookies()[0])
		h.ServeHTTP(w, r)
	})
}
func TestBrowserPreview(t *testing.T) {
	if os.Getenv("AIRLOCK_VISUAL_TEST") != "1" {
		t.Skip("opt-in browser verification harness")
	}
	h := previewHandler(t)
	listener, err := net.Listen("tcp", "127.0.0.1:18080")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{ReadHeaderTimeout: 10 * time.Second, Handler: h}
	defer server.Close()
	go server.Serve(listener)
	t.Log("Browser preview on 127.0.0.1:18080; isolated test data, no execution workers")
	<-time.After(15 * time.Minute)
}

func TestOperationsHTTPJourneys(t *testing.T) {
	server := httptest.NewServer(previewHandler(t))
	defer server.Close()
	request := func(method, path string, body any) map[string]any {
		t.Helper()
		var rd io.Reader
		if body != nil {
			b, _ := json.Marshal(body)
			rd = bytes.NewReader(b)
		}
		req, _ := http.NewRequest(method, server.URL+"/operations/api"+path, rd)
		req.Header.Set("Content-Type", "application/json")
		resp, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			t.Fatalf("%s %s: %d %s", method, path, resp.StatusCode, b)
		}
		var v map[string]any
		if err = json.Unmarshal(b, &v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	agent := request("POST", "/agents", map[string]any{"name": "Research", "pool": "claude", "enabled": true})
	task := request("POST", "/tasks", map[string]any{"title": "Review", "prompt": "Research advisories", "agentId": agent["id"], "approvalRequired": true})
	taskID := task["id"].(string)
	detail := request("GET", "/tasks/"+taskID, nil)
	if detail["task"].(map[string]any)["state"] != "awaiting_approval" {
		t.Fatal(detail)
	}
	request("POST", "/tasks/"+taskID+"/approve", map[string]any{})
	detail = request("GET", "/tasks/"+taskID, nil)
	if detail["task"].(map[string]any)["state"] != "queued" {
		t.Fatal(detail)
	}
	request("POST", "/tasks/"+taskID+"/cancel", map[string]any{})
	detail = request("GET", "/tasks/"+taskID, nil)
	if detail["task"].(map[string]any)["state"] != "cancelled" {
		t.Fatal(detail)
	}
	workflow := request("POST", "/workflows", map[string]any{"name": "Daily research", "steps": []map[string]any{{"key": "research", "agentId": agent["id"], "prompt": "Research", "needs": []string{}}}})
	schedule := request("POST", "/schedules", map[string]any{"name": "Morning research", "workflowId": workflow["id"], "cron": "0 8 * * *", "timezone": "America/Chicago", "concurrency": "forbid", "missed": "once", "enabled": true})
	request("POST", "/schedules/"+schedule["id"].(string)+"/enabled", map[string]any{"enabled": false})
	run := request("POST", "/workflows/"+workflow["id"].(string)+"/run", map[string]any{"topic": "Kubernetes"})
	request("POST", "/runs/"+run["id"].(string)+"/cancel", map[string]any{})
	snapshot := request("GET", "", nil)
	if len(snapshot["tasks"].([]any)) != 2 || len(snapshot["schedules"].([]any)) != 1 {
		t.Fatal(snapshot)
	}
}

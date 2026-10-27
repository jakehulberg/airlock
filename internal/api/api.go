// Package api holds the JSON types the orchestrator serves and the web app reads.
package api

import "time"

// Headers the web app sets on every orchestrator request, after it has
// authenticated the user with OIDC. The orchestrator only trusts them from a
// caller that also presents the shared API token.
const (
	HeaderUser     = "X-Airlock-User"      // stable OIDC subject
	HeaderUserName = "X-Airlock-User-Name" // email or username, for display
)

// Pool is a SandboxWarmPool a user can claim a sandbox from.
type Pool struct {
	Name        string `json:"name"`
	Template    string `json:"template"`
	Description string `json:"description,omitempty"`
	Replicas    int64  `json:"replicas"`
	Ready       int64  `json:"ready"`
}

// Claim is one user's SandboxClaim.
type Claim struct {
	Name         string    `json:"name"`
	Pool         string    `json:"pool"`
	Owner        string    `json:"owner"`
	Sandbox      string    `json:"sandbox,omitempty"`
	Ready        bool      `json:"ready"`
	Reason       string    `json:"reason,omitempty"`
	Message      string    `json:"message,omitempty"`
	Created      time.Time `json:"created"`
	ShutdownTime time.Time `json:"shutdownTime"`
	Deleting     bool      `json:"deleting,omitempty"`
}

// CreateClaim is the body of POST /v1/claims.
type CreateClaim struct {
	Pool       string `json:"pool"`
	Name       string `json:"name,omitempty"` // generated when empty
	TTLMinutes int    `json:"ttlMinutes,omitempty"`
}

type TaskStatus string

const (
	TaskRunning   TaskStatus = "running"
	TaskSucceeded TaskStatus = "succeeded"
	TaskFailed    TaskStatus = "failed"
)

// Task is one agent run inside a claimed sandbox.
type Task struct {
	ID       string     `json:"id"`
	Claim    string     `json:"claim"`
	Prompt   string     `json:"prompt"`
	Status   TaskStatus `json:"status"`
	ExitCode *int       `json:"exitCode,omitempty"`
	Error    string     `json:"error,omitempty"`
	Started  time.Time  `json:"started"`
	Finished *time.Time `json:"finished,omitempty"`
	Output   string     `json:"output,omitempty"` // only on GET of a single task
}

// CreateTask is the body of POST /v1/claims/{name}/tasks.
type CreateTask struct {
	Prompt string `json:"prompt"`
}

// Stream events on GET /v1/claims/{name}/tasks/{id}/stream (text/event-stream).
//
//	event: output   data: OutputEvent
//	event: done     data: Task (without Output)
type OutputEvent struct {
	Chunk string `json:"chunk"`
}

// Error is the body of every non-2xx response.
type Error struct {
	Error string `json:"error"`
}

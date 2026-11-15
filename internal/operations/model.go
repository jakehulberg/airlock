// Package operations coordinates durable work. Sandbox execution stays in orchestrator.
package operations

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
	"strings"
	"time"
	_ "time/tzdata"
)

var ErrInvalid = errors.New("invalid request")
var ErrConflict = errors.New("state conflict")
var ErrNotFound = errors.New("not found")

func ID() string { return uuid.NewString() }

type Agent struct {
	Delegates        []string `json:"delegates"`
	ApprovalRequired bool     `json:"approvalRequired"`
	ID               string   `json:"id"`
	Name             string   `json:"name"`
	Instructions     string   `json:"instructions"`
	Pool             string   `json:"pool"`
	Enabled          bool     `json:"enabled"`
}
type Task struct {
	DependencyPolicy string     `json:"dependencyPolicy"`
	ID               string     `json:"id"`
	Title            string     `json:"title"`
	Prompt           string     `json:"prompt"`
	AgentID          string     `json:"agentId"`
	RunID            string     `json:"runId,omitempty"`
	State            string     `json:"state"`
	Priority         int        `json:"priority"`
	MaxAttempts      int        `json:"maxAttempts"`
	TimeoutSeconds   int        `json:"timeoutSeconds"`
	ApprovalRequired bool       `json:"approvalRequired"`
	Dependencies     []string   `json:"dependencies,omitempty"`
	IdempotencyKey   string     `json:"idempotencyKey,omitempty"`
	AttemptCount     int        `json:"attemptCount"`
	Created          time.Time  `json:"created"`
	Finished         *time.Time `json:"finished,omitempty"`
}
type Step struct {
	DependencyPolicy string   `json:"dependencyPolicy"`
	MaxAttempts      int      `json:"maxAttempts"`
	TimeoutSeconds   int      `json:"timeoutSeconds"`
	Key              string   `json:"key"`
	Title            string   `json:"title"`
	AgentID          string   `json:"agentId"`
	Prompt           string   `json:"prompt"`
	Needs            []string `json:"needs"`
	Approval         bool     `json:"approval"`
}
type Workflow struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Steps []Step `json:"steps"`
}
type Schedule struct {
	ID          string     `json:"id"`
	Name        string     `json:"name"`
	WorkflowID  string     `json:"workflowId"`
	Cron        string     `json:"cron"`
	Timezone    string     `json:"timezone"`
	At          *time.Time `json:"at,omitempty"`
	Enabled     bool       `json:"enabled"`
	Concurrency string     `json:"concurrency"`
	Missed      string     `json:"missed"`
	Next        *time.Time `json:"next,omitempty"`
}

func (s Schedule) NextAfter(after time.Time) (time.Time, error) {
	if s.At != nil {
		if s.Cron != "" {
			return time.Time{}, fmt.Errorf("%w: choose cron or one-time", ErrInvalid)
		}
		return *s.At, nil
	}
	if strings.Contains(s.Cron, "TZ=") {
		return time.Time{}, fmt.Errorf("%w: use the timezone field", ErrInvalid)
	}
	loc, err := time.LoadLocation(s.Timezone)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: timezone", ErrInvalid)
	}
	expr, err := cron.ParseStandard(s.Cron)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: cron expression", ErrInvalid)
	}
	next := expr.Next(after.In(loc))
	if next.IsZero() {
		return next, fmt.Errorf("%w: schedule has no next run", ErrInvalid)
	}
	return next, nil
}
func (w Workflow) Validate() error {
	if strings.TrimSpace(w.Name) == "" || len(w.Steps) == 0 || len(w.Steps) > 32 {
		return fmt.Errorf("%w: workflow needs a name and 1–32 steps", ErrInvalid)
	}
	steps := map[string]Step{}
	for _, s := range w.Steps {
		if s.DependencyPolicy != "" && s.DependencyPolicy != "fail" && s.DependencyPolicy != "block" {
			return fmt.Errorf("%w: dependency policy", ErrInvalid)
		}
		if s.Key == "" || s.AgentID == "" || strings.TrimSpace(s.Prompt) == "" {
			return fmt.Errorf("%w: step requires key, agent and prompt", ErrInvalid)
		}
		if _, ok := steps[s.Key]; ok {
			return fmt.Errorf("%w: duplicate step", ErrInvalid)
		}
		steps[s.Key] = s
	}
	visiting, done := map[string]bool{}, map[string]bool{}
	var visit func(string) error
	visit = func(key string) error {
		if done[key] {
			return nil
		}
		s, ok := steps[key]
		if !ok || visiting[key] {
			return fmt.Errorf("%w: missing dependency or cycle", ErrInvalid)
		}
		visiting[key] = true
		for _, n := range s.Needs {
			if err := visit(n); err != nil {
				return err
			}
		}
		visiting[key] = false
		done[key] = true
		return nil
	}
	for k := range steps {
		if err := visit(k); err != nil {
			return err
		}
	}
	return nil
}
func Transition(from, action string) (string, error) {
	switch action {
	case "queue":
		if from == "draft" {
			return "queued", nil
		}
	case "retry":
		if from == "failed" {
			return "queued", nil
		}
	case "approve":
		if from == "awaiting_approval" {
			return "queued", nil
		}
	case "reject":
		if from == "awaiting_approval" {
			return "cancelled", nil
		}
	case "cancel":
		if from == "draft" || from == "queued" || from == "blocked" || from == "awaiting_approval" {
			return "cancelled", nil
		}
	}
	return "", fmt.Errorf("%w: cannot %s a %s task", ErrConflict, action, from)
}

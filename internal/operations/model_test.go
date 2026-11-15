package operations

import (
	"testing"
	"time"
)

func TestTransitions(t *testing.T) {
	for _, tc := range []struct{ from, action, want string }{{"draft", "queue", "queued"}, {"failed", "retry", "queued"}, {"awaiting_approval", "approve", "queued"}, {"awaiting_approval", "reject", "cancelled"}, {"queued", "cancel", "cancelled"}} {
		got, err := Transition(tc.from, tc.action)
		if err != nil || got != tc.want {
			t.Fatalf("%+v: %s %v", tc, got, err)
		}
	}
	for _, state := range []string{"completed", "running", "uncertain", "cancelled"} {
		if _, err := Transition(state, "retry"); err == nil {
			t.Fatalf("retry accepted from %s", state)
		}
	}
}
func TestWorkflowValidation(t *testing.T) {
	w := Workflow{Name: "test", Steps: []Step{{Key: "a", AgentID: "x", Prompt: "a"}, {Key: "b", AgentID: "x", Prompt: "b", Needs: []string{"a"}}}}
	if err := w.Validate(); err != nil {
		t.Fatal(err)
	}
	w.Steps[0].Needs = []string{"b"}
	if w.Validate() == nil {
		t.Fatal("cycle accepted")
	}
	w.Steps[0].Needs = []string{"missing"}
	if w.Validate() == nil {
		t.Fatal("missing dependency accepted")
	}
}
func TestScheduleTimezone(t *testing.T) {
	s := Schedule{Cron: "0 8 * * *", Timezone: "America/Chicago"}
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	next, err := s.NextAfter(at)
	if err != nil || next.Hour() != 8 || next.UTC().Hour() != 14 {
		t.Fatalf("%v %v", next, err)
	}
	s.Timezone = "invalid"
	if _, err = s.NextAfter(at); err == nil {
		t.Fatal("invalid timezone accepted")
	}
	s.Timezone = "UTC"
	s.Cron = "CRON_TZ=UTC 0 8 * * *"
	if _, err = s.NextAfter(at); err == nil {
		t.Fatal("timezone override accepted")
	}
}

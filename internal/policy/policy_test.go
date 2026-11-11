package policy

import (
	"testing"
	"time"

	"github.com/aarondcarr/agent-event-bus/internal/event"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func ev(agent string, typ event.Type, at time.Duration) event.Event {
	return event.Event{Version: 1, ID: "x", AgentID: agent, Type: typ, Timestamp: t0.Add(at)}
}

func denial(agent string, at time.Duration) event.Event {
	e := ev(agent, event.GuardDecision, at)
	no := false
	e.Allowed = &no
	return e
}

func llm(agent string, at time.Duration, tokens int64) event.Event {
	e := ev(agent, event.LLMCall, at)
	e.Tokens = tokens
	return e
}

func TestRules(t *testing.T) {
	cfg := Config{Window: time.Minute, MaxErrors: 3, MaxGuardDenials: 2, MaxTokens: 1000}
	cases := []struct {
		name   string
		events []event.Event
		reason string // "" = not killed
	}{
		{"healthy agent", []event.Event{ev("a", event.ToolCall, 0), llm("a", time.Second, 500)}, ""},
		{"errors trip at threshold", []event.Event{ev("a", event.Error, 0), ev("a", event.Error, 1), ev("a", event.Error, 2)}, "error_rate"},
		{"errors outside window don't count", []event.Event{ev("a", event.Error, 0), ev("a", event.Error, time.Second), ev("a", event.Error, 2*time.Minute)}, ""},
		{"guard denials trip", []event.Event{denial("a", 0), denial("a", time.Second)}, "guard_denials"},
		{"allowed guard decisions are fine", func() []event.Event {
			e := ev("a", event.GuardDecision, 0)
			yes := true
			e.Allowed = &yes
			return []event.Event{e, e, e}
		}(), ""},
		{"token budget is exclusive", []event.Event{llm("a", 0, 600), llm("a", 1, 400)}, ""},
		{"token budget exceeded", []event.Event{llm("a", 0, 600), llm("a", 1, 401)}, "token_budget"},
		{"agents are isolated", []event.Event{denial("a", 0), denial("b", 1)}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := New(cfg)
			for _, e := range c.events {
				p.Observe(e)
			}
			killed, reason := p.Status("a")
			if killed != (c.reason != "") || reason != c.reason {
				t.Fatalf("got killed=%v reason=%q, want %q", killed, reason, c.reason)
			}
		})
	}
}

func TestKillIsStickyAndReportedOnce(t *testing.T) {
	p := New(Config{Window: time.Minute, MaxGuardDenials: 1})
	if d := p.Observe(denial("a", 0)); !d.NewlyKilled {
		t.Fatal("first trip should be NewlyKilled")
	}
	// Ten minutes of good behaviour later, still killed, and not re-announced.
	d := p.Observe(ev("a", event.ToolCall, 10*time.Minute))
	if !d.Killed || d.NewlyKilled || d.Reason != "guard_denials" {
		t.Fatalf("got %+v", d)
	}
}

func TestZeroLimitsDisableRules(t *testing.T) {
	p := New(Config{Window: time.Minute})
	for i := 0; i < 100; i++ {
		p.Observe(ev("a", event.Error, time.Duration(i)))
	}
	if killed, _ := p.Status("a"); killed {
		t.Fatal("no limits configured, should never kill")
	}
}

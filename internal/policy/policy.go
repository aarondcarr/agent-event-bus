// Package policy is a per-agent kill switch.
//
// It watches each agent's event stream over a sliding window and trips when
// an agent misbehaves: too many errors, too many guard denials, or too many
// LLM tokens. Once tripped, an agent stays killed until an operator acts;
// a kill switch that silently re-arms is not a kill switch.
//
// Time is event time (Event.Timestamp), not wall-clock time, so replaying
// the same partition yields the same decisions. Because Kafka orders events
// per partition and agent_id is the partition key, each agent's events
// arrive in the order its producer sent them.
package policy

import (
	"sync"
	"time"

	"github.com/aarondcarr/agent-event-bus/internal/event"
)

// Config sets the thresholds. A zero limit disables that rule.
type Config struct {
	Window          time.Duration
	MaxErrors       int   // errors within Window
	MaxGuardDenials int   // guard_decision allowed=false within Window
	MaxTokens       int64 // llm_call tokens within Window
}

// Decision is the evaluator's verdict after one event.
type Decision struct {
	Killed      bool
	Reason      string
	NewlyKilled bool // true only on the event that tripped the switch
}

type sample struct {
	at time.Time
	n  int64
}

type agentState struct {
	errors, denials, tokens []sample
	killed                  bool
	reason                  string
}

// Evaluator is safe for concurrent use.
type Evaluator struct {
	cfg    Config
	mu     sync.Mutex
	agents map[string]*agentState
}

func New(cfg Config) *Evaluator {
	return &Evaluator{cfg: cfg, agents: map[string]*agentState{}}
}

// Observe folds one event into the agent's state and returns the verdict.
func (ev *Evaluator) Observe(e event.Event) Decision {
	ev.mu.Lock()
	defer ev.mu.Unlock()
	s := ev.agents[e.AgentID]
	if s == nil {
		s = &agentState{}
		ev.agents[e.AgentID] = s
	}
	if s.killed {
		return Decision{Killed: true, Reason: s.reason}
	}

	switch {
	case e.Type == event.Error:
		s.errors = append(s.errors, sample{e.Timestamp, 1})
	case e.Type == event.GuardDecision && e.Allowed != nil && !*e.Allowed:
		s.denials = append(s.denials, sample{e.Timestamp, 1})
	case e.Type == event.LLMCall && e.Tokens > 0:
		s.tokens = append(s.tokens, sample{e.Timestamp, e.Tokens})
	}

	cutoff := e.Timestamp.Add(-ev.cfg.Window)
	s.errors, s.denials, s.tokens = prune(s.errors, cutoff), prune(s.denials, cutoff), prune(s.tokens, cutoff)

	switch {
	case ev.cfg.MaxErrors > 0 && sum(s.errors) >= int64(ev.cfg.MaxErrors):
		s.reason = "error_rate"
	case ev.cfg.MaxGuardDenials > 0 && sum(s.denials) >= int64(ev.cfg.MaxGuardDenials):
		s.reason = "guard_denials"
	case ev.cfg.MaxTokens > 0 && sum(s.tokens) > ev.cfg.MaxTokens:
		s.reason = "token_budget"
	default:
		return Decision{}
	}
	s.killed = true
	s.errors, s.denials, s.tokens = nil, nil, nil // free memory; state is now just "killed"
	return Decision{Killed: true, Reason: s.reason, NewlyKilled: true}
}

// Status reports whether an agent is killed, and why.
func (ev *Evaluator) Status(agentID string) (killed bool, reason string) {
	ev.mu.Lock()
	defer ev.mu.Unlock()
	if s := ev.agents[agentID]; s != nil {
		return s.killed, s.reason
	}
	return false, ""
}

// prune drops samples older than cutoff. Samples are appended in event-time
// order (per-partition ordering), so the slice is sorted and we can cut the
// prefix. Out-of-order events are tolerated, just counted slightly late.
func prune(xs []sample, cutoff time.Time) []sample {
	i := 0
	for i < len(xs) && xs[i].at.Before(cutoff) {
		i++
	}
	return xs[i:]
}

func sum(xs []sample) (n int64) {
	for _, x := range xs {
		n += x.n
	}
	return n
}

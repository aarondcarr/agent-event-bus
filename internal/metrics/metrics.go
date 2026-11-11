// Package metrics keeps running per-agent aggregates of the event stream.
//
// It is intentionally an in-memory projection: it is rebuilt by replaying
// the topic from the beginning (a new consumer group, or offset reset).
// That is the event-sourcing trade: the log is the source of truth and
// this is a disposable read model.
package metrics

import (
	"sync"

	"github.com/aarondcarr/agent-event-bus/internal/event"
)

// AgentStats is the read model for one agent.
type AgentStats struct {
	Events        int64                `json:"events"`
	ByType        map[event.Type]int64 `json:"by_type"`
	ByName        map[string]int64     `json:"by_name"` // tool / model / guard name
	Tokens        int64                `json:"tokens"`
	GuardDenials  int64                `json:"guard_denials"`
	AvgDurationMS float64              `json:"avg_duration_ms"`
	MaxDurationMS int64                `json:"max_duration_ms"`

	durSum, durN int64
}

// Aggregator is safe for concurrent use.
type Aggregator struct {
	mu     sync.RWMutex
	agents map[string]*AgentStats
}

func New() *Aggregator { return &Aggregator{agents: map[string]*AgentStats{}} }

// Record folds one event in.
//
// TODO(aaron, TASKS_FOR_AARON.md #3): this is not idempotent. Kafka gives
// us at-least-once delivery, so a redelivered event is counted twice.
func (a *Aggregator) Record(e event.Event) {
	a.mu.Lock()
	defer a.mu.Unlock()
	s := a.agents[e.AgentID]
	if s == nil {
		s = &AgentStats{ByType: map[event.Type]int64{}, ByName: map[string]int64{}}
		a.agents[e.AgentID] = s
	}
	s.Events++
	s.ByType[e.Type]++
	if e.Name != "" {
		s.ByName[e.Name]++
	}
	s.Tokens += e.Tokens
	if e.Type == event.GuardDecision && e.Allowed != nil && !*e.Allowed {
		s.GuardDenials++
	}
	if e.DurationMS > 0 {
		s.durSum += e.DurationMS
		s.durN++
		s.AvgDurationMS = float64(s.durSum) / float64(s.durN)
		s.MaxDurationMS = max(s.MaxDurationMS, e.DurationMS)
	}
}

// Get returns a deep copy, so callers can't race with Record.
func (a *Aggregator) Get(agentID string) (AgentStats, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	s, ok := a.agents[agentID]
	if !ok {
		return AgentStats{}, false
	}
	c := *s
	c.ByType = make(map[event.Type]int64, len(s.ByType))
	for k, v := range s.ByType {
		c.ByType[k] = v
	}
	c.ByName = make(map[string]int64, len(s.ByName))
	for k, v := range s.ByName {
		c.ByName[k] = v
	}
	return c, true
}

// Agents lists known agent IDs.
func (a *Aggregator) Agents() []string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	ids := make([]string, 0, len(a.agents))
	for id := range a.agents {
		ids = append(ids, id)
	}
	return ids
}

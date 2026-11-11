// Package event defines the wire schema agents publish to the bus.
//
// The schema is deliberately small: a common envelope plus a few typed,
// optional fields. Everything is JSON so any agent runtime (Python, TS, Go)
// can publish without a codegen step. Schema evolution rule: add optional
// fields only; bump Version for breaking changes.
package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Version is the current envelope schema version.
const Version = 1

// Type is the kind of thing an agent did.
type Type string

const (
	ToolCall      Type = "tool_call"
	LLMCall       Type = "llm_call"
	GuardDecision Type = "guard_decision"
	Error         Type = "error"
)

func (t Type) valid() bool {
	switch t {
	case ToolCall, LLMCall, GuardDecision, Error:
		return true
	}
	return false
}

// Event is one structured fact emitted by an agent.
type Event struct {
	Version   int       `json:"version"`
	ID        string    `json:"id"`       // producer-assigned, unique; the idempotency key
	AgentID   string    `json:"agent_id"` // Kafka partition key: per-agent ordering
	RunID     string    `json:"run_id"`   // one agent task/session
	Type      Type      `json:"type"`
	Timestamp time.Time `json:"ts"`

	// Optional, type-dependent fields.
	Name       string `json:"name,omitempty"`        // tool name, model name, guard name
	DurationMS int64  `json:"duration_ms,omitempty"` // latency of the call
	Tokens     int64  `json:"tokens,omitempty"`      // llm_call: prompt+completion tokens
	Allowed    *bool  `json:"allowed,omitempty"`     // guard_decision: required
	Reason     string `json:"reason,omitempty"`      // guard_decision / error detail
}

// ErrInvalid wraps every validation failure so callers can map it to a 400.
var ErrInvalid = errors.New("invalid event")

// Validate checks the invariants consumers rely on.
func (e Event) Validate() error {
	bad := func(format string, a ...any) error {
		return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, a...))
	}
	switch {
	case e.Version != Version:
		return bad("version %d unsupported (want %d)", e.Version, Version)
	case e.ID == "":
		return bad("id is required")
	case e.AgentID == "":
		return bad("agent_id is required")
	case !e.Type.valid():
		return bad("unknown type %q", e.Type)
	case e.Timestamp.IsZero():
		return bad("ts is required")
	case e.DurationMS < 0 || e.Tokens < 0:
		return bad("duration_ms and tokens must be >= 0")
	case e.Type == GuardDecision && e.Allowed == nil:
		return bad("guard_decision requires allowed")
	}
	return nil
}

// Decode parses and validates one event.
func Decode(b []byte) (Event, error) {
	var e Event
	if err := json.Unmarshal(b, &e); err != nil {
		return Event{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	return e, e.Validate()
}

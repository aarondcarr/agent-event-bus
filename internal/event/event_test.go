package event

import (
	"errors"
	"testing"
	"time"
)

func valid() Event {
	return Event{Version: 1, ID: "e1", AgentID: "a1", RunID: "r1", Type: ToolCall, Timestamp: time.Unix(1, 0)}
}

func TestValidate(t *testing.T) {
	yes := true
	cases := []struct {
		name string
		mut  func(*Event)
		ok   bool
	}{
		{"valid tool call", func(*Event) {}, true},
		{"wrong version", func(e *Event) { e.Version = 2 }, false},
		{"missing id", func(e *Event) { e.ID = "" }, false},
		{"missing agent", func(e *Event) { e.AgentID = "" }, false},
		{"unknown type", func(e *Event) { e.Type = "nap" }, false},
		{"missing ts", func(e *Event) { e.Timestamp = time.Time{} }, false},
		{"negative tokens", func(e *Event) { e.Tokens = -1 }, false},
		{"guard without allowed", func(e *Event) { e.Type = GuardDecision }, false},
		{"guard with allowed", func(e *Event) { e.Type = GuardDecision; e.Allowed = &yes }, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := valid()
			c.mut(&e)
			err := e.Validate()
			if c.ok && err != nil {
				t.Fatalf("want ok, got %v", err)
			}
			if !c.ok && !errors.Is(err, ErrInvalid) {
				t.Fatalf("want ErrInvalid, got %v", err)
			}
		})
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	if _, err := Decode([]byte("{not json")); !errors.Is(err, ErrInvalid) {
		t.Fatalf("want ErrInvalid, got %v", err)
	}
}

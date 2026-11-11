package metrics

import (
	"sync"
	"testing"
	"time"

	"github.com/aarondcarr/agent-event-bus/internal/event"
)

func TestAggregates(t *testing.T) {
	a := New()
	no := false
	now := time.Now()
	a.Record(event.Event{AgentID: "a", Type: event.LLMCall, Name: "gpt", Tokens: 100, DurationMS: 200, Timestamp: now})
	a.Record(event.Event{AgentID: "a", Type: event.ToolCall, Name: "search", DurationMS: 100, Timestamp: now})
	a.Record(event.Event{AgentID: "a", Type: event.GuardDecision, Name: "pii", Allowed: &no, Timestamp: now})
	s, ok := a.Get("a")
	if !ok {
		t.Fatal("missing agent")
	}
	if s.Events != 3 || s.Tokens != 100 || s.GuardDenials != 1 || s.AvgDurationMS != 150 || s.MaxDurationMS != 200 {
		t.Fatalf("got %+v", s)
	}
	if s.ByType[event.ToolCall] != 1 || s.ByName["pii"] != 1 {
		t.Fatalf("got %+v", s)
	}
}

func TestGetReturnsACopy(t *testing.T) {
	a := New()
	a.Record(event.Event{AgentID: "a", Type: event.ToolCall})
	s, _ := a.Get("a")
	s.ByType[event.ToolCall] = 99
	if s2, _ := a.Get("a"); s2.ByType[event.ToolCall] != 1 {
		t.Fatal("Get leaked internal map")
	}
}

// Run with -race: concurrent writers and readers must not race.
func TestConcurrentUse(t *testing.T) {
	a := New()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				a.Record(event.Event{AgentID: "a", Type: event.LLMCall, Tokens: 1})
				a.Get("a")
			}
		}()
	}
	wg.Wait()
	if s, _ := a.Get("a"); s.Tokens != 8000 {
		t.Fatalf("lost updates: %d", s.Tokens)
	}
}

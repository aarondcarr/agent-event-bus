package ingest

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aarondcarr/agent-event-bus/internal/bus"
)

const good = `{"version":1,"id":"e1","agent_id":"a1","run_id":"r1","type":"tool_call","ts":"2026-01-01T00:00:00Z","name":"search"}`

func post(h http.Handler, body string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("POST", "/v1/events", strings.NewReader(body)))
	return w
}

func TestAcceptsValidEventKeyedByAgent(t *testing.T) {
	b := bus.NewBroker(4)
	if w := post(Handler(b, "agent.events"), good); w.Code != http.StatusAccepted {
		t.Fatalf("code %d: %s", w.Code, w.Body)
	}
	recs := b.Records("agent.events")
	if len(recs) != 1 || string(recs[0].Key) != "a1" {
		t.Fatalf("records = %+v", recs)
	}
}

func TestRejectsBadInput(t *testing.T) {
	b := bus.NewBroker(1)
	h := Handler(b, "agent.events")
	for name, body := range map[string]string{
		"not json":      "{",
		"unknown field": strings.Replace(good, `"name"`, `"nmae"`, 1),
		"invalid":       strings.Replace(good, `"tool_call"`, `"nap"`, 1),
	} {
		if w := post(h, body); w.Code != http.StatusBadRequest {
			t.Errorf("%s: code %d", name, w.Code)
		}
	}
	if n := len(b.Records("agent.events")); n != 0 {
		t.Fatalf("published %d bad events", n)
	}
}

type down struct{}

func (down) Produce(context.Context, bus.Record) error { return errors.New("no brokers") }
func (down) Close()                                    {}

func TestBrokerDownIs503(t *testing.T) {
	if w := post(Handler(down{}, "agent.events"), good); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("code %d", w.Code)
	}
}

package processor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/aarondcarr/agent-event-bus/internal/bus"
	"github.com/aarondcarr/agent-event-bus/internal/event"
	"github.com/aarondcarr/agent-event-bus/internal/metrics"
	"github.com/aarondcarr/agent-event-bus/internal/policy"
	"github.com/aarondcarr/agent-event-bus/internal/tracing"
)

const (
	events  = "agent.events"
	control = "agent.control"
)

var (
	t0    = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mu    sync.Mutex
	quiet = slog.New(slog.NewTextHandler(io.Discard, nil))
)

func publish(t *testing.T, ctx context.Context, p bus.Producer, e event.Event) {
	t.Helper()
	b, _ := json.Marshal(e)
	if err := p.Produce(ctx, bus.Record{Topic: events, Key: []byte(e.AgentID), Value: b}); err != nil {
		t.Fatal(err)
	}
}

func mk(id, agent string, typ event.Type, at time.Duration) event.Event {
	e := event.Event{Version: 1, ID: id, AgentID: agent, RunID: "r", Type: typ, Timestamp: t0.Add(at)}
	if typ == event.GuardDecision {
		no := false
		e.Allowed = &no
	}
	return e
}

// runUntil runs p in the background until cond holds, then stops it.
func runUntil(t *testing.T, p *Processor, cond func() bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- p.Run(ctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("condition not reached")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-errc; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// The whole pipeline, in memory: traced publish -> broker -> processor ->
// kill switch -> traced control publish -> read API.
func TestPipelineKillsRogueAgentInOneTrace(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr)))
	otel.SetTextMapPropagator(propagation.TraceContext{})

	b := bus.NewBroker(4)
	pub := tracing.TracedProducer{Producer: b}
	ev := policy.New(policy.Config{Window: time.Minute, MaxGuardDenials: 2})
	agg := metrics.New()
	p := &Processor{
		Consumer: b.Consumer("processor", events),
		Handlers: []Handler{PolicyHandler(ev, pub, control), MetricsHandler(agg)},
		Log:      quiet,
	}

	ctx := context.Background()
	publish(t, ctx, pub, mk("g1", "good", event.ToolCall, 0))
	publish(t, ctx, pub, mk("r1", "rogue", event.GuardDecision, 0))
	rootCtx, root := otel.Tracer("test").Start(ctx, "agent.run")
	publish(t, rootCtx, pub, mk("r2", "rogue", event.GuardDecision, time.Second)) // the one that trips it
	root.End()

	runUntil(t, p, func() bool { return len(b.Records(control)) == 1 })

	var c Control
	json.Unmarshal(b.Records(control)[0].Value, &c)
	if c.AgentID != "rogue" || c.Reason != "guard_denials" || c.TriggerEventID != "r2" {
		t.Fatalf("control = %+v", c)
	}
	if killed, _ := ev.Status("good"); killed {
		t.Fatal("good agent was killed")
	}

	// The control publish must be in the same trace as the triggering event.
	var controlSpan sdktrace.ReadOnlySpan
	for _, s := range sr.Ended() {
		if s.Name() == "publish "+control {
			controlSpan = s
		}
	}
	if controlSpan == nil || controlSpan.SpanContext().TraceID() != root.SpanContext().TraceID() {
		t.Fatal("control publish is not in the agent's trace")
	}

	// Read API.
	srv := httptest.NewServer(API(ev, agg))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/agents/rogue")
	if err != nil {
		t.Fatal(err)
	}
	var v AgentView
	json.NewDecoder(resp.Body).Decode(&v)
	if !v.Killed || v.Stats.GuardDenials != 2 {
		t.Fatalf("view = %+v", v)
	}
	if resp, _ := http.Get(srv.URL + "/v1/agents/nobody"); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown agent: %d", resp.StatusCode)
	}
}

func TestCommitsOnlyAfterSuccessThenRedelivers(t *testing.T) {
	b := bus.NewBroker(1)
	ctx := context.Background()
	for i, id := range []string{"e0", "e1", "e2"} {
		publish(t, ctx, b, mk(id, "a", event.ToolCall, time.Duration(i)))
	}
	var seen []string
	failOn := "e1"
	h := Handler{Name: "flaky", Fn: func(_ context.Context, e event.Event) error {
		seen = append(seen, e.ID)
		if e.ID == failOn {
			return errors.New("downstream unavailable")
		}
		return nil
	}}

	p := &Processor{Consumer: b.Consumer("g", events), Handlers: []Handler{h}, Log: quiet}
	err := p.Run(ctx)
	if err == nil {
		t.Fatal("want handler error to stop the processor")
	}
	tp := bus.TopicPartition{Topic: events, Partition: 0}
	if got := b.Committed("g", tp); got != 1 {
		t.Fatalf("committed offset = %d, want 1 (only e0 done)", got)
	}

	// "Restart": e1 is redelivered; e2 was never processed.
	failOn = ""
	seen = nil
	p.Consumer = b.Consumer("g", events)
	runUntil(t, p, func() bool { return b.Committed("g", tp) == 3 })
	if len(seen) != 2 || seen[0] != "e1" || seen[1] != "e2" {
		t.Fatalf("after restart saw %v, want [e1 e2]", seen)
	}
}

func TestPoisonRecordIsSkippedNotFatal(t *testing.T) {
	b := bus.NewBroker(1)
	ctx := context.Background()
	b.Produce(ctx, bus.Record{Topic: events, Key: []byte("a"), Value: []byte("not json")})
	publish(t, ctx, b, mk("e1", "a", event.ToolCall, 0))
	agg := metrics.New()
	p := &Processor{Consumer: b.Consumer("g", events), Handlers: []Handler{MetricsHandler(agg)}, Log: quiet}
	runUntil(t, p, func() bool { return b.Committed("g", bus.TopicPartition{Topic: events}) == 2 })
	if s, _ := agg.Get("a"); s.Events != 1 {
		t.Fatalf("events = %d", s.Events)
	}
}

func TestPartitionsProcessInParallelButInOrder(t *testing.T) {
	b := bus.NewBroker(8)
	ctx := context.Background()
	agents := []string{"a", "b", "c", "d", "e", "f"}
	const per = 50
	for i := 0; i < per; i++ {
		for _, a := range agents {
			publish(t, ctx, b, mk(fmt.Sprintf("%s-%d", a, i), a, event.ToolCall, time.Duration(i)))
		}
	}
	last := map[string]time.Time{}
	var outOfOrder bool
	ordered := Handler{Name: "order", Fn: func(_ context.Context, e event.Event) error {
		// Partitions run concurrently, so the shared map needs a lock.
		mu.Lock()
		defer mu.Unlock()
		if e.Timestamp.Before(last[e.AgentID]) {
			outOfOrder = true
		}
		last[e.AgentID] = e.Timestamp
		return nil
	}}
	agg := metrics.New()
	p := &Processor{Consumer: b.Consumer("g", events), Handlers: []Handler{ordered, MetricsHandler(agg)}, Log: quiet}
	runUntil(t, p, func() bool {
		for _, a := range agents {
			if s, _ := agg.Get(a); s.Events != per {
				return false
			}
		}
		return true
	})
	if outOfOrder {
		t.Fatal("per-agent order violated")
	}
}

// Acceptance test for TASKS_FOR_AARON.md #3. Remove the Skip when done.
func TestRedeliveryIsIdempotent(t *testing.T) {
	t.Skip("TODO(aaron, TASKS_FOR_AARON.md #3): make processing idempotent by event ID")
	b := bus.NewBroker(1)
	ctx := context.Background()
	publish(t, ctx, b, mk("e1", "a", event.LLMCall, 0))
	agg := metrics.New()
	h := []Handler{MetricsHandler(agg)}
	p := &Processor{Consumer: b.Consumer("g", events), Handlers: h, Log: quiet}
	runUntil(t, p, func() bool { s, _ := agg.Get("a"); return s.Events == 1 })
	// Same event delivered again (e.g. a producer retry before idempotence,
	// or a crash after handling but before commit).
	publish(t, ctx, b, mk("e1", "a", event.LLMCall, 0))
	publish(t, ctx, b, mk("e2", "a", event.LLMCall, 0))
	runUntil(t, p, func() bool { return b.Committed("g", bus.TopicPartition{Topic: events}) == 3 })
	if s, _ := agg.Get("a"); s.Events != 2 {
		t.Fatalf("events = %d, want 2 (e1 counted once)", s.Events)
	}
}

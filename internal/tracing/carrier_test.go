package tracing

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/aarondcarr/agent-event-bus/internal/bus"
)

func recorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return sr
}

func TestCarrierSetReplacesExistingKey(t *testing.T) {
	h := []bus.Header{{Key: "traceparent", Value: []byte("old")}}
	HeaderCarrier{&h}.Set("traceparent", "new")
	if len(h) != 1 || string(h[0].Value) != "new" {
		t.Fatalf("got %+v", h)
	}
}

// The core claim of the project: one trace spans producer -> broker -> consumer.
func TestTraceSurvivesTheBroker(t *testing.T) {
	sr := recorder(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b := bus.NewBroker(3)

	ctx, root := otel.Tracer("test").Start(ctx, "agent.run")
	if err := (TracedProducer{b}).Produce(ctx, bus.Record{Topic: "agent.events", Key: []byte("a1")}); err != nil {
		t.Fatal(err)
	}
	root.End()

	recs, err := b.Consumer("g", "agent.events").Poll(context.Background()) // fresh ctx: no in-process parent
	if err != nil {
		t.Fatal(err)
	}
	_, span := StartConsume(context.Background(), recs[0])
	span.End()

	spans := sr.Ended()
	if len(spans) != 3 {
		t.Fatalf("want 3 spans, got %d", len(spans))
	}
	byName := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range spans {
		byName[s.Name()] = s
		if s.SpanContext().TraceID() != root.SpanContext().TraceID() {
			t.Fatalf("span %q has a different trace ID", s.Name())
		}
	}
	pub, proc := byName["publish agent.events"], byName["process agent.events"]
	if pub.SpanKind() != trace.SpanKindProducer || proc.SpanKind() != trace.SpanKindConsumer {
		t.Fatalf("wrong span kinds")
	}
	if proc.Parent().SpanID() != pub.SpanContext().SpanID() {
		t.Fatalf("consumer span is not a child of the producer span")
	}
}

func TestProduceDoesNotMutateCallerHeaders(t *testing.T) {
	recorder(t)
	b := bus.NewBroker(1)
	hdrs := make([]bus.Header, 0, 4) // spare capacity would expose aliasing bugs
	r := bus.Record{Topic: "t", Key: []byte("k"), Headers: hdrs}
	ctx, span := otel.Tracer("test").Start(context.Background(), "x")
	defer span.End()
	(TracedProducer{b}).Produce(ctx, r)
	if len(r.Headers) != 0 || len(hdrs[:1][0].Key) != 0 {
		t.Fatalf("caller headers were mutated")
	}
}

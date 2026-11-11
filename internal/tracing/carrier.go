// Package tracing carries OpenTelemetry context across Kafka.
//
// HTTP gets context propagation for free from instrumentation libraries;
// Kafka does not. A producer must write the W3C traceparent (and
// tracestate/baggage) into record headers, and the consumer must read them
// back and parent its span on them. That is all this file does.
package tracing

import (
	"context"
	"strconv"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/aarondcarr/agent-event-bus/internal/bus"
)

// HeaderCarrier adapts Kafka record headers to propagation.TextMapCarrier.
type HeaderCarrier struct{ Headers *[]bus.Header }

var _ propagation.TextMapCarrier = HeaderCarrier{}

func (c HeaderCarrier) Get(key string) string {
	for _, h := range *c.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// Set replaces an existing header so re-publishing a record (e.g. to a DLQ)
// does not end up with two traceparents.
func (c HeaderCarrier) Set(key, val string) {
	for i, h := range *c.Headers {
		if h.Key == key {
			(*c.Headers)[i].Value = []byte(val)
			return
		}
	}
	*c.Headers = append(*c.Headers, bus.Header{Key: key, Value: []byte(val)})
}

func (c HeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(*c.Headers))
	for _, h := range *c.Headers {
		keys = append(keys, h.Key)
	}
	return keys
}

const tracerName = "github.com/aarondcarr/agent-event-bus"

func attrs(r bus.Record) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("messaging.system", "kafka"),
		attribute.String("messaging.destination.name", r.Topic),
		attribute.String("messaging.kafka.message.key", string(r.Key)),
	}
}

// TracedProducer wraps a Producer: every Produce gets a PRODUCER span and
// that span's context is injected into the record headers.
type TracedProducer struct{ bus.Producer }

func (p TracedProducer) Produce(ctx context.Context, r bus.Record) error {
	ctx, span := otel.Tracer(tracerName).Start(ctx, "publish "+r.Topic,
		trace.WithSpanKind(trace.SpanKindProducer), trace.WithAttributes(attrs(r)...))
	defer span.End()
	r.Headers = append([]bus.Header(nil), r.Headers...) // don't mutate the caller's slice
	otel.GetTextMapPropagator().Inject(ctx, HeaderCarrier{&r.Headers})
	if err := p.Producer.Produce(ctx, r); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "produce failed")
		return err
	}
	return nil
}

// StartConsume extracts the producer's context from the record headers and
// starts a CONSUMER span as its child, so producer and consumer share one
// trace ID. Caller must End the span.
func StartConsume(ctx context.Context, r bus.Record) (context.Context, trace.Span) {
	ctx = otel.GetTextMapPropagator().Extract(ctx, HeaderCarrier{&r.Headers})
	a := append(attrs(r),
		attribute.String("messaging.destination.partition.id", strconv.Itoa(int(r.Partition))),
		attribute.Int64("messaging.kafka.offset", r.Offset),
	)
	return otel.Tracer(tracerName).Start(ctx, "process "+r.Topic,
		trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(a...))
}

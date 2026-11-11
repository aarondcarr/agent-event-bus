// Package ingest is the HTTP front door agents publish through.
//
// Agents could produce to Kafka directly; going through a small HTTP
// service instead keeps Kafka credentials and client config out of every
// agent runtime, gives one place to validate the schema, and lets any
// language publish with a POST.
package ingest

import (
	"encoding/json"
	"errors"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/aarondcarr/agent-event-bus/internal/bus"
	"github.com/aarondcarr/agent-event-bus/internal/event"
)

const maxBody = 1 << 20

// Handler serves POST /v1/events. out should be a tracing.TracedProducer.
func Handler(out bus.Producer, topic string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("POST /v1/events", func(w http.ResponseWriter, r *http.Request) {
		// Continue the caller's trace if it sent a traceparent header.
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := otel.Tracer("ingest").Start(ctx, "POST /v1/events", trace.WithSpanKind(trace.SpanKindServer))
		defer span.End()

		var e event.Event
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody))
		dec.DisallowUnknownFields()
		err := dec.Decode(&e)
		if err == nil {
			err = e.Validate()
		}
		if err != nil {
			span.SetStatus(codes.Error, "invalid event")
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		span.SetAttributes(attribute.String("agent.id", e.AgentID), attribute.String("agent.event.type", string(e.Type)))

		b, _ := json.Marshal(e)
		if err := out.Produce(ctx, bus.Record{Topic: topic, Key: []byte(e.AgentID), Value: b}); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "produce failed")
			writeErr(w, http.StatusServiceUnavailable, errors.New("event bus unavailable"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{"id": e.ID})
	})
	return mux
}

func writeErr(w http.ResponseWriter, code int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

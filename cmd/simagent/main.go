// Command simagent pretends to be two AI agents so you can watch the system
// work: "agent-good" behaves, "agent-rogue" keeps hitting a guardrail until
// the kill switch trips. Each agent run is one trace, started here.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/aarondcarr/agent-event-bus/internal/event"
	"github.com/aarondcarr/agent-event-bus/internal/tracing"
)

func main() {
	url := flag.String("ingest", "http://localhost:8080", "ingest base URL")
	flag.Parse()
	ctx := context.Background()
	shutdown, err := tracing.Setup(ctx, "simagent")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer shutdown(ctx)

	yes, no := true, false
	runs := map[string][]event.Event{
		"agent-good": {
			{Type: event.LLMCall, Name: "planner-model", Tokens: 1200, DurationMS: 850},
			{Type: event.GuardDecision, Name: "pii-filter", Allowed: &yes},
			{Type: event.ToolCall, Name: "web_search", DurationMS: 420},
			{Type: event.LLMCall, Name: "planner-model", Tokens: 900, DurationMS: 610},
		},
		"agent-rogue": {
			{Type: event.LLMCall, Name: "planner-model", Tokens: 1500, DurationMS: 900},
			{Type: event.GuardDecision, Name: "shell-exec-guard", Allowed: &no, Reason: "rm -rf requested"},
			{Type: event.ToolCall, Name: "file_read", DurationMS: 30},
			{Type: event.GuardDecision, Name: "shell-exec-guard", Allowed: &no, Reason: "curl | sh requested"},
			{Type: event.GuardDecision, Name: "egress-guard", Allowed: &no, Reason: "unknown host"},
		},
	}
	for agent, evs := range runs {
		runID := fmt.Sprintf("run-%d", time.Now().UnixNano())
		rctx, span := otel.Tracer("simagent").Start(ctx, "agent.run", trace.WithAttributes(
			attribute.String("agent.id", agent), attribute.String("agent.run_id", runID)))
		for i, e := range evs {
			e.Version, e.AgentID, e.RunID = event.Version, agent, runID
			e.ID = fmt.Sprintf("%s-%d", runID, i)
			e.Timestamp = time.Now().UTC()
			if err := send(rctx, *url, e); err != nil {
				fmt.Fprintln(os.Stderr, agent, err)
				os.Exit(1)
			}
		}
		span.End()
		fmt.Printf("%s run=%s trace_id=%s events=%d\n", agent, runID, span.SpanContext().TraceID(), len(evs))
	}
}

func send(ctx context.Context, base string, e event.Event) error {
	b, _ := json.Marshal(e)
	req, _ := http.NewRequestWithContext(ctx, "POST", base+"/v1/events", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("ingest returned %s", resp.Status)
	}
	return nil
}

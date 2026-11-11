package processor

import (
	"context"
	"encoding/json"
	"time"

	"github.com/aarondcarr/agent-event-bus/internal/bus"
	"github.com/aarondcarr/agent-event-bus/internal/event"
	"github.com/aarondcarr/agent-event-bus/internal/metrics"
	"github.com/aarondcarr/agent-event-bus/internal/policy"
)

// Control is published to the control topic when an agent is killed.
// Agent runtimes subscribe to it (keyed by agent_id) and stop the agent.
type Control struct {
	AgentID        string    `json:"agent_id"`
	Action         string    `json:"action"` // "kill"
	Reason         string    `json:"reason"`
	TriggerEventID string    `json:"trigger_event_id"`
	TS             time.Time `json:"ts"`
}

// PolicyHandler evaluates the kill switch and announces new kills.
// out should be a tracing.TracedProducer so the control record joins the
// same trace as the event that caused it.
func PolicyHandler(ev *policy.Evaluator, out bus.Producer, topic string) Handler {
	return Handler{Name: "policy", Fn: func(ctx context.Context, e event.Event) error {
		d := ev.Observe(e)
		if !d.NewlyKilled {
			return nil
		}
		b, _ := json.Marshal(Control{AgentID: e.AgentID, Action: "kill", Reason: d.Reason, TriggerEventID: e.ID, TS: e.Timestamp})
		return out.Produce(ctx, bus.Record{Topic: topic, Key: []byte(e.AgentID), Value: b})
	}}
}

// MetricsHandler feeds the aggregator.
func MetricsHandler(agg *metrics.Aggregator) Handler {
	return Handler{Name: "metrics", Fn: func(_ context.Context, e event.Event) error {
		agg.Record(e)
		return nil
	}}
}

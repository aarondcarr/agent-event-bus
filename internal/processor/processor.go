// Package processor is the consume -> handle -> commit loop.
//
// Delivery semantics: at-least-once. A record's offset is committed only
// after every handler has succeeded for it (and for every earlier record in
// its partition). If the process dies between handling and committing, the
// record is delivered again on restart, so handlers must tolerate replays.
//
// Concurrency: records are grouped by partition. Partitions are processed
// in parallel (one goroutine each); records within a partition are
// processed strictly in offset order, preserving per-agent ordering.
package processor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/aarondcarr/agent-event-bus/internal/bus"
	"github.com/aarondcarr/agent-event-bus/internal/event"
	"github.com/aarondcarr/agent-event-bus/internal/tracing"
)

// Handler reacts to one event. Returning an error stops the partition.
type Handler struct {
	Name string
	Fn   func(context.Context, event.Event) error
}

type Processor struct {
	Consumer bus.Consumer
	Handlers []Handler
	Log      *slog.Logger

	// TODO(aaron, TASKS_FOR_AARON.md #1): dead-letter queue. Today a poison
	// record (undecodable/invalid) is logged and skipped, and a handler error
	// stops the processor so the record is redelivered after restart, which
	// blocks the partition forever if the error is permanent. Retry with
	// backoff, then publish to DLQTopic via DLQ and move on.
	DLQ      bus.Producer
	DLQTopic string
}

// Run polls until ctx is cancelled (returns nil) or a handler fails
// (returns the error after committing everything that did succeed).
func (p *Processor) Run(ctx context.Context) error {
	for {
		recs, err := p.Consumer.Poll(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			p.Log.Warn("poll error", "err", err)
		}
		if len(recs) == 0 {
			if err != nil { // don't spin on a persistent fetch error
				select {
				case <-ctx.Done():
				case <-time.After(500 * time.Millisecond):
				}
			}
			continue
		}
		done, herr := p.processBatch(ctx, recs)
		if len(done) > 0 {
			if err := p.Consumer.Commit(ctx, done...); err != nil {
				return fmt.Errorf("commit: %w", err)
			}
		}
		if herr != nil {
			return herr
		}
	}
}

// processBatch returns, per partition, the last record that was fully
// handled (committing it commits everything before it) and any error.
func (p *Processor) processBatch(ctx context.Context, recs []bus.Record) ([]bus.Record, error) {
	byTP := map[bus.TopicPartition][]bus.Record{}
	var order []bus.TopicPartition
	for _, r := range recs {
		if _, ok := byTP[r.TP()]; !ok {
			order = append(order, r.TP())
		}
		byTP[r.TP()] = append(byTP[r.TP()], r)
	}

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		done []bus.Record
		errs []error
	)
	for _, tp := range order {
		wg.Add(1)
		go func(part []bus.Record) {
			defer wg.Done()
			var last *bus.Record
			var err error
			for i := range part {
				if err = p.handle(ctx, part[i]); err != nil {
					break
				}
				last = &part[i]
			}
			mu.Lock()
			defer mu.Unlock()
			if last != nil {
				done = append(done, *last)
			}
			if err != nil {
				errs = append(errs, err)
			}
		}(byTP[tp])
	}
	wg.Wait()
	return done, errors.Join(errs...)
}

func (p *Processor) handle(ctx context.Context, r bus.Record) error {
	ctx, span := tracing.StartConsume(ctx, r)
	defer span.End()

	e, err := event.Decode(r.Value)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "poison record")
		p.Log.Warn("skipping invalid record", "topic", r.Topic, "partition", r.Partition, "offset", r.Offset, "err", err)
		return nil // TODO(aaron, #1): send to DLQ instead of dropping
	}
	span.SetAttributes(attribute.String("agent.id", e.AgentID), attribute.String("agent.event.type", string(e.Type)))

	for _, h := range p.Handlers {
		hctx, hspan := otel.Tracer("processor").Start(ctx, "handle "+h.Name)
		err := h.Fn(hctx, e)
		if err != nil {
			hspan.RecordError(err)
			hspan.SetStatus(codes.Error, err.Error())
		}
		hspan.End()
		if err != nil {
			span.SetStatus(codes.Error, "handler failed")
			return fmt.Errorf("%s: event %s (%s/%d@%d): %w", h.Name, e.ID, r.Topic, r.Partition, r.Offset, err)
		}
	}
	return nil
}

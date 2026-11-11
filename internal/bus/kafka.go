package bus

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaProducer is a Producer backed by franz-go.
//
// franz-go enables the idempotent producer by default (acks=all plus
// producer IDs and sequence numbers), so broker-side retries cannot
// create duplicates or reorder records within a partition.
type KafkaProducer struct{ cl *kgo.Client }

func NewKafkaProducer(brokers []string) (*KafkaProducer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, err
	}
	return &KafkaProducer{cl: cl}, nil
}

func (p *KafkaProducer) Produce(ctx context.Context, r Record) error {
	kr := &kgo.Record{Topic: r.Topic, Key: r.Key, Value: r.Value}
	for _, h := range r.Headers {
		kr.Headers = append(kr.Headers, kgo.RecordHeader{Key: h.Key, Value: h.Value})
	}
	return p.cl.ProduceSync(ctx, kr).FirstErr()
}

func (p *KafkaProducer) Close() { p.cl.Close() }

// KafkaConsumer is a consumer-group member with manual commits.
type KafkaConsumer struct{ cl *kgo.Client }

func NewKafkaConsumer(brokers []string, group string, topics ...string) (*KafkaConsumer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		// We commit only after handlers succeed: at-least-once.
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.AllowAutoTopicCreation(),
		// TODO(aaron, TASKS_FOR_AARON.md #2): handle rebalances. Today a
		// revoke drops in-flight work on the floor and the next owner
		// re-processes from the last commit (safe but noisy). Add
		// kgo.OnPartitionsRevoked / kgo.OnPartitionsLost hooks and
		// kgo.BlockRebalanceOnPoll so in-flight batches finish and commit
		// before ownership moves.
	)
	if err != nil {
		return nil, err
	}
	return &KafkaConsumer{cl: cl}, nil
}

func (c *KafkaConsumer) Poll(ctx context.Context) ([]Record, error) {
	fs := c.cl.PollFetches(ctx)
	if fs.IsClientClosed() {
		return nil, errors.New("kafka client closed")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var errs []error
	for _, fe := range fs.Errors() {
		errs = append(errs, fmt.Errorf("fetch %s/%d: %w", fe.Topic, fe.Partition, fe.Err))
	}
	var out []Record
	fs.EachRecord(func(kr *kgo.Record) {
		r := Record{
			Topic: kr.Topic, Partition: kr.Partition, Offset: kr.Offset,
			Key: kr.Key, Value: kr.Value, Timestamp: kr.Timestamp, raw: kr,
		}
		for _, h := range kr.Headers {
			r.Headers = append(r.Headers, Header{Key: h.Key, Value: h.Value})
		}
		out = append(out, r)
	})
	// Fetch errors are usually transient (leader moves, topic not yet
	// created); return what we got and let the caller log the error.
	return out, errors.Join(errs...)
}

func (c *KafkaConsumer) Commit(ctx context.Context, recs ...Record) error {
	krs := make([]*kgo.Record, 0, len(recs))
	for _, r := range recs {
		kr, ok := r.raw.(*kgo.Record)
		if !ok {
			return fmt.Errorf("record %s/%d@%d did not come from this consumer", r.Topic, r.Partition, r.Offset)
		}
		krs = append(krs, kr)
	}
	return c.cl.CommitRecords(ctx, krs...)
}

func (c *KafkaConsumer) Close() { c.cl.Close() }

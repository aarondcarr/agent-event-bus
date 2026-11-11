package bus

import (
	"context"
	"sync"
	"time"
)

// Broker is an in-memory, single-process stand-in for a Kafka cluster.
// It is for tests and local experiments only.
type Broker struct {
	mu         sync.Mutex
	partitions int32
	logs       map[TopicPartition][]Record
	committed  map[string]map[TopicPartition]int64 // group -> tp -> next offset
	notify     chan struct{}                       // closed and replaced on every produce
}

// NewBroker creates a broker where every topic has n partitions.
func NewBroker(partitions int32) *Broker {
	return &Broker{
		partitions: partitions,
		logs:       map[TopicPartition][]Record{},
		committed:  map[string]map[TopicPartition]int64{},
		notify:     make(chan struct{}),
	}
}

// Produce appends to the partition chosen by the record key.
func (b *Broker) Produce(_ context.Context, r Record) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	r.Partition = partitionFor(r.Key, b.partitions)
	tp := r.TP()
	r.Offset = int64(len(b.logs[tp]))
	if r.Timestamp.IsZero() {
		r.Timestamp = time.Now()
	}
	r.Headers = append([]Header(nil), r.Headers...) // broker owns its copy
	b.logs[tp] = append(b.logs[tp], r)
	close(b.notify)
	b.notify = make(chan struct{})
	return nil
}

// Close is a no-op; it satisfies Producer.
func (b *Broker) Close() {}

// Records returns a copy of everything in a topic, for assertions.
func (b *Broker) Records(topic string) []Record {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []Record
	for p := int32(0); p < b.partitions; p++ {
		out = append(out, b.logs[TopicPartition{topic, p}]...)
	}
	return out
}

// Committed returns the group's next offset for a partition.
func (b *Broker) Committed(group string, tp TopicPartition) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.committed[group][tp]
}

// Consumer joins a group. A new Consumer for the same group resumes from
// the group's committed offsets, which is how tests simulate a crash.
func (b *Broker) Consumer(group string, topics ...string) Consumer {
	return &memConsumer{b: b, group: group, topics: topics, pos: map[TopicPartition]int64{}}
}

type memConsumer struct {
	b      *Broker
	group  string
	topics []string
	pos    map[TopicPartition]int64 // next offset to hand out, this session
}

func (c *memConsumer) Poll(ctx context.Context) ([]Record, error) {
	for {
		c.b.mu.Lock()
		var out []Record
		for _, t := range c.topics {
			for p := int32(0); p < c.b.partitions; p++ {
				tp := TopicPartition{t, p}
				start, ok := c.pos[tp]
				if !ok {
					start = c.b.committed[c.group][tp]
				}
				log := c.b.logs[tp]
				if start < int64(len(log)) {
					out = append(out, log[start:]...)
					c.pos[tp] = int64(len(log))
				}
			}
		}
		wait := c.b.notify
		c.b.mu.Unlock()
		if len(out) > 0 {
			return out, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-wait:
		}
	}
}

func (c *memConsumer) Commit(_ context.Context, recs ...Record) error {
	c.b.mu.Lock()
	defer c.b.mu.Unlock()
	g := c.b.committed[c.group]
	if g == nil {
		g = map[TopicPartition]int64{}
		c.b.committed[c.group] = g
	}
	for _, r := range recs {
		if next := r.Offset + 1; next > g[r.TP()] {
			g[r.TP()] = next
		}
	}
	return nil
}

func (c *memConsumer) Close() {}

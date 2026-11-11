// Package bus is the thin seam between business logic and Kafka.
//
// Everything above this package talks to Producer and Consumer. Production
// wires in the franz-go implementation (kafka.go); tests wire in the
// in-memory Broker (memory.go), which models the parts of Kafka semantics
// the processor depends on: per-key partitioning, per-partition ordering,
// and consumer-group committed offsets that survive a consumer restart.
package bus

import (
	"context"
	"hash/fnv"
	"time"
)

// Header is a Kafka record header. Trace context rides here.
type Header struct {
	Key   string
	Value []byte
}

// Record is a Kafka record, stripped to what we use.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   []Header
	Timestamp time.Time

	raw any // backend-specific handle (e.g. *kgo.Record) used for commits
}

// Producer writes records. Produce returns once the broker has acked.
type Producer interface {
	Produce(ctx context.Context, r Record) error
	Close()
}

// Consumer reads records as a member of a consumer group.
//
// Contract: records from one partition are returned in offset order.
// Commit marks records as processed; after a restart the group resumes
// after the highest committed offset of each partition. Anything polled
// but not committed is delivered again (at-least-once).
type Consumer interface {
	Poll(ctx context.Context) ([]Record, error)
	Commit(ctx context.Context, recs ...Record) error
	Close()
}

// TopicPartition identifies one partition.
type TopicPartition struct {
	Topic     string
	Partition int32
}

// TP returns the record's topic-partition.
func (r Record) TP() TopicPartition { return TopicPartition{r.Topic, r.Partition} }

// partitionFor mirrors the idea (not the exact hash) of Kafka's default
// partitioner: same key -> same partition -> ordered per key.
func partitionFor(key []byte, n int32) int32 {
	h := fnv.New32a()
	h.Write(key)
	return int32(h.Sum32() % uint32(n))
}

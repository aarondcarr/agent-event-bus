package bus

import (
	"context"
	"testing"
	"time"
)

func TestSameKeySamePartitionInOrder(t *testing.T) {
	b := NewBroker(4)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		b.Produce(ctx, Record{Topic: "t", Key: []byte("agent-1"), Value: []byte{byte(i)}})
	}
	recs := b.Records("t")
	for i, r := range recs {
		if r.Partition != recs[0].Partition {
			t.Fatalf("key split across partitions")
		}
		if r.Offset != int64(i) || r.Value[0] != byte(i) {
			t.Fatalf("out of order at %d: %+v", i, r)
		}
	}
}

func TestUncommittedRecordsAreRedeliveredAfterRestart(t *testing.T) {
	b := NewBroker(1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for i := 0; i < 3; i++ {
		b.Produce(ctx, Record{Topic: "t", Key: []byte("k")})
	}
	c1 := b.Consumer("g", "t")
	recs, _ := c1.Poll(ctx)
	if len(recs) != 3 {
		t.Fatalf("got %d records", len(recs))
	}
	c1.Commit(ctx, recs[0]) // processed only the first, then "crashed"

	c2 := b.Consumer("g", "t")
	recs, _ = c2.Poll(ctx)
	if len(recs) != 2 || recs[0].Offset != 1 {
		t.Fatalf("want offsets 1..2 redelivered, got %+v", recs)
	}
}

func TestPollBlocksUntilProduceOrCancel(t *testing.T) {
	b := NewBroker(1)
	c := b.Consumer("g", "t")
	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan int, 1)
	go func() {
		recs, _ := c.Poll(ctx)
		got <- len(recs)
	}()
	time.Sleep(10 * time.Millisecond)
	b.Produce(ctx, Record{Topic: "t", Key: []byte("k")})
	if n := <-got; n != 1 {
		t.Fatalf("want 1 record, got %d", n)
	}
	cancel()
	if _, err := c.Poll(ctx); err == nil {
		t.Fatal("want ctx error after cancel")
	}
}

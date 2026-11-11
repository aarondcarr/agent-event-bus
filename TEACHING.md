# Teaching notes

What you need to explain this project in an interview, with pointers to the code that shows it. Read the code next to each section; every claim here maps to a file.

## 1. Kafka: partitions, offsets, consumer groups

- **Topic**: a named, append-only log. Here: `agent.events`, `agent.control`, `agent.events.dlq`.
- **Partition**: a topic is split into N partitions (we use 6). Each partition is an ordered log on one leader broker. Ordering is guaranteed within a partition, never across partitions.
- **Key**: the producer hashes the record key to pick a partition. We key by `agent_id`, so all of one agent's events land in one partition, in order. That is why the kill switch can use a simple sliding window. See `ingest.go` (`Key: []byte(e.AgentID)`).
- **Offset**: a record's position in its partition (0, 1, 2...). Consumers track progress as "next offset to read" per partition.
- **Consumer group**: consumers sharing a `group.id` split the partitions between them; each partition goes to exactly one member. Max useful parallelism = partition count. A second group (say, an audit job) reads the same data independently with its own offsets.
- **Commit**: storing the group's offset for a partition in Kafka (`__consumer_offsets`). On restart or rebalance, consumption resumes from the committed offset. Committing offset N means "everything before N is done", so committing the last processed record per partition is enough. See `processBatch` in `internal/processor/processor.go`.
- **Rebalance**: when members join or leave, partitions are reassigned. Anything processed but not committed is redelivered to the new owner. Task 2.
- **Retention**: Kafka keeps records by time or size, not until read. That is what makes replay and multiple independent readers possible.
- **KRaft**: Kafka's own Raft quorum for cluster metadata, replacing ZooKeeper (removed in 4.0). Our compose runs one node as both broker and controller.

## 2. Delivery semantics

- **At-most-once**: commit before processing. A crash loses the record. Never used here.
- **At-least-once**: process, then commit. A crash between the two redelivers the record. This is what the processor does (`Run`: `processBatch` then `Commit`). Tested in `TestCommitsOnlyAfterSuccessThenRedelivers`.
- **Exactly-once (Kafka EOS)**: the idempotent producer plus transactions. A consume-process-produce loop can commit its input offsets and its output records atomically. It only covers state that lives in Kafka. Our in-memory aggregates and any external call (HTTP, database, an email) are outside the transaction.
- **Effectively-once**: at-least-once delivery plus idempotent handling (dedupe by a unique event ID). This is the practical answer for side effects outside Kafka. Task 3.
- **Idempotent producer**: the broker drops duplicate retries from the same producer session using producer ID + sequence number, per partition. On by default in franz-go. It prevents duplicates from producer retries, not duplicates from an agent re-POSTing.

## 3. OpenTelemetry context propagation through Kafka headers

- A **trace** is a tree of **spans** sharing a trace ID. A span's parent is set from the **context** active when it starts.
- Inside one process, context flows through `context.Context`. Across processes it must be serialized. The W3C format is one header, `traceparent: 00-<trace-id>-<parent-span-id>-<flags>`.
- HTTP: the simagent injects `traceparent` into request headers; ingest extracts it (`ingest.go`). Libraries usually do this for you.
- Kafka: nothing does it for you unless you add it. `TracedProducer.Produce` starts a PRODUCER span and calls `propagator.Inject` with a `HeaderCarrier` that writes into the record headers. `StartConsume` calls `Extract` on the consumed record's headers and starts a CONSUMER span with that as parent. See `internal/tracing/carrier.go`.
- The **carrier** is just an adapter: `Get`, `Set`, `Keys` over whatever holds the headers. The **propagator** decides the format (W3C TraceContext + Baggage, set in `setup.go`).
- **Span kinds** matter: PRODUCER/CONSUMER tells backends this edge was async messaging, not a call, so latency between them is queue time.
- **Parent vs link**: for one-record-at-a-time processing, parent-child is natural (one trace end to end). For batch processing of many records into one span, you use **span links** to each producer context instead, because a span can have only one parent.
- **Collector**: services export OTLP to the collector; the collector batches and forwards to Jaeger. Changing backends is config only.
- Proof: `TestTraceSurvivesTheBroker` asserts same trace ID and that the consumer span's parent is the producer span. The e2e script checks Jaeger holds one trace with all three services.

## 4. Go concurrency patterns used

- **Fan-out per partition, ordered within**: `processBatch` groups records by partition and starts one goroutine per partition, joined with `sync.WaitGroup`. Results are collected under a `sync.Mutex`.
- **Mutex-guarded shared state**: `policy.Evaluator` (`sync.Mutex`) and `metrics.Aggregator` (`sync.RWMutex`, many readers from the HTTP API, one writer per event). `Get` returns a deep copy so callers can't race on internal maps. Tested with `-race` in `TestConcurrentUse`.
- **Context cancellation**: `signal.NotifyContext` turns SIGTERM into a cancelled context. `Poll` returns on cancel; the in-memory broker's `Poll` selects on `ctx.Done()` and a notify channel.
- **Close-and-replace broadcast channel**: the in-memory broker wakes all waiting consumers on produce by closing a channel and creating a new one. A cheap broadcast without `sync.Cond`.
- **errgroup**: `cmd/processor` runs the consumer loop and the HTTP server under `errgroup.WithContext`. If either fails, the shared context is cancelled and the other shuts down.
- **Graceful shutdown order**: stop accepting HTTP, drain in-flight requests (`srv.Shutdown`), then close the Kafka client and flush spans (deferred, so they run last).

## 5. Ten likely interview questions

**1. How do you guarantee per-agent ordering?**
Every event is keyed by `agent_id`, so Kafka sends all of an agent's events to one partition, which is an ordered log. The processor handles each partition's records sequentially in one goroutine and only parallelizes across partitions. Ordering would break if the partition count changed (keys re-hash) or if we added a retry topic.

**2. What happens if the processor crashes halfway through a batch?**
Offsets are committed only after handlers succeed, so the uncommitted records are redelivered when it restarts. Nothing is lost; some events may be processed twice. Today in-memory state is also lost on restart, which is listed as a known limitation and tied to task 2.

**3. Why not exactly-once?**
Kafka transactions make consume-transform-produce atomic only for data that lives in Kafka. The aggregates live in memory and real kill actions may call external systems. So the right answer is at-least-once plus idempotent handling keyed on the event ID (task 3), which is cheaper and covers all side effects.

**4. How does the trace survive the Kafka hop?**
The producer injects the W3C `traceparent` of its PRODUCER span into the record headers. The consumer extracts it and starts a CONSUMER span with that as its parent. Kafka stores headers with the record, so the context arrives with the data, even hours later.

**5. How would you scale the processor?**
Add replicas in the same consumer group, up to the number of partitions. Beyond that, add partitions, which re-maps keys, so do it before go-live or plan a migration. Within one replica, partitions already run in parallel. Watch consumer lag per partition to decide.

**6. What is consumer lag and how would you alert on it?**
Lag is log-end offset minus committed offset, per partition. Alert when it grows steadily, not on a fixed number, because bursty agents cause brief spikes. `kafka-consumer-groups.sh --describe` shows it; in production you'd export it as a metric.

**7. How do you handle a poison message?**
Today it's logged and skipped. The real fix (task 1): retry transient errors with capped exponential backoff, send permanent failures to a dead-letter topic with error metadata and original headers, then commit and move on, so one bad record doesn't block the whole partition.

**8. Why use event time instead of processing time in the kill switch?**
Replays and catch-up after an outage would compress hours of events into seconds of processing time and trip every rule. Event time gives the same decision every time the partition is processed, which also makes the logic testable without a clock.

**9. What does a rebalance do to this service?**
The group coordinator reassigns partitions. With franz-go's default cooperative-sticky balancer only the moving partitions are revoked. Processed-but-uncommitted records on revoked partitions are redelivered to the new owner. Task 2 commits in `OnPartitionsRevoked` and blocks rebalances mid-batch to minimize that.

**10. Why put an HTTP service in front of Kafka instead of having agents produce directly?**
Agents run in many languages and environments. One small service owns schema validation, Kafka credentials, and client tuning, and agents just POST. The cost is an extra hop and a service to run; the 202 is only returned after Kafka acks, so it doesn't weaken delivery guarantees.

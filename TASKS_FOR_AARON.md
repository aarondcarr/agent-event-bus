# Tasks for Aaron

Three tasks, each sized for one or two evenings. They are the parts interviewers push on, so building them yourself is the point. Each has a TODO in the code marked `TODO(aaron, TASKS_FOR_AARON.md #N)`.

Ground rules: keep `go test -race ./...` green, keep commits small, and add tests with the in-memory broker before touching Docker.

---

## 1. Dead-letter queue with retry and backoff

**Where:** `internal/processor/processor.go` (`Processor.DLQ`, `Processor.DLQTopic`, and the TODO in `handle`). The `agent.events.dlq` topic already exists in `docker-compose.yml`.

**Problem today:** an invalid record is logged and dropped, so it is lost. A handler error stops the processor, the record is redelivered after restart, and if the error is permanent the partition is stuck forever (a "poison pill").

**Acceptance criteria**
- [ ] A handler error is retried up to `MaxAttempts` (configurable, default 3) with exponential backoff and jitter (for example 100ms, 200ms, 400ms, each +/- 20%).
- [ ] Backoff waits are cancelled by `ctx` (shutdown does not hang on a sleeping retry).
- [ ] After the last attempt, the original record goes to `DLQTopic` with: the original key and value, the original headers including `traceparent`, and extra headers `dlq.error`, `dlq.source.topic`, `dlq.source.partition`, `dlq.source.offset`, `dlq.attempts`. Then the offset is committed and the partition moves on.
- [ ] Invalid records (fail `event.Decode`) go to the DLQ immediately, with no retries.
- [ ] If publishing to the DLQ fails, the offset is NOT committed and `Run` returns the error. Losing a record is worse than stalling.
- [ ] Tests, with the in-memory broker: transient error then success (no DLQ record, handler called twice); permanent error (DLQ record with all headers, offset committed, next record processed); poison record; DLQ produce failure (offset not committed).
- [ ] Tests do not actually sleep. Inject the sleep function or make backoff configurable.

**Hints**
- Wrap the DLQ producer in `tracing.TracedProducer` so the DLQ publish shows up in the original trace.
- A `type permanentError struct{ error }` plus `errors.As` lets handlers say "don't bother retrying".
- `HeaderCarrier.Set` already replaces existing keys, so re-injecting trace context won't duplicate `traceparent`.
- Interview angle: why not retry forever in place? (Head-of-line blocking: one bad record stalls every agent on that partition.) Why not a retry topic? (Breaks per-agent ordering. Know the trade-off.)

---

## 2. Consumer-group rebalance handling

**Where:** `internal/bus/kafka.go` (TODO in `NewKafkaConsumer`), and the `bus.Consumer` interface.

**Problem today:** when partitions move (a replica starts, stops, or misses a heartbeat), records that were processed but not yet committed get processed again by the new owner. That is safe but noisy. Also, the new owner has no in-memory state for the agents on its new partitions (see README, Known limitations #1).

**Acceptance criteria**
- [ ] Use `kgo.BlockRebalanceOnPoll()` and call `AllowRebalance()` after each batch is handled and committed, so a rebalance never interrupts a batch halfway.
- [ ] `kgo.OnPartitionsRevoked`: commit everything processed so far before the partitions are given up.
- [ ] `kgo.OnPartitionsLost`: do NOT commit (you no longer own the partitions; another member may already be ahead). Log it.
- [ ] Log assigned, revoked, and lost partitions with the member ID.
- [ ] Expose assign/revoke through the `bus` package (for example `OnAssigned func([]TopicPartition)` and `OnRevoked func([]TopicPartition)` options), so the processor can drop state for revoked agents. Add the same hooks to the in-memory broker and unit-test them.
- [ ] Manual check: `docker compose up -d --scale processor=2` (remove the fixed `8081:8081` host port first, or give each replica its own), run `make demo` a few times, `docker compose stop` one replica mid-run, and show with `kafka-consumer-groups.sh --describe --group agent-processor` that lag returns to 0 and nothing was lost.
- [ ] Stretch: on assignment, rebuild kill-switch state for the new partitions (replay `agent.control` for those agents, or replay `agent.events` from the start of the policy window).

**Hints**
- franz-go docs: "Consumer groups" and the `BlockRebalanceOnPoll` option comment explain the commit-before-revoke pattern.
- Cooperative-sticky is franz-go's default balancer. Know the difference from eager: with eager, every member gives up every partition on each rebalance; with cooperative, only the moving partitions are revoked.
- `session.timeout.ms` vs `max.poll.interval.ms` (franz-go: `SessionTimeout`, `RebalanceTimeout`) is a classic interview question. Know which one a slow handler trips.

---

## 3. Idempotent processing

**Where:** `internal/metrics/metrics.go` (TODO in `Record`), `internal/policy/policy.go`, and the skipped acceptance test `TestRedeliveryIsIdempotent` in `internal/processor/processor_test.go`.

**Problem today:** at-least-once delivery means the same event can arrive twice (crash after handling but before commit, rebalance, an agent retrying a POST that actually succeeded). Each duplicate is counted again, and a duplicated guard denial moves an agent closer to being killed.

**Acceptance criteria**
- [ ] Remove the `t.Skip` in `TestRedeliveryIsIdempotent`; it passes.
- [ ] Add a policy test: the same denial event delivered twice counts once.
- [ ] Dedupe by `event.ID`. Memory must be bounded: keep seen IDs per agent for at least the policy window (or a fixed-size LRU), and add a test that the dedupe set doesn't grow without limit.
- [ ] Dedupe is checked once per event before any handler runs (so handlers stay simple), and an ID is marked seen only after all handlers succeed. Think about why the order matters, and write a test for it.
- [ ] Count duplicates and expose the number in `GET /v1/agents/{id}` (`stats.duplicates_dropped`).
- [ ] A short section in README: why this is "effectively once" for in-memory state, and why Kafka transactions (exactly-once semantics) would only cover the `agent.control` write, not the in-memory state, and not side effects outside Kafka.

**Hints**
- Mark-seen-before-handling turns a crash between the two into a lost event. Mark-seen-after is at-least-once plus dedupe, which is what you want.
- Since processing for one partition is single-threaded, the dedupe set can be per partition with no lock contention. Is that worth it here? Measure before deciding.
- If the state ever moves out of memory (Redis, Postgres), the dedupe write and the state write need to be one transaction, or the dedupe key has to be part of the state row.

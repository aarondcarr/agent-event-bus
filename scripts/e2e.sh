#!/usr/bin/env bash
# End-to-end check against the real stack. Requires docker, curl, jq.
# Proves: events flow agent -> ingest -> Kafka -> processor, the kill switch
# trips and publishes to agent.control, and Jaeger holds ONE trace spanning
# simagent, ingest and processor (i.e. context crossed Kafka headers).
set -euo pipefail
cd "$(dirname "$0")/.."

BUILD=--build; [[ "${SKIP_BUILD:-}" == 1 ]] && BUILD=
step() { printf '\n== %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*"; exit 1; }
retry() { local n=$1; shift; for _ in $(seq "$n"); do "$@" && return 0; sleep 2; done; return 1; }

step "starting stack"
docker compose up -d $BUILD --wait ingest processor jaeger otel-collector

step "running simulated agents"
out=$(docker compose --profile demo run --rm simagent)
echo "$out"
rogue_trace=$(awk '/^agent-rogue/ { sub("trace_id=", "", $3); print $3 }' <<<"$out")
[[ -n "$rogue_trace" ]] || fail "no trace id from simagent"

step "kill switch state (processor API)"
killed() { curl -fsS localhost:8081/v1/agents/agent-rogue | jq -e '.killed == true and .reason == "guard_denials"' >/dev/null; }
retry 30 killed || fail "agent-rogue was not killed"
curl -fsS localhost:8081/v1/agents/agent-rogue | jq -c '{agent_id, killed, reason, events: .stats.events, denials: .stats.guard_denials}'
curl -fsS localhost:8081/v1/agents/agent-good | jq -e '.killed == false' >/dev/null || fail "agent-good was killed"
echo "agent-good: not killed"

step "control topic"
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic agent.control --from-beginning --max-messages 1 --timeout-ms 15000 | tee /dev/stderr | grep -q '"agent-rogue"' \
  || fail "no kill record on agent.control"

step "one trace across the bus (Jaeger)"
services() { curl -fsS "localhost:16686/api/traces/$rogue_trace" | jq -r '[.data[0].processes[].serviceName] | unique | join(",")'; }
has_all() { [[ "$(services)" == "ingest,processor,simagent" ]]; }
retry 20 has_all || fail "trace $rogue_trace services: $(services)"
curl -fsS "localhost:16686/api/traces/$rogue_trace" \
  | jq -r '.data[0] as $t | $t.spans[] | "\($t.processes[.processID].serviceName)\t\(.operationName)"' | sort | uniq -c
echo "trace $rogue_trace spans: simagent -> ingest -> Kafka -> processor"
echo "open http://localhost:16686/trace/$rogue_trace"

printf '\nE2E PASS\n'

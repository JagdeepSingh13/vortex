#!/usr/bin/env bash
# End-to-end benchmark (make e2e-bench): simulator -> Kafka -> engine -> Kafka.
#
# For each target rate, REPEAT times:
#   1. measure: start a fresh engine, wait for its partitions, send orders open-loop for
#      DURATION seconds, stop the engine; it prints its latency summaries on shutdown.
#      Nothing else runs during this phase: an earlier version ran the Python trade checker
#      alongside, and its CPU use inflated p99 from ~50-140 ms to ~500-700 ms.
#   2. verify: the checker reads that run's trades back from Kafka: every trade's key,
#      per-symbol order and partition, plus full schema validation of the first 20,000.
#
#   end-to-end  = order created by the simulator -> its trade acknowledged by Kafka
#   ingest      = order created -> decoded by the engine's consumer thread
#   delivery    = engine produce() -> broker acknowledgement
#
# Needs Kafka running with topics (make up && make topics) and the engine built.

set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash on Windows: keep /src paths as they are

RATES=${E2E_RATES:-"1000 5000 10000 20000 30000"}
DURATION=${E2E_DURATION:-30}
REPEAT=${E2E_REPEAT:-2}
OUT=${1:?usage: bench.sh <output.md>}
ENGINE_IMAGE=${ENGINE_IMAGE:?}
PYTHON_IMAGE=${PYTHON_IMAGE:?}
NETWORK=vortex_default
# the repository as Docker sees it: on Git Bash, `pwd` can give a path Docker does not
# translate (e.g. /tmp/... for the Windows temp folder); `pwd -W` gives C:/...
SRC="$(pwd -W 2>/dev/null || pwd)"
LOG=$(mktemp)

cleanup() { docker rm -f vortex-engine >/dev/null 2>&1 || true; rm -f "$LOG"; }
trap cleanup EXIT

python_run() {
    docker run --rm --network $NETWORK -e PYTHONPATH=/src/simulator -v "$SRC:/src" -w /src "$PYTHON_IMAGE" "$@"
}

field() {  # $1 summary line, $2 key -> value
    sed -E "s/.* $2=([0-9]+).*/\1/" <<< "$1"
}

rows=""
for rate in $RATES; do
    for run in $(seq 1 "$REPEAT"); do
        echo "== $rate orders/s for ${DURATION}s, run $run"
        docker rm -f vortex-engine >/dev/null 2>&1 || true

        # ---- 1. measure ----
        docker run -d --init --name vortex-engine --network $NETWORK -v "$SRC:/src" -w /src \
            "$ENGINE_IMAGE" build/engine-release/src/vortex_engine >/dev/null
        for _ in $(seq 1 90); do
            docker logs vortex-engine 2>&1 | grep -q "assigned" && break
            sleep 1
        done
        run_id=$(docker logs vortex-engine 2>&1 | sed -nE 's/^vortex engine run ([0-9a-f]{16}).*/\1/p')

        sim=$(python_run python -m vortex_sim --rate "$rate" --duration "$DURATION" 2>&1 | grep '^done')
        echo "   simulator: $sim"
        sleep 3
        docker stop -t 60 vortex-engine >/dev/null
        docker logs vortex-engine > "$LOG" 2>&1
        docker rm -f vortex-engine >/dev/null
        grep "^SUMMARY" "$LOG" | sed "s/^/   /"

        totals=$(grep '^SUMMARY run=' "$LOG")
        ingest=$(grep '^SUMMARY ingest' "$LOG")
        e2e=$(grep '^SUMMARY end-to-end' "$LOG")
        trades=$(field "$totals" trades)

        # ---- 2. verify ----
        check=$(python_run python -m vortex_sim.trades_check --from-start --run-id "$run_id" \
            --expect "$trades" --validate-limit 20000 --duration 300 2>&1 || true)
        verdict=$(grep '^RESULT' <<< "$check")
        checked=$(sed -nE 's/^trades checked ([0-9]+).*/\1/p' <<< "$check")
        echo "   checker: $verdict ($checked of $trades trades)"

        sent=$(sed -E 's/.*\(([0-9,]+)\/s\).*/\1/' <<< "$sim")
        rows+="| $rate | $run | $sent | $(field "$totals" orders) | $trades | $checked"
        rows+=" | $(field "$ingest" p50) | $(field "$ingest" p99)"
        rows+=" | $(field "$e2e" p50) | $(field "$e2e" p90) | $(field "$e2e" p99) | $(field "$e2e" p99.9) | $(field "$e2e" max)"
        rows+=" | ${verdict#RESULT } |"$'\n'
    done
done

cpu=$(docker run --rm "$ENGINE_IMAGE" sh -c "grep -m1 'model name' /proc/cpuinfo | cut -d: -f2" | sed 's/^ //')
commit="$(git rev-parse --short HEAD)$(git diff --quiet HEAD -- engine simulator infra || echo -dirty)"

cat > "$OUT" <<EOF
# End-to-end benchmark — $(date -u +%F)

## Conditions

| | |
|---|---|
| Commit | \`$commit\` |
| CPU | $cpu |
| Environment | ${BENCH_ENV:-Docker Desktop (WSL2 VM) on Windows 11} |
| Path | simulator (Python, confluent-kafka) -> Kafka \`orders\` -> engine (C++, librdkafka 2.3.0) -> Kafka \`trades\` |
| Kafka | apache/kafka 4.3.1, single node, 3 partitions; every container on one laptop |
| Producers | acks=all, idempotent, linger.ms=5, lz4, murmur2 partitioner (simulator and engine) |
| Load | open loop, ${DURATION} s per run, fresh engine per run, ${REPEAT} runs per rate, seed 42 |
| Warm-up | latency samples exclude orders created in the first 5 s of each run |

## Results

Latencies in microseconds. **End-to-end** = order created by the simulator -> the trade it caused is
acknowledged by Kafka, measured per trade in the engine's publisher. **Ingest** = order created ->
decoded by the engine. After each run the checker read that run's trades back from Kafka and checked
every one for unique key, per-symbol order and partition, plus full trade.v1 schema validation of the
first 20,000.

| Target/s | Run | Sent/s | Orders in engine | Trades | Trades checked | Ingest p50 | Ingest p99 | E2E p50 | E2E p90 | E2E p99 | E2E p99.9 | E2E max | Checker |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
${rows}
Both producers batch for up to 5 ms (linger.ms=5), so a few milliseconds of every end-to-end figure
are deliberate batching traded for throughput (see the M3 Kafka benchmark).
EOF
echo "written to $OUT"

#!/usr/bin/env bash
# Live market data benchmark (make api-bench): engine trade -> Redis candle latency.
#
# For each rate, REPEAT times: run the engine and the simulator, let the api service warm up for
# WARMUP seconds (JIT, connection pool, first batches), reset its counters, measure for DURATION
# seconds, then read its stats: latency per trade from the engine's event_time_ns to the moment
# its Redis update finished, plus how the Redis writes were batched.

set -euo pipefail
export MSYS_NO_PATHCONV=1

RATES=${API_BENCH_RATES:-"2000 5000"}
WARMUP=${API_BENCH_WARMUP:-10}
DURATION=${API_BENCH_DURATION:-60}
REPEAT=${API_BENCH_REPEAT:-2}
OUT=${1:?usage: bench.sh <output.md>}
COMPOSE="docker compose --env-file .env -f infra/docker/docker-compose.yml"
# the repository as Docker sees it (Git Bash: pwd -W gives C:/..., which Docker translates)
SRC="$(pwd -W 2>/dev/null || pwd)"
API=http://localhost:8090

wait_for_log() {  # $1 container, $2 text: fail instead of waiting forever
    local deadline=$((SECONDS + 180))
    until docker logs "$1" 2>&1 | grep -q "$2"; do
        if [ $SECONDS -ge $deadline ]; then echo "FAIL: '$2' never appeared in $1"; exit 1; fi
        sleep 2
    done
}
json() { python3 -c "import sys,json; print(json.load(sys.stdin)['$1'])" 2>/dev/null || python -c "import sys,json; print(json.load(sys.stdin)['$1'])"; }

$COMPOSE --profile api up -d api >/dev/null 2>&1
wait_for_log vortex-api "partitions assigned"

rows=""
for rate in $RATES; do
    for run in $(seq 1 "$REPEAT"); do
        echo "== $rate orders/s, run $run"
        docker rm -f vortex-engine vortex-sim >/dev/null 2>&1 || true
        docker run -d --init --name vortex-engine --network vortex_default -v "$SRC:/src" -w /src \
            "${ENGINE_IMAGE:?}" build/engine-release/src/vortex_engine >/dev/null
        wait_for_log vortex-engine "assigned"
        docker run -d --name vortex-sim --network vortex_default -e PYTHONPATH=/src/simulator \
            -v "$SRC:/src" -w /src "${PYTHON_IMAGE:?}" \
            python -m vortex_sim --rate "$rate" --duration $((WARMUP + DURATION)) >/dev/null

        sleep "$WARMUP"
        curl -sf -X POST "$API/stats/reset"
        sleep "$DURATION"
        stats=$(curl -sf "$API/stats")
        echo "   $stats"

        docker wait vortex-sim >/dev/null
        sent=$(docker logs vortex-sim 2>&1 | sed -nE 's/.*\(([0-9,]+)\/s\).*/\1/p')
        docker rm -f vortex-sim >/dev/null
        docker stop -t 30 vortex-engine >/dev/null
        trades=$(docker logs vortex-engine 2>&1 | sed -nE 's/^SUMMARY run=.* trades=([0-9]+).*/\1/p')
        docker rm -f vortex-engine >/dev/null

        get() { json "$1" <<< "$stats"; }
        ms() { awk "BEGIN { printf \"%.1f\", $(get "$1") / 1000 }"; }
        rows+="| $rate | $run | $sent | $trades | $(get applied) | $(get duplicates) | $(get avgBatchSize)"
        rows+=" | $(ms redisMicrosPerBatch) | $(ms p50Micros) | $(ms p90Micros) | $(ms p99Micros) | $(ms p999Micros) | $(ms maxMicros) |"$'\n'
    done
done

cpu=$(docker run --rm "${ENGINE_IMAGE}" sh -c "grep -m1 'model name' /proc/cpuinfo | cut -d: -f2" | sed 's/^ //')
commit="$(git rev-parse --short HEAD)$(git diff --quiet HEAD -- api infra || echo -dirty)"
cat > "$OUT" <<EOF
# Live market data benchmark — $(date -u +%F)

## Conditions

| | |
|---|---|
| Commit | \`$commit\` |
| CPU | $cpu |
| Environment | ${BENCH_ENV:-Docker Desktop (WSL2 VM) on Windows 11} |
| Path | engine -> Kafka \`trades\` -> api (Spring Boot 4.1.1, Java 25, 3 listener threads, -Xmx256m) -> Redis 8.8.2 |
| Redis writes | one atomic Lua script per trade, pipelined per Kafka poll batch, pooled connections |
| Load | simulator, ${WARMUP} s warm-up (not measured) then ${DURATION} s measured, ${REPEAT} runs per rate |

## Results

Latency = engine event time -> the trade's Redis update finished, in milliseconds. It includes the
engine's Kafka publish (batched up to 5 ms), Kafka, the consumer and Redis.

| Orders/s | Run | Sent/s | Trades (whole run) | Applied (measured window) | Duplicates | Avg batch | Redis ms/batch | p50 | p90 | p99 | p99.9 | max |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
${rows}
EOF
echo "written to $OUT"

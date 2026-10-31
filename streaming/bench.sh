#!/usr/bin/env bash
# Bronze ingestion benchmark (make bronze-bench): how fresh is bronze, and at what cost in files?
#
# For each (trigger interval, order rate), REPEAT times:
#   restart the bronze stream with that trigger and warm it up (its first batch after a start
#   is slow: JVM and catalog warm-up), run the engine and simulator for DURATION seconds,
#   let bronze catch up, then measure for that engine run:
#     ingest lag   engine event time -> start of the micro-batch that wrote the row
#     batch time   from the stream's own progress log (write + Iceberg commit)
#     files        data files written for the run (small files are the cost of short triggers)

set -euo pipefail
export MSYS_NO_PATHCONV=1

COMPOSE="docker compose --env-file .env -f infra/docker/docker-compose.yml"
CONFIGS=${BRONZE_BENCH_CONFIGS:-"10:1000 10:3000 2:3000"}   # trigger_seconds:orders_per_second
DURATION=${BRONZE_BENCH_DURATION:-60}
REPEAT=${BRONZE_BENCH_REPEAT:-2}
OUT=${1:?usage: bench.sh <output.md>}
# the repository as Docker sees it: on Git Bash, `pwd` can give a path Docker does not
# translate (e.g. /tmp/... for the Windows temp folder); `pwd -W` gives C:/...
SRC="$(pwd -W 2>/dev/null || pwd)"

wait_for_log() {  # $1 container, $2 text, [$3 timeout seconds]: fail instead of waiting forever
    local deadline=$((SECONDS + ${3:-180}))
    until docker logs "$1" 2>&1 | grep -q "$2"; do
        if [ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" != "true" ] || [ $SECONDS -ge $deadline ]; then
            echo "FAIL: '$2' never appeared in $1; last lines:"
            docker logs "$1" 2>&1 | tail -15
            exit 1
        fi
        sleep 2
    done
}
batch_count() { docker logs vortex-bronze 2>&1 | grep -c '^bronze: batch' || true; }

run_load() {  # $1 rate, $2 seconds -> prints engine run id
    docker rm -f vortex-engine >/dev/null 2>&1 || true
    docker run -d --init --name vortex-engine --network vortex_default -v "$SRC:/src" -w /src \
        "${ENGINE_IMAGE:?}" build/engine-release/src/vortex_engine >/dev/null
    wait_for_log vortex-engine "assigned"
    docker run --rm --network vortex_default -e PYTHONPATH=/src/simulator -v "$SRC:/src" -w /src \
        "${PYTHON_IMAGE:?}" python -m vortex_sim --rate "$1" --duration "$2" >/dev/null 2>&1
    sleep 3
    docker stop -t 60 vortex-engine >/dev/null
    docker logs vortex-engine 2>&1 | sed -nE 's/^vortex engine run ([0-9a-f]{16}).*/\1/p'
    docker rm -f vortex-engine >/dev/null
}

rows=""
for config in $CONFIGS; do
    trigger=${config%%:*}
    rate=${config##*:}
    for run in $(seq 1 "$REPEAT"); do
        echo "== trigger ${trigger}s, ${rate} orders/s, run $run"
        BRONZE_TRIGGER_SECONDS=$trigger $COMPOSE --profile stream up -d --force-recreate bronze >/dev/null 2>&1
        wait_for_log vortex-bronze "streaming trades"
        run_load 500 10 >/dev/null                      # warm-up load, not measured
        until [ "$(batch_count)" -ge 2 ]; do sleep 2; done
        sleep $((trigger * 2))
        before=$(batch_count)

        run_id=$(run_load "$rate" "$DURATION")
        sleep $((trigger * 3 + 15))                     # let bronze catch up with the run
        batches=$(docker logs vortex-bronze 2>&1 | grep '^bronze: batch' | tail -n +$((before + 1)))
        batch_ms=$(sed -nE 's/.* in ([0-9]+) ms.*/\1/p' <<< "$batches" | sort -n)
        n=$(wc -l <<< "$batch_ms")
        median_batch=$(sed -n "$(( (n + 1) / 2 ))p" <<< "$batch_ms")
        max_batch=$(tail -1 <<< "$batch_ms")

        lag=$($COMPOSE --profile stream run --rm --no-deps -T bronze /opt/spark/bin/spark-submit \
            --master local[1] --driver-memory 512m /opt/vortex/streaming/bronze_lag.py "$run_id" 2>/dev/null \
            | grep '^LAG')
        echo "   $lag  batch ms median=$median_batch max=$max_batch"
        get() { sed -E "s/.* $1=([0-9]+).*/\1/" <<< "$lag"; }
        rows+="| ${trigger} s | ${rate} | $run | $(get rows) | $(get batches) | $(get files) | ${median_batch} | ${max_batch}"
        rows+=" | $(get p50) | $(get p90) | $(get p99) | $(get max) |"$'\n'
    done
done

BRONZE_TRIGGER_SECONDS=10 $COMPOSE --profile stream up -d --force-recreate bronze >/dev/null 2>&1

cpu=$(docker run --rm "${ENGINE_IMAGE}" sh -c "grep -m1 'model name' /proc/cpuinfo | cut -d: -f2" | sed 's/^ //')
commit="$(git rev-parse --short HEAD)$(git diff --quiet HEAD -- streaming infra || echo -dirty)"
cat > "$OUT" <<EOF
# Bronze ingestion benchmark — $(date -u +%F)

## Conditions

| | |
|---|---|
| Commit | \`$commit\` |
| CPU | $cpu |
| Environment | ${BENCH_ENV:-Docker Desktop (WSL2 VM) on Windows 11} |
| Path | engine -> Kafka \`trades\` -> Spark 4.1.3 Structured Streaming (local[2], 1 GB driver) -> Iceberg 1.12.0 (REST catalog) on SeaweedFS |
| Load | simulator for ${DURATION} s per run after a warm-up batch, ${REPEAT} runs per configuration |

## Results

**Ingest lag** = engine event time -> start of the micro-batch that wrote the row (ms). A row is
readable once its batch commits, so add the batch time. **Batch time** = one micro-batch from
reading Kafka to the Iceberg commit (ms). **Files** = data files written for the run.

| Trigger | Orders/s | Run | Trades in bronze | Batches | Files | Batch median | Batch max | Lag p50 | Lag p90 | Lag p99 | Lag max |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
${rows}
EOF
echo "written to $OUT"

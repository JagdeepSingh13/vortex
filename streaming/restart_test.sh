#!/usr/bin/env bash
# Crash test for bronze ingestion (make bronze-restart-test).
#
# While the engine produces trades, the bronze job is killed with SIGKILL (no clean shutdown,
# exactly like a crash or an OOM kill) twice, and restarted each time. Afterwards every Kafka
# offset must be in bronze exactly once: bronze-check reconciles the whole topic.
#
# Why it should hold: Spark's checkpoint says which offsets a batch covered, and Iceberg's
# streaming sink stores the batch id in its commit. A batch that was committed before the
# kill is skipped on replay; one that was not is written again from the same offsets.

set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash on Windows: keep container paths as they are

COMPOSE="docker compose --env-file .env -f infra/docker/docker-compose.yml"
RATE=${RESTART_TEST_RATE:-3000}
DURATION=${RESTART_TEST_DURATION:-180}
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

bronze_batches() { docker logs vortex-bronze 2>&1 | grep -c '^bronze: batch' || true; }

echo "1. bronze stream running and warm"
$COMPOSE --profile stream up -d bronze >/dev/null 2>&1
wait_for_log vortex-bronze "streaming trades"

echo "2. engine + simulator: $RATE orders/s for ${DURATION}s"
docker rm -f vortex-engine >/dev/null 2>&1 || true
docker run -d --init --name vortex-engine --network vortex_default -v "$SRC:/src" -w /src \
    "${ENGINE_IMAGE:?}" build/engine-release/src/vortex_engine >/dev/null
wait_for_log vortex-engine "assigned"
docker run -d --name vortex-sim --network vortex_default -e PYTHONPATH=/src/simulator \
    -v "$SRC:/src" -w /src "${PYTHON_IMAGE:?}" \
    python -m vortex_sim --rate "$RATE" --duration "$DURATION" >/dev/null

for kill in 1 2; do
    # only kill once the job is really processing: at least one batch with rows committed
    # since it (re)started, then a few seconds into the next batch, while it is writing
    before=$(docker logs vortex-bronze 2>&1 | grep -c '^bronze: batch [0-9]* rows [1-9]' || true)
    until [ "$(docker logs vortex-bronze 2>&1 | grep -c '^bronze: batch [0-9]* rows [1-9]' || true)" -gt "$before" ]; do
        sleep 1
    done
    sleep 4
    echo "3.$kill kill -9 the bronze job while it writes ($(bronze_batches) batches logged so far)"
    docker kill --signal KILL vortex-bronze >/dev/null
    sleep 5
    $COMPOSE --profile stream up -d bronze >/dev/null 2>&1
    wait_for_log vortex-bronze "streaming trades"
    echo "    restarted"
done

docker wait vortex-sim >/dev/null
docker logs vortex-sim 2>&1 | grep '^done' | sed 's/^/   simulator: /'
docker rm -f vortex-sim >/dev/null
sleep 3
docker stop -t 60 vortex-engine >/dev/null
docker logs vortex-engine 2>&1 | grep '^SUMMARY run=' | sed 's/^/   engine: /'
docker rm -f vortex-engine >/dev/null

echo "4. reconcile once bronze has caught up (Spark skips empty batches, so just retry the check)"
for attempt in 1 2 3 4 5 6; do
    result=$($COMPOSE --profile stream run --rm --no-deps -T bronze /opt/spark/bin/spark-submit \
        --master local[2] --driver-memory 768m /opt/vortex/streaming/bronze_check.py 2>&1 \
        | grep -vE "WARN|INFO|log4j" || true)
    if grep -q "^RESULT OK" <<< "$result"; then
        break
    fi
    echo "   attempt $attempt: not reconciled yet, retrying"
    sleep 10
done
echo "$result"
echo "bronze batches logged: $(bronze_batches)"
grep -q "^RESULT OK" <<< "$result"

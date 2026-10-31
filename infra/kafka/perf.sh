#!/usr/bin/env bash
# Kafka throughput and latency on this machine (make kafka-perf).
#
# Kafka's own perf tools run inside the broker container against a temporary topic laid
# out like `orders` (3 partitions). Payloads are the real order.v1 examples.
# Every scenario runs RUNS times on a fresh topic; all runs and the median are reported,
# because a single run on a laptop VM is not trustworthy (results varied 2-5x between runs).
#
#   default   max throughput, client defaults, acks=all
#   tuned     max throughput, linger.ms=10, batch.size=64KB, lz4 compression, acks=all
#   paced     fixed 20,000 msg/s with linger.ms=0: latency when the broker is not saturated
#             (open loop: messages go out on a schedule, not only after the previous returned)
#   consumer  read 1M messages back
#
# Writes the given markdown file.

set -euo pipefail

# Git Bash on Windows would rewrite /opt/kafka/... into a Windows path
export MSYS_NO_PATHCONV=1

COMPOSE="docker compose -f infra/docker/docker-compose.yml"
KAFKA="$COMPOSE exec -T kafka /opt/kafka/bin"
BOOTSTRAP="--bootstrap-server localhost:9092"
TOPIC="vortex-perf"
RECORDS=${KAFKA_PERF_RECORDS:-1000000}
RATE=${KAFKA_PERF_RATE:-20000}
RUNS=${KAFKA_PERF_RUNS:-3}
OUT=${1:?usage: perf.sh <output.md>}
WORK=$(mktemp -d)

trap 'rm -rf "$WORK"; $KAFKA/kafka-topics.sh $BOOTSTRAP --delete --if-exists --topic $TOPIC >/dev/null 2>&1 || true' EXIT

fresh_topic() {
    $KAFKA/kafka-topics.sh $BOOTSTRAP --delete --if-exists --topic $TOPIC >/dev/null 2>&1 || true
    sleep 2
    $KAFKA/kafka-topics.sh $BOOTSTRAP --create --topic $TOPIC --partitions 3 --replication-factor 1 >/dev/null
}

# real payloads: one compact order.v1 example per line, copied into the container
for file in schemas/examples/order/valid/*.json; do
    tr -d '\r\n' < "$file" | sed 's/  */ /g'; echo
done | $COMPOSE exec -T kafka sh -c 'cat > /tmp/payloads.txt'
payload_range=$($COMPOSE exec -T kafka sh -c 'awk "NR==1 || length(\$0)<min {min=length(\$0)} length(\$0)>max {max=length(\$0)} {s+=length(\$0)} END {printf \"%d-%d bytes (mean %.0f)\", min, max, s/NR}" /tmp/payloads.txt')

producer() {  # $1 records, $2 rate, extra --command-property args...
    local records=$1 rate=$2; shift 2
    $KAFKA/kafka-producer-perf-test.sh $BOOTSTRAP --topic $TOPIC \
        --num-records "$records" --throughput "$rate" --warmup-records $((records / 10)) \
        --payload-file /tmp/payloads.txt --payload-delimiter $'\n' \
        --command-property acks=all "$@" 2>/dev/null | tail -1
}

# "N steady state records sent, X records/sec (Y MB/sec), ... A ms 50th, B ms 95th, C ms 99th, D ms 99.9th."
# -> "X Y A C D"
fields() {
    sed -E 's/.* ([0-9.]+) records\/sec \(([0-9.]+) MB\/sec\).* ([0-9]+) ms 50th, [0-9]+ ms 95th, ([0-9]+) ms 99th, ([0-9]+) ms 99.9th.*/\1 \2 \3 \4 \5/'
}

median() { sort -g | awk '{a[NR]=$1} END {print a[int((NR+1)/2)]}'; }

run_scenario() {  # $1 name, then producer args
    local name=$1; shift
    : > "$WORK/$name"
    for run in $(seq 1 "$RUNS"); do
        fresh_topic
        local line
        line=$(producer "$@" | fields)
        echo "  $name run $run: msg/s MB/s p50 p99 p99.9 = $line"
        echo "$line" >> "$WORK/$name"
    done
}

table_rows() {  # $1 name -> one markdown row per run, then the median
    local file="$WORK/$1" n=0
    while read -r rps mbs p50 p99 p999; do
        n=$((n + 1))
        printf '| %s run %d | %.0f | %.1f | %s | %s | %s |\n' "$1" $n "$rps" "$mbs" "$p50" "$p99" "$p999"
    done < "$file"
    printf '| **%s median** | **%.0f** | **%.1f** | **%s** | **%s** | **%s** |\n' "$1" \
        "$(cut -d' ' -f1 "$file" | median)" "$(cut -d' ' -f2 "$file" | median)" \
        "$(cut -d' ' -f3 "$file" | median)" "$(cut -d' ' -f4 "$file" | median)" "$(cut -d' ' -f5 "$file" | median)"
}

echo "producer, default settings, max throughput"
run_scenario default "$RECORDS" -1
echo "producer, tuned batching + lz4, max throughput"
run_scenario tuned "$RECORDS" -1 --command-property linger.ms=10 \
    --command-property batch.size=65536 --command-property compression.type=lz4
echo "producer, paced at $RATE msg/s, linger.ms=0"
run_scenario paced $((RATE * 10)) "$RATE" --command-property linger.ms=0

echo "consumer"
: > "$WORK/consumer"
for run in $(seq 1 "$RUNS"); do
    fresh_topic
    producer "$RECORDS" -1 >/dev/null
    # columns: start, end, MB, MB/s, nMsg, nMsg/s, rebalance ms, fetch ms, fetch MB/s, fetch nMsg/s
    line=$($KAFKA/kafka-consumer-perf-test.sh $BOOTSTRAP --topic $TOPIC --num-records "$RECORDS" \
        --timeout 60000 --hide-header 2>/dev/null | tail -1 | awk -F', ' '{printf "%.0f %.1f", $10, $9}')
    echo "  consumer run $run: msg/s MB/s = $line"
    echo "$line" >> "$WORK/consumer"
done

cpu=$($COMPOSE exec -T kafka sh -c "grep -m1 'model name' /proc/cpuinfo | cut -d: -f2" | sed 's/^ //')
commit="$(git rev-parse --short HEAD)$(git diff --quiet HEAD -- infra || echo -dirty)"

{
cat <<EOF
# Kafka benchmark — $(date -u +%F)

## Conditions

| | |
|---|---|
| Commit | \`$commit\` |
| CPU | $cpu |
| Environment | ${BENCH_ENV:-Docker Desktop (WSL2 VM) on Windows 11} |
| Kafka | apache/kafka 4.3.1, single node (KRaft broker + controller), heap 512 MB |
| Topic | 3 partitions, replication factor 1, fresh topic per run |
| Clients | Kafka's perf tools running **inside the broker container** (no network between machines) |
| Payload | the order.v1 examples, $payload_range, no keys |
| Producer | acks=all, $RECORDS records per max-throughput run, first 10% excluded as warm-up |
| Runs | $RUNS per scenario |

## Producer

Latency is from \`send()\` to the broker's acknowledgement. In the max-throughput runs the
producer outruns the broker, so latency mostly measures time spent queued in the client:
it shows saturation, not how fast one message travels. The paced runs show that.

| Scenario | msg/s | MB/s | p50 ms | p99 ms | p99.9 ms |
|---|---:|---:|---:|---:|---:|
EOF
table_rows default
table_rows tuned
table_rows paced
cat <<EOF

- **default**: client defaults (linger.ms=5, batch.size=16 KB, no compression)
- **tuned**: linger.ms=10, batch.size=64 KB, compression.type=lz4
- **paced**: fixed $RATE msg/s, linger.ms=0

## Consumer

| Run | msg/s (fetch) | MB/s (fetch) |
|---|---:|---:|
EOF
n=0
while read -r mps mbs; do n=$((n + 1)); echo "| $n | $mps | $mbs |"; done < "$WORK/consumer"
echo "| **median** | **$(cut -d' ' -f1 "$WORK/consumer" | median)** | **$(cut -d' ' -f2 "$WORK/consumer" | median)** |"
} > "$OUT"

echo "written to $OUT"

#!/usr/bin/env bash
# Kafka round-trip check (make kafka-check).
#
# Sends every valid order.v1 example from schemas/examples, keyed by symbol, to a
# temporary topic with the same layout as `orders` (3 partitions), reads them back and checks:
#   1. every message arrives exactly once, byte for byte
#   2. all messages with the same key land in the same partition (per-symbol ordering)
#   3. within each key, messages come back in the order they were sent
# The real `orders` topic is never touched: the engine would try to execute test orders.

set -euo pipefail

# Git Bash on Windows would rewrite /opt/kafka/... into a Windows path
export MSYS_NO_PATHCONV=1

COMPOSE="docker compose -f infra/docker/docker-compose.yml"
KAFKA="$COMPOSE exec -T kafka /opt/kafka/bin"
BOOTSTRAP="--bootstrap-server localhost:9092"
TOPIC="vortex-check"
EXAMPLES=schemas/examples/order/valid

work=$(mktemp -d)
trap 'rm -rf "$work"; $KAFKA/kafka-topics.sh $BOOTSTRAP --delete --if-exists --topic $TOPIC >/dev/null 2>&1 || true' EXIT

# a clean topic every run, so old messages cannot make the check pass
$KAFKA/kafka-topics.sh $BOOTSTRAP --delete --if-exists --topic $TOPIC >/dev/null 2>&1 || true
$KAFKA/kafka-topics.sh $BOOTSTRAP --create --topic $TOPIC --partitions 3 --replication-factor 1 >/dev/null

# one line per message: KEY|compact json (the console producer sends one message per line)
for file in "$EXAMPLES"/*.json; do
    json=$(tr -d '\r\n' < "$file" | sed 's/  */ /g')
    symbol=$(grep -o '"symbol": *"[A-Z0-9.]*"' "$file" | sed 's/.*"\([A-Z0-9.]*\)"$/\1/')
    printf '%s|%s\n' "$symbol" "$json"
done > "$work/sent"
count=$(wc -l < "$work/sent" | tr -d ' ')

# acks=all: the send only counts once the broker has written it
$KAFKA/kafka-console-producer.sh $BOOTSTRAP --topic $TOPIC \
    --command-property acks=all \
    --reader-property parse.key=true --reader-property 'key.separator=|' < "$work/sent"

# read back: "Partition:N<TAB>KEY<TAB>VALUE"
$KAFKA/kafka-console-consumer.sh $BOOTSTRAP --topic $TOPIC --from-beginning \
    --max-messages "$count" --timeout-ms 20000 \
    --formatter-property print.partition=true --formatter-property print.key=true \
    2>/dev/null | tr -d '\r' > "$work/received" || true

received=$(wc -l < "$work/received" | tr -d ' ')
echo "sent $count messages, received $received"
[ "$received" -eq "$count" ] || { echo "FAIL: message count differs"; exit 1; }

# 1. same messages, byte for byte (order across partitions is not defined, so compare sorted)
cut -f2- "$work/received" | sed 's/\t/|/' | sort > "$work/received_kv"
sort "$work/sent" > "$work/sent_sorted"
diff -q "$work/sent_sorted" "$work/received_kv" >/dev/null || {
    echo "FAIL: payloads differ"; diff "$work/sent_sorted" "$work/received_kv" | head; exit 1; }

# 2. one partition per key
echo "key -> partition:"
cut -f1,2 "$work/received" | sort -u | awk -F'\t' '{print "  " $2 " -> " $1}'
bad=$(cut -f1,2 "$work/received" | sort -u | cut -f2 | uniq -d)
[ -z "$bad" ] || { echo "FAIL: key(s) spread over several partitions: $bad"; exit 1; }

# 3. per-key order preserved
for key in $(cut -d'|' -f1 "$work/sent" | sort -u); do
    diff -q <(grep "^$key|" "$work/sent") <(awk -F'\t' -v k="$key" '$2 == k {print $2 "|" $3}' "$work/received") >/dev/null || {
        echo "FAIL: order changed for key $key"; exit 1; }
done

echo "OK: all messages round-tripped, each symbol stayed in one partition, per-symbol order preserved"

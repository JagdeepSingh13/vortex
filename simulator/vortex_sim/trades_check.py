"""Check the engine's output: python -m vortex_sim.trades_check --duration 30

Reads `trades` and, for every message:
  - validates it against trade.v1 (the M1 validator, strict integers),
  - checks the unique key (engine_run_id, symbol, match_id) is never repeated,
  - checks match ids increase per (engine_run_id, symbol): per-symbol order survived Kafka,
  - checks each symbol always arrives on the same partition.
Prints "ready" once partitions are assigned (wait for it before sending orders), then a
summary. Exit code 1 if anything is wrong.

It does not measure latency: validating every trade in Python is slower than the engine
produces them at high rates, so a receive-time latency here would measure this checker's
own backlog (the first attempt reported a misleading 6 s). The engine records end-to-end
latency itself: order created -> its trade acknowledged by Kafka.
"""

import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path

from confluent_kafka import Consumer

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "schemas" / "tests"))
from contracts import validate  # noqa: E402


def main(argv=None):
    parser = argparse.ArgumentParser(prog="vortex_sim.trades_check", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092"))
    parser.add_argument("--topic", default="trades")
    parser.add_argument("--duration", type=float, default=30, help="seconds to listen")
    parser.add_argument("--from-start", action="store_true", help="read the topic from the beginning")
    parser.add_argument("--run-id", default=None, help="only check trades of this engine run")
    parser.add_argument("--expect", type=int, default=0, help="stop as soon as this many trades were checked")
    parser.add_argument("--validate-limit", type=int, default=0,
                        help="full schema validation for the first N trades only (0 = all); "
                             "key, order and partition checks always cover every trade")
    args = parser.parse_args(argv)

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": f"vortex-trades-check-{uuid.uuid4().hex[:8]}",  # a fresh group: sees everything
        "auto.offset.reset": "earliest" if args.from_start else "latest",
        "enable.auto.commit": False,
    })

    def on_assign(_consumer, partitions):
        print(f"ready: assigned {len(partitions)} partition(s)", flush=True)

    consumer.subscribe([args.topic], on_assign=on_assign)

    seen = set()
    last_match = {}
    partition_of = {}
    invalid = duplicates = out_of_order = wrong_partition = 0
    first_problem = None
    count = 0

    run_marker = args.run_id.encode() if args.run_id else None
    validated = 0
    deadline = time.monotonic() + args.duration
    while time.monotonic() < deadline and not (args.expect and count >= args.expect):
        message = consumer.poll(0.2)
        if message is None:
            continue
        if message.error():
            print("consumer error:", message.error(), file=sys.stderr)
            continue
        # other runs' trades are skipped with a cheap byte search before parsing
        if run_marker is not None and run_marker not in message.value():
            continue
        trade = json.loads(message.value())
        if args.run_id and trade.get("engine_run_id") != args.run_id:
            continue
        count += 1

        if not args.validate_limit or validated < args.validate_limit:
            validated += 1
            problems = validate("trade", trade)
            if problems:
                invalid += 1
                first_problem = first_problem or f"invalid: {problems[0]} in {trade}"
                continue

        key = (trade["engine_run_id"], trade["symbol"], trade["match_id"])
        if key in seen:
            duplicates += 1
            first_problem = first_problem or f"duplicate key {key}"
        seen.add(key)

        book = (trade["engine_run_id"], trade["symbol"])
        if book in last_match and trade["match_id"] <= last_match[book]:
            out_of_order += 1
            first_problem = first_problem or f"match id went backwards for {book}"
        last_match[book] = trade["match_id"]

        if partition_of.setdefault(trade["symbol"], message.partition()) != message.partition():
            wrong_partition += 1
            first_problem = first_problem or f"{trade['symbol']} seen on two partitions"

    consumer.close()

    ok = count > 0 and not (invalid or duplicates or out_of_order or wrong_partition)
    print(f"trades checked {count} (schema-validated {validated}), invalid {invalid}, duplicate keys {duplicates}, "
          f"out of order {out_of_order}, wrong partition {wrong_partition}")
    print(f"symbol -> partition {dict(sorted(partition_of.items()))}")
    print("RESULT " + ("OK" if ok else "FAIL" + (f": {first_problem}" if first_problem else ": no trades received")))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

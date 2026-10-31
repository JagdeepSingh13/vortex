"""Run the order simulator: python -m vortex_sim --rate 2000 --duration 30

Sends order.v1 messages to Kafka, keyed by symbol, paced on a fixed schedule (open loop),
and prints live stats every second. --report writes a markdown summary.
"""

import argparse
import json
import os
import platform
import secrets
import sys
import time
from collections import Counter
from datetime import datetime, timezone

from confluent_kafka import Producer, libversion
from confluent_kafka import version as client_version

from .market import OrderFlow
from .symbols import load_symbols

MAX_PARTICIPANT_ID = 2**24 - 1


def parse_args(argv):
    parser = argparse.ArgumentParser(prog="vortex_sim", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092"))
    parser.add_argument("--topic", default="orders")
    parser.add_argument("--rate", type=float, default=1000, help="orders per second; 0 = as fast as possible")
    parser.add_argument("--duration", type=float, default=0, help="seconds to run; 0 = until --count or Ctrl+C")
    parser.add_argument("--count", type=int, default=0, help="orders to send; 0 = no limit")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--participant-base", type=int, default=None,
                        help="first participant id is base+1; default: random per run, so a restart "
                             "never reuses (participant, order_id) pairs of orders still in the book")
    parser.add_argument("--report", default=None, help="write a markdown summary to this file")
    return parser.parse_args(argv)


def make_producer(bootstrap):
    return Producer({
        "bootstrap.servers": bootstrap,
        "client.id": "vortex-sim",
        # a send counts only once the broker has written it
        "acks": "all",
        # retries cannot duplicate or reorder messages within a partition
        "enable.idempotence": True,
        # small batches + compression: measured ~3x throughput in M3 for a few ms of latency
        "linger.ms": 5,
        "compression.type": "lz4",
    })


def main(argv=None):
    args = parse_args(argv)
    if args.rate == 0 and args.duration == 0 and args.count == 0:
        sys.exit("--rate 0 (max speed) needs --duration or --count")

    base = args.participant_base
    if base is None:
        base = 1000 * (1 + secrets.randbelow((MAX_PARTICIPANT_ID - 1000) // 1000))
    symbols = load_symbols()
    flow = OrderFlow(symbols, seed=args.seed, participant_base=base, clock=time.time_ns)
    producer = make_producer(args.bootstrap)

    sent = Counter()          # by action / order type
    delivered = Counter()     # by partition
    errors = []

    def on_delivery(error, message):
        if error is not None:
            errors.append(str(error))
        else:
            delivered[message.partition()] += 1

    print(f"vortex-sim -> {args.bootstrap} topic={args.topic} rate={args.rate or 'max'}/s "
          f"seed={args.seed} participants {base + 1}..{base + 110} symbols={','.join(s.name for s in symbols)}")

    total = 0
    start = time.perf_counter()
    next_report = start + 1.0
    last_total = 0
    try:
        while True:
            now = time.perf_counter()
            elapsed = now - start
            if args.duration and elapsed >= args.duration:
                break
            if args.count and total >= args.count:
                break

            # open loop: how many orders should have been sent by now
            if args.rate > 0:
                due = int(elapsed * args.rate) + 1 - total
                if args.count:
                    due = min(due, args.count - total)
                if due <= 0:
                    producer.poll(0)
                    time.sleep(0.0005)
                    continue
            else:
                due = 1000 if not args.count else min(1000, args.count - total)

            for _ in range(due):
                message = flow.next()
                value = json.dumps(message, separators=(",", ":"))
                while True:
                    try:
                        producer.produce(args.topic, key=message["symbol"], value=value, on_delivery=on_delivery)
                        break
                    except BufferError:
                        # local queue full: let the client send and retry
                        producer.poll(0.01)
                total += 1
                sent[message["action"] if message["action"] != "ADD" else message["order_type"]] += 1
            producer.poll(0)

            if now >= next_report:
                rate = total - last_total
                print(f"t={elapsed:5.0f}s sent={total:>9} rate={rate:>7}/s delivered={sum(delivered.values()):>9} "
                      f"errors={len(errors)} partitions={dict(sorted(delivered.items()))}", flush=True)
                last_total = total
                next_report += 1.0
    except KeyboardInterrupt:
        print("stopping...")

    send_seconds = time.perf_counter() - start
    remaining = producer.flush(30)
    total_seconds = time.perf_counter() - start
    delivered_total = sum(delivered.values())

    print(f"done: sent {total} in {send_seconds:.2f}s ({total / send_seconds:,.0f}/s), "
          f"delivered {delivered_total}, errors {len(errors)}, undelivered {remaining}")
    if errors:
        print("first error:", errors[0])

    if args.report:
        write_report(args, base, symbols, total, send_seconds, total_seconds, delivered, errors, sent)
        print("report written to", args.report)
    return 1 if errors or remaining else 0


def write_report(args, base, symbols, total, send_seconds, total_seconds, delivered, errors, sent):
    cpu = "unknown"
    try:
        with open("/proc/cpuinfo", encoding="utf-8") as f:
            cpu = next(line.split(":", 1)[1].strip() for line in f if line.startswith("model name"))
    except (OSError, StopIteration):
        pass

    delivered_total = sum(delivered.values())
    lines = [
        f"# Order simulator run — {datetime.now(timezone.utc):%Y-%m-%d}",
        "",
        "## Conditions",
        "",
        "| | |",
        "|---|---|",
        f"| Commit | `{os.environ.get('COMMIT', 'unknown')}` |",
        f"| CPU | {cpu} |",
        f"| Environment | {os.environ.get('BENCH_ENV', 'unspecified')} |",
        f"| Python | {platform.python_version()}, confluent-kafka {client_version()[0]}, librdkafka {libversion()[0]} |",
        f"| Kafka | `{args.bootstrap}`, topic `{args.topic}` |",
        "| Producer | acks=all, idempotent, linger.ms=5, lz4 |",
        f"| Target rate | {'max' if args.rate == 0 else f'{args.rate:,.0f}/s'} |",
        f"| Seed | {args.seed}, participants {base + 1}..{base + 110} |",
        "",
        "## Results",
        "",
        "| | |",
        "|---|---|",
        f"| Orders sent | {total:,} |",
        f"| Send time | {send_seconds:.2f} s ({total / send_seconds:,.0f} orders/s) |",
        f"| Until all delivered | {total_seconds:.2f} s ({delivered_total / total_seconds:,.0f} orders/s) |",
        f"| Delivered | {delivered_total:,} |",
        f"| Errors | {len(errors)} |",
        "",
        "Messages per partition (key = symbol):",
        "",
        "| Partition | Messages | Share |",
        "|---|---:|---:|",
    ]
    for partition, count in sorted(delivered.items()):
        lines.append(f"| {partition} | {count:,} | {100 * count / max(delivered_total, 1):.1f}% |")
    lines += ["", "Mix (ADD by order type, plus CANCEL and MODIFY):", "", "| Kind | Messages | Share |", "|---|---:|---:|"]
    for kind, count in sent.most_common():
        lines.append(f"| {kind} | {count:,} | {100 * count / max(total, 1):.1f}% |")

    with open(args.report, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    sys.exit(main())

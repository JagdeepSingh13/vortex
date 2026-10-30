# 0010. Latency benchmark method

- Status: accepted
- Date: 2026-10-31
- Milestone: M2

## Context

We need engine performance numbers that are honest, repeatable and explainable in interviews. Exchanges care about latency distributions (p99, p99.9), not averages.

## Options

1. **Custom harness**: replay a seeded order stream, time every request with `rdtscp`, report percentiles per request type plus throughput.
2. **Google Benchmark**: statistically careful micro-benchmarks, but reports per-iteration averages rather than distributions.
3. **Both.**

## Decision

Option 1, in `engine/bench/latency_bench.cpp`, run with `make bench`.

- Warm-up of 10% before measuring.
- Order generator in feedback mode (only targets live orders), generated outside the timed region.
- TSC calibrated against the steady clock; two-read timer overhead measured and subtracted.
- Throughput measured in a separate run without timers.
- Pinned to one core with `taskset`.
- Every result file records commit (`-dirty` if uncommitted), CPU, kernel, compiler, flags, pinning and workload.

## Consequences

- Closed loop: measures service time, not response time under load (coordinated omission). End-to-end, open-loop measurement belongs with the Kafka path (M5).
- Numbers are from a laptop in a WSL2 VM: about ±8% run-to-run; max values are OS noise. Comparisons must use the same machine and conditions.
- Google Benchmark can still be added later for micro-operations if a specific optimisation needs it.

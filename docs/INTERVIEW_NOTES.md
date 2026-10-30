# Interview notes

One entry per completed milestone, newest last. Keep each entry short enough to say out loud in two minutes.

Format:

```
## MN — Title (YYYY-MM-DD)
- What I built: one or two sentences.
- Why it is designed this way: the key decision(s), with ADR links.
- Trade-off I can defend: what I gave up and why that was acceptable.
- Numbers: only measured figures, with machine and conditions. "None yet" is fine.
- Likely questions: 2-3 questions an interviewer could ask, with short answers.
- Study further: concepts to read up on.
```

## M0 — Repository bootstrap (2026-10-30)
- What I built: a monorepo for the platform, with my existing C++ matching engine imported into `engine/` and a one-command build (`make engine && make test`) that runs inside a pinned Linux container.
- Why it is designed this way: git subtree keeps the engine's history in one repo ([ADR 0001](decisions/0001-import-engine-with-git-subtree.md)); a container build makes results independent of my Windows machine and avoids MinGW pain with librdkafka later ([ADR 0002](decisions/0002-build-engine-in-linux-container.md)).
- Trade-off I can defend: builds need Docker and are slower the first time; in exchange anyone can reproduce them from a clean clone.
- Numbers: none yet.
- Likely questions:
  - *Why a monorepo?* The services share event schemas and symbol config; one repo keeps them versioned together.
  - *Subtree vs submodule?* Submodules pin a commit in another repo and complicate cloning and CI; subtree copies the history in.
  - *Why pin the image by digest?* A tag like `24.04` can move; a digest cannot, so the build is repeatable.
- Study further: git subtree internals, Docker image digests and layer caching, CMake build types (Release vs Debug).

## M1 — Event contracts and symbol config (2026-10-31)
- What I built: versioned JSON Schemas for the `orders` and `trades` Kafka messages, a symbol registry (`config/symbols.yaml`) with tick and lot sizes, and 49 contract tests that run in a pinned Python container.
- Why it is designed this way: events carry integer ticks and lots and the engine converts to its internal fixed point at the edge ([ADR 0004](decisions/0004-prices-in-ticks-quantities-in-lots.md)); trades are identified by (engine_run_id, match_id) so duplicates can be removed after restarts ([ADR 0005](decisions/0005-trade-identity-run-id-plus-match-id.md)); trades include the aggressor side for order-flow analytics ([ADR 0006](decisions/0006-aggressor-side-in-trades.md)).
- Trade-off I can defend: the engine needs a conversion layer (ticks ↔ engine units) instead of using ticks natively, but matching code stays untouched and proven.
- Numbers: none measured (contract milestone).
- Likely questions:
  - *Why integers for prices?* Binary floats cannot represent most decimals exactly (0.1 + 0.2 ≠ 0.3); two equal prices might not compare equal.
  - *Your validator rejects 18990.0. Isn't that a valid JSON integer?* By the JSON Schema spec it is; I made the validator strict because a float on the wire means a producer is computing prices in floating point.
  - *How do you guarantee no duplicate trades downstream?* Delivery is at-least-once, so processing is idempotent on the unique key (engine_run_id, match_id).
  - *Why are some ids capped at 2^53?* JavaScript and many JSON parsers use doubles; above 2^53 integers silently lose digits. Nanosecond timestamps exceed it, so consumers must read them as BigInt or strings.
- Study further: IEEE 754, fixed-point arithmetic, JSON Schema conditionals, idempotency and delivery semantics, schema evolution.

## M2 — Engine benchmark, event time and tests (2026-10-31)
- What I built: a latency benchmark for the C++ matching engine that times every request with `rdtscp` and reports p50–p99.9 per request type plus throughput; UTC event time on every outgoing message; a golden replay test that pins matching behaviour across 300,000 requests.
- Why it is designed this way: percentiles per request type, not averages, because the tail is what traders feel ([ADR 0010](decisions/0010-latency-benchmark-method.md)); the order book keeps a deterministic logical clock and wall time is read once per request outside it, so replays stay identical ([ADR 0009](decisions/0009-event-time-read-once-per-request.md)); a small test suite where the golden test is the safety net for optimisation ([ADR 0011](decisions/0011-minimal-engine-test-suite.md)).
- Trade-off I can defend: the benchmark is closed-loop, so it measures service time, not queueing under load (coordinated omission); end-to-end open-loop measurement comes with the Kafka path.
- Numbers (Ryzen 5 5500U laptop, Docker on WSL2, GCC 13.3 -O3, pinned to one core, 2 M requests after 200 k warm-up, ±8% run to run): p50 361 ns, p99 2.0 µs, p99.9 8.4 µs per request; 2.11 M requests/s single-threaded; trades are the slow path (p99 5.2 µs); one wall-clock read ≈ 50 ns. Source: `benchmarks/results/2026-10-30-2bcee71.md`.
- Likely questions:
  - *Why not report the average?* Latency has a long tail; the mean (504 ns) describes almost no real request. p99 says what 1 in 100 requests experiences.
  - *How do you know the timer isn't distorting the numbers?* I measure two back-to-back rdtscp reads (~40 ns) and subtract it; throughput is measured in a separate run with no timers.
  - *Your max latency is 4 ms. Is the engine slow?* No: max values are OS and VM preemption (Docker inside WSL2 under Windows); p99.9 is 8 µs. Production would isolate cores and run on bare metal.
  - *How would you make it faster without breaking it?* Profile, change one data structure at a time, and require the golden replay test to pass unchanged plus a before/after benchmark.
- Study further: rdtsc/rdtscp and invariant TSC, coordinated omission, CPU caches, vDSO clock_gettime, golden/snapshot testing, ThreadSanitizer.

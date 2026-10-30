# 6. Benchmarking and latency

How `make bench` measures the engine, how to read the result, and what the numbers do and do not mean. Everything here refers to the first real result: [benchmarks/results/2026-10-30-2bcee71.md](../../benchmarks/results/2026-10-30-2bcee71.md).

## Latency vs throughput

Two different questions:

| | Latency | Throughput |
|---|---|---|
| Question | How long does **one** request take? | How **many** requests per second? |
| Unit | nanoseconds per request | requests per second |
| Our result | p50 361 ns, p99 2,044 ns | 2.11 million/s |
| Who cares | Traders: a slow fill is a worse price | The exchange: can it keep up with peak load? |

They are related but not inverses. 1 / mean latency = 1 / 504 ns ≈ 2.0 M/s, close to the measured 2.11 M/s. The small gap is because the throughput run has no timers in the loop. They diverge completely once there is queueing or parallelism.

## Why percentiles, not the average

Latency distributions have a **long tail**: most requests are fast, a few are very slow. The average hides that.

From our result, all requests (ns):

```
p50   361   ← half of requests are faster than this (the median)
p90   762
p99  2044   ← 1 in 100 is slower than this
p99.9 8407  ← 1 in 1,000
max  4.36 ms
mean  504   ← pulled up by the tail, describes almost no real request
```

Why the tail matters: a market maker sends thousands of requests a second. At p99 = 2 µs, **every second** about 1% of its requests take over 2 µs. The tail is what users actually feel, which is why exchanges and trading firms quote p99 and p99.9, not the mean.

**Never average percentiles** across runs or machines; the result is not a percentile of anything. Compare p99 to p99.

## How the harness works

[engine/bench/latency_bench.cpp](../../engine/bench/latency_bench.cpp):

```
1. warm-up      200,000 requests, not measured
2. latency run  2,000,000 requests, each one timed individually
3. throughput   fresh engine replays the same 2.2 M requests, timed as one block
4. report       percentiles per request type + machine + build → benchmarks/results/
```

### 1. Warm-up

The first requests are slow for reasons that have nothing to do with steady-state matching:
- **Cold caches**: the order book's memory is not yet in the CPU cache.
- **Page faults**: the first touch of each new memory page traps into the kernel.
- **Branch predictor** not yet trained on the code's patterns.
- **Pools and maps** growing to their working size.

Measuring those would mix "startup" with "steady state". Real exchanges also warm up before the market opens.

### 2. Timing each request with the TSC

```cpp
const std::uint64_t start = ticks();          // __rdtscp
const auto events = engine->process_order(order);
const std::uint64_t end = ticks();
```

- **TSC** (time stamp counter) is a CPU register that counts at a fixed rate (here 2.096 ticks per ns, calibrated against the OS clock over 200 ms). Reading it takes about 20 ns and needs no system call.
- **`rdtscp`** instead of `rdtsc`: it waits until earlier instructions finish, so the work being measured cannot be reordered after the timestamp.
- **Timer overhead** (84 ticks ≈ 40 ns for two reads) is measured and **subtracted**, so the numbers describe the engine, not the stopwatch.
- `std::chrono::steady_clock::now()` would also work, at about 20–30 ns per call on Linux. The TSC is cheaper and finer-grained.

### 3. Throughput without timers

Per-request timers add work, so throughput is measured in a separate run with no timers inside the loop. The result is also added into a checksum that gets printed, so the compiler cannot decide the loop does nothing and delete it (**dead code elimination**, a classic benchmark bug).

### 4. Workload: a realistic order stream

The same deterministic generator as the golden test ([order_stream.h](../../engine/support/order_stream.h)), but in **feedback mode**: it reads the engine's events and only cancels or modifies orders that are still live. Without feedback, 35% of requests were cancels of orders that had already filled. Those are cheap rejects (110 ns), and they made the engine look faster than it is. **The workload decides the numbers**, so it is described in every result file.

Mix in the measured run: 32% adds that rest, 11% adds that trade, 13% IOC/FOK/stop, 25% cancels, 13% modifies, 5% rejects; 28.6% of requests produce a trade; 2.44 events per request.

## Reading the per-request table

| Request | p50 | p99 | Why |
|---|---:|---:|---|
| rejected | 110 | 601 | Validation only; nothing touches the book |
| cancel | 200 | 1,012 | Hash lookup + unlink from the level's list |
| add, IOC/FOK/stop | 311 | 912 | Validation, maybe a quick match attempt, no resting |
| add, rests | 361 | 1,012 | Find or create the price level in a `std::map` (tree), append to its list |
| modify | 541 | 1,974 | Often cancel + re-add, so roughly the two added together |
| add, trades | 752 | 5,211 | Walks levels, emits trade + fill events per counterparty, may trigger stops |

The **tail of "add, trades"** (p99.9 = 37 µs) is the most interesting row. Likely causes to investigate:
- an order sweeping many price levels,
- stop orders triggering other stops (cascades),
- the memory pool behind the tree grabbing a new chunk,
- the OS or the WSL2 VM interrupting the thread.

## The max column: noise from the machine

Every max is in **milliseconds** (0.3–4.4 ms), roughly 1,000× the p99. The engine does not do anything that slow. This is the operating system: the thread was paused for an interrupt, another process, or the WSL2 VM itself being scheduled by Windows. Our setup adds layers:

```
engine thread → Docker container → WSL2 Linux VM → Windows scheduler → CPU
```

What we did to reduce noise:
- **CPU pinning**: `taskset -c 2` keeps the thread on one core, so it does not migrate and lose its cache.
- **Warm-up** before measuring.
- **Several runs** compared before committing one. Three runs gave p50 331–371 ns, p99 1.9–2.1 µs, throughput 1.98–2.32 M/s: about **±8% between runs**. Differences smaller than that are not meaningful.

What production systems also do (not available on a laptop VM): isolate cores from the scheduler (`isolcpus`), disable frequency scaling and turbo, pin interrupts elsewhere, run on bare metal.

## What the benchmark does NOT measure

Be precise about this in interviews:

1. **Only the matching code.** No network, no Kafka, no serialization. End-to-end latency (order in → trade out on Kafka) will be far higher, dominated by I/O. That is measured later (M5).
2. **Service time, not response time.** The benchmark is a **closed loop**: it sends the next request only after the previous one returns. Under real load, requests arrive on their own schedule and **queue** behind a slow one. A closed loop cannot see that queueing. This mistake is called **coordinated omission**: the benchmark politely waits during a stall instead of recording all the requests that would have piled up. An open-loop load test (fixed arrival rate) is the fix, and that belongs with the Kafka path.
3. **One machine, one workload.** Different CPU, different order mix, different book depth → different numbers. That is why every result file records commit, CPU, kernel, compiler, flags, pinning and workload.

## How to say it in an interview

> "On a Ryzen 5 5500U laptop, inside Docker on WSL2, single-threaded, the matching engine's median service time is about 360 ns per request and p99 about 2 µs, with roughly 2 million requests per second throughput. That is the matching code alone, measured with rdtscp in a closed loop over 2 million requests of a seeded realistic order stream. Runs vary about ±8%, and the max values are OS noise from the VM."

Every number in that sentence is measured and comes with its conditions.

## Running it yourself

```bash
make bench                         # 2 M requests, writes benchmarks/results/<date>-<commit>.md
make bench BENCH_REQUESTS=500000   # shorter run
make bench BENCH_CPU=4             # pin to another core
```

If there are uncommitted engine changes, the file name ends in `-dirty`, so a result can always be traced to the exact code.

## Check yourself

1. Why is the mean (504 ns) higher than the median (361 ns)?
2. Why must throughput be measured without per-request timers?
3. What does `rdtscp` guarantee that `rdtsc` does not?
4. Why did the feedback mode make the engine look *slower*, and why is that the right result?
5. The max is 4 ms. Is the engine slow? How would you find out?
6. What is coordinated omission, and why does a closed-loop benchmark suffer from it?
7. Run A has p99 2.0 µs, run B 2.1 µs after a code change. Did the change make things worse?

## Go deeper

- Gil Tene, "How NOT to Measure Latency" (talk): percentiles and coordinated omission
- HdrHistogram: recording latencies with fixed memory
- `perf stat` / `perf record`: counting cache misses and finding hot functions
- CPU caches and memory hierarchy (L1 ~1 ns, L2 ~4 ns, L3 ~10–15 ns, RAM ~80–100 ns)
- `isolcpus`, `nohz_full`, IRQ affinity: how trading firms remove OS noise

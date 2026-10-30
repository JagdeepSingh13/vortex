# 3. Running the engine

The engine can run in two modes. Both start in a Linux container (see [guide 1](01-docker-build-container.md)).

| | `make run-sim` | `make run-exchange` + `run-bots` / `run-trader` |
|---|---|---|
| Processes | 1 | 1 server + 1 per client |
| Bots talk to the engine via | In-memory queues | TCP sockets |
| Terminals needed | 1 | 2–3 |
| Good for | Seeing the engine work, debugging bots | Something closer to a real exchange; trading by hand |

## Mode 1: `make run-sim`

One program, [tools/simulation.cpp](../../engine/tools/simulation.cpp), with **five threads**:

```
  ┌───────────────────┐ requests ┌─────────────────────┐
  │ market maker AAPL │─────────▶│                     │
  │ market maker MSFT │─────────▶│    ENGINE THREAD    │
  │ noise trader      │─────────▶│  matching engine    │
  └───────────────────┘◀─────────│  order book AAPL    │
          fills / reports        │  order book MSFT    │
                                 └──────────┬──────────┘
                                            │ market data
                                 ┌──────────▼──────────┐
                                 │  MAIN THREAD        │
                                 │  dashboard, 1×/sec  │
                                 └─────────────────────┘
       every arrow = one SPSC lock-free queue
```

### The participants

| Who | What it does | Settings (from `simulation.cpp`) |
|---|---|---|
| **Market maker AAPL** | Keeps buy and sell quotes on both sides of a "fair price" that drifts randomly. Earns the spread; tries not to hold too much stock. | starts at 190.00, 5 levels each side, 100 shares per level, spread ±2 ticks, max position 2,000 |
| **Market maker MSFT** | Same, different stock | starts at 415.00, 5 levels, 50 shares, spread ±3 ticks, max position 1,000 |
| **Noise trader** | Sends a random IOC order (fill now or cancel) about every 20 ms. Its orders cross the spread and create trades. | both symbols, average 20 ms between orders |
| **Engine thread** | Pulls requests from each participant's queue, matches them, pushes reports back | single-threaded on purpose |
| **Main thread** | Reads market data and prints the dashboard every second | |

A tick here is 0.01: the smallest allowed price step.

### Why the engine is single-threaded

From [exchange/engine_runner.h](../../engine/src/exchange/engine_runner.h): the order book is **only ever touched by one thread**, so matching needs **no locks at all**. Real exchanges do the same: one thread per group of symbols, fed by queues. Locks would add unpredictable waiting (latency spikes) exactly where speed matters most.

The engine loop:

```
forever:
    for each participant:
        take up to 64 requests from its queue and process them
    nothing to do?  → yield the CPU briefly
```

The batch of 64 stops one very busy client from starving the others (fairness). A real exchange would spin on a dedicated CPU core instead of yielding. Yielding keeps the laptop from running one core at 100%.

### SPSC queues, the arrows in the diagram

[core/spsc_queue.h](../../engine/src/core/spsc_queue.h): **S**ingle **P**roducer, **S**ingle **C**onsumer. Exactly one thread writes, exactly one reads. That restriction is what makes it possible without locks:

```
buffer:  [ ][ ][x][x][x][ ][ ][ ]     fixed size, power of two, preallocated
                ▲        ▲
              head      tail
          (reader moves) (writer moves)
```

- The writer only moves `tail`; the reader only moves `head`. Each is an `std::atomic`.
- Full when `tail - head == N`; empty when `head == tail`.
- `index & (N-1)` instead of `index % N`: same result for powers of two, but much cheaper.
- `head` and `tail` sit 128 bytes apart (`alignas(128)`) so they never share a CPU cache line. Otherwise two cores would fight over that line (**false sharing**).
- `memory_order_release` on store / `acquire` on load guarantee the reader sees the data that was written *before* `tail` moved.
- No heap allocation, no locks: safe for the matching thread.

### What happens when a queue is full

Two deliberate rules ([engine_runner.h](../../engine/src/exchange/engine_runner.h)):
- **Private reports** (your fill, your cancel) must never be lost, so the engine waits for room.
- **Market data** is a stream of snapshots, and the next one replaces this one. If a slow reader's queue is full, the update is **dropped** and counted (`md dropped` on the dashboard) rather than stalling the engine.

### Reading the dashboard

```
===== t=3s   engine requests 1182 (395/s)   md dropped 0 =====
  AAPL   last   189.88   trades     86   volume     5485
         bid qty      bid |      ask  ask qty
             100   189.87 |   189.91  100
             100   189.86 |   189.92  100
             ...
  mm AAPL   fair   189.91  position   +321  pnl     +50.76  fills     86  requests     419
  noise        fills    193  volume     9617  requests     120
```

| Field | Meaning |
|---|---|
| `t=3s` | Seconds since start |
| `engine requests 1182 (395/s)` | Total requests the engine processed; rate over the last second |
| `md dropped` | Market-data updates dropped because a reader was slow (0 is good) |
| `last` | Price of the most recent trade |
| `trades`, `volume` | Number of trades and total shares traded |
| bid / ask columns | Top 5 price levels: best bid (highest buy) and best ask (lowest sell) on the first row. The gap between them is the **spread**. |
| `fair` | The price the market maker currently believes is fair |
| `position` | Shares the market maker holds (+ long, − short) |
| `pnl` | Profit and loss, marked to the fair price |
| `fills` | How many of its orders were (partly) executed |

**Not a benchmark.** `395/s` is how fast the bots *send* orders, because the noise trader waits about 20 ms on purpose. It says nothing about how fast the engine *can* match. Real latency and throughput numbers come from the M2 benchmark, reported with machine and conditions.

## Mode 2: exchange server + clients

```
 Terminal 1                       Terminal 2              Terminal 3
 make run-exchange                make run-bots           make run-trader
 ┌──────────────────────────┐
 │ container vortex-exchange│
 │                          │
 │ gateway thread ◀──TCP────┼──── liquidity_bot     ┐ started with docker exec,
 │   │ SPSC                 │                       │ so they run inside the same
 │ engine thread            │◀──TCP── trader_terminal┘ container
 │   │ SPSC                 │
 │ main thread: dashboard   │◀── port 9000 also open to Windows (-p 9000:9000)
 └──────────────────────────┘
```

Differences from `run-sim`:
- A **gateway thread** owns all network sockets. It turns TCP bytes into requests and pushes them into the same kind of SPSC queues. The engine thread never touches the network.
- Up to 16 clients can connect (16 session slots).
- Clients speak a small binary protocol ([net/wire.h](../../engine/src/net/wire.h), [exchange/protocol.h](../../engine/src/exchange/protocol.h)).

Step by step:

1. **Terminal 1:** `make run-exchange` prints `exchange listening on port 9000` and then a dashboard every 2 seconds.
2. **Terminal 2:** `make run-bots` connects market makers and a noise trader. The dashboard shows `clients 3` and trades start.
3. **Terminal 3:** `make run-trader` gives an interactive prompt. Type `help`. Commands include `buy`, `sell`, `book AAPL`, `orders`, `pos`, `quit`.
4. Stop clients first (Ctrl+C or `quit`), then the exchange (Ctrl+C in terminal 1). Clients started with `docker exec` die when the exchange container stops anyway.

Because port 9000 is published, a Windows build of `trader_terminal.exe` (from the old `cpp_exchange` repo) should also be able to connect to `127.0.0.1 9000`. Not tested yet.

## What happens on Ctrl+C

1. The terminal sends `SIGINT` to the container.
2. `tini` (from `--init`) forwards it to our program.
3. The program's handler sets `g_running = false`. It does nothing else, because only very simple things are safe inside a signal handler.
4. Every thread checks `g_running` in its loop, sees `false`, and returns.
5. `main` joins the threads, prints `simulation stopped`, and exits.
6. `--rm` deletes the container.

That is a **graceful shutdown**: threads finish their current step instead of being killed mid-update.

## Check yourself

1. Why does the engine run on a single thread? What would locks cost?
2. What makes an SPSC queue safe without a lock? What breaks if two threads push?
3. Why is market data allowed to be dropped, but fill reports are not?
4. What is the spread, and who earns it?
5. Why is "395 requests/s" not a measure of engine performance?
6. In mode 2, why does the engine thread never touch a socket?

## Go deeper

- Order types: limit, market, IOC, FOK, iceberg, stop. All are supported by `order_book.h`.
- Price-time priority (FIFO) matching
- C++ memory model: `memory_order_acquire` / `release`, happens-before
- False sharing and cache lines
- The LMAX Disruptor (a famous lock-free design for exchanges)
- What async-signal-safe means

# 0009. Event time: read the UTC clock once per request

- Status: accepted, implemented in M2
- Date: 2026-10-31
- Milestone: M2

## Context

`trade.v1.event_time_ns` must be UTC nanoseconds since the epoch, set by the engine. Today the engine stamps requests with a **logical clock** (1, 2, 3, ...) in `ExchangeCore::handle`, on purpose: replays are deterministic, which the golden replay test depends on.

## Options

1. **Once per request in ExchangeCore.** Read `CLOCK_REALTIME` when a request is handled on the engine thread (a vDSO call, ~20–30 ns, no system call) and attach it to every event of that request. The order book keeps the logical clock.
2. **In the publisher thread.** Zero cost on the matching thread, but the time includes queueing delay: publish time, not match time.
3. **Replace the logical clock.** Simplest, but replays stop being deterministic, which rules out the golden test.

## Decision

Option 1. Matching stays deterministic; event time is accurate to the request.

## Consequences

- Both clocks coexist: the logical clock for ordering and replay, wall time for `event_time_ns`.
- The wall time must not influence matching; the golden replay test proves that (its text format deliberately omits wall time).
- Measured cost of one `wall_clock_ns()` read: about 50 ns (90 ns minus 40 ns timer overhead) on the Ryzen 5 5500U inside WSL2. That is ~14% of a median request, so a later optimisation could derive wall time from the TSC instead.
- Implemented in `ExchangeCore::handle`; `ExecutionReport`, `TradeUpdate` and `BookUpdate` carry `event_time_ns`. The golden replay test passed unchanged.

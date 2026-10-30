# 0009. Event time: read the UTC clock once per request

- Status: accepted (implementation in progress)
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
- Cost per request is measured with `make bench` before and after the change.

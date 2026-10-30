# 0006. Include the aggressor side in trades

- Status: accepted
- Date: 2026-10-31
- Milestone: M1

## Context

Every trade has a passive order (resting in the book) and an aggressive order (the incoming one that crossed the spread). Which side was aggressive tells you whether buyers or sellers were pushing the price. The engine's `Trade` event does not carry it today.

## Options

1. **Include `aggressor_side` in `trade.v1`.** Enables buy vs sell volume, order-flow imbalance and similar gold metrics. The engine must add it in M5; the order book already knows which order is incoming when it emits a trade.
2. **Leave it out.** The contract matches today's engine exactly. Adding it later is a breaking change (v2 schema and a backfill gap).

## Decision

Option 1: it is cheap now and expensive later.

## Consequences

- `trade.v1.aggressor_side` is required, `BUY` or `SELL`.
- M5 adds the side to the engine's `Trade` event. That changes an event struct, not matching behaviour, but it still gets a test (hard rule: no matching change without a test).

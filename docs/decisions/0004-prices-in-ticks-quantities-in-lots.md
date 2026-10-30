# 0004. Prices in ticks and quantities in lots in every event

- Status: accepted
- Date: 2026-10-31
- Milestone: M1

## Context

The hard rule is "integers, never floats" for prices and quantities. The engine already uses integers, but in its own unit: one global fixed-point scale, `core::PRICE_SCALE = 10'000` (189.90 is stored as 1,899,000). The rest of the platform needs a unit for events and tables.

## Options

1. **Ticks in the contract, engine converts at its edge.** Events carry `price_ticks` and `qty_lots`; `config/symbols.yaml` gives each symbol's tick size and lot size. The engine multiplies by `tick_size * PRICE_SCALE` when reading and divides when writing. Matching code is untouched.
2. **Engine switches to ticks internally.** Cleanest model, but changes matching code and the bots, and needs an old-vs-new equivalence test.
3. **Engine scale in the contract** (`price_e4`). Simplest, but every symbol shares one precision and the meaning of the number depends on an engine constant.

## Decision

Option 1. The contract speaks the market's language (ticks and lots), and the engine's internal representation stays an implementation detail.

## Consequences

- `config/symbols.yaml` is the single source of truth for units. Decimal values are strings (`"0.01"`), never YAML floats.
- A test checks that one tick is a whole number of engine units for every symbol, so conversion is always exact.
- Conversion code goes in the engine's Kafka I/O layer in M5, outside the matching thread's hot path (it is one multiplication anyway).
- Decimals appear only in gold models and the UI: `price = price_ticks * tick_size`.

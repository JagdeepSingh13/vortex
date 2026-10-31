# 0005. Trade identity: engine run id plus match id

- Status: accepted, corrected in M5 (see Correction below)
- Date: 2026-10-31
- Milestone: M1

## Context

Bronze and silver (M6, M7) must remove duplicates, for example after a Kafka producer retry or a Spark restart. That needs a key that is unique per trade. The engine's `match_id` restarts at 1 every time the engine starts, so two runs produce trades with the same id.

## Options

1. **`(engine_run_id, match_id)`.** At startup the engine picks a random 64-bit id, written as 16 hex characters. No disk I/O; survives restarts; collisions are practically impossible (1 in 2^64 per pair of runs).
2. **Persist the counter.** Save the last `match_id` to disk and continue from it. A single integer key, but adds file I/O at startup and breaks if the file is lost or two engines share it.
3. **Keep `match_id` only** and dedupe on `(match_id, event_time_ns)`. Least work, but relies on timestamps never colliding; hard to defend.

## Decision

Option 1.

## Consequences

- `trade.v1` has `engine_run_id` (string, `^[0-9a-f]{16}$`) and `match_id`. Silver deduplicates on both.
- `engine_run_id` is a string, not a number: a 64-bit random value exceeds 2^53 and would be corrupted by JavaScript and other double-based JSON parsers.
- Useful side effect: every trade records which engine run produced it, which helps debugging.

## Correction (2026-10-31, M5)

The key above is wrong. `match_id` is counted **per order book**, and the engine keeps one book per
symbol, so within one run AAPL and MSFT both have a trade with `match_id` 1 (visible in the golden
replay output: `match=1 MSFT`, `match=1 AAPL`). With six symbols, six different trades share every
match id. Deduplicating silver on (`engine_run_id`, `match_id`) would have **silently deleted real
trades**.

The unique key is **(`engine_run_id`, `symbol`, `match_id`)**. Found while writing the M5 trade
checker, which now verifies that this key never repeats. Only descriptions and the dedup key change;
no field and no validation rule.

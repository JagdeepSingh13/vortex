# 0011. Minimal engine test suite

- Status: accepted
- Date: 2026-10-31
- Milestone: M2
- Supersedes: the per-rule unit tests written earlier in M2

## Context

M2 first added 51 unit tests, one per order-book rule, plus sanitizer builds that ran on every `make test`. Jagdeep wants the repository focused for a resume: performance work and benchmarks front and centre, without a large test tree to explain and maintain.

## Options

1. **Keep everything**: strongest evidence of correctness, but a lot of code that is not the story of the project.
2. **Slim (~15 tests)**: golden replay, original scenarios, queue and event-time tests.
3. **Minimal**: two tests that carry the most weight per line.

## Decision

Option 3:

- **Golden replay test**: 300,000 seeded requests; the full output must match `engine/tests/golden/` exactly. It covers every order type and reject path the generator produces, and it is the safety net that makes optimisation possible.
- **SPSC multi-threaded test**: 2 million items between two threads, nothing lost or reordered; under ThreadSanitizer it also checks the memory ordering.

Sanitizer builds stay available but run only with `make test-all`.

## Consequences

- `make test` is fast (a few seconds of engine tests plus the contract tests).
- Rules the stream rarely exercises are not pinned individually (for example market orders into an empty book, pool exhaustion). If one breaks, the golden test may or may not notice. Add a targeted test when a bug is found there.
- Event time (ADR 0009) has no automated test; it was checked when implemented and is visible in every message.
- `make test-all` should be run before merging engine changes.

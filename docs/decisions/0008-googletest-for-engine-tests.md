# 0008. GoogleTest for engine tests

- Status: accepted
- Date: 2026-10-31
- Milestone: M2

## Context

The engine had one test binary with a hand-rolled `CHECK` macro: ctest saw only "one binary passed or failed". The engine builds with `-fno-exceptions`.

## Options

1. **GoogleTest**: industry standard; `gtest_discover_tests` gives one ctest entry per test; works with exceptions disabled in test code.
2. **Catch2 v3**: readable syntax, slower compiles.
3. **doctest**: fastest compiles, less common.
4. **Keep the custom macro**: no dependency, no per-test reporting or filtering.

## Decision

GoogleTest 1.14, from Ubuntu's `libgtest-dev` package pinned to `1.14.0-1` in the build image. No network access needed at build time.

## Consequences

- Each `TEST()` shows up in ctest; `--gtest_filter` runs a subset.
- The original 8 smoke scenarios were first ported unchanged; later in M2 the suite was cut down to two tests (ADR 0011).
- Upgrading GoogleTest means bumping the pinned apt version and the image tag.

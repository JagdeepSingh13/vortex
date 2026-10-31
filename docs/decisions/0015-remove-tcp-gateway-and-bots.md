# 0015. Remove the TCP gateway, bots and trader terminal

- Status: accepted
- Date: 2026-10-31
- Milestone: M5

## Context

The engine imported in M0 had two halves of about the same size:

- the **matching core**: `core/`, `matching/`, plus `exchange_core.h` and `protocol.h` (~2,440 lines), and
- a **TCP and bot layer**: `bots/`, `client/`, `net/`, `tcp_gateway.h`, `engine_runner.h`, `session.h`, `exchange_main.cpp` and `tools/` (simulation, liquidity bot, trader terminal), about 2,430 lines.

After M5 the platform's entry point is Kafka: the Python simulator (M4) replaces the bots, the Kafka service replaces the TCP gateway, and `make kafka-tail` replaces the trader terminal for watching activity. Nothing in the core includes anything from the second half. Jagdeep wants a focused repository for his resume.

## Options

1. **Remove the layer** once the Kafka entry point works. Halves the engine's code; the repository tells one story.
2. **Keep it, built by default**: two parallel front ends to maintain and explain.
3. **Keep it behind a CMake option** (off by default): the code stays in the tree but nobody builds or tests it, so it rots.

## Decision

Option 1, done as the last step of M5, after `vortex_engine` was working and benchmarked, so a runnable engine existed at every point.

## Consequences

- Removed: `src/bots/`, `src/client/`, `src/net/`, `src/exchange/{tcp_gateway,engine_runner,session}.h`, `src/exchange_main.cpp`, `tools/`, the `cpp_exchange_net` CMake library and four executables, and the `make run-sim`, `run-exchange`, `run-bots`, `run-trader` targets.
- Nothing is lost: the code remains in this repository's git history (imported with its full history in M0, ADR 0001) and in the original `cpp_exchange` repository, so it can still be shown when an interviewer asks about network programming.
- Learn guides 1–3 used these tools as examples; they now carry a short note pointing to the Kafka-based commands. Their concepts (Docker, CMake, single-threaded matching, SPSC queues) still apply.
- The golden replay test, the SPSC test and the latency benchmark only use the core, so they are unaffected.

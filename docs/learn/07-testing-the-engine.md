# 7. Testing the engine

Two tests protect the engine. They were chosen because each one covers a lot per line of code ([ADR 0011](../decisions/0011-minimal-engine-test-suite.md)).

| Test | What | Catches |
|---|---|---|
| Golden replay | 300,000 seeded requests through the engine; the exact output must match `engine/tests/golden/` | **Any** change in matching behaviour |
| SPSC threads | 2 million items from a producer thread to a consumer thread | Lost or reordered items; with ThreadSanitizer, memory-ordering bugs |

```bash
make test        # engine tests + contract tests, a few seconds
make test-all    # plus AddressSanitizer/UBSan and ThreadSanitizer builds (slow)
```

## Golden replay: the safety net for performance work

Optimising a matching engine means rewriting hot code: swapping data structures, changing memory layout. The danger is changing *what* it does while changing *how fast*. A golden test (also called a snapshot or approval test) answers that directly:

1. A deterministic generator ([order_stream.h](../../engine/support/order_stream.h)) produces the same 300,000 requests every time: limit, market, IOC, FOK, GTC, post-only, iceberg, stop, stop-limit, cancels and modifies on two symbols.
2. Every request and every event the engine emits is written as a line of text ([event_text.h](../../engine/tests/event_text.h)): 889,167 lines.
3. The first 1,000 requests' lines are stored in full (`replay_small.txt`, readable in a diff). All lines are summarised as counts plus an FNV-1a hash (`replay_large.txt`).
4. The test fails on the first line that differs and prints it.

It only works because the engine is **deterministic**: the order book uses a logical clock, not wall time, so the same input always gives the same output. That is why event time (`event_time_ns`) is read *outside* the book ([ADR 0009](../decisions/0009-event-time-read-once-per-request.md)) and is not part of the golden text.

I checked that it really catches changes: editing one trade price in the golden file made the test fail at exactly that line.

```bash
make test-engine                            # must stay green after every optimisation
VORTEX_UPDATE_GOLDEN=1 make test-engine     # only when a behaviour change is intended,
git diff engine/tests/golden                # then review the diff before committing
```

**Limit:** it only checks what the generator produces. A rare rule (a market order into an empty book, running out of order slots) may never appear in the stream. If a bug is found there, add a focused test for it.

## SPSC threads test

The lock-free queue is the one piece of code shared between threads, and concurrency bugs are the hardest to see. The test pushes 2 million numbers through a 1,024-slot queue from one thread and checks, on the other thread, that they arrive in order with none missing. The small queue keeps it full or empty most of the time, so the edge cases (wrap-around, full, empty) are hit millions of times.

Under **ThreadSanitizer** (`make test-tsan`) the compiler also records every memory access and reports any two threads touching the same memory without proper synchronisation. It found nothing, which is evidence that the queue's `acquire`/`release` ordering is correct.

## Sanitizers

| | Flag | Finds | Slowdown |
|---|---|---|---|
| AddressSanitizer | `-fsanitize=address` | out-of-bounds, use-after-free, leaks | ~2× |
| UndefinedBehaviorSanitizer | `-fsanitize=undefined` | signed overflow, bad shifts, misaligned access | small |
| ThreadSanitizer | `-fsanitize=thread` | data races | 5–15× |

ASan and TSan instrument memory in incompatible ways, so they need separate builds.

## A trap found along the way: views that do not own

```cpp
const auto a = book.add_order(...);   // span into the book's event buffer
const auto b = book.add_order(...);   // overwrites that buffer
for (auto& e : a) ...                 // reads b's events, not a's!
```

`add_order` returns a `std::span`, a non-owning view into a buffer the book reuses on every call. Avoiding a copy is part of what makes it fast, but the span is only valid until the next call. One of the earlier unit tests made exactly this mistake. Same rule as `std::string_view`: **a view never owns, so know who does and for how long.**

## Check yourself

1. Why is a golden test more useful than many small unit tests during optimisation?
2. Why must the order book be deterministic for a golden test to work?
3. When is it right to regenerate the golden files?
4. What could break without the golden test noticing?
5. Why does a 1,024-slot queue make a better concurrency test than a 1-million-slot one?

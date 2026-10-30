# 7. Testing the engine (short version)

What protects the engine while we change and optimise it. `make test` runs all of it.

## Three layers

| Layer | What | Catches |
|---|---|---|
| Unit tests (GoogleTest) | 51 small tests, one rule each: priority, every order type, stops, icebergs, modify, rejects, SPSC queue, memory pool | A specific rule breaking |
| Golden replay | 300,000 seeded requests through the engine; the exact output must match `engine/tests/golden/` | **Any** change in matching behaviour, even ones no unit test thought of |
| Sanitizers | Same tests compiled with ASan + UBSan, and with TSan | Memory errors, undefined behaviour, data races |

## The golden replay test is the safety net for performance work

Optimising a matching engine means rewriting hot code: swapping data structures, changing memory layout. The danger is changing *what* it does while changing *how fast*. The golden test answers that in one line: if any of 889,167 output lines differs, it fails and prints the first difference.

```bash
make test-engine                              # must stay green after every optimisation
VORTEX_UPDATE_GOLDEN=1 make test-engine       # only when a behaviour change is intended,
git diff engine/tests/golden                  # then review the diff before committing
```

Small file (`replay_small.txt`, first 1,000 requests): readable, diffable. Large file (`replay_large.txt`): counts + FNV-1a hash of all 300,000 requests' output. I checked that changing one trade price in the golden file makes the test fail at exactly that line.

## Sanitizers

The compiler inserts extra checks into the program:

| | Flag | Finds | Slowdown |
|---|---|---|---|
| AddressSanitizer | `-fsanitize=address` | out-of-bounds, use-after-free, leaks | ~2× |
| UndefinedBehaviorSanitizer | `-fsanitize=undefined` | signed overflow, bad shifts, misaligned access | small |
| ThreadSanitizer | `-fsanitize=thread` | data races between threads | 5–15× |

TSan runs the 2-million-item producer/consumer queue test and found no races, which is evidence that the queue's acquire/release ordering is correct. ASan and TSan cannot be combined, hence two builds (`make test-asan`, `make test-tsan`).

## A trap found while writing the tests

```cpp
const auto a = book.add_order(...);   // span into the book's event buffer
const auto b = book.add_order(...);   // overwrites that buffer
for (auto& e : a) ...                 // reads b's events, not a's!
```

`add_order` returns a `std::span`, a non-owning view into a buffer the book reuses on every call. Avoiding a copy is what makes it fast, but a span is only valid until the next call. My first sequencing test made exactly this mistake and failed. The fix was to consume each span immediately. Same rule as `std::string_view`: **a view never owns, so know who does and for how long.**

## Check yourself

1. Why is a golden test more useful than unit tests during optimisation?
2. When is it right to regenerate the golden files?
3. Why can't ASan and TSan run in the same build?
4. Why is returning a `span` faster than returning a `std::vector`, and what is the price?

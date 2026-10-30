# 5. Event contracts

## Why contracts come first

Five different programs, in three languages, will exchange messages through Kafka:

```
simulator (Python) ─┐                    ┌─▶ Redis updater (Java)
Order API (Java) ───┼─▶ orders ─▶ engine (C++) ─▶ trades ─┼─▶ Spark bronze job (Python)
                    ┘                    └─▶ ... dbt, dashboard
```

If the simulator writes `"price": 189.9` and the engine expects `"price_ticks": 18990`, nothing crashes at first. Orders are rejected or, worse, silently matched at the wrong price. A **contract** is a written, machine-checked agreement on the exact shape of every message, so each team (here: each milestone) can build its side independently and know it will fit.

A contract has three parts in this repo:

| Part | File | Role |
|---|---|---|
| The rules | `schemas/order.v1.json`, `schemas/trade.v1.json` | What a valid message is |
| Examples | `schemas/examples/{order,trade}/{valid,invalid}/` | Concrete messages, good and bad |
| Tests | `schemas/tests/` (`make test-schemas`) | Proof that the rules accept the good ones and reject the bad ones |

## Units: ticks, lots and fixed point

### Why never floats

Computers store `float`/`double` in binary. Most decimal fractions cannot be represented exactly:

```python
>>> 0.1 + 0.2
0.30000000000000004
>>> 189.90 * 100
18990.000000000004
```

In a matching engine that is a real bug: two orders at "189.90" might not compare equal, so they would not match. Money is always stored as integers.

### Three integer units in this project

| Unit | Example for 189.90 USD | Who uses it | Why |
|---|---|---|---|
| **ticks** | `18990` | events, bronze, silver | A tick is the smallest price step for a symbol (0.01 for AAPL). Prices are always whole ticks. |
| **engine units** | `1,899,000` | inside the C++ engine | The engine's global fixed point: currency × 10,000 (`PRICE_SCALE`) |
| **decimal** | `189.90` | gold models, UI | For humans. Computed as `ticks × tick_size` with decimal arithmetic. |

The conversion factors live in [config/symbols.yaml](../../config/symbols.yaml):

```yaml
- symbol: AAPL
  tick_size: "0.01"     # a string, so no tool parses it as a binary float
  lot_size: 1           # shares per lot
```

```
ticks ──× tick_size──▶ currency ──× 10000──▶ engine units
18990      0.01         189.90                1,899,000
```

A test checks, for every symbol, that one tick is a **whole number** of engine units (`0.01 × 10000 = 100`). If someone added a symbol with tick size `0.00001`, one tick would be 0.1 engine units, and conversion would have to round. The test would fail before anything broke.

**Lots** work the same way for quantity: an exchange may only trade in blocks (for example 100 shares). Today `lot_size` is 1 for both symbols, so lots equal shares.

Decision record: [ADR 0004](../decisions/0004-prices-in-ticks-quantities-in-lots.md).

## How JSON Schema works

JSON Schema is a JSON document that describes other JSON documents. A validator reads both and reports every rule that is broken. The main keywords used here:

| Keyword | Meaning | Example from our schemas |
|---|---|---|
| `type` | Must be this JSON type | `"type": "integer"` |
| `required` | These fields must be present | `["order_id", "symbol", ...]` |
| `additionalProperties: false` | No fields except the declared ones | Catches typos like `"prise_ticks"` |
| `enum` / `const` | One of these exact values / exactly this value | `"enum": ["BUY", "SELL"]`, `"const": 1` |
| `minimum` / `maximum` | Number range | `qty_lots` 1 to 4,294,967,295 |
| `pattern` | String must match a regex | `^[A-Z][A-Z0-9.]{0,7}$` |
| `$ref` + `$defs` | Reuse a definition | `"$ref": "#/$defs/safe_positive_int"` |
| `if` / `then` / `else` | Conditional rules | "if order_type is MARKET then no price" |
| `not` | The inner rule must fail | `"not": {"required": ["price_ticks"]}`: price must be absent |
| `allOf` | All of these sub-schemas must pass | A list of independent conditionals |
| `propertyNames` | Rules on the field *names* | CANCEL may only use these six names |

### Walking through `order.v1`

**1. Fields every order has** (top-level `required`):

```json
"required": ["schema_version", "order_id", "symbol", "action", "participant_id", "client_time_ns"]
```

**2. What ADD needs**: a conditional on `action`:

```json
{ "if":   { "properties": { "action": { "const": "ADD" } } },
  "then": { "required": ["side", "order_type", "qty_lots"], "allOf": [ ...per-type rules... ] } }
```

Read it as: *if* the `action` field equals `"ADD"`, *then* `side`, `order_type` and `qty_lots` are required, and the per-type rules apply.

**3. Per-type rules**, copied from what the engine's `validate_new_order` checks:

| order_type | `price_ticks` | `trigger_price_ticks` | `display_qty_lots` |
|---|---|---|---|
| LIMIT, IOC, FOK, GTC, POST_ONLY | required | forbidden | forbidden |
| MARKET | forbidden | forbidden | forbidden |
| STOP | forbidden | required | forbidden |
| STOP_LIMIT | required | required | forbidden |
| ICEBERG | required | forbidden | required |

Each column is one `if/then/else`. For example, the price column:

```json
{ "if":   { "properties": { "order_type": { "enum": ["MARKET", "STOP"] } } },
  "then": { "not": { "required": ["price_ticks"] } },
  "else": { "required": ["price_ticks"] } }
```

"Forbidden" is written `not: {required: [...]}`: "it is not the case that this field is present".

**4. CANCEL and MODIFY** restrict the allowed field names with `propertyNames`. A CANCEL that carries a `side` is rejected, because a cancel identifies the order by id only, and extra fields suggest the producer is confused.

### What JSON Schema cannot check

Some rules involve **two fields** or **outside data**, which JSON Schema cannot express:

| Rule | Where it is checked |
|---|---|
| `symbol` exists in `config/symbols.yaml` | `schemas/tests/contracts.py` (and later the simulator, engine, API) |
| `display_qty_lots ≤ qty_lots` for ICEBERG | The engine (`INVALID_ICEBERG_DISPLAY` reject) |
| MARKET needs liquidity, FOK needs enough, POST_ONLY must not cross, no duplicate ids | The engine. These depend on the current book, so only the engine can know. |

That is a general design lesson: **schemas check shape; the system that owns the state checks business rules.**

## Two traps worth knowing

### Trap 1: `18990.0` is an "integer" in JSON Schema

The JSON Schema spec says a number with a zero fractional part *is* an integer. So a standard validator accepts `"price_ticks": 18990.0`. We showed it in the M1 work:

```
spec validator errors : []          ← accepted
strict validator      : ['type']    ← rejected
```

Accepting it would be dangerous: some producer is clearly using floats, and the next value might be `18989.999999`. Our validator in [contracts.py](../../schemas/tests/contracts.py) redefines "integer" to mean an actual JSON integer:

```python
def _is_strict_integer(checker, instance):
    return isinstance(instance, int) and not isinstance(instance, bool)
```

(`bool` is excluded because in Python `True` is a subclass of `int`.) Every other consumer (Spark in M6, Java in M8) must be equally strict.

### Trap 2: numbers above 2^53 in JSON

JSON itself has no size limit for numbers, but JavaScript, and many JSON libraries, store every number as a 64-bit double. A double holds integers exactly only up to **2^53 = 9,007,199,254,740,992**. Beyond that, the last digits are silently changed:

```js
JSON.parse('{"t": 1793430000123456789}').t
// 1793430000123456800   ← last digits lost
```

What we did:
- **ids, prices, sequence numbers** are limited to 2^53 − 1 in the schema, so they are always safe.
- **`engine_run_id`** is a random 64-bit value, so it is a **hex string** (`"9f3c2a71b4e08d56"`), not a number.
- **`*_time_ns`** fields (~1.8 × 10^18) cannot fit under 2^53. They stay int64, which Python, Java and Spark read exactly, and the dashboard (M11) must read them as BigInt or strings. This is documented in the schema itself.

The lower bound on time fields (10^18 ns = 2001-09-09) also catches two classic bugs: sending **milliseconds** instead of nanoseconds (~1.8 × 10^12), and sending a **monotonic clock** value (nanoseconds since boot, ~10^13). The engine currently does the second one; M2 fixes it, and this contract would have caught it.

## Trade identity and duplicates

Kafka and Spark give **at-least-once** delivery by default: after a crash or retry, the same trade may arrive twice. The fix is **idempotent processing**: give every trade a unique key and drop repeats in silver.

The engine's `match_id` restarts at 1 each run, so on its own it is not unique. `trade.v1` adds `engine_run_id`, a random 64-bit id chosen at startup:

```
run 9f3c2a71b4e08d56: match 1, 2, 3, ... 5000      ┐ unique key =
run 1b77e0c2f9a4d310: match 1, 2, 3, ...           ┘ (engine_run_id, match_id)
```

The chance of two runs getting the same random 64-bit id is about 1 in 18 quintillion. Alternatives and trade-offs: [ADR 0005](../decisions/0005-trade-identity-run-id-plus-match-id.md).

## Aggressor side

Every trade has two orders: the **passive** one that was already resting in the book, and the **aggressive** one that arrived and crossed the spread. `aggressor_side = BUY` means a buyer was willing to pay the ask: buying pressure. Summing volume by aggressor side gives **order-flow imbalance**, a standard market-microstructure signal, which we will compute in gold. See [ADR 0006](../decisions/0006-aggressor-side-in-trades.md).

## The tests

[test_contracts.py](../../schemas/tests/test_contracts.py) runs 49 checks:

| Test | What it proves |
|---|---|
| `test_schema_is_valid_json_schema` | The schema files are themselves valid draft 2020-12 |
| `test_valid_example_passes` | Each of 14 valid examples has zero problems |
| `test_invalid_example_fails_for_stated_reason` | Each of 28 invalid examples fails, and **only** for the reason in its `expected_error`. One example tests exactly one rule. |
| `test_every_order_type_has_a_valid_add_example` | No order type was forgotten |
| `test_symbols_are_unique_and_match_schema_pattern` | `symbols.yaml` agrees with the schema's symbol pattern |
| `test_symbol_units_are_exact` | Tick sizes are decimal strings and convert exactly to engine units |
| `test_tick_conversion_example` | 18990 ticks = 189.90 USD = 1,899,000 engine units |

An invalid example looks like this:

```json
{
  "reason": "18990.0 is a float even though its value is whole",
  "expected_error": "type",
  "message": { "...": "...", "price_ticks": 18990.0 }
}
```

Why "fails for the stated reason" matters: a test that only checks "it failed" can pass for the wrong reason. If the example also had a typo in another field, the test would be green while the rule it was meant to test is broken.

Run them:

```bash
make test-schemas     # just the contracts
make test             # engine + contracts
```

## Versioning

`schema_version` is in every message, and file names carry the version (`order.v1.json`). Hard rule from `CLAUDE.md`: changing a schema needs a version bump and a note in the [ARCHITECTURE.md](../ARCHITECTURE.md) change log. Rough guide:
- **Compatible** change (add an optional field): consumers that ignore it still work. Still bump, still log.
- **Breaking** change (rename, remove, change a type or unit): new major version, and consumers must handle both during the switch.

Later, the project may move to Avro with a schema registry, which enforces these compatibility rules automatically.

## Check yourself

1. Why is `0.1 + 0.2 != 0.3` a problem for a matching engine?
2. Convert 415.07 USD of MSFT to ticks and to engine units.
3. Why is `tick_size` a string in YAML?
4. Write the `if/then/else` that makes `trigger_price_ticks` required for STOP and STOP_LIMIT and forbidden otherwise.
5. Why can't JSON Schema check `display_qty_lots ≤ qty_lots`? Where is it checked instead?
6. A browser shows a trade's `event_time_ns` ending in `...800` instead of `...789`. What happened?
7. Why is `engine_run_id` a string and not a number?
8. What does at-least-once delivery mean, and how does `(engine_run_id, match_id)` help?

## Go deeper

- IEEE 754 floating point; fixed-point and decimal arithmetic (`decimal.Decimal`, Java `BigDecimal`)
- JSON Schema docs: "Understanding JSON Schema" (json-schema.org)
- Delivery semantics: at-most-once, at-least-once, exactly-once; idempotency keys
- Schema evolution and compatibility (Avro, Confluent Schema Registry)
- Market microstructure: tick size, lot size, aggressor, order-flow imbalance

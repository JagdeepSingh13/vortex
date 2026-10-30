"""Contract tests: every valid example passes, every invalid example fails for exactly
the reason it claims, and config/symbols.yaml is consistent with the schemas."""

import re
from decimal import Decimal

import pytest
from jsonschema import Draft202012Validator

from contracts import (
    EXAMPLE_DIR, KINDS, load_json, load_schema, load_symbols, tick_size, validate,
)


def examples(kind, validity):
    paths = sorted((EXAMPLE_DIR / kind / validity).glob("*.json"))
    assert paths, f"no {validity} {kind} examples found"
    return [pytest.param(path, id=f"{kind}/{path.stem}") for path in paths]


# ---- schemas themselves ----

@pytest.mark.parametrize("kind", KINDS)
def test_schema_is_valid_json_schema(kind):
    Draft202012Validator.check_schema(load_schema(kind))


# ---- examples ----

@pytest.mark.parametrize("path", examples("order", "valid") + examples("trade", "valid"))
def test_valid_example_passes(path):
    kind = path.parts[-3]
    assert validate(kind, load_json(path)) == []


@pytest.mark.parametrize("path", examples("order", "invalid") + examples("trade", "invalid"))
def test_invalid_example_fails_for_stated_reason(path):
    kind = path.parts[-3]
    case = load_json(path)
    problems = validate(kind, case["message"])

    assert problems, f"expected failure ({case['reason']}) but message was accepted"
    # every problem must be the stated one, so each example tests exactly one rule
    assert {p.keyword for p in problems} == {case["expected_error"]}, problems


def test_every_order_type_has_a_valid_add_example():
    order_types = load_schema("order")["properties"]["order_type"]["enum"]
    covered = {
        load_json(path)["order_type"]
        for path in (EXAMPLE_DIR / "order" / "valid").glob("*.json")
        if load_json(path)["action"] == "ADD"
    }
    assert covered == set(order_types)


# ---- config/symbols.yaml ----

def test_symbols_are_unique_and_match_schema_pattern():
    pattern = re.compile(load_schema("order")["$defs"]["symbol"]["pattern"])
    names = [entry["symbol"] for entry in load_symbols()["symbols"]]

    assert len(names) == len(set(names))
    for name in names:
        assert pattern.fullmatch(name), name


@pytest.mark.parametrize("entry", load_symbols()["symbols"], ids=lambda e: e["symbol"])
def test_symbol_units_are_exact(entry):
    config = load_symbols()
    scale = config["engine_price_scale"]
    tick = tick_size(entry)

    # decimals as strings, never YAML floats
    assert isinstance(entry["tick_size"], str)
    assert tick > 0
    # one tick must be a whole number of engine price units, or conversion would round
    assert (tick * scale) == (tick * scale).to_integral_value()
    # display precision must be able to show one tick
    assert -tick.as_tuple().exponent <= entry["price_decimals"]
    assert isinstance(entry["lot_size"], int) and entry["lot_size"] >= 1


def test_tick_conversion_example():
    # 18990 ticks of AAPL at 0.01 = 189.90 USD = 1,899,000 engine units
    config = load_symbols()
    aapl = next(e for e in config["symbols"] if e["symbol"] == "AAPL")
    assert 18990 * tick_size(aapl) == Decimal("189.90")
    assert 18990 * tick_size(aapl) * config["engine_price_scale"] == 1_899_000

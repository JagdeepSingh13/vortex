"""Validation helpers for the event contracts in schemas/.

Two layers:
  1. JSON Schema (draft 2020-12) with a strict integer type: 18990.0 is rejected,
     because the hard rule is "never floats" and the spec would accept it.
  2. Checks JSON Schema cannot express: the symbol must exist in config/symbols.yaml.
"""

import json
from dataclasses import dataclass
from decimal import Decimal
from functools import lru_cache
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator, validators

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_DIR = REPO_ROOT / "schemas"
EXAMPLE_DIR = SCHEMA_DIR / "examples"
SYMBOLS_FILE = REPO_ROOT / "config" / "symbols.yaml"

KINDS = {"order": "order.v1.json", "trade": "trade.v1.json"}


def _is_strict_integer(checker, instance):
    # bool is a subclass of int in Python; float is never accepted, even 1.0
    return isinstance(instance, int) and not isinstance(instance, bool)


StrictValidator = validators.extend(
    Draft202012Validator,
    type_checker=Draft202012Validator.TYPE_CHECKER.redefine("integer", _is_strict_integer),
)


@dataclass(frozen=True)
class Problem:
    keyword: str  # JSON Schema keyword on the failing path, or "known_symbol"
    path: str     # where in the message
    message: str


def load_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_schema(kind):
    return load_json(SCHEMA_DIR / KINDS[kind])


def load_symbols():
    with open(SYMBOLS_FILE, encoding="utf-8") as f:
        return yaml.safe_load(f)


@lru_cache(maxsize=None)
def known_symbols():
    # cached: validate() is called thousands of times (simulator test), the file does not change
    return frozenset(entry["symbol"] for entry in load_symbols()["symbols"])


@lru_cache(maxsize=None)
def _validator(kind):
    return StrictValidator(load_schema(kind))


def _leaf_errors(error):
    # errors under anyOf/oneOf keep their details in .context
    if not error.context:
        yield error
    for child in error.context:
        yield from _leaf_errors(child)


def validate(kind, message):
    """Return every problem with the message; an empty list means valid."""
    validator = _validator(kind)
    problems = []

    for error in validator.iter_errors(message):
        for leaf in _leaf_errors(error):
            # report the failing keyword; for propertyNames the leaf is the enum of
            # allowed names inside it (schema path .../propertyNames/enum)
            keyword = "propertyNames" if "propertyNames" in leaf.schema_path else leaf.validator
            problems.append(Problem(keyword, "/".join(map(str, leaf.absolute_path)), leaf.message))

    # registry check only once the symbol is well formed, so one mistake is one problem
    symbol = message.get("symbol") if isinstance(message, dict) else None
    symbol_ok = not any(p.path == "symbol" for p in problems)
    if symbol_ok and isinstance(symbol, str) and symbol not in known_symbols():
        problems.append(Problem("known_symbol", "symbol", f"{symbol!r} is not in config/symbols.yaml"))

    return problems


def tick_size(entry):
    return Decimal(entry["tick_size"])

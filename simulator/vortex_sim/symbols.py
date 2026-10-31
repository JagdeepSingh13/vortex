"""Symbols and their units from config/symbols.yaml."""

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import yaml

DEFAULT_SYMBOLS_FILE = Path(__file__).resolve().parents[2] / "config" / "symbols.yaml"


@dataclass(frozen=True)
class Symbol:
    name: str
    tick_size: Decimal
    reference_ticks: int  # starting mid price, in ticks


def load_symbols(path=DEFAULT_SYMBOLS_FILE):
    with open(path, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    symbols = []
    for entry in config["symbols"]:
        tick = Decimal(entry["tick_size"])
        reference = Decimal(entry["reference_price"])
        ticks = reference / tick
        # the reference price must be a whole number of ticks, or the walk starts off-grid
        if ticks != ticks.to_integral_value():
            raise ValueError(f"{entry['symbol']}: reference_price {reference} is not a multiple of {tick}")
        symbols.append(Symbol(entry["symbol"], tick, int(ticks)))
    return symbols

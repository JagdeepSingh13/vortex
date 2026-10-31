"""The simulator only ever produces valid order.v1 messages (checked with the M1 validator)."""

import sys
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "simulator"))
sys.path.insert(0, str(REPO / "schemas" / "tests"))

from contracts import validate  # noqa: E402
from vortex_sim.market import OrderFlow  # noqa: E402
from vortex_sim.symbols import load_symbols  # noqa: E402

FIXED_TIME_NS = 1_793_430_000_000_000_000


def generate(count, seed=1):
    flow = OrderFlow(load_symbols(), seed=seed, participant_base=1000, clock=lambda: FIXED_TIME_NS)
    return [flow.next() for _ in range(count)]


def test_ten_thousand_orders_are_valid_and_cover_the_contract():
    messages = generate(10_000)

    for index, message in enumerate(messages):
        problems = validate("order", message)
        assert problems == [], f"message {index} is invalid: {problems}\n{message}"

    # every symbol, action and order type actually appears
    assert {m["symbol"] for m in messages} == {s.name for s in load_symbols()}
    assert {m["action"] for m in messages} == {"ADD", "CANCEL", "MODIFY"}
    types = Counter(m["order_type"] for m in messages if m["action"] == "ADD")
    assert set(types) == {"LIMIT", "MARKET", "IOC", "FOK", "GTC", "STOP", "STOP_LIMIT", "ICEBERG", "POST_ONLY"}

    # a new order never reuses an id of the same participant
    adds = [(m["participant_id"], m["order_id"]) for m in messages if m["action"] == "ADD"]
    assert len(adds) == len(set(adds))

    # same seed, same stream
    assert generate(1_000) == messages[:1_000]

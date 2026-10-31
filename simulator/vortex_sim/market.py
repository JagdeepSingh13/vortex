"""Order-flow model: turns a seeded random process into order.v1 messages.

No Kafka here, so it can be tested on its own and replayed exactly (same seed, same
messages). Everything is in integer ticks and lots.

Who trades:
  - one market maker per symbol keeps up to QUOTE_LEVELS buy and sell quotes around the
    mid price, and cancels its oldest quote to make room for a fresh one as the mid moves
  - NOISE_TRADERS noise traders send random orders of every type; many cross the spread
    and create trades, some rest, and they cancel or modify their own orders now and then

The mid of each symbol takes a random walk of one tick at a time.

The simulator does not see trades (open loop), so a cancel may target an order that is
already filled; the engine rejects it. That mirrors real clients, whose cancels race fills.
"""

import random
from collections import Counter, deque
from dataclasses import dataclass

QUOTE_LEVELS = 5          # per side, per market maker
QUOTE_QTY = (50, 200)     # lots
NOISE_TRADERS = 10
MAX_NOISE_LIVE = 2_000    # oldest noise orders are canceled beyond this

# what one step does, in percent
STEP_WEIGHTS = {"maker_quote": 50, "noise_order": 35, "noise_cancel": 10, "noise_modify": 5}

# noise order types, in percent
NOISE_TYPES = {
    "IOC": 35, "LIMIT": 25, "MARKET": 10, "POST_ONLY": 8, "FOK": 6,
    "ICEBERG": 6, "GTC": 4, "STOP": 3, "STOP_LIMIT": 3,
}
RESTING_TYPES = {"LIMIT", "GTC", "POST_ONLY", "ICEBERG", "STOP", "STOP_LIMIT"}
STOP_TYPES = {"STOP", "STOP_LIMIT"}


@dataclass
class LiveOrder:
    participant_id: int
    order_id: int
    symbol: str
    side: str
    order_type: str


class OrderFlow:
    def __init__(self, symbols, seed, participant_base, clock):
        """symbols: list of Symbol; participant_base: first participant id is base + 1;
        clock: function returning UTC ns (time.time_ns, or a fixed value in tests)."""
        self.random = random.Random(seed)
        self.clock = clock
        self.symbols = [s.name for s in symbols]
        self.mid = {s.name: s.reference_ticks for s in symbols}

        # one market maker per symbol, then the noise traders
        self.maker = {name: participant_base + 1 + i for i, name in enumerate(self.symbols)}
        self.noise = [participant_base + 101 + i for i in range(NOISE_TRADERS)]
        self.next_id = Counter()  # per participant: order ids are unique per participant

        self.quotes = {name: deque() for name in self.symbols}  # a maker's live quotes, oldest first
        self.noise_live = []

        self.steps = list(STEP_WEIGHTS)
        self.step_weights = list(STEP_WEIGHTS.values())
        self.noise_types = list(NOISE_TYPES)
        self.noise_type_weights = list(NOISE_TYPES.values())

    # ---------------- public ----------------

    def next(self):
        """The next order.v1 message, as a dict."""
        symbol = self.random.choice(self.symbols)
        if self.random.random() < 0.05:
            self.mid[symbol] += self.random.choice((-1, 1))

        step = self.random.choices(self.steps, self.step_weights)[0]
        if step == "noise_cancel" and self.noise_live:
            return self._cancel(self._take_noise_order())
        if step == "noise_modify" and self.noise_live:
            return self._modify_noise()
        if step == "noise_order":
            return self._noise_order(symbol)
        return self._maker_quote(symbol)

    # ---------------- market maker ----------------

    def _maker_quote(self, symbol):
        quotes = self.quotes[symbol]
        if len(quotes) >= 2 * QUOTE_LEVELS:
            return self._cancel(quotes.popleft())

        side = self.random.choice(("BUY", "SELL"))
        level = self.random.randint(1, QUOTE_LEVELS)
        price = self.mid[symbol] - level if side == "BUY" else self.mid[symbol] + level
        qty = self.random.randint(*QUOTE_QTY)
        message = self._add(self.maker[symbol], symbol, side, "LIMIT", qty, price_ticks=price)
        quotes.append(LiveOrder(self.maker[symbol], message["order_id"], symbol, side, "LIMIT"))
        return message

    # ---------------- noise traders ----------------

    def _noise_order(self, symbol):
        participant = self.random.choice(self.noise)
        side = self.random.choice(("BUY", "SELL"))
        order_type = self.random.choices(self.noise_types, self.noise_type_weights)[0]
        mid = self.mid[symbol]
        toward = 1 if side == "BUY" else -1   # direction that crosses the spread
        qty = self.random.randint(1, 100)
        fields = {}

        if order_type in ("IOC", "FOK", "LIMIT", "GTC"):
            # aggressive: at or through the opposite quotes
            fields["price_ticks"] = mid + toward * self.random.randint(0, 3)
        elif order_type == "POST_ONLY":
            fields["price_ticks"] = mid - toward * self.random.randint(1, 5)
        elif order_type == "ICEBERG":
            qty = self.random.randint(50, 500)
            fields["price_ticks"] = mid - toward * self.random.randint(0, 3)
            fields["display_qty_lots"] = self.random.randint(10, 50)
        elif order_type == "STOP":
            fields["trigger_price_ticks"] = mid + toward * self.random.randint(2, 10)
        elif order_type == "STOP_LIMIT":
            trigger = mid + toward * self.random.randint(2, 10)
            fields["trigger_price_ticks"] = trigger
            fields["price_ticks"] = trigger + toward
        # MARKET: no price

        message = self._add(participant, symbol, side, order_type, qty, **fields)
        if order_type in RESTING_TYPES:
            self.noise_live.append(LiveOrder(participant, message["order_id"], symbol, side, order_type))
            if len(self.noise_live) > MAX_NOISE_LIVE:
                self.noise_live.pop(0)  # forgotten, it simply stays in the book
        return message

    def _take_noise_order(self):
        index = self.random.randrange(len(self.noise_live))
        self.noise_live[index], self.noise_live[-1] = self.noise_live[-1], self.noise_live[index]
        return self.noise_live.pop()

    def _modify_noise(self):
        # stops cannot be modified (the engine rejects it): try a few random picks for a
        # limit-style order instead of scanning the whole list on every modify
        for _ in range(5):
            order = self.random.choice(self.noise_live)
            if order.order_type not in STOP_TYPES:
                break
        else:
            return self._noise_order(self.random.choice(self.symbols))
        away = -1 if order.side == "BUY" else 1
        return self._message(order.participant_id, order.order_id, order.symbol, "MODIFY",
                             price_ticks=self.mid[order.symbol] + away * self.random.randint(1, 5),
                             qty_lots=self.random.randint(1, 100))

    # ---------------- messages ----------------

    def _add(self, participant, symbol, side, order_type, qty, **fields):
        self.next_id[participant] += 1
        return self._message(participant, self.next_id[participant], symbol, "ADD",
                             side=side, order_type=order_type, qty_lots=qty, **fields)

    def _cancel(self, order):
        return self._message(order.participant_id, order.order_id, order.symbol, "CANCEL")

    def _message(self, participant, order_id, symbol, action, **fields):
        message = {
            "schema_version": 1,
            "order_id": order_id,
            "symbol": symbol,
            "action": action,
            "participant_id": participant,
            "client_time_ns": self.clock(),
        }
        message.update(fields)
        return message

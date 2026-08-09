"""The observed market state, held flat between observations (spec 3.4's rule, generalised).

Everything in this module is a *state*: the best bid, the ladder, the last print. States are
observed at instants and held flat until the next observation, exactly as spec 3.4 requires
of mark price, and for the same reason -- interpolating between two book snapshots would
invent a book that never existed and fill orders against it.

**Holding flat needs a bound, or it becomes fiction.** A ladder observed at 09:00 and held
flat until 09:45 is not a stale book, it is no book at all, and an order walking it would be
priced against liquidity that has had three quarters of an hour to disappear. So every
lookup takes the current clock and returns `None` past `MAX_QUOTE_STALENESS_MS`. The engine
turns that `None` into a named rejection, which is spec 4.5's `HALT_TRADING` behaviour --
"no fills, no new orders" during an outage -- applied at the granularity the data supports.

**The rolling volume window exists for one term in one formula.** Spec 6.4's `BOOK_TICKER`
impact is `k x sqrt(order_notional / recent_1min_notional_volume)`, so the denominator has to
be maintained as trades arrive. It is the traded notional over the last 60 000 ms inclusive
of the current instant -- not a per-second bucketing, because the window slides past every
event and a bucketed version would step rather than slide, making an order's modelled impact
depend on where in the second it happened to land.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

from perplab.core.types import DepthSnapshot
from perplab.engine.ticks import TopOfBook, TradePrint

__all__ = [
    "MAX_QUOTE_STALENESS_MS",
    "VOLUME_WINDOW_MS",
    "MarketView",
]

MAX_QUOTE_STALENESS_MS = 60_000
"""How long an observation may be held flat before it stops counting as one.

Spec 4.5's own number: *"gap if the inter-record interval exceeds a threshold (default 60 s)
during a period where klines show non-zero volume."* Using the same threshold the gap
detector uses means "the engine refused to fill here" and "the gap report flags this" are
the same condition rather than two nearby ones that can disagree -- and it means the
constant has a stated origin instead of being a number someone liked.

It is deliberately not tighter for `depth20` than for `bookTicker` despite `depth20` being a
1 s stream and `bookTicker` being event-driven. A second threshold would need its own
justification, and the failure it would catch -- a few missed depth samples in an otherwise
healthy minute -- is a fidelity question the gap report already answers, not a correctness
one this module can decide.
"""

VOLUME_WINDOW_MS = 60_000
"""Spec 6.4's `recent_1min_notional_volume`, in milliseconds."""


def _sane(bid_px: int, ask_px: int) -> bool:
    """Whether a quote is a market state at all: both sides priced, and not crossed.

    One predicate, used by both entry points into `MarketView`. They had two copies and the
    copies disagreed -- which is the shape of defect a shared helper exists to prevent, not a
    style preference.
    """
    return bid_px > 0 and ask_px > 0 and bid_px < ask_px


@dataclass
class _SymbolState:
    top: TopOfBook | None = None
    ladder: DepthSnapshot | None = None
    last_trade: TradePrint | None = None
    volume: deque[tuple[int, int]] = field(default_factory=deque)
    """`(ts_ms, notional)` for each trade in the window, oldest first.

    `notional` is `price x qty` in the lake's scaling, so it carries 10^16 rather than 10^8.
    It is only ever used as a ratio against another notional at the same scaling, so the
    scale cancels and is never unwound -- unwinding it would be a division per trade on the
    hottest path in the engine to produce a number nothing reads.
    """
    volume_total: int = 0


@dataclass
class MarketView:
    """What the engine knows about each symbol's market, right now.

    Updated by the event loop as `BOOK_UPDATE` and `TRADE` events are dispatched, and read
    by the fill models, the resting book, and `ctx.spread()` / `ctx.book()`. One object
    rather than three dictionaries on the engine, because the staleness rule has to be
    applied identically wherever a quote is read and a rule spread across three call sites
    is a rule with three chances to be forgotten.
    """

    _symbols: dict[str, _SymbolState] = field(default_factory=dict, init=False, repr=False)

    # ------------------------------------------------------------------------- updating

    def _state(self, symbol: str) -> _SymbolState:
        state = self._symbols.get(symbol)
        if state is None:
            state = _SymbolState()
            self._symbols[symbol] = state
        return state

    def apply_top(self, top: TopOfBook) -> None:
        """Record a `bookTicker` observation.

        Crossed or inverted quotes are refused rather than stored. A bid at or above the ask
        is not a market state; it is a mis-parsed row or an interleaving artefact, and every
        model downstream assumes `bid < ask` when deciding whether a limit order crosses.
        Storing one would make a post-only order that should have rested look marketable.
        """
        if not _sane(top.bid_px, top.ask_px):
            return
        self._state(top.symbol).top = top

    def apply_ladder(self, ladder: DepthSnapshot) -> None:
        """Record a `depth20` observation, and mirror its touch into the top of book.

        The mirror matters for a `BOOK_WALK` run over a range with no `bookTicker`
        coverage: without it `ctx.spread()` would return `None` on a run that is walking a
        full ladder, which reads as "no book" beside a fill model that plainly has one. When
        both streams are present the ladder is the richer observation and wins at an equal
        timestamp; a strictly fresher `bookTicker` row wins over an older ladder.

        The guard is deliberately the *same* predicate `apply_top` uses. It was briefly only
        the crossed-quote half, so a ladder whose best bid was zero could mirror a zero into
        the top of book while a `bookTicker` row carrying the same zero was refused -- and a
        zero-priced side halves the mid, which is what `_reference_price` measures every
        fill's slippage against.
        """
        state = self._state(ladder.symbol)
        if not _sane(ladder.bid_px[0], ladder.ask_px[0]):
            return
        state.ladder = ladder
        mirrored = TopOfBook(
            symbol=ladder.symbol,
            ts_ms=ladder.ts_ms,
            bid_px=ladder.bid_px[0],
            bid_qty=ladder.bid_qty[0],
            ask_px=ladder.ask_px[0],
            ask_qty=ladder.ask_qty[0],
        )
        if state.top is None or state.top.ts_ms <= ladder.ts_ms:
            state.top = mirrored

    def apply_trade(self, trade: TradePrint) -> None:
        """Record a print and add its notional to the rolling window."""
        state = self._state(trade.symbol)
        state.last_trade = trade
        notional = trade.price_scaled * trade.qty_scaled
        state.volume.append((trade.ts_ms, notional))
        state.volume_total += notional
        self._expire(state, trade.ts_ms)

    @staticmethod
    def _expire(state: _SymbolState, now_ms: int) -> None:
        """Drop window entries older than `VOLUME_WINDOW_MS`.

        Strictly older: the window is the half-open interval `(now - 60 000, now]`, so a
        trade exactly 60 000 ms ago has left and one at the current instant is in. Trades at
        the current instant have already printed, so counting them is causal.
        """
        cutoff = now_ms - VOLUME_WINDOW_MS
        volume = state.volume
        while volume and volume[0][0] <= cutoff:
            state.volume_total -= volume.popleft()[1]

    # -------------------------------------------------------------------------- reading

    def top_of_book(self, symbol: str, now_ms: int) -> TopOfBook | None:
        """The best bid/ask in force at `now_ms`, or `None` if there is none fresh enough."""
        state = self._symbols.get(symbol)
        if state is None or state.top is None:
            return None
        if now_ms - state.top.ts_ms > MAX_QUOTE_STALENESS_MS:
            return None
        return state.top

    def ladder(self, symbol: str, now_ms: int) -> DepthSnapshot | None:
        """The depth snapshot in force at `now_ms`, or `None` if there is none fresh enough."""
        state = self._symbols.get(symbol)
        if state is None or state.ladder is None:
            return None
        if now_ms - state.ladder.ts_ms > MAX_QUOTE_STALENESS_MS:
            return None
        return state.ladder

    def last_trade(self, symbol: str, now_ms: int) -> TradePrint | None:
        """The most recent print at or before `now_ms`, or `None` if it is too old.

        Staleness-bounded like the book, and for a sharper reason: at the `TRADE_ONLY` tier
        the last print is the only price in the model, so an hour-old one would price a fill
        against a market that has since moved without us seeing it.
        """
        state = self._symbols.get(symbol)
        if state is None or state.last_trade is None:
            return None
        if now_ms - state.last_trade.ts_ms > MAX_QUOTE_STALENESS_MS:
            return None
        return state.last_trade

    def recent_notional(self, symbol: str, now_ms: int) -> int:
        """Traded notional over the last `VOLUME_WINDOW_MS`, in `10^16` scaling.

        Expires on read as well as on write. Without that, a symbol that stopped trading
        would keep reporting the notional of its last active minute forever, and spec 6.4's
        impact term -- which divides by this -- would model a liquid market inside an
        outage.
        """
        state = self._symbols.get(symbol)
        if state is None:
            return 0
        self._expire(state, now_ms)
        return state.volume_total

    def has_any(self, symbol: str) -> bool:
        """Whether any observation for this symbol has ever arrived.

        Distinguishes "the run has not reached the first tick yet" from "this instant has no
        coverage", which are the same `None` from every other reader here and want different
        messages.
        """
        state = self._symbols.get(symbol)
        if state is None:
            return False
        return state.top is not None or state.ladder is not None or state.last_trade is not None

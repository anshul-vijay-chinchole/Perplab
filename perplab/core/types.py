"""Shared vocabulary for PerpLab.

Two conventions are enforced by these types and must never be relaxed:

**Time is integer epoch milliseconds, UTC** (spec 3.1). There are no naive datetimes
anywhere in this codebase. A `datetime` in a market-data path is a bug, not a style
choice -- it invites local-timezone contamination that silently shifts bar boundaries.

**Numeric market data is scaled int64**, never float. See `perplab.core.money` for the
scaling contract. Floats are permitted in indicator math only (spec 3.1); they are never
permitted in anything that will later touch a balance.

All records are frozen dataclasses. Immutability is load-bearing for the no-look-ahead
guarantee (spec 6.2): a `Bar` that cannot be mutated cannot be retroactively edited by a
strategy, and a partially-formed bar has no representation at all.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

__all__ = [
    "Side",
    "PositionSide",
    "MarginMode",
    "CollectorEventKind",
    "Bar",
    "AggTrade",
    "DepthSnapshot",
    "MarkPriceSample",
    "LiquidationEvent",
    "FundingRate",
    "CollectorEvent",
]


class Side(Enum):
    """Order/trade direction.

    Used by `money.quantize_price` to decide rounding direction: prices round *against*
    the trader (spec 3.1), so a buy rounds down and a sell rounds up.
    """

    BUY = "BUY"
    SELL = "SELL"


class PositionSide(Enum):
    """Which of a symbol's positions a fill belongs to (spec 3.3, extended for hedge mode).

    **Binance's own vocabulary, deliberately.** `BOTH` is what the exchange sends for a
    one-way account and `LONG`/`SHORT` for a hedge account, so the value on an
    `OrderIntent` is the value that goes on the wire and the value that comes back on an
    execution report. Inventing a parallel spelling would put a translation layer between
    the ledger and the venue at exactly the seam where a mistake is a real position.

    The distinction this makes possible is that **side stops being derivable from the sign
    of the quantity**. In one-way mode a position is long because its quantity is positive;
    a `BOTH` position that crosses zero flips. In hedge mode the short side is the short
    side whatever its quantity does -- it is an identity, not an observation -- and a sell
    that exceeds the long position does not flip it, it is refused.
    """

    BOTH = "BOTH"
    LONG = "LONG"
    SHORT = "SHORT"

    @property
    def is_hedged(self) -> bool:
        """Whether this side only exists in a hedge-mode account."""
        return self is not PositionSide.BOTH

    @property
    def opening_sign(self) -> int:
        """The sign of a fill that opens or increases this side: `+1`, `-1`, or `0` for either.

        Expressed as a sign rather than as a predicate over a quantity so that this module
        needs no `Decimal`. That is not a stylistic choice -- `types.py` is imported by the
        collector and the whole data path, and `test_money.py` structurally forbids
        `decimal` outside the accounting seam. A helper here that took a `Decimal` would
        drag the ledger's numeric type into every module that wants to name a side.

        `BOTH` returns `0`: one-way mode accepts a fill in either direction, and which one
        it is decides between spec 3.3's cases rather than between two positions.
        """
        if self is PositionSide.BOTH:
            return 0
        return 1 if self is PositionSide.LONG else -1


class MarginMode(Enum):
    """Which balance pool backs a position (spec 3.7).

    `ISOLATED` is the only mode the ledger implements, and that is a statement about the
    arithmetic rather than about effort. Under isolated margin a position's liquidation
    price is a closed form in its *own* allocation, which is what `margin.liquidation_price`
    solves. Under cross it depends on the unrealised PnL of every other open position, so
    there is no closed form, the solve becomes account-wide and iterative, and one strategy
    can liquidate every other position in the account.

    `CROSSED` therefore exists here as a *value the operator can express and be refused*,
    not as a mode that runs. The alternative -- omitting it -- would leave someone who
    wants cross margin with no answer at all; the alternative that matters more --
    accepting it and computing isolated math under a cross label -- is the platform
    quietly giving a wrong answer, and the liquidation price is the worst possible number
    to be quietly wrong about.

    The spelling is Binance's: `CROSSED`, not `CROSS`.
    """

    ISOLATED = "ISOLATED"
    CROSSED = "CROSSED"

    @property
    def is_implemented(self) -> bool:
        """Whether the ledger can actually price a position in this mode."""
        return self is MarginMode.ISOLATED

    @classmethod
    def parse(cls, value: str) -> MarginMode:
        """Read a stored or submitted margin mode, refusing one the ledger cannot price.

        One function so that the API, the run spec and the exchange preflight all refuse
        with the same words. A message that differs by entry point invites the reading that
        one of them is a softer check than the others.

        `CROSS` is accepted as a spelling of `CROSSED` purely so the refusal explains the
        real problem rather than complaining about the name.
        """
        text = str(value).strip().upper()
        if text == "CROSS":
            text = "CROSSED"
        try:
            mode = cls(text)
        except ValueError:
            raise ValueError(
                f"unknown margin mode {value!r}; expected "
                f"{' or '.join(m.value for m in cls)}"
            ) from None
        if not mode.is_implemented:
            raise ValueError(
                f"margin mode {mode.value} is not implemented. Under cross margin a "
                f"position's liquidation price depends on the unrealised PnL of every "
                f"other open position, so there is no closed form for it -- PerpLab's "
                f"ledger solves the isolated form against each position's own allocation "
                f"and would report a liquidation price the exchange does not agree with. "
                f"Isolated also bounds the damage one strategy can do: under cross, one "
                f"bad position can liquidate every other one in the account. Use ISOLATED."
            )
        return mode


class CollectorEventKind(Enum):
    """Records written to the collector's heartbeat stream.

    The heartbeat stream is what makes a gap *explainable* (spec 4.5). A gap in market
    data accompanied by a DISCONNECT/RECONNECT or RESTART record is accounted for; a gap
    with no matching record is a real, unexplained failure. This distinction is the whole
    basis of the Phase 1b exit criterion, so these records are data, not logging.
    """

    HEARTBEAT = "HEARTBEAT"
    CONNECT = "CONNECT"
    DISCONNECT = "DISCONNECT"
    RECONNECT = "RECONNECT"
    RESTART = "RESTART"
    SHUTDOWN = "SHUTDOWN"
    STALE = "STALE"
    """A subscribed stream stopped delivering while the connection stayed up.

    This is the quietest failure mode the collector has: the socket is healthy,
    heartbeats keep being written, other streams keep flowing, and one dataset simply
    stops. Nothing about the connection looks wrong, so without an explicit check it
    surfaces weeks later as a dataset that mysteriously ends mid-run.
    """

    UNAVAILABLE = "UNAVAILABLE"
    """A dataset has no source on this deployment, recorded once per run at startup.

    Distinct from STALE, and the distinction is the point. STALE means "this should be
    arriving and is not" -- a fault to investigate. UNAVAILABLE means "there is nowhere to
    get this", which is a standing fact about the deployment rather than an incident.
    Collapsing them would either bury a real fault in noise that repeats every run, or
    leave a permanently empty dataset looking like an unexplained gap forever.

    Written for `liquidations` since 2026-08-02: `!forceOrder@arr` is suppressed on this
    WebSocket endpoint and `GET /fapi/v1/allForceOrders` has been withdrawn (HTTP 404), so
    no public source remains. The record carries that reasoning into the lake, so the gap
    detector can account for the empty dataset from data rather than from a human
    remembering why (spec 4.5).
    """


@dataclass(frozen=True, slots=True)
class Bar:
    """An OHLCV bar, keyed by open time (Binance convention, spec 3.1).

    `open_time` and `close_time` are deliberately distinct fields. A strategy may only
    observe a bar at or after its `close_time`; keying by open time while gating on close
    time is what keeps timeframe aggregation honest at boundaries (spec 4.3). Collapsing
    these two into one timestamp is the classic way look-ahead re-enters a system that
    had otherwise eliminated it.

    Nothing in Phase 1b constructs a Bar. It is defined here so the convention is fixed
    before the code that depends on it exists.
    """

    symbol: str
    open_time: int
    close_time: int
    open: int
    high: int
    low: int
    close: int
    volume: int
    quote_volume: int
    trades: int


@dataclass(frozen=True, slots=True)
class AggTrade:
    """A Binance aggregate trade (`<symbol>@aggTrade`).

    `is_buyer_maker` is the aggressor flag and must be preserved exactly: `True` means the
    buyer was the maker, so the trade was *sell*-aggressive and consumed bid-side queue.
    The limit-order queue model (spec 6.4) is built entirely on this field, and inverting
    it silently reverses every queue-consumption decision in the backtester.
    """

    symbol: str
    ts_ms: int
    """Trade time (`T`) -- when the trade occurred, not when we received it."""
    recv_ms: int
    """Local receive time. Kept separate from `ts_ms` so collector-side latency and clock
    drift are measurable after the fact rather than baked invisibly into the data."""
    agg_id: int
    price: int
    qty: int
    first_trade_id: int
    last_trade_id: int
    is_buyer_maker: bool


@dataclass(frozen=True, slots=True)
class DepthSnapshot:
    """A 20-level order book snapshot (`<symbol>@depth20@100ms`), downsampled to 1s.

    Levels are stored as parallel arrays ordered best-first: `bid_px[0]` is the highest
    bid, `ask_px[0]` the lowest ask. Parallel arrays rather than a list of pairs because
    Parquet stores them as two flat list columns, which DuckDB can slice without
    materialising per-level structs.
    """

    symbol: str
    ts_ms: int
    recv_ms: int
    last_update_id: int
    bid_px: tuple[int, ...]
    bid_qty: tuple[int, ...]
    ask_px: tuple[int, ...]
    ask_qty: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class MarkPriceSample:
    """A mark price sample (`<symbol>@markPrice@1s`).

    Mark price drives unrealised PnL and liquidation triggers, and is *not* the last
    traded price (spec 3.4). PerpLab never computes it -- doing so would require
    multi-exchange index constituents we deliberately do not have, and any divergence
    from the exchange's own number is a divergence from the price that actually
    liquidates you. We record what Binance publishes and treat it as ground truth.

    Between samples the series is held flat (LOCF, spec 3.4). It is never interpolated:
    interpolation invents prices that never existed and can fabricate or hide
    liquidations.
    """

    symbol: str
    ts_ms: int
    recv_ms: int
    mark_price: int
    index_price: int
    estimated_settle_price: int
    last_funding_rate: int
    next_funding_ms: int


@dataclass(frozen=True, slots=True)
class LiquidationEvent:
    """A forced-order event from the market-wide `!forceOrder@arr` stream.

    These are *other* participants' liquidations. They drive cascade detection and the
    `on_market_liquidation` hook (spec 5.1), not our own account state.

    Binance's public stream is throttled -- it publishes at most one order per symbol per
    second, so this is a sample of the cascade, not a complete record of it. Treat counts
    derived from it as a lower bound.
    """

    symbol: str
    ts_ms: int
    recv_ms: int
    side: str
    order_type: str
    time_in_force: str
    qty: int
    price: int
    avg_price: int
    status: str
    last_filled_qty: int
    filled_accum_qty: int
    trade_ms: int


@dataclass(frozen=True, slots=True)
class FundingRate:
    """A realised funding settlement.

    Settlement times are read from the historical record rather than assumed. Binance
    runs different funding intervals on different symbols and has changed the interval on
    existing symbols, so a hardcoded 8-hour schedule is wrong (spec 3.5, R17).

    Not populated in Phase 1b -- defined here to fix the convention.
    """

    symbol: str
    funding_ms: int
    funding_rate: int
    mark_price: int


@dataclass(frozen=True, slots=True)
class CollectorEvent:
    """A heartbeat or lifecycle record from the collector.

    Written every `HEARTBEAT_INTERVAL_MS` even when nothing else is happening, so a
    WebSocket dropout or process death is *unambiguous* rather than inferred from absence
    of data (spec 4.5). Absence-of-data is indistinguishable from a quiet market; a
    missing heartbeat is not.
    """

    ts_ms: int
    kind: str
    stream: str
    detail: str
    downtime_ms: int

"""The backtest engine (spec 6.1, 6.2, 6.4, 6.5).

Everything about a backtest that could be wrong in an interesting way is a question about
*ordering*, and spec 6.2's total order is the answer. This module's job is to feed that
order into `core.account` without ever letting the strategy see something it could not have
seen.

**The no-look-ahead guarantee, and where each half of it lives.**

*Structural.* A `Bar` exists only at its `close_time` -- `feed` constructs it from a closed
bucket and the queue emits it there -- so there is no representation of a forming bar for a
strategy to reach. `ctx` exposes no way to ask for one. Book state is pulled to a horizon the
event loop sets (`_book_horizon`), never to "the next row", so a quote from the future is not
reachable even by accident.

*Behavioural.* Every price a fill is taken from was published before the order arrived.
`EventQueue.push` refuses to schedule anything at or before the instant being processed, so
an engine bug that tried to fill in the past raises instead of quietly producing a better
number. The look-ahead test (spec 12.3) is what checks the two together.

**What the engine can do depends on the tier, and it says so rather than approximating.**

| Tier | Market orders | Limit orders + TIF | Stop / TP / trailing |
|---|---|---|---|
| `BOOK_WALK` | ladder walk | visible queue | yes |
| `BOOK_TICKER` | touch + sqrt impact | queue seen at the touch | yes |
| `TRADE_ONLY` | next print | **refused** | yes |
| `BAR_CLOSE` | last print | **refused** | **refused** |

The two refusals are spec 6.4's own argument, applied where its inputs are missing. A limit
order needs a resting size to sit behind, and a tier with no book has none -- *"Touching a
limit price is not a fill"* is unenforceable without one, and an unenforced version of that
rule is the exact fiction the spec calls "the single most common way limit strategies look
profitable and are not". A stop needs a price path fine enough to fill against promptly once
it triggers; at `BAR_CLOSE` the fill would land at the next bar's open, up to a whole
timeframe after the trigger, which is not a stop but a delayed market order wearing one's
name. A strategy that gets a clear "not at this tier" learns something true; one that gets a
plausible number learns something false.

**What it does not do at the end of a run.** It does not close open positions. Spec 5.2 ends
a run with `on_stop` followed by a final mark-to-market, and a synthetic closing fill would
both invent a trade the strategy never asked for and break spec 12.3's prefix property for a
reason that has nothing to do with look-ahead. The open position is marked to market, its
unrealised PnL is in `equity`, and its round-trip is reported `close_reason="open"`.
"""

from __future__ import annotations

import json
import os
import random
import time
from bisect import bisect_right
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from perplab.analytics.attribution import Attribution, build_attribution
from perplab.analytics.metrics import Metrics, compute_metrics
from perplab.analytics.trades import Trade, TradeBuilder
from perplab.core.account import (
    _SIDE_ORDER,
    Account,
    FeeSchedule,
    HedgeFlipRefused,
    InsufficientMargin,
    LiquidationResult,
    PositionKey,
    position_label,
)
from perplab.core.invariants import InvariantViolation
from perplab.core.margin import BracketTable
from perplab.core.money import (
    SCALE,
    Money,
    accounting,
    decimal_to_scaled,
    from_scaled,
    money_to_str,
    parse_money,
    quantize_money,
    quantize_qty,
)
from perplab.core.types import DepthSnapshot, PositionSide, Side
from perplab.data.query import query
from perplab.data.schemas import MACRO_USD_UNIT
from perplab.engine.book import MAX_QUOTE_STALENESS_MS, MarketView
from perplab.engine.clock import Event, EventKind, EventQueue
from perplab.engine.executor_base import (
    MAX_EVENTS,
    EngineRuntime,
    EventLogFull,
    Order,
    OrderStatus,
)
from perplab.engine.feed import (
    BarStep,
    FundingPoint,
    MarkBar,
    bar_events,
    funding_events,
    load_bars,
    load_funding,
    load_marks,
    mark_events,
    warmup_start_ms,
)
from perplab.engine.fills import (
    FillQuote,
    MarketInputs,
    NoQuote,
    cross_book,
    fill_model_for_tier,
)
from perplab.engine.latency import FixedLatency, LatencyModel
from perplab.engine.resting import MakerFill, RestingBook, TRIGGER_TYPES
from perplab.engine.source import LakeSource, MarketSource
from perplab.engine.transport import OrderTransport, SimulatedTransport
from perplab.engine.ticks import (
    StateStream,
    TradePrint,
    book_ticker_stream,
    depth_events,
    depth_stream,
    trade_events,
)
from perplab.core.risk import (
    KillSwitch,
    RiskAction,
    RiskBreach,
    RiskEngine,
    RiskLimits,
    WorkingExposure,
    side_projected_exposure,
)
from perplab.exchange.filters import SymbolFilters, validate_order
from perplab.strategy.base import Strategy
from perplab.strategy.context import (
    Context,
    FillTier,
    Fill,
    FundingEvent,
    FundingView,
    OrderEnd,
    OrderIntent,
    OrderType,
    StrategyEvent,
    TimeInForce,
    UnsupportedOrder,
    WorkingType,
)
from perplab.strategy.dryrun import event_hash
from perplab.strategy.indicators import IndicatorSet
from perplab.strategy.params import Requirements

__all__ = [
    "BacktestConfig",
    "BacktestResult",
    "BacktestEngine",
    "EquityProgress",
    "LIVE_EQUITY_POINTS",
    "UnsupportedOrder",
    "RunAborted",
    "AutoFlatten",
    "MAX_ORDER_ENDS_PER_EVENT",
    "LATENCY_SEED_SALT",
    "PROGRESS_EVERY",
    "MARKET_WAIT_MS",
    "LIMIT_TIERS",
    "TRIGGER_TIERS",
]

LATENCY_SEED_SALT = 0x1A7E_9C4D
"""Offset separating the latency RNG from `ctx.rng`.

Two independent streams, not one. Sharing would make every order's latency depend on how
many random numbers the *strategy* had consumed by then, so adding a `ctx.rng.random()` to a
diagnostic line would re-price every fill in the run. The salt is a constant so the
separation is reproducible.
"""

PROGRESS_EVERY = 2_000
"""Events between wall-clock checks.

Small enough that a strategy spending tens of milliseconds per bar still checks in inside
the store's liveness window -- at 20 000 events a 40 ms `on_bar` went silent for nearly three
minutes and was reaped as dead -- and still far too coarse to be measurable against a loop
that dispatches ~150 000 events per second."""

PROGRESS_INTERVAL_S = 2.0
"""Minimum wall time between progress writes. See `BacktestEngine._checkpoint`."""

LIVE_EQUITY_POINTS = 1_200
"""Ceiling on the samples an in-progress equity snapshot carries.

The full series is one sample per mark bar -- 525 600 of them for a year of 1-minute data --
and it is rewritten on every checkpoint, so an uncapped snapshot would turn a 2-second
heartbeat into a multi-megabyte write. 1 200 is comfortably denser than the ~600 px the chart
is drawn into, so the cap costs nothing visible, and `_thin_extremes` keeps the peaks rather
than sampling past them."""

MAX_ORDER_ENDS_PER_EVENT = 10_000
"""Ceiling on `on_cancel` dispatches from one engine event. See `_drain_order_ends`."""

MARKET_WAIT_MS = 60_000
"""How long a `TRADE_ONLY` market order waits for a print before it expires.

Spec 4.5's tick-dataset gap threshold, reused: a minute with no trades is a gap by the
platform's own definition, and an order that finally executes an hour later against a market
that moved without it is a worse artefact than one that plainly did not fill.
"""

LIMIT_TIERS = frozenset({FillTier.BOOK_TICKER, FillTier.BOOK_WALK})
"""Tiers with a resting size for a limit order to queue behind. See the module docstring."""

TRIGGER_TIERS = frozenset({FillTier.TRADE_ONLY, FillTier.BOOK_TICKER, FillTier.BOOK_WALK})
"""Tiers with a trade tape fine enough to fill a triggered stop promptly."""

_MARK_BAR_MS = 60_000
"""Mark klines are always 1 m, whatever timeframe the strategy runs on."""

_ZERO = parse_money("0")


class RunAborted(RuntimeError):
    """The run stopped before the data did -- timeout, or an unrecoverable engine state."""


@dataclass(frozen=True, slots=True)
class AutoFlatten:
    """A platform-enforced exit, so that a strategy does not have to implement one.

    Crypto has no market close, so there is no end-of-day flatten to inherit from equities.
    What it has instead are two deadlines that matter just as much and that every strategy
    otherwise re-implements slightly differently:

    - `max_hold_ms` -- a position older than this is closed. The guarantee is about the
      *position*, not the order: a strategy that scales in resets nothing, because the risk
      being bounded is how long the account has been exposed, and a fresh increment does not
      make an old position young.
    - `before_funding_ms` -- close this long before a funding settlement. Spec 3.5's
      settlement is charged on whatever is open at the timestamp, so a strategy that does not
      want to pay it has to be flat *before* it, and "before" has to be far enough ahead that
      the closing order's own latency fits inside the window.

    Both are off by default. A platform that flattens positions nobody asked it to flatten is
    a platform whose equity curve is not the strategy's.

    **The exit is a market order through the ordinary path**, not a mark-price adjustment. It
    pays the spread, the latency and the fees that any other exit pays, because a flatten that
    settles at the mark would make "hold for four hours" look cheaper than it is -- which is
    exactly the kind of quiet optimism this platform exists to refuse.
    """

    max_hold_ms: int | None = None
    before_funding_ms: int | None = None

    @property
    def enabled(self) -> bool:
        return self.max_hold_ms is not None or self.before_funding_ms is not None

    def __post_init__(self) -> None:
        for name in ("max_hold_ms", "before_funding_ms"):
            value = getattr(self, name)
            if value is not None and value <= 0:
                raise ValueError(f"{name} must be positive when set, got {value}")
        if self.before_funding_ms is not None and self.before_funding_ms <= _MARK_BAR_MS:
            # **A window narrower than the sampling grid cannot be honoured, so it is
            # refused rather than half-kept.** The deadline is evaluated once per mark bar,
            # and mark bars are one minute. At `before_funding_ms = 30_000` the first check
            # that sees the deadline is the bar closing one millisecond before the
            # settlement -- the exit is submitted, takes its latency, and lands *after* the
            # payment it was meant to avoid. The run then reported an `AUTO_FLATTEN` and a
            # non-zero `funding_pnl`, which is the platform saying it kept a promise it had
            # broken. Anything above one bar leaves a whole bar plus the order's flight.
            raise ValueError(
                f"before_funding_ms must exceed the {_MARK_BAR_MS} ms mark cadence, got "
                f"{self.before_funding_ms}. The deadline is checked once per mark bar, so a "
                f"shorter window is first noticed too late for the closing order to land "
                f"before the settlement."
            )

    def to_json(self) -> dict[str, Any]:
        return {
            "max_hold_ms": self.max_hold_ms,
            "before_funding_ms": self.before_funding_ms,
        }

    @classmethod
    def from_json(cls, obj: Mapping[str, Any] | None) -> AutoFlatten:
        if not obj:
            return cls()
        unknown = sorted(set(obj) - set(cls.__dataclass_fields__))
        if unknown:
            raise ValueError(f"unknown auto-flatten field(s) {unknown}")
        max_hold = obj.get("max_hold_ms")
        before = obj.get("before_funding_ms")
        return cls(
            max_hold_ms=None if max_hold is None else int(max_hold),
            before_funding_ms=None if before is None else int(before),
        )


@dataclass(frozen=True, slots=True)
class BacktestConfig:
    """Everything about a run that is not the strategy or the data.

    Every field is written into the run manifest (spec 12.1) and every one of them can move
    the result, which is the test for whether something belongs here rather than being a
    module-level constant.
    """

    symbols: tuple[str, ...]
    timeframe: str
    start_ms: int
    end_ms: int
    seed: int = 0
    opening_balance: Money = field(default_factory=lambda: parse_money("10000"))
    leverage: int = 1
    hedge_mode: bool = False
    """Whether this run holds a long **and** a short position per symbol (spec 3.3 extended).

    Off by default, which is one-way mode and what every run before this existed as. It is
    a run-level setting rather than a strategy-level one because it is account state at the
    exchange -- `dualSidePosition` is one flag for the whole Binance account -- and a
    backtest that modelled it per strategy would be modelling an account that cannot exist.
    """
    fees: FeeSchedule = field(
        default_factory=lambda: FeeSchedule.all_taker(parse_money("0.0005"), "default-taker")
    )
    """Spec 3.8's recommended starting point: charge the taker rate on everything until a
    maker/taker classifier has been validated against real fills. 5 bps is Binance's
    standard USD-M taker rate; it is a *default*, is recorded as one, and should be replaced
    with the account's own `commissionRate` snapshot before any number here is believed."""

    latency: LatencyModel = field(default_factory=FixedLatency)
    fill_tier: FillTier = FillTier.BAR_CLOSE
    """The tier this run *executes* at, already resolved against what the lake holds.

    Resolution and its flags happen before the engine is built (`tiers.resolve_tier`), so
    that a degraded run is degraded once, in one place, with a recorded reason -- rather
    than in whichever fill path first noticed a missing dataset.
    """
    fill_model: Any = None
    """The tier's model. Defaults to `fill_model_for_tier(fill_tier)` when omitted."""

    liquidation_recovery_pct: Money = field(default_factory=lambda: parse_money("0"))
    timeout_s: float = 900.0
    """Wall-clock ceiling (spec 2.3: "workers get a wall-clock timeout")."""

    risk: RiskLimits = field(default_factory=RiskLimits.unlimited)
    """Spec 7's limits. **Unlimited by default, and that is deliberate.**

    A default of spec 7's own table -- 5x leverage, 2% daily loss, 15% drawdown -- would
    silently change the answer of every run written before Phase 6, and a strategy whose
    backtest halted on day three would look like a strategy that stopped trading rather than
    one the platform stopped. The API and the CLI apply the spec defaults where a person is
    choosing; the engine's own default is "nobody said", and nobody said means no limit.
    """

    auto_flatten: AutoFlatten = field(default_factory=AutoFlatten)
    kill_switch_flatten: bool = False
    """What a halt does with open positions. Spec 7.3's default is cancel-only."""

    def resolved_fill_model(self) -> Any:
        return self.fill_model or fill_model_for_tier(self.fill_tier.name)

    def to_json(self) -> dict[str, Any]:
        return {
            "symbols": list(self.symbols),
            "timeframe": self.timeframe,
            "start_ms": self.start_ms,
            "end_ms": self.end_ms,
            "seed": self.seed,
            "opening_balance": money_to_str(self.opening_balance),
            "leverage": self.leverage,
            # Omitting this made two byte-identical manifests out of a one-way run and a
            # hedge run -- position modes whose ledgers disagree about how many positions
            # one symbol can hold. `RunSpec.to_json` records it; the two artefacts must
            # not tell different stories about the same run (spec 12.1).
            "hedge_mode": self.hedge_mode,
            "fees": {
                "maker_rate": money_to_str(self.fees.maker_rate),
                "taker_rate": money_to_str(self.fees.taker_rate),
                "source": self.fees.source,
            },
            "latency": self.latency.to_json(),
            "fill_tier": self.fill_tier.name,
            "fill_model": self.resolved_fill_model().to_json(),
            "liquidation_recovery_pct": money_to_str(self.liquidation_recovery_pct),
            "timeout_s": self.timeout_s,
            "risk_limits": self.risk.to_json(),
            "auto_flatten": self.auto_flatten.to_json(),
            "kill_switch_flatten": self.kill_switch_flatten,
        }


@dataclass(frozen=True, slots=True)
class EquityProgress:
    """The equity curve of a run that has not finished yet, thinned for drawing.

    **Deliberately not a `BacktestResult`, and deliberately not persisted as `equity.parquet`.**
    A result is measured against the run's whole range and carries metrics computed once, at
    the end; this is a partial view whose last point moves. Publishing it under the finished
    artefact's name would make "the run has an equity series" stop meaning "the run finished",
    which is the check every reader of that file makes.

    `samples` is the length of the *full* series, so a reader can say how much was thinned
    away rather than mistaking `len(equity)` for how much the run has done.
    """

    ts: tuple[int, ...]
    equity: tuple[float, ...]
    low: tuple[float, ...]
    high: tuple[float, ...]
    samples: int
    bars: int

    def to_json(self) -> dict[str, Any]:
        return {
            "ts": list(self.ts),
            "equity": list(self.equity),
            "low": list(self.low),
            "high": list(self.high),
            "samples": self.samples,
            "bars": self.bars,
        }

    def publish(self, path: Path) -> None:
        """Write atomically to `path`, and never raise.

        This is a preview: a reader that misses one update sees the next one seconds later.
        A run that died because the directory was momentarily unwritable -- an editor holding
        the file, a backup tool, an antivirus scanner -- would be a real result lost to a
        decoration, so every error here is swallowed and the only consequence is that the
        chart stops advancing until the next attempt succeeds.

        The temporary name carries the pid because a backtest worker and a session worker can
        both be writing runs at once, and two writers sharing one `.tmp` would let each
        replace the other's half-written file.

        Written without `indent`, unlike the finished artefacts: this one is rewritten every
        few seconds for the life of the run and nobody reads it by hand.
        """
        tmp = path.parent / f".{path.name}.{os.getpid()}.tmp"
        try:
            tmp.write_text(json.dumps(self.to_json()), encoding="utf-8")
            os.replace(tmp, path)
        except OSError:
            try:
                tmp.unlink()
            except OSError:
                pass


def _thin_extremes(
    values: Sequence[float], limit: int
) -> tuple[int, ...]:
    """Indices of at most `limit` samples, keeping each bucket's min and max.

    Stride sampling would step straight past a spike, and the spike is the part of an equity
    curve worth looking at. Bucketing and keeping both extremes of each bucket costs two
    points per bucket and cannot skip a peak: whatever the highest sample in a window is, it
    is that window's max. First and last are always kept so the line starts at the opening
    balance and ends where the run actually is.
    """
    count = len(values)
    if count <= limit:
        return tuple(range(count))
    buckets = max(1, limit // 2)
    keep: set[int] = {0, count - 1}
    for bucket in range(buckets):
        start = (bucket * count) // buckets
        stop = ((bucket + 1) * count) // buckets
        if stop <= start:
            continue
        window = range(start, stop)
        keep.add(min(window, key=values.__getitem__))
        keep.add(max(window, key=values.__getitem__))
    return tuple(sorted(keep))


@dataclass(frozen=True, slots=True)
class BacktestResult:
    """One completed run, in memory. `store.runs` is what puts it on disk."""

    events: tuple[StrategyEvent, ...]
    event_hash: str
    trades: tuple[Trade, ...]
    metrics: Metrics
    attribution: Attribution
    equity_ms: tuple[int, ...]
    equity: tuple[float, ...]
    equity_low: tuple[float, ...]
    """Equity at each position's own *adverse* extreme within the mark bar.

    Not a sample the account is claimed to have taken at that timestamp -- it is the trough
    of the band the mark provably traversed, which is what spec 8.2's intraperiod drawdown
    is about. Equal to `equity` when flat or when no range is available."""
    equity_high: tuple[float, ...]
    position_open: tuple[bool, ...]
    data_start_ms: int
    """First instant of data the run actually read, warm-up included.

    Reported because it, not `config.start_ms`, is the range the dataset manifest must
    cover (spec 4.6): a strategy declaring 400 bars of history read four hundred bars of
    data from before its own start date, and a manifest that omitted them would fail to
    notice when *those* files changed underneath a re-run.
    """
    warnings: tuple[str, ...]
    flags: tuple[str, ...]
    fill_tier: str
    orders: int
    fills: int
    rejects: int
    liquidations: int
    bars: int
    ticks: int
    maker_fills: int
    partial_fills: int
    depth_exhausted: int
    engine_events: int
    wall_s: float
    final_equity: Money
    final_wallet: Money
    opening_balance: Money
    risk_breaches: tuple[RiskBreach, ...] = ()
    halt_reason: RiskBreach | None = None
    risk_rejects: int = 0
    auto_flattens: int = 0
    risk_summary: Mapping[str, Any] = field(default_factory=dict)
    symbol_pnl: Mapping[str, tuple[float, ...]] = field(default_factory=dict)
    """Per-symbol cumulative net PnL, sampled on the same cadence as `equity_ms`.

    Populated only for multi-symbol runs -- the spec 9.5 correlation analysis is what it
    exists for, and a single-symbol run's series is its equity curve minus the opening
    balance, already stored once. Symbols that never traded are absent, not flat zeros
    (see `TradeBuilder.net_pnl_by_symbol`)."""

    def summary(self) -> dict[str, Any]:
        return {
            "event_hash": self.event_hash,
            "risk_rejects": self.risk_rejects,
            "auto_flattens": self.auto_flattens,
            "halted": self.halt_reason is not None,
            "halt_reason": None if self.halt_reason is None else self.halt_reason.to_json(),
            "risk": dict(self.risk_summary),
            "fill_tier": self.fill_tier,
            "orders": self.orders,
            "fills": self.fills,
            "maker_fills": self.maker_fills,
            "partial_fills": self.partial_fills,
            "depth_exhausted": self.depth_exhausted,
            "rejects": self.rejects,
            "liquidations": self.liquidations,
            "bars": self.bars,
            "ticks": self.ticks,
            "engine_events": self.engine_events,
            "wall_s": self.wall_s,
            "final_equity": money_to_str(self.final_equity),
            "final_wallet": money_to_str(self.final_wallet),
            "opening_balance": money_to_str(self.opening_balance),
            "net_pnl": money_to_str(self.attribution.net_pnl),
            "flags": list(self.flags),
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True, slots=True)
class _Instruction:
    """One thing arriving at the matching engine at a scheduled time.

    Three actions share `ORDER_ARRIVAL` (spec 6.2's priority 8) because all three are the
    same event: an instruction the strategy issued earlier reaching the exchange after its
    latency has elapsed. Giving a cancel its own priority would mean renumbering spec 6.2's
    table, and a priority table that changes is a reproducibility contract that does not
    exist.
    """

    order_id: str
    action: str
    """`submit`, `cancel`, `expire`, or `modify`."""

    price: Money | None = None
    qty: Money | None = None
    """The amendment `modify` carries. `qty` is the order's new **total** size, matching
    Binance's own endpoint -- not the new remainder, which would mean something different
    for an order that had already partly filled."""

    reference_price: Money | None = None
    """The price when the amendment was *decided*, for spec 8.4's slippage measurement.

    Captured at `ctx.modify()` rather than at arrival, for the same reason `_submit`
    captures it at submission: the latency drift between deciding and arriving is an
    execution cost, and measuring against the arrival price is how a backtest hides it.
    """


class BacktestEngine:
    """Drives one strategy over one range. Single use -- build a new one per run."""

    def __init__(
        self,
        *,
        root: Path | str,
        strategy: Strategy,
        requirements: Requirements,
        config: BacktestConfig,
        filters: dict[str, SymbolFilters],
        brackets: dict[str, BracketTable],
        flags: Sequence[str] = (),
        progress: Callable[[int, int], None] | None = None,
        on_equity: Callable[["EquityProgress"], None] | None = None,
        source: MarketSource | None = None,
        transport: OrderTransport | None = None,
    ) -> None:
        self.root = Path(root)
        self.strategy = strategy
        self.requirements = requirements
        self.config = config
        self.filters = filters
        self.progress = progress
        self.on_equity = on_equity
        """Called on the progress cadence with the series so far, or never if `None`.

        Separate from `progress` because the two answer different questions and cost
        different amounts: `progress` is a two-integer heartbeat the store's liveness rule
        depends on, and must stay cheap enough to write every 2 s forever. This one carries
        a thinned copy of the equity curve so a running run can be *drawn*, and a caller
        that does not need the picture should not pay for it."""
        self.tier = config.fill_tier
        self.fill_model = config.resolved_fill_model()

        self.account = Account(
            opening_balance=config.opening_balance,
            fees=config.fees,
            brackets=dict(brackets),
            filters=dict(filters),
            liquidation_recovery_pct=config.liquidation_recovery_pct,
            hedge_mode=config.hedge_mode,
        )
        for symbol in config.symbols:
            self.account.set_leverage(symbol, config.leverage)
        self._sides: tuple[PositionSide, ...] = (
            (PositionSide.LONG, PositionSide.SHORT)
            if config.hedge_mode
            else (PositionSide.BOTH,)
        )
        """The position slots this run can address. One in one-way mode, two in hedge.

        Held rather than recomputed so that every loop over sides -- the equity sample, the
        auto-flatten sweep, the halt flatten -- iterates in the same order, which is what
        makes a run reproducible when two legs move on the same event."""

        self.market = MarketView()
        self.resting = RestingBook(market=self.market)

        self.risk = RiskEngine(
            limits=config.risk,
            starting_equity=config.opening_balance,
            kill_switch=KillSwitch(flatten=config.kill_switch_flatten),
        )
        self.effective_end_ms = config.end_ms
        """The instant the run actually stopped observing. See `_finalise`."""
        self._halt_pending: RiskBreach | None = None
        """A halt decided inside an event, to be carried out once that event finishes.

        Nothing halts *during* dispatch. A breach can be detected in the middle of booking a
        fill -- the equity sample that trips the drawdown limit is taken from inside the
        mark handler -- and unwinding there would leave the ledger half-updated, the trade
        builder holding an open leg, and the invariants failing for a reason that has
        nothing to do with the breach. So the breach is recorded, the event completes, and
        `run` performs the halt between events, where the account is consistent by
        construction.
        """
        self._closed_trades_seen = 0
        self._pending_order_ends: deque[OrderEnd] = deque()
        self._position_since: dict[PositionKey, int] = {}
        """When each currently-open position was first opened, for `AutoFlatten`.

        Per `(symbol, side)`: the two legs of a hedge are opened at different times and a
        max-hold deadline applies to each on its own clock. Keyed by symbol, closing the
        long would have reset the short's deadline."""
        self._platform_orders: set[str] = set()
        """Orders the engine issued rather than the strategy -- halts and auto-flattens.

        Their refusals belong to the platform, and are reported as such: they do not raise
        `ORDERS_REJECTED`, do not enter `rejects`, and do not feed the consecutive-rejection
        auto-trigger."""
        self._flatten_orders: dict[str, PositionKey] = {}
        """Auto-flatten order id -> position, so a refused exit can unlatch its own."""
        self._flatten_sent: set[PositionKey] = set()
        """Positions with an auto-flatten order already in flight, so the same deadline does
        not send a second one on the next mark before the first has arrived.

        Per side: a hedge whose long has hit its deadline still has a short that has not,
        and latching the symbol would suppress the short's exit when its own time came."""

        self.runtime = EngineRuntime(
            account=self.account,
            symbols=config.symbols,
            filters=filters,
            tier=self.tier,
            submit_hook=self._submit,
            cancel_hook=self._cancel,
            cancel_all_hook=self._cancel_all,
            open_orders_hook=self._open_order_ids,
            modify_hook=self._modify,
            # Backtest and paper apply a strategy's leverage change straight to the ledger,
            # because the ledger is the only account there is. A live session clears this
            # (see `PaperSession`) rather than letting the two disagree with the venue.
            leverage_hook=self._set_leverage,
            market=self.market if self.tier is not FillTier.BAR_CLOSE else None,
            sync_hook=self._sync_market,
        )
        self.indicators = IndicatorSet(
            primary_symbol=config.symbols[0],
            symbols=config.symbols,
            bar_ms=requirements.timeframe_ms,
        )
        self.context = Context(
            _runtime=self.runtime,
            symbols=config.symbols,
            timeframe=config.timeframe,
            indicators=self.indicators,
            rng=random.Random(config.seed),
        )
        self.latency_rng = random.Random(config.seed ^ LATENCY_SEED_SALT)

        self.queue = EventQueue()
        self.orders: dict[str, Order] = {}
        self.order_seq = 0
        self.last_print: dict[str, int] = {}
        """Most recent print price per symbol, **scaled**.

        Integers rather than `Decimal`: this is written on every one of up to eighteen
        million trade events and read only when an order is submitted, so paying for the
        exact-arithmetic construction on the write side would be paying it ten thousand
        times per read.

        **Read through `_recent_print`, never directly.** Every other market read in the
        engine is staleness-bounded (`book.MarketView`'s 60 s), and this one was not: a
        three-hour kline hole did not stop fills, it made them happen at the pre-hole
        price, and the same unbounded read fed `_reference_price` (hence every
        `slippage_cost` and `price_pnl`) and `_risk_price` (hence the notional every risk
        ceiling was measured against). `last_print_ts` carries the stamp that makes the
        bound checkable.
        """
        self.last_print_ts: dict[str, int] = {}
        """When each `last_print` entry was published, epoch ms. See `_recent_print`."""
        self._print_staleness_ms = MAX_QUOTE_STALENESS_MS + (
            requirements.timeframe_ms if self.tier is FillTier.BAR_CLOSE else 0
        )
        """How old a print may be before `_recent_print` refuses it.

        At the tick tiers this is exactly `book.MAX_QUOTE_STALENESS_MS` -- the same 60 s
        every `MarketView` read enforces, and spec 4.5's own gap threshold, so "the engine
        refused to price here" and "the gap report flags this" stay one condition. At
        `BAR_CLOSE` the only datable prints are each bar's open and close, so within a
        healthy 4 h-timeframe run the last print is legitimately up to a whole timeframe
        old mid-bar; the bound is therefore one timeframe plus the 60 s threshold, which
        passes normal operation at any timeframe and still refuses a genuine kline hole.
        """
        self.on_halt: Callable[[], None] | None = None
        """Called at the top of `_perform_halt`, before the halt does anything else.

        Spec 7.6's persistent kill switch used to be armed only in the session worker's
        `finally` -- so an OOM kill or `TerminateProcess` between the halt and process exit
        left it un-armed on disk and the next session's `require_clear()` passed against
        the very account the risk layer had just declared untrustworthy. A live worker
        hangs the durable arming here, *before* the cancels and exits that can block on the
        network, so the interlock is crash-ordered ahead of everything the halt does. The
        callback must not raise; the engine guards it anyway, because a halt that cannot be
        recorded must still cancel the book.
        """
        self.pending_range: dict[str, tuple[Money, Money, Money]] = {}
        self._liq_scheduled_ts: int | None = None
        self._liq_seq = 0
        self._fill_check_ts: int | None = None
        self._fill_seq = 0
        self._pending_trades: list[TradePrint] = []
        self._instruction_seq = 0

        self.top_streams: dict[str, StateStream] = {}
        self.depth_streams: dict[str, StateStream] = {}
        self._trade_stream: Any = None
        self._depth_stream: Any = None
        self._depth_as_events = False
        """Set in `_check_indicator_feeds` once the indicator set is frozen."""
        self._book_horizon = -1
        self._book_synced = -1

        self.trades = TradeBuilder()
        self.symbol_pnl: dict[str, list[float]] = (
            {symbol: [] for symbol in config.symbols} if len(config.symbols) > 1 else {}
        )
        self.equity_ms: list[int] = []
        self.equity: list[float] = []
        self.equity_low: list[float] = []
        self.equity_high: list[float] = []
        self.position_open: list[bool] = []
        self.slippage_cost = _ZERO
        self.slippage_abs = _ZERO
        self.traded_notional = _ZERO

        self.warnings: list[str] = []
        self.flags: set[str] = set(flags)
        if self.tier is FillTier.BAR_CLOSE:
            self.flags.add("LOW_FIDELITY")
        if getattr(config.latency, "is_zero", False):
            self.flags.add("ZERO_LATENCY")
            self.warnings.append(
                "latency is zero, so every market order fills at the print that triggered "
                "it. Spec 6.3 calls this out as systematically optimistic; the numbers in "
                "this run are an upper bound, not an estimate."
            )

        self.counts = {
            "orders": 0,
            "fills": 0,
            "maker_fills": 0,
            "partial_fills": 0,
            "depth_exhausted": 0,
            "rejects": 0,
            "liquidations": 0,
            "bars": 0,
            "ticks": 0,
            "unsettled_funding": 0,
            "triggers": 0,
            "risk_rejects": 0,
            "auto_flattens": 0,
        }
        # **Two independent facts, not one.** `RISK_LIMITED` says a risk layer was in
        # force; `RISK_UNBOUNDED` says nothing capped position size. A run with only a
        # rate limit has both -- it had limits, and it had no ceiling on size -- and
        # treating them as opposites badged a run whose limit refused twenty orders as
        # having had no risk layer at all.
        if config.risk.any_limit:
            self.flags.add("RISK_LIMITED")
        if config.risk.unbounded_exposure:
            self.flags.add("RISK_UNBOUNDED")
        self._hooks = type(strategy).implemented_hooks()
        self._funding_times: dict[str, list[int]] = {}
        self._oi_points: list[tuple[int, str, float]] = []
        self._oi_index = 0
        self._macro_points: list[tuple[int, str, float, int]] = []
        self._macro_index = 0
        self._deadline = 0.0
        self._last_progress = 0.0
        self._started_at = 0.0
        self._total_bars = 0
        self._warmup = requirements.history
        self.data_start_ms = config.start_ms

        self.halted = False
        """Whether a risk halt has been performed. `step` refuses everything afterwards.

        An instance field rather than a local of `run`, because a live session owns its own
        loop and has to be able to ask."""

        self._liq_proximity_noted: set[tuple[str, PositionSide]] = set()
        """Positions already warned about crossing the ledger's own liquidation price in a
        live session, so a mark oscillating around the level does not write the same
        warning once a minute for hours. See `_note_liquidation_proximity`."""

        self.transport: OrderTransport = (
            transport if transport is not None else SimulatedTransport(self)
        )
        """Where accepted orders go. The other row of spec 6.1's table that may differ.

        See `engine.transport`: everything up to and including the risk verdict is shared,
        and everything after a fill comes back is shared, so a live transport is a delivery
        mechanism rather than a second engine.
        """

        self.source: MarketSource = source if source is not None else LakeSource()
        """Where events come from. The one row of spec 6.1's table that may differ by mode.

        `LakeSource` replays Parquet and is what every backtest uses; `TapeSource` replays a
        paper session's recording, and the live feed pushes events straight onto `queue`.
        Everything downstream of `step` -- the order lifecycle, the risk checks, the fill
        booking, the ledger, the metrics -- is the same code in all three cases, which is
        what spec 6.1 requires and what makes the parity report measure the fill model
        rather than measuring two implementations of the same rules.
        """

    # ------------------------------------------------------------------------ running

    def run(self) -> BacktestResult:
        """Drive the whole run from the lake and return its result.

        A composition of the five lifecycle methods below rather than a monolith, because a
        paper session needs the same four phases driven by a wall clock instead of by a
        queue that is known to empty. Spec 6.1's rule is that the mode-specific classes stay
        thin and everything else is shared; keeping the loop *body* here and letting the
        caller own the loop is what makes that literally true rather than aspirational --
        `step` dispatches the same event through the same handlers whether it came from a
        Parquet scan or from a WebSocket a hundred milliseconds ago.
        """
        started = self.start()
        processed = 0
        try:
            halted = False
            while self.queue:
                if not self.step(self.queue.pop()):
                    halted = True
                    processed += 1
                    break
                processed += 1
                if processed % PROGRESS_EVERY == 0:
                    self._checkpoint(processed)

            processed += self.drain(halted=halted)
            self.finish()
        except InvariantViolation as exc:
            self.note_invariant_failure(exc)
            raise
        finally:
            self._close_streams()

        wall = time.perf_counter() - started
        return self._result(processed, wall)

    # ------------------------------------------------------------------ lifecycle

    def start(self) -> float:
        """Preflight, `on_start`, then register the market source's streams.

        Returns the `perf_counter` reading the run began at, which `run` needs for its wall
        figure and a live session ignores.

        **`on_start` runs before any data is read, and the ordering is load-bearing.**
        Freezing the indicator set is what makes the warm-up length knowable in advance
        (spec 5.4 rule 4), and the warm-up length is what decides how much history to load.
        Loading first and asking afterwards would mean a strategy whose indicators need more
        bars than its `requires["history"]` declares would silently begin trading later than
        its own start date -- correct, but not what was asked for.
        """
        started = time.perf_counter()
        self._deadline = (
            float("inf") if self.config.timeout_s <= 0 else started + self.config.timeout_s
        )
        self._last_progress = started
        self._started_at = started

        self._check_declarations()
        self.strategy.on_start(self.context)
        self.indicators.freeze()
        self._warmup = max(self.indicators.warmup, self.requirements.history)
        self.context._set_warmup_bars(self._warmup)
        self._check_indicator_feeds()

        prepared = self.source.prepare(self)
        self.data_start_ms = prepared.data_start_ms
        self._total_bars = prepared.total_bars
        self.flags.update(prepared.flags)
        self.warnings.extend(prepared.warnings)
        self._funding_times = dict(prepared.funding_times)
        for stream in prepared.streams:
            self.queue.add_stream(stream)
        return started

    def step(self, event: Event) -> bool:
        """Dispatch one event. Returns `False` once the run has halted and must stop.

        **The halt is performed between events, never inside one.** A breach can be detected
        halfway through booking a fill -- the equity sample that trips the drawdown limit is
        taken from inside the mark handler -- and unwinding there would leave the ledger
        half-updated, the trade builder holding an open leg, and the invariants failing for a
        reason that has nothing to do with the breach. So the breach is recorded, the event
        completes, and the halt happens here, where the account is consistent by
        construction.
        """
        if self.halted:
            return False
        self._dispatch(event)
        if self._halt_pending is not None:
            self._perform_halt()
            self.halted = True
            return False
        return True

    def perform_pending_halt(self) -> bool:
        """Carry out a halt asked for between events. Returns whether one was performed.

        `step` performs a halt the moment the event that caused it finishes, which covers
        every breach the engine detects itself. A breach raised by the session process --
        a disconnect outlasting its limit, a reconciliation mismatch -- has no event to ride,
        and on a quiet market the next one may be minutes away. Without this the kill switch
        would be recorded as tripped while orders kept being accepted, which is worse than
        not having the trigger at all.
        """
        if self._halt_pending is None or self.halted:
            return False
        self._perform_halt()
        self.halted = True
        return True

    def drain(self, *, halted: bool) -> int:
        """Run `on_stop`, then finish whatever is still queued. Returns events processed.

        **Nothing is drained after a halt.** The remaining events are market data the run has
        stopped participating in, and processing them would keep sampling equity past the
        point the operator's limit said to stop. `finish` completes the picture by ending the
        run *at* the halt rather than at the requested range's end, so the metrics describe
        the period actually observed. Anything `on_stop` submitted is refused by the risk
        layer, which is what `halted` means.
        """
        # `on_stop` runs at the last event's clock, then anything it submitted is drained. A
        # strategy that flattens in `on_stop` is doing something legitimate and the fill has
        # to actually happen, or the run reports a position the strategy closed.
        if "on_stop" in self._hooks:
            self.strategy.on_stop(self.context)
            self._drain_order_ends()
        if halted:
            return 0

        # The same budget as the main loop. Without it a strategy that submits from
        # `on_fill` after `on_stop` has no bound at all: each fill schedules the next
        # arrival, the queue never empties, and the run executes indefinitely at simulated
        # timestamps drifting further past `end_ms` -- all of them priced against the last
        # stale print, all booked into the trade table.
        processed = 0
        while self.queue:
            self._dispatch(self.queue.pop())
            processed += 1
            if processed % PROGRESS_EVERY == 0:
                self._checkpoint(processed)
        return processed

    def finish(self) -> None:
        """Final mark-to-market and the ledger's end-of-run reconciliation."""
        self._finalise()

    def result(self, processed: int) -> BacktestResult:
        """Compute metrics and freeze the run. Single use -- `TradeBuilder.finish` is
        destructive, so a live session must call `trades.snapshot()` for its interim
        tables and reach here exactly once, at the end."""
        return self._result(processed, time.perf_counter() - self._started_at)

    def _close_streams(self) -> None:
        """Release every open lake handle, whether the run finished or raised.

        `stream_query` closes its connection when the generator is exhausted, and a run that
        raises exhausts nothing -- the streams stop wherever the clock did. Without this a
        worker that ran a hundred backtests in one process would hold a hundred DuckDB
        connections and their memory-mapped Parquet footers.
        """
        closers = [s.close for s in self.top_streams.values()]
        closers += [s.close for s in self.depth_streams.values()]
        for stream in (self._trade_stream, self._depth_stream):
            if stream is not None:
                closers.append(stream.close)
        closers.append(self.source.close)
        for close in closers:
            try:
                close()
            except Exception:  # pragma: no cover - closing must never mask the real error
                pass

    # --------------------------------------------------------------------- preflight

    def _check_declarations(self) -> None:
        """Refuse the run rather than serve a strategy data this tier does not have.

        Spec 5.1 has `requires` checked for coverage *before* a run starts, and spec 4.2's
        rule is that a fidelity downgrade must never be silent. A strategy written against
        the order book that runs anyway -- taking a `None` branch it never intended --
        produces a curve that looks like a strategy result and is a misconfiguration.
        """
        declared = set(self.requirements.datasets)
        unsupported = sorted(
            (declared & set(_DATASET_TIERS)) - self._available_datasets()
        )
        if unsupported:
            raise UnsupportedOrder(
                f"this strategy declares requires['datasets'] = {unsupported}, which the "
                f"{self.tier.name} tier does not feed. "
                + ", ".join(
                    f"{name} needs {_DATASET_TIERS[name].name} or better"
                    for name in unsupported
                )
                + ". Shorten the range to one the collector covers, ingest the dataset, or "
                "remove the declaration."
            )
        if "on_tick" in self._hooks and self.tier is FillTier.BAR_CLOSE:
            raise UnsupportedOrder(
                "this strategy implements on_tick, and the BAR_CLOSE tier has no trade "
                "tape to drive it. Running anyway would call the hook zero times and "
                "report a strategy that never traded as one that chose not to."
            )
        missing = [s for s in self.config.symbols if s not in self.filters]
        if missing:
            raise RunAborted(
                f"no exchange filters for {missing}; order quantisation and the tick grid "
                f"would have to be guessed at (spec 3.2)"
            )
        undeclared = [s for s in self.config.symbols if s not in self.requirements.symbols]
        if undeclared:
            raise RunAborted(
                f"the run asks for {undeclared} but the strategy declares "
                f"{list(self.requirements.symbols)}. `ctx` refuses any symbol outside the "
                f"declaration, so those legs would raise on the first access rather than "
                f"trade."
            )

    def _available_datasets(self) -> frozenset[str]:
        return frozenset(
            name for name, need in _DATASET_TIERS.items() if self.tier >= need
        )

    def _check_indicator_feeds(self) -> None:
        """Run only after `on_start`, when the indicator set is known and frozen.

        Two jobs, and they used to be one because the second was missing entirely.

        **Refuse an indicator this tier cannot feed.** A `trade`- or `depth`-driven indicator
        below its tier is served nothing, so `ready` never turns true. What the strategy then
        does depends on how it was written -- a guard on `.ready` makes it silently never
        trade, and an unguarded read raises `TypeError` comparing `None` on some bar in the
        middle of the run. Neither reads as "this tier has no order book".

        **Switch depth to event replay when an indicator needs it.** At a sufficient tier the
        refusal above does not fire, and the indicator was *still* fed nothing: depth is
        pulled as state for the fill models, which materialises only the row in force at each
        instant the engine happens to ask. `BookImbalance` -- spec 4.2's headline `BOOK_WALK`
        capability -- therefore stayed `ready=False` for an entire run over a lake full of
        depth, with no warning and no flag. A registered depth indicator moves the whole
        dataset onto the event queue; see `ticks.depth_events`.
        """
        self._depth_as_events = any(i.feed == "depth" for i in self.indicators.all())
        needed = {"trade": FillTier.TRADE_ONLY, "depth": FillTier.BOOK_WALK}
        blocked = sorted(
            {
                i.feed
                for i in self.indicators.all()
                if i.feed in needed and self.tier < needed[i.feed]
            }
        )
        if blocked:
            raise UnsupportedOrder(
                f"this strategy builds indicators driven by {blocked}, and the "
                f"{self.tier.name} tier does not replay "
                + " or ".join(_FEED_NAMES[feed] for feed in blocked)
                + ". They would never become ready, so the strategy would either never "
                "trade or raise comparing None on some bar in the middle of the run. "
                + "; ".join(
                    f"{feed} indicators need the {needed[feed].name} tier or better"
                    for feed in blocked
                )
                + "."
            )

    @property
    def warmup_bars(self) -> int:
        """Bars of history this run needs before the strategy may trade.

        Only meaningful once `start` has frozen the indicator set; a source reads it to
        decide how far back to reach."""
        return self._warmup

    @property
    def depth_as_events(self) -> bool:
        """Whether a depth-driven indicator forces every ladder through the queue."""
        return self._depth_as_events

    def open_book_streams(self, data_start_ms: int) -> None:
        """Open one book stream per symbol, for the tiers that read one.

        Depth is opened as a *pulled state stream* unless a depth-driven indicator is
        registered, in which case it is replaced by an event stream so the indicator sees
        every snapshot rather than the subsample the engine happened to advance past.
        Never both: two sources of ladder state is two states that can disagree.

        Called by `LakeSource`. A source replaying a tape does not call it -- the tape
        carries book updates as events, already interleaved in the order the session saw
        them, which is the whole point of recording one.
        """
        if self.tier in (FillTier.BAR_CLOSE, FillTier.TRADE_ONLY):
            return
        for symbol in self.config.symbols:
            self.top_streams[symbol] = book_ticker_stream(
                self.root, symbol, data_start_ms, self.config.end_ms
            )
            if self.tier is FillTier.BOOK_WALK and not self._depth_as_events:
                self.depth_streams[symbol] = depth_stream(
                    self.root, symbol, data_start_ms, self.config.end_ms
                )

    def load_open_interest(self, data_start: int) -> None:
        """Read the `metrics` dataset, but only for a strategy that asked for it.

        Open interest is a 5-minute snapshot series and the dataset is large; a run that
        never calls `ctx.oi()` should not pay to scan it. Loaded when the strategy declares
        `metrics` or builds an OI-driven indicator, and refused loudly when it asked and the
        lake has nothing -- `ctx.oi()` returning `None` forever is a silent lie about
        available data, and an `OIDelta` that never becomes ready would hold the warm-up
        gate shut for a reason no message explains.

        **Gated on `create_time`, and that is a stated, one-sided approximation -- not the
        no-look-ahead guarantee the book and macro loaders carry.** The `metrics` schema
        has no receive clock at all (it is a bulk-archive dataset, published daily, and
        the archive records only the snapshot instant), so there is no `recv_ms` to
        `COALESCE` on and nothing here fabricates one. The gate is therefore causal with
        respect to the *exchange's* clock -- a point is consumed at the first bar close at
        or after its `create_time` -- but earlier than any live platform could have had
        it: a 5 m snapshot reaches a poller seconds-to-minutes after its stamp, and the
        bulk archive up to a day after. The error is bounded by that delivery lag, always
        in the favourable direction, and is the price of the dataset; a strategy whose
        edge depends on reacting to OI inside its publication lag is one this replay
        cannot honestly evaluate, and this docstring is where that is on the record.
        """
        wants = "metrics" in self.requirements.datasets or any(
            i.feed == "oi" for i in self.indicators.all()
        )
        if not wants:
            return

        placeholders = ", ".join("?" for _ in self.config.symbols)
        table = query(
            self.root,
            f"""
            SELECT "symbol", "create_time", "sum_open_interest"
            FROM "metrics"
            WHERE "symbol" IN ({placeholders})
              AND "create_time" >= ? AND "create_time" < ?
            ORDER BY "create_time", "symbol"
            """,
            datasets=("metrics",),
            params=[*self.config.symbols, int(data_start), int(self.config.end_ms)],
        )
        if table.num_rows == 0:
            raise RunAborted(
                "this strategy uses open interest, and the metrics dataset holds no rows "
                f"for {list(self.config.symbols)} in this range. Ingest it, or remove the "
                "dependency -- ctx.oi() would otherwise return None for the whole run."
            )
        symbols = table.column("symbol").to_pylist()
        times = table.column("create_time").to_pylist()
        values = table.column("sum_open_interest").to_pylist()
        # `create_time` is the instant of an open-interest snapshot, not the start of a
        # window, so consuming a point at the first bar close at or after it is causal
        # with respect to the exchange's clock -- and only that clock; the dataset has no
        # receive stamp, so the platform's own delivery lag is unmodelled. See the
        # docstring above for the bound and the direction of the error.
        self._oi_points = [
            (times[i], symbols[i], float(from_scaled(values[i])))
            for i in range(table.num_rows)
        ]
        self._oi_index = 0

    MACRO_SERIES_COLUMNS = (
        ("btc_dominance", "btc_dominance"),
        ("eth_dominance", "eth_dominance"),
        ("total_market_cap_usd", "total_market_cap_usd"),
        ("total_volume_usd", "total_volume_usd"),
    )
    """`macroGlobal` columns published as named series to `ctx.macro()` (Phase 11)."""

    def load_macro(self, data_start: int) -> None:
        """Read the macro datasets, for a strategy that declared one (Phase 11).

        Gated on declaration for the same reason `load_open_interest` is -- a run that
        never calls `ctx.macro()` should not pay to scan them -- but with the opposite
        answer when the lake is empty. Open interest is a *dependency*: a strategy that
        asked for it and got nothing is misconfigured, so that loader aborts. Macro is a
        *signal input* the phase specifies as optional, so an empty lake is a run that
        proceeds with `ctx.macro()` answering `None` -- and a `MACRO_MISSING` flag plus a
        warning, so the absence is on the record rather than mistaken for a signal that
        said nothing.

        The rows are read into one time-ordered list across both datasets, because
        `_consume_macro` walks a single cursor and interleaving at load time is cheaper
        than merging per bar.

        **Every macro row carries two clocks, and they are not interchangeable.** `ts_ms`
        is the *provider's* stamp -- CoinGecko's `updated_at`, Yahoo's `regularMarketTime`
        -- the instant the value holds for. `recv_ms` is when our poller actually had it
        in hand. They differ by the provider's publication lag plus our poll interval:
        measured on 2026-08-03 at ~3.4 min for CoinGecko and ~10.0 min for DXY, and
        bounded above by the hourly default poll.

        Ordering and windowing therefore key on `recv_ms`, not `ts_ms`. Keying on `ts_ms`
        would hand the strategy a reading minutes-to-an-hour before the platform could
        possibly have known it -- a look-ahead that is invisible in a backtest, always
        favourable, and would not survive contact with a live session, where the engine
        can only read rows already written to the lake. `ts_ms` is still carried through
        to `ctx.macro()` so `MacroView.age_ms` reports the value's true age.

        `COALESCE` covers a row with no `recv_ms`: nothing backfills macro today, but a
        historical archive would have no honest receive time and its own stamp is then
        the best available estimate.
        """
        declared = set(self.requirements.datasets)
        wanted = declared & {"macroGlobal", "macroFx"}
        if not wanted:
            return
        self.runtime.macro_declared = True

        points: list[tuple[int, str, float, int]] = []

        if "macroGlobal" in wanted:
            table = query(
                self.root,
                """
                SELECT COALESCE("recv_ms", "ts_ms") AS "visible_ms", "ts_ms",
                       "btc_dominance", "eth_dominance",
                       "total_market_cap_usd", "total_volume_usd"
                FROM "macroGlobal"
                WHERE COALESCE("recv_ms", "ts_ms") >= ?
                  AND COALESCE("recv_ms", "ts_ms") < ?
                ORDER BY "visible_ms"
                """,
                datasets=("macroGlobal",),
                params=[int(data_start), int(self.config.end_ms)],
            )
            visible = table.column("visible_ms").to_pylist()
            times = table.column("ts_ms").to_pylist()
            for name, column in self.MACRO_SERIES_COLUMNS:
                values = table.column(column).to_pylist()
                scale = (
                    MACRO_USD_UNIT if column.endswith("_usd") else SCALE
                )
                for index in range(table.num_rows):
                    raw = values[index]
                    if raw is None:
                        continue
                    points.append(
                        (visible[index], name, raw / scale, times[index])
                    )

        if "macroFx" in wanted:
            table = query(
                self.root,
                """
                SELECT COALESCE("recv_ms", "ts_ms") AS "visible_ms", "ts_ms",
                       "series", "value"
                FROM "macroFx"
                WHERE COALESCE("recv_ms", "ts_ms") >= ?
                  AND COALESCE("recv_ms", "ts_ms") < ?
                ORDER BY "visible_ms"
                """,
                datasets=("macroFx",),
                params=[int(data_start), int(self.config.end_ms)],
            )
            visible = table.column("visible_ms").to_pylist()
            times = table.column("ts_ms").to_pylist()
            names = table.column("series").to_pylist()
            values = table.column("value").to_pylist()
            for index in range(table.num_rows):
                if values[index] is None or not names[index]:
                    continue
                points.append(
                    (
                        visible[index],
                        str(names[index]).lower(),
                        values[index] / SCALE,
                        times[index],
                    )
                )

        # Sorted by *visibility* time, with a stable sort, so two readings that became
        # visible in the same millisecond keep the order they were read in rather than
        # depending on how Python compares their names.
        points.sort(key=lambda point: point[0])
        self._macro_points = points
        self._macro_index = 0

        if not points:
            self.flags.add("MACRO_MISSING")
            self.warnings.append(
                f"this strategy declared {sorted(wanted)} but the lake holds no macro rows "
                f"in this range, so ctx.macro() returns None throughout. The run is not "
                f"invalid -- macro data is a signal input, not a dependency -- but a flat "
                f"result here is the absence of data rather than the absence of a signal. "
                f"Run `perplab macro` to start collecting."
            )

    def note_funding_time(self, symbol: str, ts_ms: int) -> None:
        """Record a settlement instant learned while the run is in flight.

        A backtest gets the whole schedule up front from the funding rows; a live session
        learns the next one from `premiumIndex.nextFundingTime` and has to be able to add it
        as it goes. Without this `_next_funding_ms` returns `None`, `_flatten_reason` returns
        `None`, and `AutoFlatten.before_funding_ms` -- a platform guarantee the operator
        asked for -- silently never fires.

        Kept sorted because `_next_funding_ms` bisects it.
        """
        times = self._funding_times.setdefault(symbol, [])
        index = bisect_right(times, ts_ms)
        if index and times[index - 1] == ts_ms:
            return
        times.insert(index, ts_ms)

    # -------------------------------------------------------------------- dispatching

    def _dispatch(self, event: Event) -> None:
        self.runtime.advance(event.ts_ms)
        # The horizon the book may be pulled to, and the whole of spec 6.2's `BOOK_UPDATE`
        # ordering in one line. Book state is a *state*, so applying every row at or before
        # T and keeping the last is the same thing as reading the row in force at T -- and
        # the three priorities that precede `BOOK_UPDATE` must not see rows stamped T, so
        # their horizon is one millisecond earlier. See `ticks.StateStream`.
        self._book_horizon = (
            event.ts_ms if event.kind >= EventKind.BOOK_UPDATE else event.ts_ms - 1
        )
        kind = event.kind
        if kind is EventKind.TRADE:
            self._on_trade(event.payload)
        elif kind is EventKind.MARK_PRICE_UPDATE:
            self._on_mark(event.ts_ms, event.payload)
        elif kind is EventKind.FUNDING_SETTLEMENT:
            self._on_funding(event.ts_ms, event.payload)
        elif kind is EventKind.LIQUIDATION_CHECK:
            self._on_liquidation_check(event.ts_ms)
        elif kind is EventKind.ORDER_FILL_CHECK:
            self._on_fill_check(event.ts_ms)
        elif kind is EventKind.BAR_CLOSE:
            self._on_bar_close(event.payload)
        elif kind is EventKind.BOOK_UPDATE:
            self._on_book_update(event.payload)
        elif kind is EventKind.ORDER_ARRIVAL:
            self._on_arrival(event.ts_ms, event.payload)
        else:  # pragma: no cover - every kind this engine emits is handled above
            raise RunAborted(f"the engine received an unexpected {kind.name}")

        # One safe point per event, after the ledger and the book are consistent again.
        # See `_note_order_end`.
        if self._pending_order_ends:
            self._drain_order_ends()

    def _on_book_update(self, payload: Any) -> None:
        """A book observation, dispatched as a real event rather than pulled as state.

        Two shapes arrive here and the discriminator is the payload's own type. A
        `DepthSnapshot` is a ladder; anything else is a top-of-book quote.

        **Top-of-book has no priority of its own, and must not be given one.** Spec 6.2
        fixes the nine-kind table, and renumbering it would silently reorder every stored
        run. In a backtest the quote is *pulled state* -- `ticks.StateStream` advances it to
        the horizon -- but a live session has no stream to pull from: the quote arrives as a
        frame at a moment, and the tape has to record it in the order it was observed or the
        shadow backtest cannot reproduce the session. So it rides `BOOK_UPDATE`, which is
        where a book observation belongs, and both engines apply it identically.

        A ladder additionally feeds the depth indicators; a quote does not, because there is
        no depth-driven indicator that a two-level quote could serve without lying about how
        much of the book it saw.
        """
        if isinstance(payload, DepthSnapshot):
            self.market.apply_ladder(payload)
            self.indicators.on_depth(payload)
        else:
            self.market.apply_top(payload)

    def _sync_market(self) -> None:
        """Pull every book stream up to the current horizon, at most once per horizon."""
        horizon = self._book_horizon
        if horizon <= self._book_synced:
            return
        self._book_synced = horizon
        for stream in self.top_streams.values():
            row = stream.advance_to(horizon)
            if row is not None:
                self.market.apply_top(row)
        for stream in self.depth_streams.values():
            row = stream.advance_to(horizon)
            if row is not None:
                self.market.apply_ladder(row)

    # -------------------------------------------------------------------------- ticks

    def _on_trade(self, payload: Any) -> None:
        """A print, from the tick tape or from a bar's first/last trade.

        Two shapes reach here and the difference is the tier. At `BAR_CLOSE` the payload is
        a `(symbol, scaled_price)` pair from `feed.bar_events`: a kline's `open` *is* the
        bar's first trade and its `close` is the last, so both are datable prints, but a
        kline says nothing about which side was hit and there is nothing to queue behind.
        At every other tier it is a real `TradePrint` off `aggTrades`.
        """
        if type(payload) is tuple:
            symbol, price = payload
            self.last_print[symbol] = price
            self.last_print_ts[symbol] = self.runtime.now_ms
            return

        trade: TradePrint = payload
        self.last_print[trade.symbol] = trade.price_scaled
        self.last_print_ts[trade.symbol] = trade.ts_ms
        self.market.apply_trade(trade)
        self.indicators.on_trade(trade)
        self.counts["ticks"] += 1
        if self.resting.orders_for(trade.symbol):
            self._pending_trades.append(trade)
            self._schedule_fill_check(trade.ts_ms)
        if "on_tick" in self._hooks:
            self.strategy.on_tick(self.context, trade)

    def _schedule_fill_check(self, ts_ms: int) -> None:
        """Queue one `ORDER_FILL_CHECK` for this instant, at spec 6.2's priority 5.

        Scheduled rather than run inline so that *every* trade at this millisecond has
        printed before any resting order is evaluated against them -- which is what priority
        4 before 5 means. Running inline would fire `on_fill` between two prints that the
        exchange treated as one sweep.

        Only scheduled when something is actually resting. A run of market orders would
        otherwise pay for an extra queued event per trade millisecond to discover, eighteen
        million times, that there is nothing to check.
        """
        if self._fill_check_ts == ts_ms:
            return
        self._fill_check_ts = ts_ms
        self._fill_seq += 1
        self.queue.push(
            Event(
                ts_ms=ts_ms,
                kind=EventKind.ORDER_FILL_CHECK,
                source_seq=self._fill_seq,
                dataset_id="engine",
            )
        )

    def _on_fill_check(self, ts_ms: int) -> None:
        """Evaluate resting orders against this millisecond's prints (spec 6.2 priority 5).

        Order of operations inside the check is itself load-bearing:

        1. The book is pulled to this instant, so the queue bound is refined against the
           freshest observation before anything consumes it.
        2. Contract-price triggers fire on the prints, because a stop watching the trade
           tape should see the print that hit its level, not the one after.
        3. Resting limits consume the prints in arrival order.

        A trigger firing before the limits means a stop and a resting limit at the same
        level resolve in the order the exchange would: the trigger becomes a market order
        that leaves on its own latency, while the limit is still passive.
        """
        self._fill_check_ts = None
        trades = self._pending_trades
        self._pending_trades = []
        if not trades:
            return
        self._sync_market()

        by_symbol: dict[str, list[TradePrint]] = {}
        for trade in trades:
            by_symbol.setdefault(trade.symbol, []).append(trade)

        for symbol, prints in by_symbol.items():
            self.resting.observe_book(symbol, ts_ms)
            # `ordered=True`: this is the tape, keyed by `agg_id`. See `observe_price`.
            for trigger in self.resting.observe_price(
                symbol,
                [from_scaled(p.price_scaled) for p in prints],
                WorkingType.CONTRACT_PRICE,
                ordered=True,
            ):
                self._fire_trigger(ts_ms, trigger.order_id, trigger.price)
            for trade in prints:
                for fill in self.resting.on_trade(trade):
                    self._book_maker_fill(ts_ms, fill)

        self._service_parked_markets(ts_ms, trades)

    def _service_parked_markets(self, ts_ms: int, trades: Sequence[TradePrint]) -> None:
        """Fill `TRADE_ONLY` market orders against the first print at or after arrival.

        Spec 6.4: *"fill at the next trade price after arrival"*. The order genuinely waits,
        and its fill carries the print's timestamp rather than the arrival timestamp, so a
        market order into a quiet minute shows the delay it actually suffered instead of
        pretending to have filled instantly at a price from before it was sent.
        """
        if self.tier is not FillTier.TRADE_ONLY:
            return
        for trade in trades:
            for order in self.resting.orders_for(trade.symbol):
                if not _is_market_now(order) or order.limit_scaled:
                    continue
                if trade.ts_ms < order.arrival_ts:
                    continue
                self.resting.discard(order)
                self._execute_market(ts_ms, order, print_scaled=trade.price_scaled)

    # -------------------------------------------------------------------------- marks

    def _on_mark(self, ts_ms: int, mark: MarkBar) -> None:
        """Record the sample and remember the range it traversed.

        The *sample* is the close, held flat until the next one -- spec 3.4's LOCF rule,
        which is the only mark this engine ever settles funding or values equity against.
        The high and low are stashed for the liquidation check, which is the one question
        that is about a range rather than an instant.
        """
        close = from_scaled(mark.close)
        self.account.update_mark(ts_ms, mark.symbol, close)
        self.pending_range[mark.symbol] = (
            from_scaled(mark.low),
            from_scaled(mark.high),
            close,
        )
        self._schedule_liquidation_check(ts_ms)

    def _schedule_liquidation_check(self, ts_ms: int) -> None:
        """Queue one `LIQUIDATION_CHECK` for this instant, at spec 6.2's priority 2.

        Scheduled rather than run inline, and that is the whole point of the priority
        table. Running it inside `_on_mark` would put it *before* any funding settling at
        the same millisecond, and spec 6.2's R5 says funding must come first because a
        funding payment reduces margin and can itself cause the liquidation. Inline, that
        rule would be silently inverted and a position one payment away from liquidation
        would survive.
        """
        if self._liq_scheduled_ts == ts_ms:
            return
        self._liq_scheduled_ts = ts_ms
        self._liq_seq += 1
        self.queue.push(
            Event(
                ts_ms=ts_ms,
                kind=EventKind.LIQUIDATION_CHECK,
                source_seq=self._liq_seq,
                dataset_id="engine",
            )
        )

    def _on_liquidation_check(self, ts_ms: int) -> None:
        """Probe the mark's traversed range, settle back on the LOCF sample, *then* sample.

        A minute of mark data says the price reached `low` and `high` at some point inside
        it. A position whose `P_liq` sits between them *was* liquidated, and checking only
        the close would miss every liquidation the market immediately recovered from --
        which is most of them, and exactly the ones an equity curve most needs to show.

        Longs are taken at the low and shorts at the high, so both extremes are probed in a
        fixed order regardless of what is open. Under isolated margin (spec 3.7) positions
        do not defend each other, so probing them together cannot make one liquidation
        cause or prevent another, and the order is therefore free to be a constant -- which
        is what determinism requires.

        **Nothing observes the probed mark.** Two consequences follow and both were defects
        before they were rules:

        *No equity sample is taken during the probe.* Doing so put fabricated states into
        the run's own equity series: every positioned symbol was moved to its low together
        and then to its high together, which for a market-neutral pair cancels in both
        samples and reports a maximum drawdown of exactly zero for a book that traversed 4%.
        And because the low was always scored first, against a peak the high had not yet
        raised, a long book reported -30% where its own two samples implied -46% while the
        mirrored short reported the -46%. The intrabar band is carried alongside the close
        sample instead -- see `_sample_equity`.

        *No strategy hook runs while a probe price is in place.* `on_liquidation` used to be
        dispatched from inside the probe, so `ctx.mark()` returned a price this method's own
        docstring calls evidence rather than an observation -- and any order the hook placed
        took that price as its slippage reference. The ledger work happens during the probe,
        where it belongs; the hooks are deferred until the close has been restored.

        Mark-price stop triggers are evaluated **after** the probe, from the recorded range
        rather than from the probed marks, for exactly the same reason: a triggered stop
        submits an order, and an order priced against a probe price is priced against
        evidence rather than an observation.
        """
        ranges = self.pending_range
        self.pending_range = {}
        self._liq_scheduled_ts = None
        pending_hooks: list[LiquidationResult] = []

        # **Against a real venue, the venue owns liquidation.** The probe below closes a
        # position at a *modelled* trigger price with a modelled recovery haircut, and
        # locally cancels the symbol's orders -- which, live, are still working at the
        # exchange, so their real fills would arrive to find their orders terminal and be
        # dropped. Meanwhile the ledger's own P_liq agrees with Binance's only to the 0.5%
        # tolerance `live.reconcile` documents, so the model can fire when the venue does
        # not, or stay silent when it does. Live, the marks advance to the close, the
        # crossing is reported loudly (`_note_liquidation_proximity`), and the truth
        # arrives as the venue's own `autoclose-` reports and the reconciliation pass.
        if ranges and self.account.positions:
            if self.transport.simulated:
                for index in (0, 1):
                    for symbol, values in ranges.items():
                        if self.account.has_position(symbol):
                            self.account.update_mark(ts_ms, symbol, values[index])
                    pending_hooks.extend(
                        self._record_liquidations(self.account.check_liquidations(ts_ms))
                    )
                for symbol, values in ranges.items():
                    self.account.update_mark(ts_ms, symbol, values[2])
            else:
                self._note_liquidation_proximity(ts_ms, ranges)
                for symbol, values in ranges.items():
                    self.account.update_mark(ts_ms, symbol, values[2])

        if self.transport.simulated:
            pending_hooks.extend(
                self._record_liquidations(self.account.check_liquidations(ts_ms))
            )
        self._sample_equity(ts_ms, ranges)
        for result in pending_hooks:
            if "on_liquidation" in self._hooks:
                self.strategy.on_liquidation(self.context, result)

        self._check_mark_triggers(ts_ms, ranges)
        self._expire_parked(ts_ms)
        self._check_auto_flatten(ts_ms)

    # -------------------------------------------------------------------- auto-flatten

    def _check_auto_flatten(self, ts_ms: int) -> None:
        """Close positions that have hit a platform deadline. See `AutoFlatten`.

        Runs once per mark bar, which is once a minute -- the same cadence as the
        liquidation check and for the same reason: it is the finest grid on which the
        account's own valuation actually moves.

        The exit is submitted through `_submit`, so it takes latency, pays the spread, and
        appears in the event log as an order like any other. The one thing it does not do is
        wait for a fill before it stops re-sending: `_flatten_sent` holds the symbol until
        the position is gone, because a deadline that is still past on the next mark would
        otherwise queue a second exit against a position the first one is already closing.
        """
        flatten = self.config.auto_flatten
        if not flatten.enabled or self._halt_pending is not None:
            return
        for key in self._open_keys():
            if key in self._flatten_sent:
                continue
            reason = self._flatten_reason(ts_ms, key, flatten)
            if reason is None:
                continue
            order_id = self._submit_flatten(ts_ms, key, reason)
            if order_id is not None:
                self._platform_orders.add(order_id)
                self._flatten_sent.add(key)
                self._flatten_orders[order_id] = key
                self.counts["auto_flattens"] += 1
                self.flags.add("AUTO_FLATTENED")
                self.runtime.emit(
                    "AUTO_FLATTEN",
                    {
                        "symbol": key[0],
                        "position_side": key[1].value,
                        "reason": reason,
                        "order_id": order_id,
                    },
                )

    def _flatten_reason(
        self, ts_ms: int, key: PositionKey, flatten: AutoFlatten
    ) -> str | None:
        """Why this position must close now, or `None`.

        Hold time is measured from when the position was **opened**, not from the last
        increment. A strategy that adds to a winner every hour would otherwise never reach
        any deadline, which is the opposite of what a maximum-hold guarantee is for.

        Per side, on both counts. The hold clock is per leg; the funding deadline is per
        symbol but the *exit* is per leg, and a hedge whose legs are equal pays zero net
        funding -- so flattening only one of them before a settlement would turn a neutral
        position into a directional one at the worst possible moment. `_check_auto_flatten`
        sweeps every open key, so both legs exit on the same bar.
        """
        if flatten.max_hold_ms is not None:
            since = self._position_since.get(key)
            if since is not None and ts_ms - since >= flatten.max_hold_ms:
                return f"max_hold:{(ts_ms - since) // 1000}s"
        if flatten.before_funding_ms is not None:
            due = self._next_funding_ms(key[0], ts_ms)
            if due is not None and due - ts_ms <= flatten.before_funding_ms:
                return f"before_funding:{due}"
        return None

    def _next_funding_ms(self, symbol: str, ts_ms: int) -> int | None:
        # `bisect_right`, not `bisect_left`: a settlement stamped exactly `ts_ms` has
        # already been charged. Funding settles at spec 6.2's priority 1 and this check runs
        # from the liquidation check at priority 2, so by the time it is asked, "now" is in
        # the past. Treating it as upcoming made the engine flatten against a payment it had
        # already made.
        """The next settlement at or after `ts_ms`, from the schedule the run loaded.

        Read from the funding series rather than from Binance's nominal 8-hour grid.
        Settlements have moved -- the rate is published per interval and the interval itself
        has changed on some symbols -- so a hard-coded 00:00/08:00/16:00 would flatten
        against a timetable the data does not have, and would do it silently.
        """
        times = self._funding_times.get(symbol)
        if not times:
            return None
        index = bisect_right(times, ts_ms)
        return times[index] if index < len(times) else None

    def _open_keys(self) -> tuple[PositionKey, ...]:
        """Every open position, in a reproducible order. See `Account._SIDE_ORDER`."""
        return tuple(
            p.key
            for p in sorted(
                self.account.positions.values(),
                key=lambda p: (p.symbol, _SIDE_ORDER[p.position_side]),
            )
        )

    def _submit_flatten(self, ts_ms: int, key: PositionKey, reason: str) -> str | None:
        """Send a market exit for the whole position on one side."""
        intent = self._flatten_intent(key, tag=f"auto_flatten:{reason}")
        if intent is None:
            return None
        try:
            return self._submit(intent)
        except UnsupportedOrder:  # pragma: no cover - market orders work at every tier
            return None

    def _flatten_intent(self, key: PositionKey, *, tag: str) -> OrderIntent | None:
        symbol, side = key
        position = self.account.position(symbol, side)
        if position is None or position.qty == 0:
            return None
        qty = quantize_qty(abs(position.qty), from_scaled(self.filters[symbol].step_size))
        if qty <= 0:
            return None
        return OrderIntent(
            symbol=symbol,
            side="SELL" if position.qty > 0 else "BUY",
            qty=qty,
            type=OrderType.MARKET,
            # `reduce_only` and a hedged `positionSide` are mutually exclusive at the
            # exchange, and the flag is redundant there anyway: a sell routed to the LONG
            # side can only reduce it. See `OrderIntent.__post_init__`.
            reduce_only=not side.is_hedged,
            position_side=side,
            tag=tag,
        )

    def _force_flatten(self, ts_ms: int, key: PositionKey, *, reason: str) -> bool:
        """Close a position **now**, bypassing the latency queue.

        Used only by the halt path. A halt is the platform closing its own book, and there
        are no further events to fill a scheduled order against -- an exit that waited a
        latency window would simply never happen, and the run would report an open position
        it had decided to close.

        Priced by the ordinary fill path all the same, so it pays the spread and the taker
        fee. `False` when the tier could not price it -- at `TRADE_ONLY` with no print in
        hand, there is no honest exit price -- and the `KILL_SWITCH` event's `open_after`
        list is what says so.
        """
        intent = self._flatten_intent(key, tag=reason)
        if intent is None:
            return False
        reference = self._reference_price(key[0])
        if reference is None:
            return False
        self.order_seq += 1
        order_id = f"o{self.order_seq}"
        order = Order(
            id=order_id,
            intent=intent,
            submit_ts=ts_ms,
            arrival_ts=ts_ms,
            reference_price=reference,
            remaining=decimal_to_scaled(intent.qty),
        )
        self.orders[order_id] = order
        self.counts["orders"] += 1
        self._platform_orders.add(order_id)
        self.runtime.emit(
            "FORCE_FLATTEN", {"order_id": order_id, "reason": reason, **intent.to_json()}
        )
        # **The platform's own order is not the strategy's.** Booking its refusal into
        # `rejects` raised `ORDERS_REJECTED` and produced a warning telling the reader that
        # "1 order(s) were rejected at arrival; see the REJECT entries for the filter or
        # margin that refused them" -- about an order the strategy never sent. Worse, it fed
        # `observe_rejection`, so a flatten the exchange would not accept counted towards the
        # kill switch's rejection trigger.
        self._place_market(ts_ms, order)
        label = position_label(*key)
        if order.status is OrderStatus.REJECTED:
            self.runtime.emit(
                "FORCE_FLATTEN_FAILED",
                {
                    "order_id": order_id,
                    "reason": order.reason,
                    "symbol": key[0],
                    "position_side": key[1].value,
                },
            )
            self.warnings.append(
                f"{label}: the halt could not close the position -- {order.reason}. The "
                f"exposure is still open at the end of this run."
            )
        if order.is_open:
            # `TRADE_ONLY` parks a market order until the next print, and the halt is the
            # last thing this run does -- there is no next print. Retiring it keeps
            # `ctx.open_orders()` and the order table honest about what was live at the end.
            self._remove(
                order,
                OrderStatus.EXPIRED,
                "the halt could not price an exit at this tier before the run stopped",
            )
        return self.account.position(*key) is None

    def _transport_flatten(self, ts_ms: int, key: PositionKey, *, reason: str) -> str | None:
        """Ask the venue to close a position: `_force_flatten`'s sibling for a real transport.

        `_force_flatten` is a statement about a *model*: the book is the engine's own, so
        "close now" is something it can simply do, priced by its own fill path. Against a
        real exchange the same sentence is a request -- a reduce-only market order whose
        fill comes back on the user-data stream and enters through `_book_fill` like every
        other execution. Nothing is booked here, the position stays open in the ledger until
        the venue says otherwise, and the return value is the order id in flight rather than
        a claim of closure, because closure is not this method's to claim.

        Bypasses `_submit` exactly as `_force_flatten` does and for the same reason: by the
        time a halt is flattening, `RiskEngine.halted` is already true and `check_order`
        refuses everything -- an account that has breached a limit is precisely the account
        whose exits have to work. The order is minted with the same identity scheme, counted
        in `_platform_orders` so its refusal is never blamed on the strategy, and handed to
        the transport, which owns everything from here.
        """
        intent = self._flatten_intent(key, tag=reason)
        if intent is None:
            return None
        reference = self._reference_price(key[0])
        if reference is None:
            return None
        self.order_seq += 1
        order_id = f"o{self.order_seq}"
        order = Order(
            id=order_id,
            intent=intent,
            submit_ts=ts_ms,
            arrival_ts=ts_ms,
            reference_price=reference,
            remaining=decimal_to_scaled(intent.qty),
        )
        self.orders[order_id] = order
        self.counts["orders"] += 1
        self._platform_orders.add(order_id)
        self.runtime.emit(
            "FORCE_FLATTEN", {"order_id": order_id, "reason": reason, **intent.to_json()}
        )
        try:
            self.transport.place(order)
        except Exception as exc:  # noqa: BLE001 - a failed exit must be loud, never fatal
            # The request never left. The order is retired so the book stays honest, and the
            # warning is what tells the operator the exposure is still standing -- which it
            # genuinely is, at the exchange, whatever this process does next.
            self._remove(
                order,
                OrderStatus.REJECTED,
                f"the exit could not be handed to the transport: {type(exc).__name__}: {exc}",
            )
            self.warnings.append(
                f"{position_label(*key)}: the halt could not send a closing order to the "
                f"exchange ({type(exc).__name__}: {exc}). The position is still open at the "
                f"venue and needs attention."
            )
            return None
        return order_id

    def _check_mark_triggers(
        self, ts_ms: int, ranges: Mapping[str, tuple[Money, Money, Money]]
    ) -> None:
        """Fire mark-triggered stops the mark bar's range reached (spec 6.4).

        The prices are offered as an **unordered set** -- a mark bar says the mark reached
        `high` and `low` somewhere inside the minute and says nothing about when -- so
        `observe_price` is called with `ordered=False` and takes a trailing stop's extreme
        over the whole range before testing any of it. That is the pessimistic reading for
        every order type this feeds: the trigger level ends as close as the bar permits, and
        a stop or take-profit anywhere inside the range is taken rather than missed.

        The trigger is stamped at the bar's `close_time`, because the bar says the mark
        reached that level *somewhere inside the minute* and says nothing about when. Late is
        the conservative direction for both a stop (fills further away) and a take-profit
        (the move may have retraced), so the uncertainty is spent against the strategy.
        """
        if not self.resting.symbols():
            return
        for symbol in tuple(self.resting.symbols()):
            values = ranges.get(symbol)
            if values is None:
                mark = self.account.marks.get(symbol)
                if mark is None:
                    continue
                prices: list[Money] = [mark]
            else:
                low, high, close = values
                prices = [high, low, close] if high != low else [close]
            # `ordered=False`: a mark bar's range says *what* the mark reached, never when.
            for trigger in self.resting.observe_price(
                symbol, prices, WorkingType.MARK_PRICE, ordered=False
            ):
                self._fire_trigger(ts_ms, trigger.order_id, trigger.price)

    def _note_liquidation_proximity(
        self, ts_ms: int, ranges: Mapping[str, tuple[Money, Money, Money]]
    ) -> None:
        """Live mode's replacement for the liquidation probe: report the crossing, book
        nothing.

        The mark range straying across the ledger's own `P_liq` is the strongest warning
        this process can produce about a real position -- and also not a fact about the
        account, because the venue resolves its own bracket table against its own mark and
        the two liquidation prices agree only to `live.reconcile`'s stated tolerance. So
        the crossing goes into the event log and the warnings, once per position, and the
        outcome belongs to the venue: an actual liquidation arrives as an `autoclose-`
        report and a reconciliation mismatch, which halt the session through the paths
        built for them.
        """
        for symbol, values in ranges.items():
            for side in self._sides:
                if self.account.qty(symbol, side) == 0:
                    continue
                key = (symbol, side)
                if key in self._liq_proximity_noted:
                    continue
                try:
                    liq = self.account.liquidation_price(symbol, side)
                except LookupError:
                    continue
                if liq is None:
                    continue
                low, high = min(values[0], values[1]), max(values[0], values[1])
                if not low <= liq <= high:
                    continue
                self._liq_proximity_noted.add(key)
                self.runtime.emit(
                    "LIQUIDATION_PROXIMITY",
                    {
                        "symbol": symbol,
                        "position_side": side.value,
                        "liquidation_price": money_to_str(liq),
                        "range_low": money_to_str(low),
                        "range_high": money_to_str(high),
                    },
                )
                self.warnings.append(
                    f"{position_label(symbol, side)}: the mark range "
                    f"[{money_to_str(low)}, {money_to_str(high)}] crossed the ledger's "
                    f"liquidation price {money_to_str(liq)} at {ts_ms}. The venue decides "
                    f"whether the position was liquidated -- watch the reconciliation "
                    f"panel and the exchange directly."
                )

    def _record_liquidations(
        self, results: Sequence[LiquidationResult]
    ) -> list[LiquidationResult]:
        """Book each liquidation into the ledger's satellites; defer the strategy hook."""
        for result in results:
            self.counts["liquidations"] += 1
            self.flags.add("LIQUIDATED")
            self.trades.liquidation(
                ts_ms=result.event.ts_ms,
                symbol=result.symbol,
                closed_qty=result.closed_qty,
                price=result.trigger_price,
                realized=result.event.realized,
                position_side=result.position_side,
            )
            # Resting orders are cancelled by a liquidation (spec 6.6), and an order already
            # in flight is exactly the case the rule is about: letting it arrive would
            # reopen a position the exchange had just closed.
            self._cancel_all(result.symbol, reason="liquidated")
            # A liquidation never opens or flips a position, so the only transition it can
            # produce is "gone". Passing the closed magnitude as `before` would read as a
            # flip on a short, where the magnitude is positive and the position was not.
            if self.account.qty(result.symbol, result.position_side) == 0:
                self._note_position_change(
                    result.event.ts_ms,
                    (result.symbol, result.position_side),
                    _ZERO,
                    _ZERO,
                )
            self._observe_closed_trades(result.event.ts_ms)
            self.runtime.emit(
                "LIQUIDATION",
                {
                    "symbol": result.symbol,
                    "position_side": result.position_side.value,
                    "trigger_price": money_to_str(result.trigger_price),
                    "mark_price": money_to_str(result.mark_price),
                    "qty": money_to_str(result.closed_qty),
                    "margin_lost": money_to_str(result.margin_lost),
                },
            )
            breach = self.risk.observe_liquidation(result.event.ts_ms, result.symbol)
            if breach is not None:
                self._request_halt(breach)
        return list(results)

    # -------------------------------------------------------------- order-end dispatch

    def _note_order_end(self, order: Order, status: OrderStatus, reason: str) -> None:
        """Queue an `on_cancel` notification for the next safe point.

        Queued rather than dispatched, for two independent reasons and either would be
        enough. A cancel can originate inside `_record_liquidations`, which runs with a
        *probed* mark in force -- an order the hook placed there would be priced against a
        price the engine's own docstring calls evidence rather than an observation. And
        `_cancel_all` iterates the order book while removing from it; a hook that cancelled
        or submitted from inside that loop would mutate the collection being walked.
        """
        if "on_cancel" not in self._hooks:
            return
        if order.id in self._platform_orders:
            # A halt's forced exit or an auto-flatten. The strategy never placed it, so
            # telling it the order was cancelled would report a decision it did not make --
            # and a strategy that requotes from `on_cancel` would requote against one.
            # `_reject` already returns before this for the same reason; `_remove` reaches
            # here for a parked flatten that expired, so the guard belongs in one place.
            return
        self._pending_order_ends.append(
            OrderEnd(
                order_id=order.id,
                symbol=order.intent.symbol,
                side=order.intent.side,
                status=status.value,
                reason=reason,
                ts_ms=self.runtime.now_ms,
                filled_qty=from_scaled(order.filled_scaled),
                remaining_qty=from_scaled(order.remaining),
                tag=order.intent.tag,
            )
        )

    def _drain_order_ends(self) -> None:
        """Dispatch queued `on_cancel` calls. Called once per engine event.

        Re-entrant by design: a hook that cancels another order appends to the same list and
        the loop picks it up on the next turn rather than recursing.

        **Bounded explicitly, because the obvious bound does not hold.** The tempting
        argument -- one notification per order, and an order can only leave the book once --
        is false: a hook can *create* orders, and an order created inside `on_cancel` that is
        refused by the risk layer terminates synchronously and appends to the list being
        drained. One cancel then produces an unbounded chain, and `timeout_s` cannot rescue
        it because `_checkpoint` only runs between top-level events. So the drain has its own
        ceiling and its own named failure, and a `deque` replaces the `pop(0)` that made it
        quadratic on the way there.
        """
        drained = 0
        while self._pending_order_ends:
            drained += 1
            if drained > MAX_ORDER_ENDS_PER_EVENT:
                raise RunAborted(
                    f"more than {MAX_ORDER_ENDS_PER_EVENT:,} order-end notifications were "
                    f"dispatched from a single event. An `on_cancel` hook that submits an "
                    f"order which is then refused will do this forever -- check that "
                    f"anything it places can actually be accepted."
                )
            event = self._pending_order_ends.popleft()
            self.strategy.on_cancel(self.context, event)

    # ------------------------------------------------------------------- risk feedback

    def _release_flatten(self, order: Order) -> None:
        """An auto-flatten order left the book without closing anything: unlatch and say so.

        `_flatten_sent` exists so one deadline does not queue a second exit while the first
        is in flight. Clearing it only on a *fill* meant a flatten the exchange refused --
        a residue below `MIN_NOTIONAL`, a parked `TRADE_ONLY` order that expired -- latched
        the symbol for the rest of the run. The position was never retried, and the run
        still reported `auto_flattens: 1` with the `AUTO_FLATTENED` flag: a platform
        guarantee recorded as kept, and not kept.
        """
        key = self._flatten_orders.pop(order.id, None)
        if key is None:
            return
        symbol, routed = key
        if self.account.qty(symbol, routed) == 0:
            return
        self._flatten_sent.discard(key)
        self.counts["auto_flattens"] -= 1
        self.runtime.emit(
            "AUTO_FLATTEN_FAILED",
            {
                "symbol": symbol,
                "position_side": routed.value,
                "order_id": order.id,
                "reason": order.reason,
            },
        )
        if "AUTO_FLATTEN_UNMET" not in self.flags:
            self.flags.add("AUTO_FLATTEN_UNMET")
            self.warnings.append(
                f"{symbol}: a platform auto-flatten did not close the position "
                f"({order.reason}). The deadline is retried on the next mark, and until it "
                f"succeeds the position is held past the limit that was asked for."
            )

    def _note_position_change(
        self, ts_ms: int, key: PositionKey, before: Money, after: Money
    ) -> None:
        """Maintain the hold clock and the auto-flatten latch, **per position side**.

        A **flip** restarts the clock. A fill that takes a long straight to a short closes
        one round-trip and opens another -- `TradeBuilder` says so, and the exposure the
        hold limit bounds is the new one. Treating it as a continuation would let a strategy
        that reverses every hour hold exposure indefinitely under a one-hour cap. (A hedged
        side cannot flip, so that branch only ever fires for `BOTH`.)
        """
        if after == 0:
            self._position_since.pop(key, None)
            self._flatten_sent.discard(key)
            return
        if before == 0 or (before > 0) != (after > 0):
            self._position_since[key] = ts_ms
            self._flatten_sent.discard(key)

    def _observe_closed_trades(self, ts_ms: int) -> None:
        """Feed newly closed round-trips to the consecutive-loss limit.

        Reads `TradeBuilder.trades` by index rather than taking a callback, because the
        builder closes a trade from three different places -- a fill that flattens, a fill
        that flips, and a liquidation -- and a counter wired to only some of them counts a
        losing streak that never breaks.
        """
        closed = self.trades.trades
        while self._closed_trades_seen < len(closed):
            trade = closed[self._closed_trades_seen]
            self._closed_trades_seen += 1
            breach = self.risk.observe_trade_closed(ts_ms, trade.net_pnl)
            if breach is not None:
                self._request_halt(breach)

    # ------------------------------------------------------------------------ funding

    def _on_funding(self, ts_ms: int, point: FundingPoint) -> None:
        rate = from_scaled(point.rate)
        if point.symbol not in self.account.marks:
            # No mark to settle against. Spec 3.4 forbids inferring one, and settling a real
            # cashflow against a guess is the fiction it rules out -- so the *cashflow* is
            # skipped. Everything else about the settlement still happens, and that split is
            # the fix: this used to return early from the whole handler, which dropped the
            # funding view, the indicator update and the liquidation check as well.
            #
            # It was also reachable on a complete lake. `load_marks` now carries a lead-in
            # bar so LOCF has an anchor at `data_start`, but a genuine hole at the front of
            # the range still lands here, and 400 USDT of cashflow on an open position used
            # to vanish with `funding_pnl` reporting a confident 0 and no flag at all.
            self._note_unsettled_funding(ts_ms, point, rate)
            return
        # Per side, not netted. A hedge pays on its long and receives on its short, and the
        # two are cashflows on two different round-trips whose *sum* happens to be zero --
        # so a single netted figure would attribute nothing to either trade and make a carry
        # strategy's whole edge invisible in `Trade.funding`.
        settled = self.account.apply_funding_by_side(ts_ms, point.symbol, rate)
        cashflow = sum(settled.values(), _ZERO)
        times = self._funding_times.get(point.symbol, [])
        following = next((t for t in times if t > ts_ms), None)
        self.runtime.funding_views[point.symbol] = FundingView(
            last_rate=rate, next_settlement_ms=following, predicted_rate=None
        )
        self.indicators.on_funding(point.symbol, float(rate))

        for routed, amount in settled.items():
            if amount != 0:
                self.trades.funding(
                    symbol=point.symbol, cashflow=amount, position_side=routed
                )
        if settled:
            self.runtime.emit(
                "FUNDING",
                {
                    "symbol": point.symbol,
                    "rate": money_to_str(rate),
                    "payment": money_to_str(cashflow),
                    "by_side": {
                        side.value: money_to_str(amount)
                        for side, amount in settled.items()
                    },
                },
            )
            if "on_funding" in self._hooks:
                self.strategy.on_funding(
                    self.context,
                    FundingEvent(
                        symbol=point.symbol,
                        ts_ms=ts_ms,
                        rate=rate,
                        mark_price=self.account.marks[point.symbol],
                        payment=cashflow,
                    ),
                )
        self._schedule_liquidation_check(ts_ms)

    def _note_unsettled_funding(
        self, ts_ms: int, point: FundingPoint, rate: Money
    ) -> None:
        """Record a settlement that could not be priced -- loudly, and only once per symbol.

        Everything that does not need a mark still happens: the funding view a strategy
        reads, the indicator that consumes the rate, and the liquidation check the ordering
        owes. Only the cashflow is withheld, and the run says so rather than reporting a
        confident zero.
        """
        following = next(
            (t for t in self._funding_times.get(point.symbol, []) if t > ts_ms), None
        )
        # `last_withheld=True` is the strategy-visible half of the withholding. The rate is
        # a market fact and is still published -- to `ctx.funding()` and to the funding
        # indicators, because the venue genuinely settled at it whether or not our lake
        # held a mark to book it against. What must not happen silently is a carry
        # strategy reading `last_rate` as a cashflow its own ledger booked: `funding_pnl`
        # omits this settlement (the `FUNDING_UNSETTLED` warning says so run-wide), and
        # the flag is how strategy code can see it per settlement. `on_funding` is not
        # dispatched -- `FundingEvent.payment` would have to be invented, and its
        # docstring's sign convention leaves no honest spelling of "unknown".
        self.runtime.funding_views[point.symbol] = FundingView(
            last_rate=rate,
            next_settlement_ms=following,
            predicted_rate=None,
            last_withheld=True,
        )
        self.indicators.on_funding(point.symbol, float(rate))
        self.counts["unsettled_funding"] += 1
        if "FUNDING_UNSETTLED" not in self.flags:
            self.flags.add("FUNDING_UNSETTLED")
            self.warnings.append(
                f"{point.symbol}: a funding settlement at {ts_ms} has no mark price to "
                f"settle against, so its cashflow is not booked. Spec 3.4 forbids inferring "
                f"a mark, and a made-up one would move a real balance. The funding column "
                f"understates what the position actually paid or received; ingest the "
                f"missing markPriceKlines before trusting the attribution."
            )
        self.runtime.emit(
            "FUNDING_UNSETTLED",
            {"symbol": point.symbol, "rate": money_to_str(rate), "reason": "no mark price"},
        )
        self._schedule_liquidation_check(ts_ms)

    # --------------------------------------------------------------------------- bars

    def _on_bar_close(self, step: BarStep) -> None:
        """Advance indicators for every symbol, then run `on_bar` for each.

        All indicators first, then all hooks. A pairs strategy reading the other leg's
        indicator inside its first `on_bar` would otherwise see a value one bar stale, and
        *which* leg was stale would depend on the order symbols came out of the query --
        a result that changes with an unrelated edit.
        """
        self._consume_open_interest(step.close_time)
        self._consume_macro(step.close_time)
        for bar in step.bars:
            self.indicators.on_bar(bar)
        self.context._note_bar()
        # Two conditions, and the second is not redundant. The bar count opens the gate
        # once the declared warm-up has been fed; the timestamp keeps it shut until the
        # range the user actually asked for. Without it, a strategy declaring no history at
        # all would place its first trade on a bar from *before* its own start date, and
        # the run's first trade would sit outside the run's own range.
        self.context._set_warm(
            self.context.bars_seen >= self._warmup
            and step.close_time >= self.config.start_ms
        )
        self.counts["bars"] += 1

        if "on_bar" in self._hooks:
            for bar in step.bars:
                self.strategy.on_bar(self.context, bar)

    def _consume_open_interest(self, up_to_ms: int) -> None:
        """Apply open-interest observations published at or before this bar's close."""
        while self._oi_index < len(self._oi_points):
            ts_ms, symbol, value = self._oi_points[self._oi_index]
            if ts_ms > up_to_ms:
                break
            self.runtime.open_interest_values[symbol] = value
            self.indicators.on_open_interest(symbol, value)
            self._oi_index += 1

    def _consume_macro(self, up_to_ms: int) -> None:
        """Apply macro readings *received* at or before this bar's close (Phase 11).

        **This single comparison is the no-look-ahead guarantee for macro.** The cursor
        never advances past `up_to_ms`, so a reading the platform did not yet have when
        the bar closed is invisible until the bar that follows its arrival -- the same
        rule, and the same shape, as open interest.

        The comparison is against `visible_ms` (`recv_ms`), not the provider's `ts_ms`;
        see `load_macro` for why the two differ and why using the wrong one is a silent,
        systematically favourable look-ahead. The source stamp travels alongside so
        `ctx.macro()` reports the reading's real age rather than implying a freshness it
        does not have -- a value that published 40 minutes ago and reached us 10 minutes
        ago is 40 minutes old, and the strategy is entitled to know that.
        """
        while self._macro_index < len(self._macro_points):
            visible_ms, name, value, ts_ms = self._macro_points[self._macro_index]
            if visible_ms > up_to_ms:
                break
            self.runtime.macro_values[name] = (value, ts_ms)
            self._macro_index += 1

    # ------------------------------------------------------------------------- orders

    def _submit(self, intent: OrderIntent) -> str:
        """Accept an order into the latency queue, or refuse it with the tier's reason."""
        self._check_supported(intent)
        symbol = intent.symbol
        reference = self._reference_price(symbol)
        if reference is None:
            raise RunAborted(
                f"{symbol}: an order was submitted before any price for it existed"
            )

        self.order_seq += 1
        order_id = f"o{self.order_seq}"
        latency = self.config.latency.submit_ms(self.latency_rng)
        arrival = self.runtime.now_ms + latency
        order = Order(
            id=order_id,
            intent=intent,
            submit_ts=self.runtime.now_ms,
            arrival_ts=arrival,
            reference_price=reference,
            # Quantised **at submission**, not only when a fill is priced. `remaining` is
            # what decides whether an order is finished, and an unquantised one can never
            # reach zero: a `qty` of 1.0005 against a 0.001 step fills 1.0 and leaves 0.0005
            # that no fill path can take, so the order sat open for the rest of the run,
            # counted as a partial fill, and stayed in `ctx.open_orders()` forever.
            remaining=decimal_to_scaled(
                quantize_qty(intent.qty, from_scaled(self.filters[symbol].step_size))
            ),
            trigger_price=intent.stop_price,
        )
        self.orders[order_id] = order
        self.counts["orders"] += 1
        self.runtime.emit(
            "ORDER",
            {
                "order_id": order_id,
                "arrival_ms": arrival,
                "reference_price": money_to_str(reference),
                **intent.to_json(),
            },
        )

        # **The risk verdict comes after the order exists and before it is scheduled.**
        # Both halves of that matter. Emitting `ORDER` first means the log shows what the
        # strategy asked for and then why it did not happen -- a refusal with no
        # corresponding request reads as an engine fault rather than a strategy one. And
        # refusing before `_schedule` means a rejected order never enters the latency queue,
        # so there is no window in which it could fill.
        breach = self._risk_check(order)
        if breach is not None:
            self._risk_reject(order, breach)
            return order_id

        # **A real venue's filters are checked before the wire, not discovered on it.**
        # The simulated path validates at its modelled arrival (`_place_limit`) and at
        # fill time, which is where a simulation can be honest about them -- but neither
        # runs when the transport is real, so for one release live orders reached Binance
        # with no filter validation at all. An off-tick limit the backtest would have
        # refused locally went out, came back -1111, fed the rejection streak, and the
        # kill switch halted a healthy session -- while the parity report compared a run
        # that refused the order against a run that sent it. The check validates exactly
        # what will ride the wire: the limit price for a limit, the trigger for a stop,
        # and no price at all for a market order (see `validate_order` on why a stand-in
        # anchor is worse than the skip).
        if not self.transport.simulated:
            # **Reduce-only is clamped before the wire, mirroring the simulator's arrival
            # clamp.** The risk layer exempts reduce-only orders from every size check on
            # the argument that they can only shrink a position -- true of what they *do*,
            # not of what they *say*: `_submit` used to hand the unclamped intent to
            # `transport.place`, so against a real venue a reduce-only order went out for
            # the full requested quantity however small the position had become. The venue
            # clamps server-side, but the ledger's working-order book then disagreed with
            # the wire about the order's size, and `_check_wire_filters` below validated a
            # quantity that was never the real request. The simulator clamps at its
            # modelled arrival (`_clamp`); a real transport has no modelled arrival, so
            # submit time -- the last instant the platform holds the order -- is where the
            # clamp can honestly live.
            if order.intent.reduce_only and not self._clamp_reduce_only_for_wire(order):
                return order_id
            reason = self._check_wire_filters(order)
            if reason is not None:
                self._reject(order, reason)
                return order_id

        # **The single seam between simulated execution and a real exchange.** Spec 6.1's
        # table permits exactly one difference on this path -- the order destination -- and
        # this is it. The simulator schedules an arrival and the modelled matching engine
        # takes over; a testnet transport signs a POST and the fill comes back on the
        # user-data stream, re-entering through `_book_fill`, the same ledger, the same trade
        # builder, the same risk feedback. Everything above this line is shared, which is why
        # a risk limit that stops a backtest stops a paper session identically.
        self.transport.place(order)
        return order_id

    def _place_simulated(self, order: Order) -> None:
        """The default transport: hand the order to the modelled matching engine."""
        self._schedule(order.arrival_ts, order.id, "submit")

    def _clamp_reduce_only_for_wire(self, order: Order) -> bool:
        """Bound a reduce-only order to the position before a real transport sends it.

        The wire twin of `_clamp`'s reduce-only branch, applied at submit time because a
        real venue has no modelled arrival for the engine to clamp at (see `_submit`).
        Returns `False` when the order was retired instead of clamped: a position already
        flat -- or facing the same way as the order -- leaves nothing to reduce, and that
        is a `CANCELLED` bracket outliving its position, not a rejection (`_clamp`'s own
        distinction, kept identical so the two paths report one story).

        Reduce-only is mutually exclusive with a hedged `position_side`
        (`OrderIntent.__post_init__`), so the one-way position is always the right one to
        measure against.
        """
        symbol = order.intent.symbol
        held = self.account.qty(symbol)
        side = _side(order)
        if held == 0 or (held > 0) == (side is Side.BUY):
            self._remove(
                order,
                OrderStatus.CANCELLED,
                "reduce-only: the position it would have reduced is already flat",
            )
            return False
        capacity = abs(held)
        if capacity < from_scaled(order.remaining):
            step = from_scaled(self.filters[symbol].step_size)
            clamped = quantize_qty(capacity, step)
            if clamped <= 0:
                self._remove(
                    order,
                    OrderStatus.CANCELLED,
                    f"reduce-only: the remaining position {capacity} is below the {step} "
                    f"lot step, so no order the venue would accept can reduce it",
                )
                return False
            order.remaining = decimal_to_scaled(clamped)
        return True

    def _check_wire_filters(self, order: Order) -> str | None:
        """Spec 3.2's filters against what a real transport is about to send (C6).

        Returns the refusal in the exchange's own filter vocabulary, or `None`. Only the
        live path calls this -- the simulated path keeps validating at its modelled
        arrival and at fill time, where the fill price gives it an anchor a request does
        not have. The two paths refuse the same orders for the same reasons; they differ
        only in *when* they can honestly evaluate the price-anchored filters.

        The price validated is the one that rides the wire: `price` for a limit,
        `stop_price` (Binance's `stopPrice`/`activationPrice`, on the same tick grid) for
        the stop family, and none for a market order. `PERCENT_PRICE` is anchored to the
        mark only for a limit -- a stop's trigger is *supposed* to sit far from the
        current mark, and bounding it to the limit's collar would refuse every distant
        protective stop a strategy places.
        """
        intent = order.intent
        filters = self.filters[intent.symbol]
        mark = self.account.marks.get(intent.symbol)
        is_market = intent.type is not OrderType.LIMIT
        if intent.type is OrderType.LIMIT:
            wire_price = intent.price
        else:
            # STOP_MARKET / TAKE_PROFIT_MARKET carry `stopPrice`; TRAILING_STOP_MARKET
            # optionally carries `activationPrice` (stored on the same field). All are
            # prices the venue holds to the tick grid; a plain MARKET carries nothing.
            wire_price = intent.stop_price
        check = validate_order(
            filters,
            qty=order.remaining,
            price=None if wire_price is None else decimal_to_scaled(wire_price),
            mark_price=(
                None
                if mark is None or intent.type is not OrderType.LIMIT
                else decimal_to_scaled(mark)
            ),
            is_market=is_market,
        )
        return None if check else check.reason

    def _risk_check(self, order: Order, *, qty: Money | None = None) -> RiskBreach | None:
        """Spec 7's pre-submission evaluation, against the state this order would produce.

        **Every notional here is valued at the mark**, not at the reference price the fill
        will be measured against. Leverage and position size are margin properties, and
        margin is marked (spec 3.3, 3.6); pricing a limit against the far touch would make
        the same position breach or not breach depending on which side it was entered from,
        and pricing it against the last print would make it depend on the tape. The
        reference price is used only where no mark has arrived yet, which is the first
        instants of a run.
        """
        intent = order.intent
        symbol = intent.symbol
        # `qty` overrides for an amendment, where the size under consideration is the one
        # being asked for rather than the one currently working. The working exposure
        # excludes this order either way, so the amended size is counted once, not twice.
        if qty is None:
            qty = from_scaled(order.remaining)
        if qty <= 0 and not self.risk.halted:
            # Quantisation floored it to nothing. Not a *size* question -- `_on_arrival`'s
            # filter check owns that refusal, and it names `LOT_SIZE`, which is the answer
            # the strategy needs. The halted gate still applies: skipping it let a sub-step
            # order be scheduled into a halted run's latency queue, where it finished the
            # run `PENDING` with no reason and sat in `ctx.open_orders()`.
            return None
        price = self._risk_price(symbol) or order.reference_price
        routed = intent.position_side
        position = self.account.position(symbol, routed)
        return self.risk.check_order(
            ts_ms=self.runtime.now_ms,
            symbol=symbol,
            side=intent.side,
            qty=qty,
            price=price,
            reduce_only=intent.reduce_only,
            position_qty=_ZERO if position is None else position.qty,
            working=self._working_exposure(symbol, routed, exclude=order.id),
            position_side=routed,
            # **The other leg counts.** The agreed hedge rule is the sum of both sides
            # (`risk.gross_projected_exposure`), so an order growing the short has to be
            # measured against a limit the long is already consuming. Excluding it would
            # give a hedged account twice the ceiling a one-way account gets under the same
            # number -- and a hedge is not half the risk, it is two positions either of
            # which can be liquidated alone.
            other_side_exposure=self._other_side_exposure(symbol, routed, exclude=order.id),
            equity=self.account.equity,
            # **Excluding this order.** It is already in `self.orders` -- it has to be, so
            # that a rejection has something to attach its reason to -- and counting it
            # would make `max_open_orders=3` admit two. The limit is a ceiling on what may
            # be live, and this order is asking to become the next one.
            open_orders=sum(
                1
                for other_id, other in self.orders.items()
                if other.is_open and other_id != order.id
            ),
            other_notional=self._other_notional(symbol),
        )

    def _working_exposure(
        self,
        symbol: str,
        position_side: PositionSide = PositionSide.BOTH,
        *,
        exclude: str | None = None,
    ) -> WorkingExposure:
        """Quantity on this symbol **and this position side** that could still reach the book.

        Counts `PENDING` as well as `WORKING`: an order crossing the wire is an order that
        will fill, and a strategy that submits ten orders inside one bar has ten of them in
        flight before the first one arrives. Excluding the pending ones is the version of
        this check that lets a loop breach any ceiling one order at a time.

        Filtered by side because a working buy means opposite things on the two legs -- it
        grows the long and shrinks the short -- and pooling them would let a resting exit on
        one leg cancel out a growing entry on the other.
        """
        exposure = WorkingExposure.zero()
        for order_id, order in self.orders.items():
            if order_id == exclude or not order.is_open:
                continue
            if order.intent.symbol != symbol or order.intent.reduce_only:
                continue
            if order.intent.position_side is not position_side:
                continue
            remaining = from_scaled(order.remaining)
            if remaining > 0:
                exposure = exposure.plus(order.intent.side, remaining)
        return exposure

    def _side_exposure(
        self,
        symbol: str,
        position_side: PositionSide,
        *,
        exclude: str | None = None,
    ) -> Money:
        """One side's reachable quantity: what it holds plus what could still grow it."""
        position = self.account.position(symbol, position_side)
        return side_projected_exposure(
            position_side,
            _ZERO if position is None else position.qty,
            self._working_exposure(symbol, position_side, exclude=exclude),
        )

    def _other_side_exposure(
        self, symbol: str, position_side: PositionSide, *, exclude: str | None = None
    ) -> Money:
        """The reachable quantity on the *other* leg of this symbol. Zero in one-way mode."""
        if not position_side.is_hedged:
            return _ZERO
        other = (
            PositionSide.SHORT if position_side is PositionSide.LONG else PositionSide.LONG
        )
        return self._side_exposure(symbol, other, exclude=exclude)

    def _symbol_exposure(self, symbol: str, *, exclude: str | None = None) -> Money:
        """Every side of one symbol, summed. See `risk.gross_projected_exposure`."""
        return sum(
            (self._side_exposure(symbol, side, exclude=exclude) for side in self._sides),
            _ZERO,
        )

    def _other_notional(self, exclude: str) -> Money:
        """Projected notional on every symbol but this one, for the account-wide leverage.

        Leverage is a property of the account, not of a symbol. Checking it per symbol would
        pass two symbols each projected at 4x while the account ran at 8x, which is the same
        shape of error as checking a position against itself instead of against the order.

        Sums **both** sides of each other symbol under hedge mode, for the same reason
        `max_position_notional` does: two legs post two lots of margin.
        """
        total = _ZERO
        for symbol in self.config.symbols:
            if symbol == exclude:
                continue
            exposure = self._symbol_exposure(symbol)
            if exposure <= 0:
                continue
            price = self._risk_price(symbol)
            if price is None:
                # Nothing has ever priced this symbol -- no mark, no print, and no position
                # to take an entry from. There is no exposure to value either, because
                # `exposure > 0` implies a position or a working order, and a working order
                # implies a reference price. Unreachable, and skipped rather than guessed at.
                continue
            with accounting():
                total += exposure * price
        return total

    def _risk_price(self, symbol: str) -> Money | None:
        """What a risk limit values exposure at: the mark, then a *recent* print, then entry.

        **Never `None` for a symbol that has exposure**, which is the point. Skipping an
        unmarked symbol made account leverage understate itself by exactly that symbol's
        notional: 100 ETH worth 300 000 against 100 000 of equity admitted a BTC order at a
        reported 4x while the account was running at 7x, whenever the ETH mark happened to
        be late. The entry price is the last resort and is a real price -- it is what the
        exposure was taken at -- rather than a guess.

        The print candidate is staleness-bounded (`_recent_print`): valuing
        `max_position_notional` and the account leverage ceiling at a pre-gap price let a
        limit be measured against a market that had since moved without us. The tiering is
        unchanged -- a symbol with exposure still always resolves, because a stale print
        falls through to the entry price, which needs no clock to be a price the exposure
        genuinely carries.
        """
        mark = self.account.marks.get(symbol)
        if mark is not None:
            return mark
        scaled = self._recent_print(symbol)
        if scaled is not None:
            return from_scaled(scaled)
        # The last resort is an entry price, and with two legs open either one is a real
        # price this symbol traded at. The first by `_SIDE_ORDER` is taken so the choice is
        # reproducible rather than dependent on which leg opened first.
        for position in self.account.positions_for(symbol):
            return position.entry_price
        return None

    def _recent_print(self, symbol: str) -> int | None:
        """The last print, scaled -- or `None` once it is older than the staleness bound.

        The one sanctioned read of `last_print`. Every consumer of a stale print was a
        different lie: `_execute_market` filled at a pre-gap price, `_reference_price`
        measured slippage against one, `_risk_price` valued exposure at one, and
        `_trigger_price_now` armed a trail from one. All four now fall through to their
        own documented next candidate -- a `NoQuote` refusal, the mark, the entry price --
        which is spec 4.5's "no fills, not cheap ones" applied to the tape. See
        `_print_staleness_ms` for the bound and why `BAR_CLOSE` gets a wider one.
        """
        scaled = self.last_print.get(symbol)
        if scaled is None:
            return None
        stamped = self.last_print_ts.get(symbol)
        if stamped is None or self.runtime.now_ms - stamped > self._print_staleness_ms:
            return None
        return scaled

    def risk_usage(self) -> list[dict[str, Any]]:
        """Per-limit usage for the live monitor (spec 10.3), from this engine's own state.

        Lives here rather than in the session because the quantities a limit is checked
        against -- projected exposure, the price it is valued at, the open-order count --
        are computed by the helpers directly above, and a second copy in the monitor would
        be a second opinion about how much of a limit had been spent. `RiskEngine.usage`
        owns the arithmetic; this owns only the measuring.

        Symbols are deduplicated for the same reason `monitor()` deduplicates its position
        rows: `config.symbols` is caller-supplied, and a repeat would count one symbol's
        notional twice into account leverage.
        """
        seen: set[str] = set()
        total = _ZERO
        largest = _ZERO
        for symbol in self.config.symbols:
            if symbol in seen:
                continue
            seen.add(symbol)
            exposure = self._symbol_exposure(symbol)
            if exposure <= 0:
                continue
            price = self._risk_price(symbol)
            if price is None:
                # Unreachable for a symbol with exposure -- see `_risk_price`. Skipped
                # rather than guessed at, exactly as `_other_notional` does.
                continue
            with accounting():
                notional = exposure * price
                total += notional
            if notional > largest:
                largest = notional
        return self.risk.usage(
            ts_ms=self.runtime.now_ms,
            equity=self.account.equity,
            notional=total,
            max_symbol_notional=largest,
            open_orders=sum(1 for order in self.orders.values() if order.is_open),
        )

    def _risk_reject(self, order: Order, breach: RiskBreach) -> None:
        """Refuse an order on the risk layer's own verdict.

        **Counted separately from `rejects`, and deliberately not fed back into
        `observe_rejection`.** `rejects` means the exchange or the ledger refused the order,
        which says the sizing is wrong; a risk rejection says the operator's limit did its
        job. Merging them would make one number mean two things on the results page, and
        feeding risk rejections into the consecutive-rejection auto-trigger would make the
        kill switch fire on a strategy that was merely being told no -- which is precisely
        the outcome `REJECT` exists to avoid.
        """
        order.status = OrderStatus.REJECTED
        order.reason = breach.message
        self.resting.discard(order)
        self.counts["risk_rejects"] += 1
        self.flags.add("RISK_REJECTED")
        self._note_order_end(order, OrderStatus.REJECTED, breach.message)
        self.runtime.emit("RISK_REJECT", {"order_id": order.id, **breach.to_json()})

    def _reference_price(self, symbol: str) -> Money | None:
        """The price at signal time that spec 8.4 measures execution against.

        **The best side-neutral price this tier can observe**, in that order of preference:

        1. *The top-of-book mid*, where a book is in force. A fill takes the far touch, so
           measuring against the mid charges each side an honest half-spread -- which is a
           real execution cost -- and charges them the *same* half-spread. Every other
           candidate is side-dependent.
        2. *The last print*, where there is a tape but no book. This is what Phase 4's review
           established: the fill comes from the trade series, so comparing it against the
           **mark** made the reported figure absorb the mark-trade basis, whose sign follows
           the side. On identical data, with slippage modelled at exactly zero and a basis of
           20, a long-biased run reported `slippage_cost = -20` and a price leg of 0 on a
           position that made +20 on price, while a short-biased one reported the opposite.
        3. *The mark*, only when neither exists. Reachable at a book tier whose range has
           depth but no `aggTrades` -- the collector runs ahead of the bulk trade archive --
           and at the very first instant of a `BAR_CLOSE` run.

        The same basis argument that ruled out the mark in Phase 4 rules out the last print
        at a book tier: a buy filling at the ask shows a full spread of slippage when the
        last print happened to be at the bid and none when it happened to be at the ask, so
        the reported figure would carry a side-dependent term that is a property of the tape
        rather than of execution.

        Explicit `is None` rather than `or` throughout: a `Decimal` of exactly zero is falsy,
        and a price of zero should raise rather than silently fall through to the next
        candidate.
        """
        if self.tier in LIMIT_TIERS:
            self._sync_market()
            top = self.market.top_of_book(symbol, self.runtime.now_ms)
            if top is not None:
                with accounting():
                    return (from_scaled(top.bid_px) + from_scaled(top.ask_px)) / 2
        # Staleness-bounded (`_recent_print`): a reference taken from a pre-gap print
        # would launder the whole gap move into `slippage_cost` on the first order after
        # the hole. The mark below is minute-sampled and LOCF-bounded by its own series.
        scaled = self._recent_print(symbol)
        if scaled is not None:
            return from_scaled(scaled)
        return self.account.marks.get(symbol)

    def _check_supported(self, intent: OrderIntent) -> None:
        if intent.type is OrderType.LIMIT and self.tier not in LIMIT_TIERS:
            raise UnsupportedOrder(
                f"LIMIT orders need a book to queue behind and the {self.tier.name} tier "
                "has none. Spec 6.4's rule -- a touch is not a fill, the queue ahead has to "
                "be consumed -- is unenforceable without a resting size, and an unenforced "
                "version of it is the fiction the spec calls the single most common way "
                "limit strategies look profitable and are not. Run over a range with "
                "bookTicker or depth20 coverage."
            )
        if intent.type in TRIGGER_TYPES and self.tier not in TRIGGER_TIERS:
            raise UnsupportedOrder(
                f"{intent.type.value} orders need a trade tape to fill against once they "
                f"trigger, and the {self.tier.name} tier has only bar prints. The fill "
                "would land at the next bar's open, up to a whole timeframe after the "
                "trigger -- a delayed market order wearing a stop's name. Run over a range "
                "with aggTrades coverage."
            )

    def _schedule(
        self,
        ts_ms: int,
        order_id: str,
        action: str,
        *,
        price: Money | None = None,
        qty: Money | None = None,
        reference_price: Money | None = None,
    ) -> None:
        self._instruction_seq += 1
        self.queue.push(
            Event(
                ts_ms=ts_ms,
                kind=EventKind.ORDER_ARRIVAL,
                source_seq=self._instruction_seq,
                dataset_id="engine",
                payload=_Instruction(
                    order_id=order_id,
                    action=action,
                    price=price,
                    qty=qty,
                    reference_price=reference_price,
                ),
            )
        )

    def _set_leverage(self, symbol: str, leverage: int) -> None:
        """Apply a strategy's leverage change to the ledger, and record it in the event log.

        **The event is not decoration.** `RunSpec.leverage` is a reproducibility input
        (spec 12.1) and a run whose leverage moved is no longer described by that one number.
        The log is what the hash is taken over and what the shadow backtest replays, so a
        change that is not in it would make two runs with different leverage schedules
        indistinguishable by their manifest -- which is exactly the claim the hash exists to
        support.

        `Account.set_leverage` does the refusing: below 1x, above the symbol's top bracket,
        or while any side of the symbol is open. Its message is already specific, so it
        propagates rather than being restated here.
        """
        before = self.account.leverage(symbol)
        self.account.set_leverage(symbol, leverage)
        if before != leverage:
            self.runtime.emit(
                "LEVERAGE",
                {"symbol": symbol, "from": before, "to": leverage},
            )

    def _modify(self, order_id: str, price: Money | None, qty: Money | None) -> None:
        """Amend a resting limit order's price and/or size (spec 6.1; Binance `PUT /order`).

        **An amendment is a race, exactly like a cancel.** It takes its own latency to reach
        the exchange, and anything that fills the order in the meantime fills it at the old
        price and the old size. This is R19 again -- a strategy that repriced a quote every
        tick and was modelled as repricing instantly would show a quote that was never
        stale, which no market maker has ever had.

        **Queue priority is lost when the price moves or the size grows**, and kept when the
        size only shrinks. That is the exchange's rule and it is the whole reason an amend is
        worth modelling separately from cancel-and-replace: a strategy that shaves size off a
        resting order keeps its place, and one that chases the market with the price goes to
        the back. A model that kept priority through a reprice would make queue position
        free, which is the single most valuable thing a maker strategy owns.

        Implemented by sending the amended order back through `_place_limit`, so an
        amendment that becomes marketable crosses, an amendment that breaches a filter is
        rejected, and post-only still refuses to take liquidity -- all on exactly the code
        an original submission uses, rather than on a second copy of it.
        """
        order = self.orders.get(order_id)
        if order is None or not order.is_open:
            return
        if order.intent.type is not OrderType.LIMIT:
            raise UnsupportedOrder(
                f"only LIMIT orders can be amended; {order_id} is a "
                f"{order.intent.type.value}. Cancel it and submit a new one -- which is "
                "what the exchange would make you do, and it costs the queue position that "
                "an amendment of a limit order can sometimes keep."
            )
        if price is None and qty is None:
            raise ValueError("ctx.modify needs a new price, a new qty, or both")

        new_price = order.intent.price if price is None else price
        new_qty = order.intent.qty if qty is None else qty
        step = from_scaled(self.filters[order.intent.symbol].step_size)
        quantised = quantize_qty(new_qty, step)
        if decimal_to_scaled(quantised) <= order.filled_scaled:
            raise ValueError(
                f"the amended quantity {new_qty} is not more than the "
                f"{from_scaled(order.filled_scaled)} already filled on {order_id}; the "
                "exchange refuses this rather than un-filling anything"
            )

        # **A shrink is not new risk.** `core.risk` exempts reduce-only orders from every
        # count and size limit on the argument that an order which can only lower exposure
        # must never be refused; an amendment that lowers the working quantity is the same
        # argument. Risk-checking it refused a de-risking amendment on the *rate* limit,
        # silently -- the order kept its original size, no hook fired, and the run reported a
        # rejection for an order nobody had rejected.
        new_working = quantised - from_scaled(order.filled_scaled)
        shrinking = new_working <= from_scaled(order.remaining)
        breach = None if shrinking else self._risk_check(order, qty=new_working)
        if breach is not None:
            # `instruction`, not `action`: `breach.to_json()` carries its own `action`
            # (REJECT/HALT), and the two collided -- so the log could not tell a refused
            # amendment from a refused order.
            self.runtime.emit(
                "RISK_REJECT",
                {"order_id": order_id, "instruction": "modify", **breach.to_json()},
            )
            self.counts["risk_rejects"] += 1
            self.flags.add("RISK_REJECTED")
            return

        self.transport.modify(order, new_price, quantised)

    def _schedule_modify(
        self, order: Order, new_price: Money | None, quantised: Money
    ) -> None:
        """The simulated transport's amend: a modelled flight time, then `_apply_modify`."""
        arrival = self.runtime.now_ms + self.config.latency.cancel_ms(self.latency_rng)
        self.runtime.emit(
            "MODIFY",
            {
                "order_id": order.id,
                "price": None if new_price is None else money_to_str(quantize_money(new_price)),
                "qty": money_to_str(quantize_money(quantised)),
                "arrival_ms": arrival,
            },
        )
        self._schedule(
            arrival,
            order.id,
            "modify",
            price=new_price,
            qty=quantised,
            reference_price=self._reference_price(order.intent.symbol),
        )

    def _apply_modify(self, ts_ms: int, order: Order, instruction: _Instruction) -> None:
        """The amendment reaching the exchange. See `_modify` for the priority rule.

        **An amendment can overtake the order it amends, and the exchange refuses it when it
        does.** Submit and cancel latencies are drawn independently, so an amend issued in
        the same hook as the order lands first on roughly two-thirds of seeds under the
        default lognormal model. Applying it anyway put the order on the resting book twice
        -- `_place_limit` added it, and the original `submit` instruction added it again --
        which let a single print consume our queue position twice and filled the order from a
        tick published *before* its own `arrival_ts`. That is look-ahead, in the one module
        whose docstring promises there is none.

        Refusing is not a workaround for the double-add; it is what Binance does. An amend
        for an order the matching engine has never seen comes back `Unknown order sent`.
        """
        if order.status is OrderStatus.PENDING:
            self.runtime.emit(
                "MODIFY_REJECTED",
                {
                    "order_id": order.id,
                    "reason": (
                        f"the amendment arrived at {ts_ms}, before the order itself was due "
                        f"at {order.arrival_ts}; the exchange has no such order to amend"
                    ),
                },
            )
            return
        new_price = instruction.price
        new_qty = instruction.qty
        if new_qty is None:  # pragma: no cover - `_modify` always supplies one
            return
        filled = order.filled_scaled
        new_total = decimal_to_scaled(new_qty)
        if new_total <= filled:
            # The order filled further while the amendment was in flight, so the new size is
            # now behind the fills. The exchange rejects the amendment and leaves the order
            # alone; it does not cancel it.
            self.runtime.emit(
                "MODIFY_REJECTED",
                {
                    "order_id": order.id,
                    "reason": (
                        f"{from_scaled(filled)} had filled by the time the amendment "
                        f"arrived, which is at least the amended size {new_qty}"
                    ),
                },
            )
            return

        new_remaining = new_total - filled
        price_moved = (
            new_price is not None and decimal_to_scaled(new_price) != order.limit_scaled
        )
        grew = new_remaining > order.remaining

        self.resting.discard(order)
        order.intent = replace(order.intent, price=new_price, qty=new_qty)
        order.remaining = new_remaining
        order.status = OrderStatus.PENDING
        if instruction.reference_price is not None:
            order.reference_price = instruction.reference_price
        if price_moved or grew:
            # Back of the queue. `None` rather than a large number: it means "not yet
            # observed", so `observe_book` re-measures against the size actually resting
            # now, which is precisely the set of orders that are now ahead of us.
            order.queue_ahead = None
        self.runtime.emit(
            "MODIFIED",
            {
                "order_id": order.id,
                "price": None if new_price is None else money_to_str(quantize_money(new_price)),
                "remaining": money_to_str(from_scaled(new_remaining)),
                "queue_priority": "lost" if (price_moved or grew) else "kept",
            },
        )
        self._place_limit(ts_ms, order)
        if order.status is not OrderStatus.REJECTED:
            self.risk.observe_acceptance()

    def _cancel(self, order_id: str) -> None:
        """Cancel an order -- if the cancel can actually beat the market there.

        Spec 6.3: *"A cancel issued at T does not protect you from a fill at T + 50 ms if
        cancel latency is 120 ms."* So the cancel is **scheduled**, not applied: it takes its
        own latency to reach the exchange, and anything that fills the order in the meantime
        fills it. That is R19, and modelling it is the difference between a backtest that
        knows a cancel is a race and one that treats it as a veto.

        **The cancel is always delivered, even when it cannot beat the order there.** It used
        to be dropped whenever it would arrive at or after a still-`PENDING` order, which is
        right for a market order -- that fills at arrival, so a late cancel genuinely does
        nothing -- and wrong for everything that *rests*. A GTC limit, an armed stop and a
        parked market order are all still cancellable one millisecond after they land, and
        discarding the instruction meant a strategy that quoted and pulled inside one latency
        window could never pull: the order stayed on the book and filled sixty seconds later,
        with the run holding a position the strategy had explicitly cancelled.

        Losing the race is `_on_arrival`'s job, and it already does it exactly right -- a
        cancel for an order that is no longer open no-ops, which *is* R19. The diagnostic
        stays, because "your cancel could not have got there first" is worth reading in the
        log; it is now a note rather than a decision.
        """
        order = self.orders.get(order_id)
        if order is None or not order.is_open:
            return
        self.transport.cancel(order, reason="cancelled by strategy")

    def _schedule_cancel(self, order: Order, *, reason: str) -> None:
        """The simulated transport's cancel: a modelled flight time, then `_on_arrival`."""
        arrival = self.runtime.now_ms + self.config.latency.cancel_ms(self.latency_rng)
        if order.status is OrderStatus.PENDING and arrival >= order.arrival_ts:
            self.runtime.emit(
                "CANCEL_TOO_LATE",
                {
                    "order_id": order.id,
                    "cancel_arrival_ms": arrival,
                    "order_arrival_ms": order.arrival_ts,
                },
            )
        self._schedule(arrival, order.id, "cancel")

    def _cancel_all(self, symbol: str | None, *, reason: str = "cancelled by strategy") -> None:
        for order_id, order in list(self.orders.items()):
            if not order.is_open:
                continue
            if symbol is not None and order.intent.symbol != symbol:
                continue
            if reason == "cancelled by strategy":
                self._cancel(order_id)
            else:
                # An engine-initiated removal -- a liquidation cancelling the symbol's book
                # (spec 6.6) -- is immediate. It is the exchange acting, not an instruction
                # crossing the wire, so there is no latency to model.
                self._remove(order, OrderStatus.CANCELLED, reason)

    def _open_order_ids(self, symbol: str | None) -> Sequence[str]:
        return tuple(
            order_id
            for order_id, order in self.orders.items()
            if order.is_open and (symbol is None or order.intent.symbol == symbol)
        )

    def _remove(self, order: Order, status: OrderStatus, reason: str) -> None:
        """Take an order off the book and record why it left."""
        order.status = status
        order.reason = reason
        self.resting.discard(order)
        self._release_flatten(order)
        self._note_order_end(order, status, reason)
        self.runtime.emit(
            _REMOVAL_EVENTS[status], {"order_id": order.id, "reason": reason}
        )

    # -------------------------------------------------------------------- order arrival

    def _on_arrival(self, ts_ms: int, instruction: _Instruction) -> None:
        order = self.orders.get(instruction.order_id)
        if order is None or not order.is_open:
            return
        if instruction.action == "cancel":
            self._remove(order, OrderStatus.CANCELLED, "cancelled by strategy")
            return
        if instruction.action == "modify":
            self._apply_modify(ts_ms, order, instruction)
            return
        if instruction.action == "expire":
            if order.is_resting and _is_market_now(order) and not order.limit_scaled:
                self._remove(
                    order,
                    OrderStatus.EXPIRED,
                    f"no trade printed within {MARKET_WAIT_MS} ms of arrival, so this "
                    "market order had nothing to execute against",
                )
            return

        self._sync_market()
        kind = order.intent.type
        # A triggered stop *is* a market order by the time it arrives -- spec 6.4: "On
        # trigger, they become market orders and take the market-order path". Dispatching on
        # the intent's type alone would send it back through `_arm_trigger` and arm it a
        # second time, so the stop would trigger, travel its latency, and then start waiting
        # for its own level all over again.
        if kind is OrderType.MARKET or order.triggered:
            self._place_market(ts_ms, order)
        elif kind is OrderType.LIMIT:
            self._place_limit(ts_ms, order)
        else:
            self._arm_trigger(ts_ms, order)

        # The order reached the exchange without being refused, so spec 7's consecutive
        # rejection streak is over. Read from the order's own status rather than tracked
        # through the three branches above: each of them has more than one way to reject,
        # and a streak counter that misses one of them is a kill switch that does not fire.
        if order.status is not OrderStatus.REJECTED:
            self.risk.observe_acceptance()

    def _place_market(self, ts_ms: int, order: Order) -> None:
        """A market order reaching the exchange.

        At `TRADE_ONLY` it parks unless a print landed at exactly this millisecond, because
        spec 6.4 fills it at the *next* trade. At every other tier the book or the last print
        is already in force, so it executes here.
        """
        if self.tier is FillTier.TRADE_ONLY:
            recent = self.market.last_trade(order.intent.symbol, ts_ms)
            if recent is None or recent.ts_ms < ts_ms:
                order.status = OrderStatus.WORKING
                self.resting.add(order)
                self._schedule(ts_ms + MARKET_WAIT_MS, order.id, "expire")
                self.runtime.emit(
                    "ORDER_WORKING",
                    {"order_id": order.id, "reason": "waiting for the next print"},
                )
                return
            self._execute_market(ts_ms, order, print_scaled=recent.price_scaled)
            return
        self._execute_market(ts_ms, order)

    def _execute_market(
        self, ts_ms: int, order: Order, *, print_scaled: int | None = None
    ) -> None:
        """Price a market order at this tier and book the fill."""
        symbol = order.intent.symbol
        qty = self._clamp(order, ts_ms)
        if qty is None:
            return

        if print_scaled is None:
            # Bounded: an explicit `print_scaled` is the print that is filling a parked
            # order right now, but this fallback is "whatever printed last", and unbounded
            # it priced fills at the far side of a data hole. `None` here reaches the fill
            # model as no print, and the model's own `NoQuote` names the refusal.
            print_scaled = self._recent_print(symbol)
        inputs = MarketInputs(
            side=_side(order),
            qty=qty,
            tick_size=from_scaled(self.filters[symbol].tick_size),
            reference_price=order.reference_price,
            print_price=None if print_scaled is None else from_scaled(print_scaled),
            top=self.market.top_of_book(symbol, ts_ms),
            ladder=self.market.ladder(symbol, ts_ms),
            recent_notional=self.market.recent_notional(symbol, ts_ms),
        )
        try:
            quote = self.fill_model.quote(inputs)
        except NoQuote as exc:
            self._reject(order, str(exc))
            return

        if quote.exhausted_qty > 0:
            self.counts["depth_exhausted"] += 1
            if "DEPTH_EXHAUSTED" not in self.flags:
                self.flags.add("DEPTH_EXHAUSTED")
                self.warnings.append(
                    f"{symbol}: an order exceeded all published depth levels and the "
                    f"remainder was priced at the depth-exhaustion penalty (spec 6.4). If "
                    f"this recurs the position sizing is unrealistic for the instrument, "
                    f"and the excess is filling at a price nobody quoted."
                )
        self._book_fill(ts_ms, order, qty, quote.price, is_maker=False, quote=quote)
        # **A market order never leaves a remainder behind.** Spec 6.5 fills it "to the size
        # available", and at every tier that is the whole quantity -- `BOOK_WALK` prices the
        # excess at the exhaustion penalty rather than dropping it. What *can* be left is a
        # residue the reduce-only clamp shaved off, and there is nowhere for it to go: a
        # market order is not on the resting book, so nothing will ever revisit it. Retiring
        # it here is what keeps `ctx.open_orders()` true.
        if order.is_open and order.remaining > 0:
            self._remove(
                order,
                OrderStatus.EXPIRED,
                f"a market order carries no remainder; {from_scaled(order.remaining)} was "
                f"clamped away at arrival",
            )

    def _place_limit(self, ts_ms: int, order: Order) -> None:
        """A limit order reaching the exchange: cross what it can, rest the remainder.

        The four times-in-force differ only in what happens to the part that cannot fill
        immediately, and each of those four answers is a real exchange behaviour:

        - `GTX` (post-only) is **cancelled outright if it would cross at all**. That is how
          maker fees are guaranteed and, per spec 6.5, *"also how orders silently fail to
          enter"* -- so it leaves as `EXPIRED`, not as a cancel the strategy asked for.
        - `FOK` fills in full or not at all, and the check happens **before** any fill is
          booked; a fill-or-kill that had already half-filled when it decided to kill would
          be neither.
        - `IOC` takes what is there and expires the rest.
        - `GTC` rests.
        """
        symbol = order.intent.symbol
        qty = self._clamp(order, ts_ms)
        if qty is None:
            return
        limit = order.intent.price
        if limit is None:  # pragma: no cover - validated by ctx before submission
            self._reject(order, "a LIMIT order needs a price")
            return

        filters = self.filters[symbol]
        limit_scaled = decimal_to_scaled(limit)
        mark = self.account.marks.get(symbol)
        check = validate_order(
            filters,
            qty=decimal_to_scaled(qty),
            price=limit_scaled,
            mark_price=None if mark is None else decimal_to_scaled(mark),
            is_market=False,
        )
        if not check:
            self._reject(order, check.reason)
            return

        side = _side(order)
        top = self.market.top_of_book(symbol, ts_ms)
        ladder = self.market.ladder(symbol, ts_ms)
        available = cross_book(
            side=side,
            qty=qty,
            limit_price=limit,
            tick_size=from_scaled(filters.tick_size),
            top=top,
            ladder=ladder,
        )
        tif = order.intent.tif

        if tif is TimeInForce.GTX:
            # **Post-only is adjudicated on the book, and no book is not a pass.** The
            # crossing test used to be `available.qty > 0`, which `cross_book` also
            # returns during a book outage -- exactly what `MarketView` reports past
            # `MAX_QUOTE_STALENESS_MS` -- so during any >60 s gap a plainly-crossing GTX
            # order was not rejected: it rested inside the spread and then filled as
            # maker, a failure pointing in the favourable direction. Spec 6.5's honesty
            # rule ("also how orders silently fail to enter") decides both branches: with
            # no observation the question "would this cross" has no answer, so the order
            # is refused loudly rather than admitted on silence; with one, crossing is a
            # *price* fact (the far touch at or through the limit), not a fill fact, so a
            # zero-size touch no longer smuggles a crossing order onto the book either.
            if top is None and ladder is None:
                self._remove(
                    order,
                    OrderStatus.EXPIRED,
                    "post-only (GTX): no book observation is in force at this instant, so "
                    "whether this order would cross cannot be adjudicated. During a book "
                    "outage there are no resting orders, not unexamined ones (spec 4.5).",
                )
                return
            if _limit_crosses(side, limit_scaled, top, ladder):
                self._remove(
                    order,
                    OrderStatus.EXPIRED,
                    "post-only (GTX): this order would have crossed the book, so the "
                    "exchange would have rejected it rather than take liquidity",
                )
                return
        if tif is TimeInForce.FOK and available.qty < qty:
            self._remove(
                order,
                OrderStatus.EXPIRED,
                f"fill-or-kill: only {available.qty} of {qty} was available at or better "
                f"than {limit}",
            )
            return

        order.limit_scaled = limit_scaled
        if available.qty > 0:
            self._book_fill(ts_ms, order, available.qty, available.price, is_maker=False)
            if order.remaining <= 0 or not order.is_open:
                return

        if tif is TimeInForce.IOC:
            self._remove(
                order,
                OrderStatus.EXPIRED,
                "immediate-or-cancel: the remainder had nothing to fill against",
            )
            return

        order.status = OrderStatus.WORKING
        self.resting.add(order)
        if _rests_through_book(side, limit_scaled, top, ladder):
            # The remainder is resting at a price beyond every level the model could see
            # it consume -- through the touch at `BOOK_TICKER`, past the deepest visible
            # level at `BOOK_WALK`. At the venue it would have kept *taking* liquidity the
            # published book does not show, so until an observation shows the market past
            # it, its fills are charged as taker flow. See `RestingBook._crossed` (H21).
            self.resting.note_crossed(order)
        self.resting.observe_book(symbol, ts_ms)
        self.runtime.emit(
            "ORDER_WORKING",
            {
                "order_id": order.id,
                "price": money_to_str(limit),
                "remaining": money_to_str(from_scaled(order.remaining)),
                "queue_ahead": (
                    None
                    if order.queue_ahead is None
                    else money_to_str(from_scaled(order.queue_ahead))
                ),
            },
        )

    def _arm_trigger(self, ts_ms: int, order: Order) -> None:
        """A stop / take-profit / trailing order reaching the exchange.

        Nothing is validated against the exchange's price filters here, and that is
        deliberate: a stop price is not an order price, it is a level to watch. What gets
        validated is the *market order* the trigger produces, at the moment it produces one.
        """
        order.status = OrderStatus.WORKING
        self.resting.add(order)
        if order.intent.type is OrderType.TRAILING_STOP_MARKET:
            # Arm the trail from the price in force now, so a stop attached at entry starts
            # trailing from entry rather than from the first tick that happens to arrive.
            anchor = self._trigger_price_now(order.intent.symbol, order.intent.working_type)
            if anchor is not None:
                self.resting.advance_trail(order, anchor)
        self.runtime.emit(
            "ORDER_WORKING",
            {
                "order_id": order.id,
                "type": order.intent.type.value,
                "working_type": order.intent.working_type.value,
                "trigger_price": (
                    None
                    if order.trigger_price is None
                    else money_to_str(order.trigger_price)
                ),
            },
        )

    def _trigger_price_now(self, symbol: str, working: WorkingType) -> Money | None:
        if working is WorkingType.MARK_PRICE:
            return self.account.marks.get(symbol)
        scaled = self._recent_print(symbol)
        return None if scaled is None else from_scaled(scaled)

    def _fire_trigger(self, ts_ms: int, order_id: str, price: Money) -> None:
        """A trigger has fired: the order becomes a market order and leaves on its latency.

        Spec 6.4: *"On trigger, they become market orders and take the market-order path --
        including latency and slippage. A stop is not a guaranteed price."*

        The slippage reference is re-stamped to the trigger price. For a protective order the
        decision was not the moment the stop was attached -- which may be days earlier -- but
        the moment it fired, and leaving the original reference would report the whole move
        from entry to stop as execution slippage.
        """
        order = self.orders.get(order_id)
        if order is None or not order.is_open:
            return
        self.resting.discard(order)
        order.reference_price = price
        order.trigger_price = price
        self.counts["triggers"] += 1
        arrival = ts_ms + self.config.latency.submit_ms(self.latency_rng)
        order.arrival_ts = arrival
        order.status = OrderStatus.PENDING
        self.runtime.emit(
            "TRIGGER",
            {
                "order_id": order_id,
                "type": order.intent.type.value,
                "trigger_price": money_to_str(price),
                "arrival_ms": arrival,
            },
        )
        self._schedule(arrival, order_id, "submit")

    # --------------------------------------------------------------------------- fills

    def _clamp(self, order: Order, ts_ms: int) -> Money | None:
        """Quantity this order may still fill, or `None` if it cannot fill at all.

        Reduce-only is clamped at *arrival*, not at submission. The position can have moved
        during the latency window -- another fill, or a liquidation -- and the exchange
        clamps against what is there when the order lands. Clamping at submission would let
        a reduce-only order flip a position that shrank underneath it.
        """
        symbol = order.intent.symbol
        qty = from_scaled(order.remaining)
        if order.intent.reduce_only:
            held = self.account.qty(symbol)
            side = _side(order)
            if held == 0 or (held > 0) == (side is Side.BUY):
                # Cancelled, not rejected. A stop outliving the position it protected is an
                # ordinary bracket, not a sizing mistake -- and `_reject` raises
                # `ORDERS_REJECTED` with a warning blaming "the filter or margin that refused
                # them", which is a different and untrue story. `_book_maker_fill` already
                # used `CANCELLED` for the identical condition; two terminal states for one
                # condition is one of them being wrong.
                self._remove(
                    order,
                    OrderStatus.CANCELLED,
                    "reduce-only: the position it would have reduced is already flat",
                )
                return None
            if abs(held) < qty:
                qty = abs(held)

        step = from_scaled(self.filters[symbol].step_size)
        qty = quantize_qty(qty, step)
        if qty <= 0:
            self._reject(order, f"quantity floors to zero at step size {step}")
            return None
        return qty

    def _book_maker_fill(self, ts_ms: int, fill: MakerFill) -> None:
        """Book one queue-model increment against the ledger."""
        order = self.orders.get(fill.order_id)
        if order is None or not order.is_open:
            return
        qty = fill.qty
        if order.intent.reduce_only:
            held = self.account.qty(order.intent.symbol)
            side = _side(order)
            if held == 0 or (held > 0) == (side is Side.BUY):
                self._remove(
                    order,
                    OrderStatus.CANCELLED,
                    "reduce-only: the position it would have reduced is already flat",
                )
                return
            if abs(held) < qty:
                qty = abs(held)
        step = from_scaled(self.filters[order.intent.symbol].step_size)
        qty = quantize_qty(qty, step)
        if qty <= 0:
            # Below one lot step, so no fill can ever take it. Retiring the order beats
            # leaving it on the book forever reporting a `remaining` nothing will work.
            if 0 < order.remaining and from_scaled(order.remaining) < step:
                self._remove(
                    order,
                    OrderStatus.EXPIRED,
                    f"the residual {from_scaled(order.remaining)} is below the {step} lot "
                    f"step and can never fill",
                )
            return
        self._book_fill(ts_ms, order, qty, fill.price, is_maker=fill.is_maker)

    def _book_fill(
        self,
        ts_ms: int,
        order: Order,
        qty: Money,
        price: Money,
        *,
        is_maker: bool,
        quote: FillQuote | None = None,
        fee: Money | None = None,
    ) -> None:
        """The one place a fill enters the ledger, whatever produced it.

        Market orders, marketable limits, queue increments and triggered stops all arrive
        here. That is the point: the filter check, the margin failure path, the slippage
        accumulator, the round-trip builder, the event log and `on_fill` are each written
        once, so a new order type cannot acquire a subtly different version of any of them.

        `fee` is the venue's actual commission for this execution, passed by the live
        transport when the report carried one in USDT, and `None` everywhere else. The
        ledger books it verbatim in place of the modelled `|q| x P x rate` (spec 3.8: the
        rates vary by account and the venue's charge is the ground truth spec 6.7.3
        reconciles the wallet against). The simulated paths never pass it -- their fee is
        the model, which is the honest thing a simulation has.
        """
        symbol = order.intent.symbol
        filters = self.filters[symbol]
        mark = self.account.marks.get(symbol)
        # **`partial` means "this is an increment", not "this increment was passive".** The
        # size floors are admission criteria for an *order*, and this order was already
        # admitted; applying them per increment rejected a 0.001 BTC sweep against a live
        # 1 BTC bid for being worth 39.99 against a 50 minimum, and took the whole order off
        # the book with it. Keying that on `is_maker` fixed the queue path and left the taker
        # one: the crossing part of a marketable limit into a thin touch is also an increment,
        # and it died the same way. The price grid and the lot step still apply to every
        # increment, because those are properties of any printable execution.
        scaled_qty = decimal_to_scaled(qty)
        check = validate_order(
            filters,
            qty=scaled_qty,
            price=decimal_to_scaled(price),
            mark_price=None if mark is None else decimal_to_scaled(mark),
            is_market=_is_market_now(order),
            # **Every increment from a real venue is `partial`.** The admission floors
            # (`minQty`, `MIN_NOTIONAL`, `PERCENT_PRICE` against our own polled mark) are
            # criteria for admitting an *order*, and an order a real exchange is reporting
            # executions for was admitted by the venue itself -- the authority on its own
            # floors. Without this, the final taker increment of a multi-execution fill
            # (`scaled_qty == remaining`) ran the whole-order floors: a 0.006 BTC limit
            # filled 0.004-then-0.002 had its last increment refused for notional, the
            # order marked REJECTED while FILLED at the venue, and every later report for
            # it dropped as a duplicate. The price grid and lot step still apply -- a
            # report that fails those was misread, not merely small.
            partial=is_maker or scaled_qty < order.remaining or not self.transport.simulated,
        )
        if not check:
            self._reject(order, check.reason)
            return

        side = _side(order)
        signed = qty if side is Side.BUY else -qty
        routed = order.intent.position_side
        held = self.account.qty(symbol, routed)
        try:
            result = self.account.apply_fill(
                ts_ms,
                symbol,
                signed,
                price,
                position_side=routed,
                is_maker=is_maker,
                fee=fee,
            )
        except InsufficientMargin as exc:
            self._reject(order, str(exc))
            return
        except HedgeFlipRefused as exc:
            # The exchange refuses this order for the same reason, so it is a rejection
            # rather than a fault: a sell that exceeds the long side of a hedge is not an
            # order the venue would have filled either. Routed through `_reject` so it
            # reaches the strategy's `on_cancel` and the Feed exactly as a filter refusal
            # does, rather than aborting a run over an order that was simply not legal.
            self._reject(order, str(exc))
            return

        filled_scaled = scaled_qty
        order.filled_scaled += filled_scaled
        order.remaining = max(0, order.remaining - filled_scaled)
        with accounting():
            order.filled_cost = (order.filled_cost or _ZERO) + qty * price
            order.filled_qty = from_scaled(order.filled_scaled)
            order.filled_price = order.filled_cost / order.filled_qty
            # Signed against the trader for both sides: a buy paying above its reference is
            # a cost, and so is a sell receiving below it. The market models compute the
            # same expression; recomputing it here rather than reading `quote` keeps maker
            # increments -- which have no quote -- on exactly the same rule.
            per_unit = (price - order.reference_price) * (
                1 if side is Side.BUY else -1
            )
            self.slippage_cost += per_unit * qty
            self.slippage_abs += abs(per_unit) * qty
            # **Windowed to the run's own range, because its consumer is.** `turnover`
            # divides this by mean equity over `[start_ms, end_ms]` (the window rule --
            # see `metrics.compute_metrics`), and an order submitted on the final bar
            # legitimately fills *after* `end_ms` during the drain: counting that fill
            # here divided a whole-run numerator by a windowed denominator. The slippage
            # accumulators above deliberately stay whole-run -- they feed the attribution
            # identity, which is a statement about the ledger, and the ledger booked the
            # drain fill.
            if ts_ms <= self.config.end_ms:
                self.traded_notional += qty * price

        self.counts["fills"] += 1
        if is_maker:
            self.counts["maker_fills"] += 1
        if order.remaining > 0:
            self.counts["partial_fills"] += 1
        else:
            order.status = OrderStatus.FILLED
            self.resting.discard(order)
            # A flatten that *worked* leaves the book here rather than through `_remove` or
            # `_reject`, so `_release_flatten` -- the only other popper -- never sees it and
            # the entry stayed in `_flatten_orders` for the rest of the run. Harmless in a
            # backtest of a few hours and unbounded in a 48-hour session, where every
            # deadline that fires adds one and nothing takes any away.
            self._flatten_orders.pop(order.id, None)
            self._platform_orders.discard(order.id)

        after = self.account.qty(symbol, routed)
        self.trades.fill(
            ts_ms=ts_ms,
            symbol=symbol,
            signed_qty=signed,
            price=price,
            fee=result.fee,
            realized=result.realized,
            qty_before=held,
            qty_after=after,
            position_side=routed,
        )
        self._note_position_change(ts_ms, (symbol, routed), held, after)

        fill = Fill(
            order_id=order.id,
            symbol=symbol,
            side=order.intent.side,
            qty=qty,
            price=price,
            ts_ms=ts_ms,
            is_maker=is_maker,
            reduce_only=order.intent.reduce_only,
            tag=order.intent.tag,
        )
        payload: dict[str, Any] = {
            **fill.to_json(),
            "reference_price": money_to_str(order.reference_price),
            "slippage": money_to_str(per_unit * qty),
            "fee": money_to_str(result.fee),
            "realized": money_to_str(result.realized),
            "remaining": money_to_str(from_scaled(order.remaining)),
        }
        if quote is not None:
            payload["print_price"] = money_to_str(quote.print_price)
            payload["levels_walked"] = quote.levels_walked
            if quote.exhausted_qty > 0:
                payload["exhausted_qty"] = money_to_str(quote.exhausted_qty)
            if quote.impact_bps > 0:
                payload["impact_bps"] = money_to_str(quote.impact_bps)
        self.runtime.emit("FILL", payload)
        # **After the FILL, not before it.** A close that trips `max_consecutive_losses`
        # used to write `RISK_HALT` at sequence N and the fill that caused it at N+1, so the
        # event log -- which is the reproducibility artefact and the audit trail -- recorded
        # the halt before its own cause.
        self._observe_closed_trades(ts_ms)
        if "on_fill" in self._hooks:
            self.strategy.on_fill(self.context, fill)

    def _reject(self, order: Order, reason: str) -> None:
        """Refuse a fill and say why, in the exchange's own vocabulary where possible.

        A rejection is data about the strategy, not an engine failure: it usually means the
        sizing produced something the exchange would have refused, and a run that silently
        skipped those orders would report a strategy that never took the trade rather than
        one whose orders were invalid.
        """
        order.status = OrderStatus.REJECTED
        order.reason = reason
        self.resting.discard(order)
        # **The platform's own orders are not the strategy's.** A halt's forced exit and an
        # auto-flatten are issued by the engine; booking their refusals here raised
        # `ORDERS_REJECTED` and produced a warning telling the reader that an order of
        # *theirs* had been refused "at arrival; see the REJECT entries for the filter or
        # margin that refused them" -- about an order they never sent. Worse, it fed
        # `observe_rejection`, so an exit the exchange would not accept counted towards the
        # kill switch's own rejection trigger.
        if order.id in self._platform_orders:
            self._release_flatten(order)
            self.runtime.emit(
                "PLATFORM_ORDER_REJECTED", {"order_id": order.id, "reason": reason}
            )
            return
        self.counts["rejects"] += 1
        self.flags.add("ORDERS_REJECTED")
        self._note_order_end(order, OrderStatus.REJECTED, reason)
        self.runtime.emit("REJECT", {"order_id": order.id, "reason": reason})
        breach = self.risk.observe_rejection(self.runtime.now_ms, reason)
        if breach is not None:
            self._request_halt(breach)

    def _expire_parked(self, ts_ms: int) -> None:
        """Retire parked market orders whose deadline has passed.

        The scheduled `expire` instruction is the primary mechanism; this is the sweep that
        catches an order whose deadline event was consumed while it was still `PENDING`, and
        it runs once per mark bar, which is the same cadence as the deadline itself.
        """
        if self.tier is not FillTier.TRADE_ONLY:
            return
        for removal in tuple(self.resting.expire_stale(ts_ms, MARKET_WAIT_MS)):
            order = self.orders.get(removal.order_id)
            if order is not None and order.is_open:
                self._remove(order, removal.status, removal.reason)

    # --------------------------------------------------------------------- accounting

    def equity_snapshot(self, limit: int = LIVE_EQUITY_POINTS) -> EquityProgress:
        """The curve so far, thinned to `limit` points. Safe to call mid-run.

        Reads the accumulators `_sample_equity` appends to, and copies out of them: the
        caller gets tuples rather than the live lists, so a snapshot handed to a writer
        cannot grow underneath it while the write is in flight.
        """
        keep = _thin_extremes(self.equity, limit)
        return EquityProgress(
            ts=tuple(self.equity_ms[i] for i in keep),
            equity=tuple(self.equity[i] for i in keep),
            low=tuple(self.equity_low[i] for i in keep),
            high=tuple(self.equity_high[i] for i in keep),
            samples=len(self.equity),
            bars=self.counts["bars"],
        )

    def _sample_equity(
        self, ts_ms: int, ranges: Mapping[str, tuple[Money, Money, Money]] | None = None
    ) -> None:
        """One mark-to-market point, with the band the mark traversed to reach it.

        **One sample per mark bar, at the sample the mark actually took** (spec 3.4's LOCF
        close). Evenly spaced whether or not a position is open, which is what keeps
        `turnover`'s mean equity and `ulcer_index`'s mean-square from being weighted by how
        often the strategy happened to be holding -- the same unweighted-counting error
        `_exposure` avoids by weighting by interval.

        **The intrabar excursion rides along as a band rather than as extra samples.** Spec
        8.2 wants drawdown on every mark-to-market tick and warns that grid closes understate
        it; a 1-minute close series would understate it too. So `low` and `high` carry the
        equity the account would have had at each position's *own* adverse and favourable
        extreme -- computed directly from the positions, with no mark mutation and no
        fabricated joint state -- and `drawdown_stats` scores the trough against a peak that
        includes the crest. Symmetric between longs and shorts, and correct for a multi-leg
        book, neither of which was true when the extremes were sampled as ordered points.
        """
        equity = self.account.equity
        low = high = equity
        if ranges and self.account.positions:
            with accounting():
                adverse = favourable = self.account.wallet
                for position in self.account.positions.values():
                    symbol = position.symbol
                    values = ranges.get(symbol)
                    if values is None:
                        mark = self.account.marks.get(symbol, position.entry_price)
                        adverse += position.unrealized_pnl(mark)
                        favourable += position.unrealized_pnl(mark)
                        continue
                    bottom, top = values[0], values[1]
                    worse, better = (bottom, top) if position.qty > 0 else (top, bottom)
                    adverse += position.unrealized_pnl(worse)
                    favourable += position.unrealized_pnl(better)
            low, high = min(adverse, equity), max(favourable, equity)

        self.equity_ms.append(ts_ms)
        self.equity.append(float(equity))
        self.equity_low.append(float(low))
        self.equity_high.append(float(high))
        self.position_open.append(bool(self.account.positions))

        # **Scored against the trough, not the close.** Spec 7 measures drawdown on
        # mark-to-market equity and spec 8.2 says a close-only series understates it, and
        # the two together decide this line: a 15% limit that only ever sees minute closes
        # lets a strategy trade through a 20% intrabar excursion and reports that it never
        # breached. `low` is the equity at each position's own adverse extreme within the
        # bar, which is the same band `drawdown_stats` scores -- so the limit and the
        # reported drawdown cannot disagree about whether the run breached.
        for key in self._open_keys():
            position = self.account.positions[key]
            symbol = key[0]
            mark = self.account.marks.get(symbol)
            if mark is None:  # pragma: no cover - a position implies a print, not a mark
                continue
            # Per-trade excursions see the **whole traversed range**, not just the close.
            # Spec 8.3's point is that the MAE distribution is what stops get sized from,
            # and a stop is hit by the intrabar low, not by where the minute happened to
            # finish -- so a close-only series systematically understates it.
            #
            # This is safe here in a way it was not for the equity curve: MAE and MFE are
            # per *trade*, hence per symbol, so there is no joint state to fabricate and no
            # ordering between symbols to get wrong. Only the extremes of one position are
            # ever compared with each other.
            prices = [mark]
            values = ranges.get(symbol) if ranges else None
            if values is not None:
                prices = [values[0], values[1], mark]
            for price in prices:
                self.trades.mark(
                    symbol=symbol,
                    mark_price=price,
                    unrealized=position.unrealized_pnl(price),
                    position_side=position.position_side,
                )

        if self.symbol_pnl:
            # After the mark loop, whose final push per position was the close -- so each
            # open trade's running total stands at the same instant as the equity sample
            # three appends up. Sampling before the loop would lag every open position by
            # one bar and the portfolio curves would sum to yesterday's equity.
            totals = self.trades.net_pnl_by_symbol()
            for symbol, series in self.symbol_pnl.items():
                value = totals.get(symbol)
                series.append(0.0 if value is None else float(value))

        self._observe_risk_equity(
            ts_ms, low if low < equity else equity, high if high > equity else equity
        )

    def _observe_risk_equity(self, ts_ms: int, equity: Money, high: Money) -> None:
        """Hand the risk layer the whole band, not one end of it.

        The trough scores the drawdown and the crest raises the peak. Passing the trough
        for both made the risk layer's peak lower than the equity curve's, so its drawdown
        was smaller than the one the results page reported -- and a run whose published
        `max_drawdown` was 19.00% sailed past a 15% ceiling without halting.
        """
        breach = self.risk.observe_equity(ts_ms, equity, high=high)
        if breach is not None:
            self._request_halt(breach)

    def note_invariant_failure(self, exc: InvariantViolation) -> None:
        """Record spec 7's first auto-trigger. The caller re-raises; this does not.

        The ledger's own arithmetic has disagreed with itself, so every other number in this
        run was computed from state that cannot be trusted -- which is why the kill switch is
        recorded and the exception is then allowed to propagate. Returning a result here
        would publish an equity curve the platform has just proved wrong.

        A method rather than a block inside `run`, because a paper session drives the engine
        with its own loop and would otherwise not have this trigger at all: an invariant
        failure would kill the session worker with a bare traceback, no `KILL_SWITCH` event
        in the log, and nothing on the run row to say the ledger had broken. Spec 7 lists
        four auto-triggers and does not make any of them conditional on which loop is
        driving.
        """
        self.risk.observe_invariant_failure(
            self.runtime.now_ms, exc.invariant, exc.message
        )
        self.flags.add("RISK_HALTED")
        try:
            self.runtime.emit(
                "KILL_SWITCH",
                {
                    "trigger": "INVARIANT",
                    "detail": str(exc),
                    "invariant": exc.invariant,
                    "flatten": False,
                    "flattened": [],
                    "open_after": [position_label(*key) for key in self._open_keys()],
                },
            )
        except EventLogFull:
            # The log is full, which is a real condition and a much less important one than
            # the invariant that just failed. The switch is already tripped in the
            # `RiskEngine`; letting this replace the `InvariantViolation` would hand the
            # caller a message about log size while the ledger's arithmetic is broken.
            pass

    def request_halt(self, breach: RiskBreach) -> None:
        """Ask for a halt from outside the dispatch loop (spec 7's live auto-triggers).

        Spec 7's four auto-triggers do not all originate inside an event. An invariant
        failure and a repeated-rejection streak do, and the engine records those itself; a WS
        disconnection outlasting `max_disconnect_seconds` and a live-exchange reconciliation
        mismatch are observed by the session process between events. They halt through the
        same path so the sequencing guarantee holds either way: the breach is *recorded* here
        and carried out by `step`, with the ledger consistent by construction.
        """
        self._request_halt(breach)

    def _request_halt(self, breach: RiskBreach) -> None:
        """Record a halt to be carried out between events. See `_halt_pending`."""
        if self._halt_pending is None:
            self._halt_pending = breach
            self.flags.add("RISK_HALTED")
            self.runtime.emit("RISK_HALT", breach.to_json())

    def _perform_halt(self) -> None:
        """Cancel, optionally flatten, and stop. Spec 7's `HALT` action and 7.1-7.3.

        Cancels are **immediate**, not scheduled: this is the platform acting on its own
        book rather than an instruction crossing the wire, which is the same reasoning
        `_cancel_all(reason="liquidated")` already uses for spec 6.6. A halt that waited a
        latency window would let the very orders it is trying to stop fill inside it.

        Flattening is opt-in (spec 7.3: *"default: cancel-only, because force-closing
        everything at market during a flash crash can be worse than the exposure"*). When it
        is armed, the close is a market order priced by the ordinary fill path -- it pays the
        spread and the taker fee, because a halt that settled at the mark would report an
        exit nobody could have got.
        """
        breach = self._halt_pending
        if breach is None:  # pragma: no cover - guarded by the caller
            return
        # **First, before anything that can block.** A live worker hangs the durable
        # kill-switch arming here (spec 7.6); the cancels and exits below cross the
        # network, and a process killed inside them used to leave the on-disk switch
        # un-armed with the account it protects mid-halt. Guarded, because a halt that
        # could not be *recorded* must still cancel the book -- the worker's `finally`
        # backstop re-arms with the final outcome, and the warning is what tells the
        # operator the early write failed.
        if self.on_halt is not None:
            try:
                self.on_halt()
            except Exception as exc:  # noqa: BLE001 - the halt itself must proceed
                self.warnings.append(
                    f"the halt's persistent kill-switch write failed "
                    f"({type(exc).__name__}: {exc}); the switch is armed in memory and "
                    f"will be re-armed on disk at session end -- unless this process dies "
                    f"first, in which case the next session must be blocked by hand."
                )
        ts_ms = self.runtime.now_ms
        flattened: list[str] = []
        exits_in_flight: list[str] = []
        if self.transport.simulated:
            self._cancel_all(None, reason=f"halted: {breach.limit}")
            # The halt cancels the book, and every one of those cancels is an `on_cancel` a
            # strategy asked to hear about. `_drain_order_ends` normally runs at the tail of
            # `_dispatch`, and this method runs *outside* dispatch with `run()` about to
            # break -- so without this the notifications were built and then thrown away. A
            # strategy with a resting quote at the moment of a halt saw its order vanish and
            # was never told.
            self._drain_order_ends()
            if self.risk.kill_switch.flatten:
                # Snapshotted before the loop: `_force_flatten` deletes the position it
                # closes, and iterating the live dict while mutating it would skip the
                # second leg of a hedge -- leaving a position open on a halt that reported
                # having flattened.
                for key in self._open_keys():
                    if self._force_flatten(ts_ms, key, reason=f"halt:{breach.limit}"):
                        flattened.append(position_label(*key))
        else:
            # **A real venue: the halt sends requests and books nothing.** Removing orders
            # locally here would open a window in which a fill for a "removed" order arrives
            # from the user-data stream, finds its order terminal, and is dropped as a
            # duplicate -- a position at the exchange with nothing in the ledger to say so.
            # And `_force_flatten` would book an exit at a locally computed price while the
            # venue still held the position. So every open order gets a cancel *through the
            # transport*, every open position gets a real reduce-only market exit, and the
            # session's settle path (`PaperSession._end_live`) drains the requests, waits
            # for the venue's answers, and runs a final reconciliation against the account.
            # `open_after` below is therefore the honest reading at the halt instant: still
            # open, exits in flight.
            for order_id in self._open_order_ids(None):
                try:
                    self.transport.cancel(
                        self.orders[order_id], reason=f"halted: {breach.limit}"
                    )
                except Exception as exc:  # noqa: BLE001 - the halt must be recorded even so
                    # A transport refusing cancels at halt time is already the worst state
                    # it models (`TransportStalled`); losing the KILL_SWITCH record over it
                    # would hide the halt as well as the stall. The session's settle path
                    # still runs cancel-all at the venue, which is the recovery.
                    self.warnings.append(
                        f"the halt could not queue a cancel for {order_id}: "
                        f"{type(exc).__name__}: {exc}. The venue-side cancel-all at "
                        f"session end is what will clear the book."
                    )
            if self.risk.kill_switch.flatten:
                for key in self._open_keys():
                    order_id = self._transport_flatten(
                        ts_ms, key, reason=f"halt:{breach.limit}"
                    )
                    if order_id is not None:
                        exits_in_flight.append(order_id)
        self.runtime.emit(
            "KILL_SWITCH",
            {
                "trigger": self.risk.kill_switch.trigger,
                "detail": self.risk.kill_switch.detail,
                "flatten": self.risk.kill_switch.flatten,
                "flattened": flattened,
                "exits_in_flight": exits_in_flight,
                "open_after": [position_label(*key) for key in self._open_keys()],
                **breach.to_json(),
            },
        )
        self.warnings.append(
            f"the run halted at {ts_ms} on {breach.limit} ({breach.detail}; observed "
            f"{breach.observed}, limit {breach.allowed}). Every metric below is measured "
            f"over the period up to this instant rather than over the range that was asked "
            f"for, so returns, Sharpe and exposure describe a run that was stopped rather "
            f"than one that finished."
        )

    def _checkpoint(self, processed: int) -> None:
        """Enforce the wall-clock budget and, at most every `PROGRESS_INTERVAL_S`, report.

        Throttled by *time* rather than by event count, and that is what makes the store's
        liveness rule sound. Progress is the only heartbeat a worker writes, so with an
        event-count trigger a strategy spending 40 ms per bar could stay silent for minutes
        and be presumed dead; with a fixed 20 000-event interval a fast run would instead
        write thousands of rows nobody reads. A time bound gives a responsive heartbeat and
        a small number of writes at the same time.
        """
        now = time.perf_counter()
        if now > self._deadline:
            raise RunAborted(
                f"the run exceeded its {self.config.timeout_s:g}s wall-clock budget after "
                f"{processed:,} events and {self.counts['bars']:,} bars (spec 2.3). Either "
                f"the range is longer than the budget allows or a strategy hook is looping."
            )
        if now - self._last_progress >= PROGRESS_INTERVAL_S:
            self._last_progress = now
            if self.progress is not None:
                self.progress(self.counts["bars"], self._total_bars)
            if self.on_equity is not None:
                self.on_equity(self.equity_snapshot())

    def _finalise(self) -> None:
        """Final mark-to-market (spec 5.2), then the ledger's own end-of-run check.

        **A halted run is finalised where it stopped, not at the end of the range it was
        asked for.** Advancing to `end_ms - 1` after a halt appended an equity sample at a
        timestamp nothing had been observed at, carrying the halt's equity flat across the
        rest of the range -- and every ratio metric is computed on a grid over that range.
        A run that lost 10% in twenty-one minutes of a three-day window reported
        `volatility = 0.0` and `sharpe = None`, because all seventy-one hourly grid points
        fell after the halt and carried forward the same number. With the kill switch armed
        it was worse: the single grid step containing the forced exit was the only non-zero
        return in the series, and the run reported a **Sharpe of +11.1** and 99.95%
        exposure while flat.

        So the effective end is the halt, and `_result` measures against the same instant.
        The requested range is still recorded -- `config.end_ms` is unchanged and the halt's
        own timestamp is in the event log -- so nothing is hidden; what changes is that the
        metrics describe the period the run actually observed.
        """
        end = self.runtime.now_ms
        if self._halt_pending is None:
            # Unhalted, the run is finalised at the end of the range it was asked for --
            # and the *sample* may still land past it, because an order submitted on the
            # last bar fills after it. Only the halted case shortens the measured window.
            end = max(end, self.config.end_ms - 1)
        else:
            self.effective_end_ms = end + 1
        self.runtime.advance(end)
        self._sample_equity(end)
        # `reconcile` replays the event log from the opening balance and compares it with
        # the live state. It is independent of every accumulator the run used, so a fill
        # mis-booked identically into both the wallet and the totals -- the one failure I1
        # is blind to -- surfaces here.
        self.account.reconcile()

    def _check_slippage_accumulator(self) -> None:
        """Rebuild `slippage_cost` from the recorded fill prices and compare.

        **Why this exists at all.** Spec 8.4's identity cannot check the slippage term:
        `price_pnl` is defined as the ledger's figure *plus* the signed slippage, so the term
        enters the sum with `+1` and leaves with `-1` and cancels. Whatever the accumulator
        holds -- zero, or a figure that missed half the fills -- `build_attribution` still
        closes. And `price_pnl` is the number that answers "is this a price edge or an
        execution artefact": on the Phase 4 exit run the ledger realised -1 071 and the
        reported price leg is +428, a sign change produced entirely by adding slippage back.

        **Why it is independent.** The reconstruction reads `side`, `qty`, `price` and
        `reference_price` out of the FILL events -- not the `slippage` field they also carry,
        which is the same expression the accumulator used and would therefore agree with it
        by construction. Recomputing from the prices catches a missed fill, a sign error on
        one side only, and a quantity that disagrees with what the ledger booked.

        **Skipped, and said so, once the log has dropped events.** The log is capped at
        `MAX_EVENTS` (a live driver registers `on_log_full` and events past the cap are
        dropped); the accumulator saw every fill and a rebuilt figure sees only the logged
        ones, so on a capped run the check compared two different populations and aborted
        blaming the accumulator for the log's own ceiling. A log at the cap is already a
        named stop with its own explanation; the cross-check only means something when the
        two sides counted the same fills.
        """
        if len(self.runtime.events) >= MAX_EVENTS:
            self.warnings.append(
                "the slippage cross-check was skipped: the event log reached its "
                f"{MAX_EVENTS:,}-event cap, so a figure rebuilt from it would cover only "
                "the logged fills while the accumulator covers them all. The attribution "
                "identity still holds; what is missing is this one independent check."
            )
            return
        with accounting():
            rebuilt = _ZERO
            rebuilt_abs = _ZERO
            for event in self.runtime.events:
                if event.kind != "FILL":
                    continue
                payload = event.payload
                qty = parse_money(str(payload["qty"]))
                price = parse_money(str(payload["price"]))
                reference = parse_money(str(payload["reference_price"]))
                sign = 1 if payload["side"] == "BUY" else -1
                per_unit = sign * (price - reference)
                rebuilt += per_unit * qty
                rebuilt_abs += abs(per_unit) * qty

        if rebuilt != self.slippage_cost or rebuilt_abs != self.slippage_abs:
            raise RunAborted(
                "the slippage accumulator disagrees with the fills it was accumulated from: "
                f"signed {self.slippage_cost} vs {rebuilt} rebuilt from the event log, "
                f"absolute {self.slippage_abs} vs {rebuilt_abs}. Spec 8.4's price leg is "
                "computed from this figure, so a mismatch means the reported price edge is "
                "not a measurement."
            )

    def _result(self, processed: int, wall: float) -> BacktestResult:
        self._check_slippage_accumulator()
        trades = self.trades.finish(self.runtime.now_ms)
        metrics = compute_metrics(
            times=self.equity_ms,
            equity=self.equity,
            equity_low=self.equity_low,
            equity_high=self.equity_high,
            positions_open=self.position_open,
            trades=trades,
            start_ms=self.config.start_ms,
            # The end of what was *observed*, which differs from the requested end only
            # when a halt stopped the run early. Measuring a halted run against the range
            # it never reached puts an unobserved flat period into every ratio metric --
            # see `_finalise`.
            end_ms=self.effective_end_ms,
            traded_notional=float(self.traded_notional),
        )
        attribution = build_attribution(
            self.account.attribution(),
            slippage_cost=self.slippage_cost,
            slippage_abs=self.slippage_abs,
        )
        if self.counts["rejects"]:
            self.warnings.append(
                f"{self.counts['rejects']} order(s) were rejected at arrival; see the "
                f"REJECT entries in the event log for the filter or margin that refused "
                f"them."
            )
        if self.counts["maker_fills"]:
            self.flags.add("MAKER_FILLS")
        if self.counts["risk_rejects"]:
            self.warnings.append(
                f"{self.counts['risk_rejects']} order(s) were refused by the risk layer. "
                f"The equity curve is of a strategy that was partly prevented from trading, "
                f"not of the strategy as written; see the RISK_REJECT entries for which "
                f"limit refused what."
            )
        return BacktestResult(
            events=tuple(self.runtime.events),
            event_hash=event_hash(self.runtime.events),
            trades=trades,
            metrics=metrics,
            attribution=attribution,
            equity_ms=tuple(self.equity_ms),
            equity=tuple(self.equity),
            equity_low=tuple(self.equity_low),
            equity_high=tuple(self.equity_high),
            position_open=tuple(self.position_open),
            data_start_ms=self.data_start_ms,
            warnings=tuple(self.warnings),
            flags=tuple(sorted(self.flags)),
            fill_tier=self.tier.name,
            orders=self.counts["orders"],
            fills=self.counts["fills"],
            rejects=self.counts["rejects"],
            liquidations=self.counts["liquidations"],
            bars=self.counts["bars"],
            ticks=self.counts["ticks"],
            maker_fills=self.counts["maker_fills"],
            partial_fills=self.counts["partial_fills"],
            depth_exhausted=self.counts["depth_exhausted"],
            engine_events=processed,
            wall_s=wall,
            final_equity=self.account.equity,
            final_wallet=self.account.wallet,
            opening_balance=self.config.opening_balance,
            risk_breaches=tuple(self.risk.breaches),
            halt_reason=self.risk.halt_breach,
            risk_rejects=self.counts["risk_rejects"],
            auto_flattens=self.counts["auto_flattens"],
            risk_summary=self.risk.summary(),
            symbol_pnl={
                symbol: tuple(series) for symbol, series in self.symbol_pnl.items()
            },
        )


_DATASET_TIERS: dict[str, FillTier] = {
    "aggTrades": FillTier.TRADE_ONLY,
    "bookTicker": FillTier.BOOK_TICKER,
    "depth20": FillTier.BOOK_WALK,
}
"""Lowest tier at which each declarable dataset is actually replayed.

`liquidations` is deliberately absent: it has no public source on this deployment
(`collector.UNAVAILABLE_DATASETS`), so declaring it is a validation warning at save time
rather than a tier question at run time.
"""

_REMOVAL_EVENTS: dict[OrderStatus, str] = {
    OrderStatus.CANCELLED: "CANCEL",
    OrderStatus.EXPIRED: "EXPIRE",
    OrderStatus.REJECTED: "REJECT",
}


_FEED_NAMES = {"trade": "the trade tape", "depth": "the order book"}


def _side(order: Order) -> Side:
    return Side.BUY if order.intent.side == "BUY" else Side.SELL


def _is_market_now(order: Order) -> bool:
    """Whether this order takes the market-order path at this moment.

    True for a plain market order and for a stop/take-profit/trailing order that has already
    fired -- spec 6.4 turns the second into the first on trigger. Called rather than
    open-coded because the three places that ask (arrival dispatch, the parked-order sweep,
    and the `MARKET_LOT_SIZE` choice) must agree, and a stop that counted as a market order
    in two of them and not the third would validate against the wrong lot filter.
    """
    return order.intent.type is OrderType.MARKET or order.triggered


def _far_touch(side: Side, top: Any, ladder: DepthSnapshot | None) -> int | None:
    """The best price on the side a limit order would take from, scaled, or `None`.

    The ladder wins when present -- it is the richer observation and the one `cross_book`
    walked -- and its level 0 is its touch. `top` is the `bookTicker` fallback.
    """
    buying = side is Side.BUY
    if ladder is not None:
        return ladder.ask_px[0] if buying else ladder.bid_px[0]
    if top is not None:
        return top.ask_px if buying else top.bid_px
    return None


def _limit_crosses(
    side: Side, limit_scaled: int, top: Any, ladder: DepthSnapshot | None
) -> bool:
    """Whether a limit at this price takes liquidity: the far touch is at or through it.

    A **price** fact, deliberately not a fill fact. `cross_book`'s quantity is zero both
    when nothing crosses and when the crossing levels happen to show zero size, and a
    post-only decision keyed on it admitted crossing orders in exactly the second case.
    Returns `False` when there is no observation at all -- the caller owns that branch,
    because what it means differs by time-in-force (see `_place_limit`'s GTX handling).
    """
    far = _far_touch(side, top, ladder)
    if far is None or far <= 0:
        return False
    return far <= limit_scaled if side is Side.BUY else far >= limit_scaled


def _rests_through_book(
    side: Side, limit_scaled: int, top: Any, ladder: DepthSnapshot | None
) -> bool:
    """Whether a resting remainder sits **beyond every level the model consumed** (H21).

    With only a touch (`BOOK_TICKER`), that is a price strictly through it: at exactly the
    touch the published size was consumed in full and the remainder becomes the new best
    quote, which genuinely rests. With a ladder (`BOOK_WALK`), `cross_book` consumed every
    visible level at or better than the limit, so only a limit strictly past the deepest
    published level leaves unseen liquidity between the book and the order. Either way a
    `True` means the venue would still have been *taking* -- see `RestingBook._crossed`.
    """
    buying = side is Side.BUY
    if ladder is not None:
        prices = ladder.ask_px if buying else ladder.bid_px
        if not prices:
            return False
        deepest = prices[-1]
        return limit_scaled > deepest if buying else limit_scaled < deepest
    if top is not None:
        far = top.ask_px if buying else top.bid_px
        if far <= 0:
            return False
        return limit_scaled > far if buying else limit_scaled < far
    return False

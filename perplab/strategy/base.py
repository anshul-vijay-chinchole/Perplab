"""The `Strategy` base class — the user-facing API (spec 5.1).

Everything a strategy author writes hangs off this class, so its shape is a product
decision rather than an implementation one. Three choices are load-bearing:

**Hooks default to doing nothing, and are never abstract.** A strategy that only trades on
bars should not have to write six empty methods. `on_bar`/`on_tick` presence is checked by
the validator (spec 5.5 step 3) with a message, rather than by `abstractmethod`, which
would fail at instantiation with a traceback listing every unimplemented name.

**`self.p` is bound before `on_start`.** Params are resolved once, at construction, so
there is no window in which a hook can observe half-bound parameters, and no way for a
strategy to mutate them mid-run and leave the recorded param set describing a run that did
not happen (spec 12.1).

**Warm-up is enforced by the engine, not by the strategy.** `ctx.warm` is offered as a
convenience for skipping early bars, but orders are *blocked* during warm-up by the
context itself (spec 5.2). A guarantee that depends on the author remembering to check a
flag is not a guarantee.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Mapping

from perplab.strategy.params import (
    ParamSet,
    ParamSpec,
    Requirements,
    bind_params,
    parse_param_specs,
    parse_requirements,
)

if TYPE_CHECKING:  # pragma: no cover - import cycle only matters to type checkers
    from perplab.core.types import Bar
    from perplab.engine.ticks import TradePrint
    from perplab.strategy.context import Context

__all__ = ["Strategy"]


class Strategy:
    """Base class for every PerpLab strategy.

    ```python
    class EMACross(Strategy):
        params = {"fast": {"type": "int", "default": 12, "min": 2, "max": 200}}
        requires = {"symbols": ["BTCUSDT"], "timeframe": "15m", "history": 400}

        def on_start(self, ctx):
            self.fast = ctx.indicators.ema(self.p.fast)

        def on_bar(self, ctx, bar):
            ...
    ```
    """

    params: ClassVar[Mapping[str, Any]] = {}
    """Declared parameters. Drives the auto-generated config form (spec 5.1)."""

    requires: ClassVar[Mapping[str, Any]] = {}
    """Declared data needs, checked for coverage *before* a run starts (spec 5.1)."""

    def __init__(self, overrides: Mapping[str, Any] | None = None) -> None:
        specs = self.param_specs()
        self.p: ParamSet = bind_params(specs, overrides)
        self._requirements: Requirements | None = None

    # ------------------------------------------------------------------ declarations

    @classmethod
    def param_specs(cls) -> tuple[ParamSpec, ...]:
        """Parse and validate `params`. Raises `ParamError` with the offending key."""
        return parse_param_specs(cls.params)

    @classmethod
    def requirements(cls) -> Requirements:
        """Parse and validate `requires`. Raises `ParamError` with the offending key."""
        return parse_requirements(cls.requires)

    @property
    def declared(self) -> Requirements:
        """This instance's parsed `requires`, computed once.

        Cached on the instance rather than the class: a subclass that overrides `requires`
        would otherwise inherit its parent's cached value, and the mismatch would only
        show up as a coverage check run against the wrong symbol.
        """
        if self._requirements is None:
            self._requirements = self.requirements()
        return self._requirements

    # ----------------------------------------------------------------------- hooks

    def on_start(self, ctx: Context) -> None:
        """Called once before any data. Build indicators here.

        Indicators must be constructed here rather than in `__init__` because they attach
        to the run's `ctx.indicators` registry, which is what drives them on each closed
        bar and what the engine reads to derive the warm-up length (spec 5.4 rule 4).
        """

    def on_bar(self, ctx: Context, bar: Bar) -> None:
        """Called once per *closed* bar of `requires["timeframe"]`.

        The bar is closed, always. There is no representation of a forming bar in this
        codebase, which is the structural half of the no-look-ahead guarantee (spec 6.2).
        """

    def on_tick(self, ctx: Context, trade: TradePrint) -> None:
        """Called per aggregate trade, at the `TRADE_ONLY` tier and above.

        The argument is a `TradePrint`, not `core.types.AggTrade`. It carries `price`/`qty`
        as floats for indicator-style code, `price_scaled`/`qty_scaled` as the lake's exact
        integers, and `is_sell_aggressive` / `consumes_bids` for the aggressor side. What it
        does *not* carry is the collector's receive clock or the underlying trade-id span --
        a strategy cannot act on those identically in backtest and live, so they are not on
        this surface.
        """

    def on_fill(self, ctx: Context, fill: Any) -> None:
        """Called when one of this strategy's orders fills, in whole or in part."""

    def on_cancel(self, ctx: Context, event: Any) -> None:
        """Called when one of this strategy's orders leaves the book unfilled.

        The argument is a `context.OrderEnd`. It fires for cancels, expiries and rejections
        alike, with `status` distinguishing them -- see `OrderEnd` for why collapsing them
        into one hook is the right shape and firing on only one of them is a trap.

        Dispatched **between engine events**, never inside one. A cancel can originate in
        the middle of a liquidation clearing the symbol's book, and a hook running there
        would see a probed mark price rather than an observed one -- the same reason
        `on_liquidation` is deferred (spec 6.2).
        """

    def on_funding(self, ctx: Context, event: Any) -> None:
        """Called at each funding settlement affecting an open position (spec 3.5)."""

    def on_liquidation(self, ctx: Context, event: Any) -> None:
        """Called when *this* strategy's position is liquidated (spec 3.7)."""

    def on_market_liquidation(self, ctx: Context, event: Any) -> None:
        """Called on another participant's liquidation — cascade signals (spec 5.1).

        Not fired on this deployment: the market-wide forced-order feed has no public
        source (see `UNAVAILABLE_DATASETS` in the collector). Declaring `liquidations` in
        `requires` produces a validation warning saying so, rather than a run that
        completes having silently never called this.
        """

    def on_stop(self, ctx: Context) -> None:
        """Called once after the last event, before final mark-to-market (spec 5.2)."""

    # ------------------------------------------------------------------ introspection

    @classmethod
    def implemented_hooks(cls) -> frozenset[str]:
        """Hook names this subclass actually overrides.

        Used by the validator to check that at least one of `on_bar`/`on_tick` exists, and
        by the engine to skip dispatching events nobody listens for. Identity comparison
        against the base function is deliberate: a subclass that assigns
        `on_bar = Strategy.on_bar` has not implemented it, and should not be counted.
        """
        names = (
            "on_start",
            "on_bar",
            "on_tick",
            "on_fill",
            "on_cancel",
            "on_funding",
            "on_liquidation",
            "on_market_liquidation",
            "on_stop",
        )
        return frozenset(
            name
            for name in names
            if getattr(cls, name, None) is not getattr(Strategy, name)
        )

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.p!r})"

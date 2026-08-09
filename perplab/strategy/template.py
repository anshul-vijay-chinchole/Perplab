"""The starting code Monaco opens for a new strategy (spec 5.6).

Kept as source in the package rather than as a string in the frontend so it is covered by
the same tests as everything else: `tests/unit/test_validate.py` validates it, which means
the template a user is handed cannot drift into failing the validator they are about to
meet.
"""

from __future__ import annotations

__all__ = ["NEW_STRATEGY_TEMPLATE", "EMA_CROSS_EXAMPLE"]

NEW_STRATEGY_TEMPLATE = '''\
from perplab import Strategy


class MyStrategy(Strategy):
    """One-line description of the edge this is trying to capture."""

    # Declared params drive the config form. Decimals are written as strings —
    # 0.01 as a Python float is not 0.01, and that error would multiply a notional.
    params = {
        "fast": {"type": "int", "default": 12, "min": 2, "max": 200},
        "slow": {"type": "int", "default": 26, "min": 3, "max": 400},
    }

    # Declared up front so the engine can check data coverage before the run starts,
    # instead of failing 60% of the way through.
    requires = {
        "symbols": ["BTCUSDT"],
        "timeframe": "15m",
        "history": 26,          # warm-up bars; must cover the slowest indicator
        "datasets": ["klines"],
    }

    def on_start(self, ctx):
        # Build indicators here, not in __init__ — this is where they register with
        # the run and where the engine derives the warm-up length from.
        self.fast = ctx.indicators.ema(self.p.fast)
        self.slow = ctx.indicators.ema(self.p.slow)

    def on_bar(self, ctx, bar):
        if not ctx.warm:
            return

        position = ctx.position()

        if self.fast.crossed_above(self.slow) and position.is_flat:
            ctx.buy(qty=ctx.risk.size_by_notional(ctx.account.equity / 10))
        elif self.fast.crossed_below(self.slow) and position.is_long:
            ctx.close()
'''

EMA_CROSS_EXAMPLE = '''\
from perplab import Strategy


class EMACross(Strategy):
    """Long-only EMA crossover, sized by the distance to the stop.

    The reference strategy from spec 5.1, written out in full. It is here to be read:
    every deliberate choice below has a reason attached.
    """

    params = {
        "fast": {"type": "int", "default": 12, "min": 2, "max": 200},
        "slow": {"type": "int", "default": 26, "min": 3, "max": 400},
        "stop_atr": {"type": "decimal", "default": "2.0", "min": "0.5", "max": "10"},
        "risk": {"type": "decimal", "default": "0.01", "min": "0.001", "max": "0.05"},
    }

    requires = {
        "symbols": ["BTCUSDT"],
        "timeframe": "15m",
        "history": 40,
        "datasets": ["klines"],
    }

    def on_start(self, ctx):
        self.fast = ctx.indicators.ema(self.p.fast)
        self.slow = ctx.indicators.ema(self.p.slow)
        self.atr = ctx.indicators.atr(14)

    def on_bar(self, ctx, bar):
        if not ctx.warm:
            return

        position = ctx.position()
        price = ctx.mark()

        if self.fast.crossed_above(self.slow) and position.is_flat:
            # Size from the stop distance, not from a fixed notional: the risk taken
            # is then the same whether the stop is near or far.
            # ctx.money() is the one place indicator floats become exact quantities.
            stop = price - self.p.stop_atr * ctx.money(self.atr.value)
            qty = ctx.risk.size_by_stop(
                entry=price, stop=stop, risk_fraction=self.p.risk
            )
            if qty > 0:
                ctx.buy(qty=qty)
                ctx.log.info("entry", price=str(price), stop=str(stop))

        elif self.fast.crossed_below(self.slow) and position.is_long:
            ctx.close()
            ctx.log.info("exit", price=str(price))

        ctx.record("ema_gap_pct", (self.fast.value - self.slow.value) / self.slow.value * 100)
'''

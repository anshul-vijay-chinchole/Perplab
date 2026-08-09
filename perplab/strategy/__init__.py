"""Strategy API (spec 5).

The user-facing surface of PerpLab. Everything here is imported *by strategy code*, so
the names, the error messages, and the failure timing are part of the product rather than
an implementation detail.

Three rules shape the whole package:

**Causality is structural, not documented.** An indicator that could look forward is not
offered (spec 5.4), a bar is immutable, and warm-up is enforced by the engine rather than
by asking the strategy to check. Anything a comment has to warn you about is a bug waiting
for the day nobody reads the comment.

**Failures arrive at save time, not at hour three of a backtest.** The validator (spec
5.5) runs the strategy on synthetic data before it is ever allowed near a run, so an
unbound name costs a second instead of twenty minutes.

**Nothing here touches the clock or the network.** `ctx.now` is the engine's clock and
`ctx.rng` is the run's seeded RNG. A strategy that reaches past them is rejected, because
both are look-ahead vectors and both break reproducibility (spec 12.1).
"""

from __future__ import annotations

from perplab.strategy.base import Strategy
from perplab.strategy.context import Context
from perplab.strategy.params import ParamSet, ParamSpec, Requirements

__all__ = [
    "Strategy",
    "Context",
    "ParamSet",
    "ParamSpec",
    "Requirements",
]

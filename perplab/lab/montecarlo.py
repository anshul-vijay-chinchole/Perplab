"""Monte Carlo resampling of a completed run (spec 9.2).

Four methods, four different questions -- the table in spec 9.2 is the contract:

| method              | question                                                        |
|---------------------|-----------------------------------------------------------------|
| `trade_permutation` | how much of my drawdown profile was luck of sequencing?         |
| `trade_bootstrap`   | what is the sampling distribution of my performance?            |
| `block_bootstrap`   | same, preserving autocorrelation -- the honest time-series form |
| `random_start`      | how dependent is the result on when I happened to start?        |

## The caveat spec 9.2 makes mandatory

The two trade-level methods build **additive** PnL paths: opening balance plus a running
sum of round-trip PnLs. That is fixed-notional sizing by construction, and under it
permuting trade order *cannot* change final equity -- only the path, and therefore the
drawdown. Spec 9.2: "That is not a bug, it is the entire point of that test." The artefact
states this, and the implementation goes one further: it *asserts* the invariance every
iteration, because a permutation method whose finals drifted would mean the arithmetic is
wrong, and the assert is what tells us before a user does.

The two return-level methods build **multiplicative** paths from the metric grid's
returns, which is compounding. The methods therefore answer their questions under
different sizing assumptions, and the per-method `sizing` field says which, every time.

## What each method can honestly report

Spec 9.2 asks for distributions of final equity, MaxDD and Sharpe. A Sharpe is a
statistic of a *return series on a time grid*; a shuffled bag of trade PnLs has no time
grid, so the trade methods report final equity and MaxDD only, and say why rather than
inventing an annualisation. The return methods report all three. A `null` with a reason
beats a number with a footnote nobody reads.

Drawdown resolution differs the same way: trade paths measure it at trade-close
resolution (the worst mark-to-market moment *inside* a trade is invisible to a shuffled
PnL sequence), return paths at grid resolution. Both are stated in the artefact; both
understate the tick-resolution figure the run itself reports (spec 8.2), which is one
more reason these distributions contextualise the run's number rather than replace it.

## Ruin

`prob_ruin` is the fraction of paths whose equity reached zero, and **every method
reports it**. An earlier version returned `null` for the two compounding methods on the
grounds that `1 + r` is always positive. That was wrong, and wrong in the flattering
direction: `build_grid` emits a return at or below -100% whenever a run's equity crosses
zero inside a grid step, the path floors at zero, and the account is gone. Withholding
the ruin probability of exactly the runs that blew up -- while citing an arithmetic
guarantee the code did not have -- is the failure this platform exists to avoid.

A path that reaches zero stops contributing returns to its own Sharpe, for the same
reason `build_grid` truncates: periods after the account died are not periods anyone
could have traded.

Everything is deterministic given the config seed. Each method seeds its own generator
from `f"{seed}:{method}"`, so adding a method or reordering the list cannot silently
change another method's draws (spec 12.1's reproducibility argument, applied to
resampling).
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from statistics import median
from typing import Any, Mapping, Sequence

from perplab.lab.stats import sharpe_ratio

__all__ = [
    "METHODS",
    "MonteCarloConfig",
    "MonteCarloInputs",
    "run_montecarlo",
]


METHODS = ("trade_permutation", "trade_bootstrap", "block_bootstrap", "random_start")

PERCENTILES = (5, 25, 50, 75, 95)
"""Spec 9.2's summary points."""


@dataclass(frozen=True, slots=True)
class MonteCarloConfig:
    """What the Lab form chose. Defaults are spec 9.2's."""

    iterations: int = 10_000
    seed: int = 0
    methods: tuple[str, ...] = METHODS
    block_length: int | None = None
    """Moving-block bootstrap block size. `None` -> `round(sqrt(n))`, the standard
    rule-of-thumb balance between preserving autocorrelation (long blocks) and variety of
    resamples (short ones). Recorded in the artefact either way, because the answer moves
    with it and a reader comparing two artefacts needs to see that it did not."""
    max_skip_fraction: float = 0.25
    """`random_start` evaluates every start in `[0, floor(n * this)]`. A quarter by
    default: skipping more than that is asking about a different backtest, not about
    sensitivity to the start date."""

    def __post_init__(self) -> None:
        # `from_json` checks these too; this catches a config constructed directly in
        # Python, where a `block_length=0` would be read as "unset" by the `or` in
        # `_block_bootstrap` and silently become the sqrt default.
        if self.block_length is not None and self.block_length < 1:
            raise ValueError(f"block_length must be positive, got {self.block_length}")
        if self.iterations < 1:
            raise ValueError(f"iterations must be positive, got {self.iterations}")
        if not 0.0 < self.max_skip_fraction <= 0.5:
            raise ValueError(
                f"max_skip_fraction must be in (0, 0.5], got {self.max_skip_fraction}"
            )

    def to_json(self) -> dict[str, Any]:
        return {
            "iterations": self.iterations,
            "seed": self.seed,
            "methods": list(self.methods),
            "block_length": self.block_length,
            "max_skip_fraction": self.max_skip_fraction,
        }

    @classmethod
    def from_json(cls, obj: Mapping[str, Any]) -> "MonteCarloConfig":
        methods = tuple(obj.get("methods") or METHODS)
        unknown = [m for m in methods if m not in METHODS]
        if unknown:
            raise ValueError(f"unknown Monte Carlo methods {unknown}: use {list(METHODS)}")
        iterations = int(obj.get("iterations", 10_000))
        if iterations < 100:
            raise ValueError(
                f"iterations must be at least 100, got {iterations}: percentile tails "
                "estimated from fewer draws than that are noise wearing digits"
            )
        if iterations > 1_000_000:
            raise ValueError(f"iterations capped at 1,000,000, got {iterations}")
        block = obj.get("block_length")
        if block is not None and int(block) < 1:
            raise ValueError(f"block_length must be positive, got {block}")
        # `_block_bootstrap` reads `config.block_length or <default>`, so a zero would be
        # silently treated as "unset" rather than refused. Caught here *and* in
        # `__post_init__`, because a config built directly in Python never passes through
        # this classmethod.
        skip = float(obj.get("max_skip_fraction", 0.25))
        if not 0.0 < skip <= 0.5:
            raise ValueError(
                f"max_skip_fraction must be in (0, 0.5], got {skip}"
            )
        return cls(
            iterations=iterations,
            seed=int(obj.get("seed", 0)),
            methods=methods,
            block_length=None if block is None else int(block),
            max_skip_fraction=skip,
        )


@dataclass(frozen=True, slots=True)
class MonteCarloInputs:
    """What the run artefacts supplied. Assembled by the Lab worker, not read here --
    this module stays pure so every branch of it is testable without a run directory."""

    trade_pnls: tuple[float, ...]
    """Closed round-trips' net PnL, in entry order. Open trades are excluded upstream:
    an unrealised result is not a draw from the trade distribution (spec 8.1's argument
    against counting open trades in ratios, applied to resampling)."""
    grid_returns: tuple[float, ...]
    periods_per_year: int
    grid_label: str
    opening_balance: float
    max_drawdown_limit: float | None
    """The run's configured `max_drawdown_pct`, as a positive fraction, or `None`."""


def run_montecarlo(
    inputs: MonteCarloInputs, config: MonteCarloConfig
) -> dict[str, Any]:
    """Every configured method, summarised. Pure and deterministic given its arguments."""
    if inputs.opening_balance <= 0:
        raise ValueError(
            f"opening balance must be positive, got {inputs.opening_balance}"
        )
    methods: dict[str, Any] = {}
    for name in config.methods:
        if name == "trade_permutation":
            methods[name] = _trade_permutation(inputs, config)
        elif name == "trade_bootstrap":
            methods[name] = _trade_bootstrap(inputs, config)
        elif name == "block_bootstrap":
            methods[name] = _block_bootstrap(inputs, config)
        elif name == "random_start":
            methods[name] = _random_start(inputs, config)
        else:  # pragma: no cover - from_json refuses these
            raise ValueError(f"unknown method {name!r}")
    return {
        "config": config.to_json(),
        "inputs": {
            "trades": len(inputs.trade_pnls),
            "grid_returns": len(inputs.grid_returns),
            "grid": inputs.grid_label,
            "periods_per_year": inputs.periods_per_year,
            "opening_balance": inputs.opening_balance,
            "max_drawdown_limit": inputs.max_drawdown_limit,
        },
        "methods": methods,
        "caveats": [
            (
                "trade_permutation and trade_bootstrap build additive PnL paths -- "
                "fixed-notional sizing by construction. Under it, permuting trade order "
                "does not change final equity at all; the drawdown distribution is the "
                "result (spec 9.2). A strategy that compounds its sizing would show "
                "order-dependence these methods cannot."
            ),
            (
                "block_bootstrap and random_start compound the run's "
                f"{inputs.grid_label} grid returns -- percent-of-equity sizing by "
                "construction."
            ),
            (
                "trade-path drawdowns are measured at trade-close resolution and "
                "return-path drawdowns at grid resolution; both understate the "
                "tick-resolution figure the run itself reports (spec 8.2)."
            ),
        ],
    }


def _rng(config: MonteCarloConfig, method: str) -> random.Random:
    return random.Random(f"{config.seed}:{method}")


# ------------------------------------------------------------------------ trade paths


def _additive_path_stats(
    pnls: Sequence[float], opening: float
) -> tuple[float, float | None, float]:
    """`(final, max_drawdown, minimum)` of `opening + cumsum(pnls)`.

    Drawdown as a fraction of the running peak, same convention as `drawdown_stats`; a
    non-positive peak contributes nothing rather than an infinity.
    """
    equity = opening
    peak = opening
    worst = 0.0
    minimum = opening
    saw_positive_peak = peak > 0
    for pnl in pnls:
        equity += pnl
        if equity > peak:
            peak = equity
        if equity < minimum:
            minimum = equity
        if peak > 0:
            saw_positive_peak = True
            drawdown = equity / peak - 1.0
            if drawdown < worst:
                worst = drawdown
    return equity, (worst if saw_positive_peak else None), minimum


def _trade_permutation(
    inputs: MonteCarloInputs, config: MonteCarloConfig
) -> dict[str, Any]:
    pnls = list(inputs.trade_pnls)
    if len(pnls) < 2:
        return _too_few(
            "trade_permutation",
            "additive",
            "random",
            f"{len(pnls)} closed trades; need at least 2",
        )
    rng = _rng(config, "trade_permutation")
    expected_final = inputs.opening_balance + sum(pnls)
    finals: list[float] = []
    drawdowns: list[float] = []
    ruins = 0
    for _ in range(config.iterations):
        rng.shuffle(pnls)
        final, drawdown, minimum = _additive_path_stats(pnls, inputs.opening_balance)
        # The invariance spec 9.2 documents, checked rather than assumed. Tolerance is
        # float addition reordering, which is the one legitimate source of difference.
        if abs(final - expected_final) > 1e-6 * max(1.0, abs(expected_final)):
            raise AssertionError(
                f"a permutation changed final equity ({final} != {expected_final}): "
                "the additive path arithmetic is broken"
            )
        finals.append(final)
        if drawdown is not None:
            drawdowns.append(drawdown)
        if minimum <= 0:
            ruins += 1
    return _summary(
        method="trade_permutation",
        sizing="additive",
        sampling="random",
        iterations=config.iterations,
        finals=finals,
        drawdowns=drawdowns,
        sharpes=None,
        sharpe_reason=(
            "a shuffled bag of trade PnLs has no time grid, so an annualised Sharpe "
            "cannot be computed from it"
        ),
        ruins=ruins,
        ruin_reason=None,
        limit=inputs.max_drawdown_limit,
        notes=[
            "final equity is identical across iterations by construction; the drawdown "
            "distribution is the result"
        ],
    )


def _trade_bootstrap(
    inputs: MonteCarloInputs, config: MonteCarloConfig
) -> dict[str, Any]:
    pnls = inputs.trade_pnls
    if len(pnls) < 2:
        return _too_few(
            "trade_bootstrap",
            "additive",
            "random",
            f"{len(pnls)} closed trades; need at least 2",
        )
    rng = _rng(config, "trade_bootstrap")
    count = len(pnls)
    finals: list[float] = []
    drawdowns: list[float] = []
    ruins = 0
    for _ in range(config.iterations):
        sample = rng.choices(pnls, k=count)
        final, drawdown, minimum = _additive_path_stats(sample, inputs.opening_balance)
        finals.append(final)
        if drawdown is not None:
            drawdowns.append(drawdown)
        if minimum <= 0:
            ruins += 1
    return _summary(
        method="trade_bootstrap",
        sizing="additive",
        sampling="random",
        iterations=config.iterations,
        finals=finals,
        drawdowns=drawdowns,
        sharpes=None,
        sharpe_reason=(
            "a resampled bag of trade PnLs has no time grid, so an annualised Sharpe "
            "cannot be computed from it"
        ),
        ruins=ruins,
        ruin_reason=None,
        limit=inputs.max_drawdown_limit,
        notes=[],
    )


# ----------------------------------------------------------------------- return paths


def _multiplicative_path_stats(
    returns: Sequence[float], opening: float, periods_per_year: int
) -> tuple[float, float | None, float | None, bool]:
    """`(final, max_drawdown, sharpe, ruined)` of `opening * prod(1 + r)`.

    **A compounded path can reach zero, and this is where.** A return at or below -100%
    floors the factor at zero and the account is gone; letting the factor go negative
    would flip the sign of every later sample, which is not a balance any account can
    hold. Such returns are not hypothetical -- `build_grid` emits one whenever a run's
    equity crosses zero within a grid step, before its truncation rule stops the series on
    the *next* boundary -- so `ruined` is returned rather than the whole method claiming
    ruin is unreachable.

    **The Sharpe stops at ruin.** Once the account is zero every later factor multiplies
    zero, so the remaining "returns" describe a balance that no longer exists; annualising
    over them dilutes the collapse with periods that could never have been traded. The
    ratio is computed over the returns up to and including the one that killed the path,
    which is the same rule `build_grid` applies when it truncates at non-positive equity.
    """
    equity = opening
    peak = opening
    worst = 0.0
    ruined = False
    survived = 0
    for r in returns:
        survived += 1
        equity *= max(0.0, 1.0 + r)
        if equity > peak:
            peak = equity
        if peak > 0:
            drawdown = equity / peak - 1.0
            if drawdown < worst:
                worst = drawdown
        if equity <= 0.0:
            ruined = True
            break
    scored = returns[:survived] if ruined else returns
    return equity, worst, sharpe_ratio(scored, periods_per_year), ruined


def _block_bootstrap(
    inputs: MonteCarloInputs, config: MonteCarloConfig
) -> dict[str, Any]:
    returns = inputs.grid_returns
    n = len(returns)
    if n < 4:
        return _too_few(
            "block_bootstrap",
            "multiplicative",
            "random",
            f"{n} grid returns; need at least 4 for blocks to mean anything",
        )
    block = config.block_length or max(1, round(math.sqrt(n)))
    block = min(block, n)
    rng = _rng(config, "block_bootstrap")
    finals: list[float] = []
    drawdowns: list[float] = []
    sharpes: list[float] = []
    ruins = 0
    last_start = n - block
    for _ in range(config.iterations):
        sample: list[float] = []
        while len(sample) < n:
            start = rng.randint(0, last_start)
            sample.extend(returns[start : start + block])
        del sample[n:]
        final, drawdown, sharpe, ruined = _multiplicative_path_stats(
            sample, inputs.opening_balance, inputs.periods_per_year
        )
        finals.append(final)
        if drawdown is not None:
            drawdowns.append(drawdown)
        if sharpe is not None:
            sharpes.append(sharpe)
        ruins += ruined
    return _summary(
        method="block_bootstrap",
        sizing="multiplicative",
        sampling="random",
        iterations=config.iterations,
        finals=finals,
        drawdowns=drawdowns,
        sharpes=sharpes,
        sharpe_reason=None,
        ruins=ruins,
        ruin_reason=None,
        limit=inputs.max_drawdown_limit,
        notes=[
            f"moving blocks of {block} {inputs.grid_label} returns",
            # Disclosed rather than left for a reader to deduce from a count: the last
            # return is reachable from one block start and an interior one from `block`,
            # so the endpoints are under-represented in every resample.
            "non-circular blocks, so the first and last returns appear in fewer "
            "resamples than interior ones",
        ],
        block_length=block,
    )


def _random_start(inputs: MonteCarloInputs, config: MonteCarloConfig) -> dict[str, Any]:
    """Every start in the window, once each -- exact, not sampled.

    The question is "how dependent is the result on when I started", and with at most a
    few hundred candidate starts the full set is cheaper than 10,000 random draws of it.
    Enumerating kills the sampling noise a reader would otherwise have to mentally
    subtract, and makes the per-start series plottable as a curve rather than a cloud.
    """
    returns = inputs.grid_returns
    n = len(returns)
    if n < 4:
        return _too_few(
            "random_start",
            "multiplicative",
            "exhaustive",
            f"{n} grid returns; need at least 4",
        )
    max_skip = max(1, math.floor(n * config.max_skip_fraction))
    finals: list[float] = []
    drawdowns: list[float] = []
    sharpes: list[float] = []
    ruins = 0
    series: list[dict[str, Any]] = []
    for skip in range(0, max_skip + 1):
        final, drawdown, sharpe, ruined = _multiplicative_path_stats(
            returns[skip:], inputs.opening_balance, inputs.periods_per_year
        )
        finals.append(final)
        if drawdown is not None:
            drawdowns.append(drawdown)
        if sharpe is not None:
            sharpes.append(sharpe)
        ruins += ruined
        series.append(
            {
                "skipped": skip,
                "final_equity": final,
                "max_drawdown": drawdown,
                "sharpe": sharpe,
                "ruined": ruined,
            }
        )
    result = _summary(
        method="random_start",
        sizing="multiplicative",
        sampling="exhaustive",
        iterations=len(series),
        finals=finals,
        drawdowns=drawdowns,
        sharpes=sharpes,
        sharpe_reason=None,
        ruins=ruins,
        ruin_reason=None,
        limit=inputs.max_drawdown_limit,
        notes=[
            f"every start from 0 to {max_skip} skipped {inputs.grid_label} returns, "
            "evaluated once each -- exact, not sampled"
        ],
    )
    result["series"] = series
    return result


# ------------------------------------------------------------------------- summaries


def _percentile(sorted_values: Sequence[float], q: float) -> float:
    """Linear interpolation between order statistics (the numpy `linear` convention).

    Stated because percentile conventions differ enough to move a 5th percentile
    visibly on small samples, and this artefact will be compared against other tools.
    """
    if not sorted_values:
        raise ValueError("no values")
    if len(sorted_values) == 1:
        return sorted_values[0]
    rank = (len(sorted_values) - 1) * (q / 100.0)
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return sorted_values[low]
    weight = rank - low
    return sorted_values[low] * (1.0 - weight) + sorted_values[high] * weight


def _distribution(values: Sequence[float]) -> dict[str, Any] | None:
    if not values:
        return None
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "mean": sum(ordered) / len(ordered),
        "median": float(median(ordered)),
        "min": ordered[0],
        "max": ordered[-1],
        "percentiles": {str(q): _percentile(ordered, q) for q in PERCENTILES},
    }


def _summary(
    *,
    method: str,
    sizing: str,
    sampling: str,
    iterations: int,
    finals: Sequence[float],
    drawdowns: Sequence[float],
    sharpes: Sequence[float] | None,
    sharpe_reason: str | None,
    ruins: int | None,
    ruin_reason: str | None,
    limit: float | None,
    notes: list[str],
    block_length: int | None = None,
) -> dict[str, Any]:
    breaches = None
    if limit is not None and drawdowns:
        # Spec 7's convention: a drawdown limit is *reached*, not exceeded, so equality
        # breaches. Drawdowns are negative fractions, the limit a positive one.
        breaches = sum(1 for d in drawdowns if -d >= limit) / len(drawdowns)
    # **The three distributions can cover different subsets of the iterations**, and that
    # has to be said rather than left in a `count` field for the reader to notice. A path
    # whose returns have zero dispersion has no Sharpe (`sharpe_ratio` refuses it), which
    # on an hourly grid full of exactly-zero returns is a systematic exclusion of the
    # quietest resamples -- precisely the ones that drag a Sharpe distribution down.
    sharpe_dropped = (
        iterations - len(sharpes) if sharpes is not None and iterations else 0
    )
    payload: dict[str, Any] = {
        "method": method,
        "sizing": sizing,
        # "random" or "exhaustive", machine-readable on purpose: `random_start`'s
        # `iterations` is a candidate count (every start, once each), not a draw count,
        # and it sits in one artefact beside methods that really did draw
        # `config.iterations` paths. The distinction lived only in prose notes, and a
        # consumer must never have to parse prose to avoid mistaking an exact
        # enumeration for a Monte Carlo sample.
        "sampling": sampling,
        "iterations": iterations,
        "final_equity": _distribution(finals),
        "max_drawdown": _distribution(drawdowns),
        "sharpe": _distribution(sharpes) if sharpes is not None else None,
        "sharpe_undefined_iterations": max(0, sharpe_dropped),
        "sharpe_unavailable_reason": sharpe_reason,
        "prob_drawdown_breach": breaches,
        "drawdown_limit": limit,
        "prob_ruin": None if ruins is None else ruins / iterations,
        "ruin_unavailable_reason": ruin_reason,
        "notes": notes,
        "error": None,
    }
    if block_length is not None:
        payload["block_length"] = block_length
    return payload


def _too_few(method: str, sizing: str, sampling: str, reason: str) -> dict[str, Any]:
    """A method that cannot run reports why, in the same shape as one that did.

    Silently omitting it from the artefact would make "the panel does not show trade
    bootstrap" indistinguishable from "nobody asked for it".
    """
    return {
        "method": method,
        "sizing": sizing,
        "sampling": sampling,
        "iterations": 0,
        "final_equity": None,
        "max_drawdown": None,
        "sharpe": None,
        "sharpe_undefined_iterations": 0,
        "sharpe_unavailable_reason": None,
        "prob_drawdown_breach": None,
        "drawdown_limit": None,
        "prob_ruin": None,
        "ruin_unavailable_reason": None,
        "notes": [],
        "error": f"not run: {reason}",
    }

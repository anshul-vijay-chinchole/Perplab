"""The shadow-backtest parity report (spec 6.7).

Spec 6.7 opens with the sentence this module exists to serve: *"Architecture alone does not
prevent divergence -- it has to be measured."* A shared core makes the backtester and the
live engine execute the same accounting; it does not make the *fill model* right, and there
is nothing inside either run that can tell you whether it is. The only evidence that exists
is a paper session and a backtest re-run over the same window, compared fill by fill.

Without that comparison the failure is silent and it compounds. The backtester keeps pricing
fills at levels the exchange would never have given, every strategy is then selected on those
prices, and the first honest measurement of the fill model arrives as a live loss on the
strategy that looked best. Spec 6.7.2's feedback loop -- *"persistent divergence means the
fill model needs recalibration"* -- has to close on a number, and this module produces it.

## The four quantities (spec 6.7.1)

1. **Fill-count delta** -- how many fills each run produced.
2. **Average fill-price delta, in basis points** -- over fills that occurred in *both* runs,
   signed against the trader (see below).
3. **Final-PnL delta** -- from each run's `metrics.json`.
4. **Orders that filled in one and not the other**, listed rather than counted, because a
   fill the backtest invented and a fill the session got and the backtest missed are
   different diseases and the list is the only place they can be told apart.

Quantity 4 is the one that is easy to lose. A fill with no counterpart has no price to
compare against, so it contributes to no average, and folding it in as "zero deviation"
would let a run whose orders half missed report perfect fill parity. It is carried out
whole instead.

## What makes two fills the same fill

**The order the engine allocated, and which increment of that order this is.**
`BacktestEngine` mints an id at submission (`o1`, `o2`, ...) and emits one `FILL` per
increment, so `(order_id, n-th fill of that order)` names a fill exactly -- and both runs
mint from the same counter, driven by the same strategy code, the same seed and the same
tape. A fill whose identity has no counterpart in the other run is *unmatched*: it goes out
whole in quantity 4 rather than being paired with something that merely resembles it.

Resembling is not enough, and this module learned that the expensive way. The rule here used
to pair fills greedily on `(symbol, side, quantity)` inside a two-second window, and a
strategy scaling in with two equal clips 1.5 s apart is enough to break it. With the shadow
missing the first clip, the first paper fill consumed the *second* clip's shadow fill and the
report read **50 bps average deviation, diverged, against a fill model that was exactly
right** -- naming the wrong fill as the missing one, so the real cause was hidden too. The
same collision the other way round reported **0.00 bps and `diverged=False` on a shadow that
filled 909 bps better than the session did**. Both are the wrong-number-that-looks-right this
codebase treats as its worst outcome, and the second is the direction that flatters a
strategy into being traded.

Identity is corroborated by `(symbol, side, tag)` before it is believed. Two runs whose order
streams have genuinely parted company will file different orders under the same id, and those
three fields are what the engine already records about what an order *was*. A disagreement
there sends both fills to the unmatched lists, which is the honest reading: if the ids no
longer line up, the fills under them are not the same fills.

**What corroboration cannot catch is a uniform slip.** If the shadow never submitted an order
the session did, every later id is off by one and the fills under them still agree on symbol,
side and tag -- so a scale-in's clips would be compared against each other again, one place
along. The report is loud rather than quiet in that state (the deviations are large and the
tail of the run is unmatched), and it was no better under the old rule, but it is the residual
and it is not closable from here: it would take the engine recording something
order-distinguishing on the fill itself -- the strategy's `client_id`, or the instant the
order was submitted -- and `Fill.to_json` carries neither today.

Quantity is deliberately *not* corroborated. A limit order that fills in one increment in one
run and two in the other is the fill model differing, which is the quantity being measured;
requiring the sizes to agree would drop exactly those fills out of the average and report
nothing about them.

The window survives for the one thing identity cannot cover: a `FILL` that carries no
`order_id` at all, which the engine does not write but a log from any other producer might.
There, and only there, two fills pair on `(symbol, side, quantity)` inside
`FILL_MATCH_WINDOW_MS` -- and only when that pairing is the sole possibility on *both* sides.
An ambiguous pair is unmatched, because an ambiguous pair is precisely what produced both
figures above.

## Direction

Every delta is **shadow minus paper**: the paper session is what happened, the shadow
backtest is the model of it, so a positive delta is the model overstating. Stated here
because a sign error in a divergence report is exactly the wrong-number-that-looks-right
this codebase treats as its worst outcome.

The fill deviation carries one more sign, and it is the convention `executor_base` already
applies to slippage: the difference is **signed against the trader**, so a shadow buy filling
above the session's price and a shadow sell filling below it are both positive. A plain
price difference would make those two cancel in the mean, and a fill model that is five basis
points optimistic on every buy and five pessimistic on every sell would average to zero and
pass. With this sign a *negative* average is the reading that matters: the backtest filled
better than the session did, which is the direction that flatters a strategy into being
traded.

## Gross PnL, which did not exist before this module

Spec 6.7.2's PnL threshold is a fraction *"of gross PnL"*, and nothing in the codebase
computes one: `analytics.metrics.trade_stats` builds `gross_win` and `gross_loss` as locals
and stores neither. So it is defined here, and this is the definition:

> **Gross PnL is the sum of the absolute realised PnL of every round-trip in the paper
> session's `trades.json`.**

Three parts of that are choices rather than restatements:

- *Absolute, per round-trip.* A session that made 300 on one trade and lost 200 on the next
  has a gross of 500 and a net of 100. Scaling the threshold by the net would make a
  session that traded all day and finished flat impossible to compare at all.
- *Realised, not net.* A round-trip's `net_pnl` also carries fees and funding, and neither
  is an execution difference: both runs pay the same schedule over the same window, so
  putting them in the denominator would let a strategy with heavy funding absorb a real
  fill-model divergence in its own funding bill.
- *The paper session's trades, not the shadow's.* The session is the ground truth being
  reproduced. Taking the larger of the two, or their mean, would let a shadow run that went
  haywire and traded ten times as much inflate the denominator that is supposed to catch it.

Gross can legitimately be zero -- a session that opened a position and was still holding it
when the window closed has realised nothing -- and the fraction is then undefined rather
than infinite. That case is handled explicitly: see `ParityReport.pnl_delta_fraction`.

## Exactness

**PnL is read from `metrics.json` -> `attribution.net_pnl` and parsed with
`money.parse_money`.** It is an exact decimal string there, and the two alternatives on disk
are both lossy: `Metrics` is `float64` by design (spec 3.1 puts analytics in float), and
`equity.parquet` is cast to `pa.float64()` by the worker. The threshold is a *comparison*,
so a figure that is out by one part in 10^16 can decide the verdict on its own when the
delta lands near five percent of gross -- and a divergence report that flips on a rounding
is worse than no report, because it will be believed.

No monetary quantity in this module is ever a `float`, and there is no conversion to one
anywhere in it. The basis-point figures are ratios rather than amounts, so they would have
been permissible as floats; they are exact `Decimal`s anyway, because spec 6.7.2's 3 bps
threshold is decided on them and the argument above applies one layer down unchanged.

## What sets the flag

Spec 6.7.2 names two conditions and this module implements those two and no others: the
final-PnL gap exceeding `PNL_DIVERGENCE_FRACTION` of gross PnL, or the average fill
deviation exceeding `FILL_DEVIATION_BPS_LIMIT`. Unmatched fills are reported in full and do
not raise the flag by themselves. That is deliberate: they reach the verdict through the PnL
delta they caused, and adding a third gate here would flag runs on a rule nobody wrote and
that no operator could tune. Both thresholds are declared in this module and nowhere else.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from perplab.core.money import (
    Money,
    accounting,
    money_to_str,
    parse_money,
    quantize_money,
)

__all__ = [
    "PNL_DIVERGENCE_FRACTION",
    "FILL_DEVIATION_BPS_LIMIT",
    "FILL_MATCH_WINDOW_MS",
    "ParityInputMissing",
    "ParityInputMalformed",
    "FillRecord",
    "ParityReport",
    "build_parity",
]

PNL_DIVERGENCE_FRACTION = parse_money("0.05")
"""Spec 6.7.2's *"5% of gross PnL"*, and this module is its only source of truth.

Exceeded strictly: spec 6.7.2 says the flag is raised when PnL differs by *more than* the
threshold, so a delta of exactly five percent of gross is inside it. This is the same
reading `core.risk` applies to every ceiling on a value.
"""

FILL_DEVIATION_BPS_LIMIT = parse_money("3")
"""Spec 6.7.2's *"3 bps average fill deviation"*, exceeded strictly for the same reason."""

FILL_MATCH_WINDOW_MS = 2_000
"""How far apart two fills may be and still be the same fill **when neither carries an id**.

This is the fallback rule, not the rule: fills the engine wrote are matched on the order
identity it stamped on them, and that comparison ignores time entirely because a replayed
order arriving late is still the same order. See the module docstring.

Two seconds, against the thing that actually separates two recordings of one decision:
latency. Spec 6.3's own p99 is 600 ms, so the same decision can reach the tape a second apart
without anything being wrong. Wider than that and a strategy re-entering the same size a few
seconds later starts competing with its own previous fill -- which is why a pairing inside
this window is only taken when it is the sole candidate on both sides.
"""

_BPS = parse_money("10000")
_ZERO = parse_money("0")

_SIDE_DIRECTION = {"BUY": 1, "SELL": -1}
"""Which way "worse" points for each side -- a buy pays, a sell receives.

The same expression `executor_base` signs slippage with, and the reason a fill deviation is
averaged at all rather than being reported as two columns.
"""


class ParityInputMissing(FileNotFoundError):
    """A run directory is missing an artefact the report is built from."""


class ParityInputMalformed(ValueError):
    """An artefact exists but does not hold what a finished run writes.

    Separate from `ParityInputMissing` because the two call for different actions: a missing
    file usually means the run has not finished, and a malformed one means the file on disk
    was not written by this worker.
    """


@dataclass(frozen=True, slots=True)
class FillRecord:
    """One `FILL` event, reduced to the fields parity needs.

    Deliberately not the whole payload. The report is written to `parity.json` and read by
    someone deciding whether to trust a fill model; carrying every field of every unmatched
    fill would bury the four numbers the page is for.
    """

    ts_ms: int
    seq: int
    """Sequence within its own log -- the tie-break for reporting order, so the unmatched
    lists cannot depend on how fast the machine ran (the same argument spec 6.2 makes)."""
    symbol: str
    side: str
    qty: Money
    price: Money
    order_id: str
    fill_no: int
    """Which increment of `order_id` this is, counted from 0 within this log alone.

    An order fills per increment (`Fill`'s own docstring: *"a limit order that fills in four
    pieces calls `on_fill` four times"*), so the id alone names up to four fills. Counted
    while streaming rather than read from the payload because nothing in the log carries it.
    """
    tag: str | None

    @property
    def identity(self) -> tuple[str, int] | None:
        """The engine's own name for this fill, or `None` if the log did not record one.

        `None` is the only route to the time-window fallback, and it is reachable only for a
        `FILL` with no `order_id` -- which this engine always writes. A log that has one is
        matched on it and never on resemblance.
        """
        return None if not self.order_id else (self.order_id, self.fill_no)

    @property
    def order_key(self) -> tuple[str, str, str | None]:
        """What must also agree before two fills sharing an identity are believed to be one.

        Not quantity: two runs filling the same order in different sized pieces is the fill
        model differing, which is the thing being measured.
        """
        return (self.symbol, self.side, self.tag)

    @property
    def match_key(self) -> tuple[str, str, Money]:
        """What makes two *identity-less* fills candidates for being the same fill.

        Used by the fallback matcher alone. Quantity is in it here precisely because there is
        no id to lean on, and a candidate is still only taken when it is unambiguous.
        """
        return (self.symbol, self.side, self.qty)

    def to_json(self) -> dict[str, Any]:
        return {
            "ts_ms": self.ts_ms,
            "seq": self.seq,
            "symbol": self.symbol,
            "side": self.side,
            "qty": money_to_str(self.qty),
            "price": money_to_str(self.price),
            "order_id": self.order_id,
            "fill_no": self.fill_no,
            "tag": self.tag,
        }


@dataclass(frozen=True, slots=True)
class ParityReport:
    """Spec 6.7.1's report, plus spec 6.7.2's verdict. Written by the caller to `parity.json`.

    Every monetary field is an exact `Decimal`; the basis-point fields are exact too, and
    both are rendered as plain decimal strings by `to_json` so that reading the artefact back
    cannot introduce the float this module refuses to use.
    """

    paper_fills: int
    shadow_fills: int
    fill_count_delta: int
    """Spec 6.7.1 quantity 1, shadow minus paper.

    Zero does not mean the fills agreed: a run that missed one fill and invented another
    reports zero here, which is why quantity 4 is a list and not a count.
    """
    matched_fills: int
    """Fills that occurred in both runs -- the population the bps averages are taken over."""

    avg_fill_delta_bps: Money | None
    """Spec 6.7.1 quantity 2: mean deviation in basis points of the paper fill's own price.

    Signed against the trader, so positive means the shadow backtest filled *worse* than the
    session did on average and negative means it filled better -- the flattering direction,
    and the one worth acting on. See the module docstring for why a plain price difference
    would be the wrong sign to average.

    `None` when nothing matched, never zero. Zero is a claim that the fills agreed, which is
    the opposite of what "there were no comparable fills" means, and it would read as passing
    the spec 6.7.2 threshold rather than as having had nothing to test.
    """
    avg_abs_fill_delta_bps: Money | None
    """Mean *absolute* deviation over the same matched fills.

    Reported because even a trader-signed mean can cancel: a model that is 10 bps optimistic
    on half its fills and 10 bps pessimistic on the other half averages to zero while being
    wrong on every one of them. It does not enter the verdict -- spec 6.7.2 states one fill
    threshold and this is not it -- but a report showing 0 bps signed and 10 bps absolute is
    saying something the signed figure alone cannot.
    """

    paper_net_pnl: Money
    shadow_net_pnl: Money
    net_pnl_delta: Money
    """Spec 6.7.1 quantity 3, shadow minus paper, from `attribution.net_pnl` on both sides."""
    gross_pnl: Money
    """The paper session's gross -- see the module docstring for the definition."""
    pnl_delta_fraction: Money | None
    """`net_pnl_delta / gross_pnl`, or `None` when the session realised nothing.

    `None` rather than zero or infinity. A session holding one position all the way to the
    end has a gross of zero and no scale to measure a difference against; reporting `0`
    would say the two runs agreed and reporting a large number would say they disagreed by
    a specific amount. Neither is known. The verdict handles the case separately -- with a
    gross of zero, *any* PnL difference is flagged, because there is no size of position
    that would make it small.
    """

    paper_only: tuple[FillRecord, ...]
    """Spec 6.7.1 quantity 4: fills the session got that the backtest never produced."""
    shadow_only: tuple[FillRecord, ...]
    """Fills the backtest produced that the session never got."""

    diverged: bool
    """Spec 6.7.2's flag, for the Runs tab."""
    reasons: tuple[str, ...]
    """Why it was flagged, in the numbers that flagged it. Empty when it was not.

    A boolean on its own sends the reader back to recompute the comparison by hand to find
    out which threshold moved, and the two call for different work: a PnL breach with clean
    fills is an accounting difference, and a bps breach is the fill model.
    """

    def to_json(self) -> dict[str, Any]:
        """The `parity.json` payload.

        The thresholds travel with the verdict. A stored report whose flag cannot be
        explained without knowing which constants were in force when it was written is a
        number nobody can re-derive, and these two are exactly the values that will be tuned
        as spec 6.7.2's feedback loop runs.
        """
        return {
            "fills": {
                "paper": self.paper_fills,
                "shadow": self.shadow_fills,
                "delta": self.fill_count_delta,
                "matched": self.matched_fills,
                "avg_delta_bps": _bps_json(self.avg_fill_delta_bps),
                "avg_abs_delta_bps": _bps_json(self.avg_abs_fill_delta_bps),
                "paper_only": [fill.to_json() for fill in self.paper_only],
                "shadow_only": [fill.to_json() for fill in self.shadow_only],
            },
            "pnl": {
                "paper_net": money_to_str(self.paper_net_pnl),
                "shadow_net": money_to_str(self.shadow_net_pnl),
                "delta": money_to_str(self.net_pnl_delta),
                "gross": money_to_str(self.gross_pnl),
                "delta_fraction": (
                    None
                    if self.pnl_delta_fraction is None
                    else money_to_str(quantize_money(self.pnl_delta_fraction))
                ),
            },
            "diverged": self.diverged,
            "reasons": list(self.reasons),
            "thresholds": {
                "pnl_fraction_of_gross": money_to_str(PNL_DIVERGENCE_FRACTION),
                "avg_fill_delta_bps": money_to_str(FILL_DEVIATION_BPS_LIMIT),
                "match_window_ms": FILL_MATCH_WINDOW_MS,
            },
        }


def build_parity(paper_dir: Path, shadow_dir: Path) -> ParityReport:
    """Compare a paper session against the backtest re-run over its own window (spec 6.7.1).

    Both arguments are run directories: the artefacts read are `metrics.json` (for
    `attribution.net_pnl`), `trades.json` (for the paper session's gross PnL) and
    `events.jsonl` (for the fills). The event logs are **read line by line and never whole**;
    a 48-hour session's log runs to hundreds of megabytes and the fills are a thin slice of
    it, so what is retained is the fills, not the log.

    Fills are paired on the identity the engine stamped on them -- `(order_id, increment)`,
    corroborated by symbol, side and tag -- and a fill with no counterpart is reported
    unmatched rather than paired with a resemblance. The module docstring gives the two
    figures the resemblance rule produced. Matching on identity ignores time, so a replayed
    order that arrived a minute late is still the same order.

    Raises `ParityInputMissing` if either directory is not a finished run, and
    `ParityInputMalformed` if an artefact is present but does not hold what the worker
    writes -- notably a `net_pnl` that is a JSON number rather than an exact decimal string.
    """
    paper_net = _net_pnl(paper_dir)
    shadow_net = _net_pnl(shadow_dir)
    gross = _gross_pnl(paper_dir)

    fills = _compare_fills(
        _iter_fills(paper_dir / "events.jsonl"),
        _iter_fills(shadow_dir / "events.jsonl"),
    )

    with accounting():
        net_delta = shadow_net - paper_net
        avg_bps = (
            None if fills.matched == 0 else fills.bps_sum / fills.matched
        )
        avg_abs_bps = (
            None if fills.matched == 0 else fills.abs_bps_sum / fills.matched
        )
        fraction = None if gross == _ZERO else net_delta / gross
        reasons = _verdict(net_delta, gross, avg_bps)

    return ParityReport(
        paper_fills=fills.paper_count,
        shadow_fills=fills.shadow_count,
        fill_count_delta=fills.shadow_count - fills.paper_count,
        matched_fills=fills.matched,
        avg_fill_delta_bps=avg_bps,
        avg_abs_fill_delta_bps=avg_abs_bps,
        paper_net_pnl=paper_net,
        shadow_net_pnl=shadow_net,
        net_pnl_delta=net_delta,
        gross_pnl=gross,
        pnl_delta_fraction=fraction,
        paper_only=tuple(fills.paper_only),
        shadow_only=tuple(fills.shadow_only),
        diverged=bool(reasons),
        reasons=reasons,
    )


# ---------------------------------------------------------------------------- the verdict


def _verdict(
    net_delta: Money, gross: Money, avg_bps: Money | None
) -> tuple[str, ...]:
    """Spec 6.7.2's two conditions, each stated in the numbers that decided it.

    Runs inside `accounting()`: the allowance is a *product* of the threshold and the gross,
    and it is compared against a delta that can carry twenty significant digits, so it is
    computed at the accounting precision like every other figure that decides something.

    **Every number in a reason is a magnitude, and the direction is in words.** These
    sentences used to print the signed delta next to the allowance it breached -- "differs by
    -30.00000000, which is more than 25.00000000" -- which is a true statement about
    magnitudes wearing the form of a false one about numbers, on every second flagged run.
    The rounded corner is the same defect one step further along: an exact delta a hair over
    its allowance rendered at eight decimals as "5.00000000 ... more than 5.00000000". A
    reader who checks a reason against its own figures and finds them not to say what it says
    has been given a reason to distrust a verdict that is correct, and this verdict is the
    whole output of the module. So the compared pair is always two magnitudes
    (`_shown_breach` picks a precision at which the breach is still visible) and the sign --
    which is half of what a delta means here -- is carried as English.
    """
    reasons: list[str] = []
    if gross > _ZERO:
        allowed = PNL_DIVERGENCE_FRACTION * gross
        if abs(net_delta) > allowed:
            gap, allowance = _shown_breach(abs(net_delta), allowed)
            reasons.append(
                f"final PnL differs by {gap} -- the shadow finished "
                f"{_higher_or_lower(net_delta)} than the session -- more than the "
                f"{allowance} spec 6.7.2 allows, which is "
                f"{money_to_str(PNL_DIVERGENCE_FRACTION)} of the session's gross PnL "
                f"{money_to_str(quantize_money(gross))}"
            )
    elif net_delta != _ZERO:
        # No gross to scale against, and a difference all the same. Flagged rather than
        # skipped: the fraction is undefined, but "the two runs disagree about money on a
        # session that realised none" is not a borderline case that a threshold would have
        # let through -- it is the clearest divergence this report can see.
        gap, _ = _shown_breach(abs(net_delta), _ZERO)
        reasons.append(
            f"final PnL differs by {gap} -- the shadow finished "
            f"{_higher_or_lower(net_delta)} than the session -- on a session whose "
            "round-trips realised nothing, so there is no gross PnL to scale the difference "
            "against and no size at which it would be small"
        )
    if avg_bps is not None and abs(avg_bps) > FILL_DEVIATION_BPS_LIMIT:
        gap, allowance = _shown_breach(abs(avg_bps), FILL_DEVIATION_BPS_LIMIT)
        filled = "better" if avg_bps < _ZERO else "worse"
        reasons.append(
            f"fills deviate by {gap} bps on average -- the shadow filled {filled} than the "
            f"session did -- more than the {allowance} bps spec 6.7.2 allows"
        )
    return tuple(reasons)


def _higher_or_lower(delta: Money) -> str:
    """Which way a shadow-minus-paper delta points, said rather than signed."""
    return "lower" if delta < _ZERO else "higher"


def _shown_breach(value: Money, limit: Money) -> tuple[str, str]:
    """Render a figure and the limit it exceeded, in a precision that still shows the breach.

    The verdict is decided on unrounded figures and the house rendering is eight decimals, so
    a delta 1e-10 over its allowance rounded to a string identical to the allowance's and the
    reason said "5.00000000 ... more than 5.00000000". Rounding is monotone, so the sentence
    could never read backwards, but a reader who checks it and finds two equal numbers has
    been given a reason to doubt a verdict that is correct. When the rounded pair no longer
    demonstrates the breach, **both** figures fall back to exact -- both, because rendering
    one exactly against a rounded limit can invert the pair rather than merely flatten it.
    """
    rounded_value = quantize_money(value)
    rounded_limit = quantize_money(limit)
    if rounded_value > rounded_limit:
        return money_to_str(rounded_value), money_to_str(rounded_limit)
    return money_to_str(value), money_to_str(limit)


# ------------------------------------------------------------------------------- the money


def _net_pnl(directory: Path) -> Money:
    """`attribution.net_pnl` from one run's `metrics.json`, exact.

    Refuses a JSON number outright. By the time `json.load` has returned one the digits are
    already gone -- it is a `float` -- and parsing its repr back into a `Decimal` would
    produce an exact-looking figure that is not the one the run computed. That is the
    failure this module is least able to survive, because the threshold it feeds is a
    comparison.
    """
    payload = _read_json_object(directory / "metrics.json")
    attribution = payload.get("attribution")
    if not isinstance(attribution, dict) or "net_pnl" not in attribution:
        raise ParityInputMalformed(
            f"{directory / 'metrics.json'} has no attribution.net_pnl. A parity report "
            "compares the spec 8.4 net PnL of two finished runs; re-run the worker for "
            "this run, or point the report at the directory that has its artefacts."
        )
    value = attribution["net_pnl"]
    if not isinstance(value, str):
        raise ParityInputMalformed(
            f"attribution.net_pnl in {directory / 'metrics.json'} is {value!r}, a JSON "
            "number rather than the exact decimal string the worker writes. json.load has "
            "already rounded it to a float, so the exact figure cannot be recovered from "
            "this file -- regenerate it rather than comparing against a rounded PnL."
        )
    try:
        return parse_money(value)
    except ValueError as exc:
        raise ParityInputMalformed(
            f"attribution.net_pnl in {directory / 'metrics.json'} is not a decimal number: "
            f"{exc}"
        ) from None


def _gross_pnl(directory: Path) -> Money:
    """Sum of `|realized_pnl|` over the round-trips in one run's `trades.json`.

    The definition and the three choices inside it are set out in the module docstring. A
    still-open round-trip contributes whatever its scale-outs have already realised, which
    is the honest reading of "realised" and keeps the denominator non-zero for a session
    that traded all day and happened to end holding something.
    """
    payload = _read_json_object(directory / "trades.json")
    records = payload.get("trades")
    if not isinstance(records, list):
        raise ParityInputMalformed(
            f"{directory / 'trades.json'} has no trades array, so the session's gross PnL "
            "cannot be computed and spec 6.7.2's threshold has no denominator. Re-run the "
            "worker for this run."
        )
    with accounting():
        total = _ZERO
        for index, record in enumerate(records):
            realized = record.get("realized_pnl") if isinstance(record, dict) else None
            if not isinstance(realized, str):
                raise ParityInputMalformed(
                    f"trade {index} in {directory / 'trades.json'} has realized_pnl "
                    f"{realized!r}, not the exact decimal string `Trade.to_json` writes. "
                    "Gross PnL is the denominator of a threshold, so it is not computed "
                    "from a rounded figure."
                )
            total += abs(parse_money(realized))
    return total


# ------------------------------------------------------------------------------- the fills


@dataclass(slots=True)
class _FillComparison:
    """Running state of one pass over the two fill streams."""

    paper_count: int = 0
    shadow_count: int = 0
    matched: int = 0
    bps_sum: Money = _ZERO
    abs_bps_sum: Money = _ZERO
    paper_only: list[FillRecord] = field(default_factory=list)
    shadow_only: list[FillRecord] = field(default_factory=list)


def _compare_fills(
    paper: Iterator[FillRecord], shadow: Iterator[FillRecord]
) -> _FillComparison:
    """Pair the two fill streams on the engine's own identity for a fill.

    The shadow log's fills are indexed by `(order_id, increment)` first and the paper log is
    then streamed against that index. **This holds the shadow run's fills, and that is a
    deliberate trade against the streaming discipline the rest of this module keeps.** The
    alternative -- the one-pass window this replaced -- bounded memory at two seconds of
    fills and paid for it by pairing a scale-in's second clip with its first, which reported
    50 bps of divergence against a fill model that was exactly right and 0.00 bps against one
    that was 909 bps optimistic (module docstring). The log itself is still never loaded:
    `_iter_fills` reads it line by line and keeps only the `FILL`s, which are a thin slice of
    it, and the unmatched lists this report exists to publish were already unbounded in
    exactly the same way.

    Identity carries no time component on purpose. Two recordings of one order can land a
    second apart -- that is what the latency model is -- and they are still one order.
    """
    state = _FillComparison()
    by_identity: dict[tuple[str, int], FillRecord] = {}
    shadow_unnamed: list[FillRecord] = []
    for record in shadow:
        state.shadow_count += 1
        identity = record.identity
        if identity is None:
            shadow_unnamed.append(record)
        elif identity in by_identity:
            # Unique by construction from *this* engine -- `fill_no` counts increments of
            # one id within one log -- but this function reads a file, and a duplicated
            # identity used to be silently overwritten here: the earlier record vanished
            # from every population, so `shadow_count` disagreed with
            # `matched + len(shadow_only) + unnamed` and the average bps was computed over
            # fewer fills than the counts beside it claimed. A duplicate cannot be paired
            # -- which of the two the paper fill matches is exactly what is ambiguous --
            # so, like every ambiguity in this module, it goes to the unmatched list
            # rather than being resolved by a guess. First occurrence keeps the identity.
            state.shadow_only.append(record)
        else:
            by_identity[identity] = record

    matched_identities: set[tuple[str, int]] = set()
    paper_unnamed: list[FillRecord] = []
    for record in paper:
        state.paper_count += 1
        identity = record.identity
        if identity is None:
            paper_unnamed.append(record)
            continue
        partner = by_identity.get(identity)
        if partner is None or partner.order_key != record.order_key:
            # Either the shadow never produced this fill, or it produced a different order
            # under the same id -- which means the two runs' order streams have parted
            # company and the id no longer names anything shared. Unmatched either way: a
            # deviation measured between two different orders is a number about nothing.
            state.paper_only.append(record)
            continue
        matched_identities.add(identity)
        _score(state, record, partner)

    _match_unnamed(state, paper_unnamed, shadow_unnamed)
    state.shadow_only.extend(
        record
        for identity, record in by_identity.items()
        if identity not in matched_identities
    )
    # Sorted rather than appended in order: two matchers contribute to these lists now, and
    # a report whose unmatched fills read out of sequence invites the reader to reconstruct
    # the run's chronology by hand. `(ts_ms, seq)` admits no ties within one log.
    state.paper_only.sort(key=_chronological)
    state.shadow_only.sort(key=_chronological)
    return state


def _chronological(record: FillRecord) -> tuple[int, int]:
    return (record.ts_ms, record.seq)


def _score(state: _FillComparison, paper: FillRecord, shadow: FillRecord) -> None:
    """Book one matched pair's deviation, signed against the trader.

    Measured against the *paper* price because the session is what happened; see the module
    docstring for why the sign is the one `executor_base` applies to slippage.
    """
    state.matched += 1
    with accounting():
        deviation = (
            (shadow.price - paper.price)
            / paper.price
            * _BPS
            * _SIDE_DIRECTION[paper.side]
        )
        state.bps_sum += deviation
        state.abs_bps_sum += abs(deviation)


def _match_unnamed(
    state: _FillComparison, paper: list[FillRecord], shadow: list[FillRecord]
) -> None:
    """Pair the fills that carry no order id, and only where the pairing is unambiguous.

    Reached only by a `FILL` this engine did not write -- it always records an id -- so both
    lists are empty for every run the platform produces, and the cost of the quadratic-looking
    scan below is bounded by `FILL_MATCH_WINDOW_MS` of fills either way.

    **A candidate is taken only when it is the sole candidate for that paper fill and that
    paper fill is its sole claimant.** Ambiguity is reported unmatched. The rule this replaced
    resolved ambiguity by ranking -- tag, then order id, then proximity -- and ranking always
    produces a winner, so a lone shadow fill that two paper fills could both claim was handed
    to whichever came first in the log. That is how a scale-in with equal clips 1.5 s apart
    reported 50 bps on a correct fill model, and it is the reason this refuses instead of
    choosing: the unmatched lists are spec 6.7.1 quantity 4 and can hold an honest "we could
    not tell", where the average cannot.
    """
    candidates: list[list[int]] = [[] for _ in paper]
    claims = [0] * len(shadow)
    lo = 0
    for index, fill in enumerate(paper):
        # `lo` only ever advances: the paper list runs forwards, so a shadow fill already
        # behind one paper fill's window is behind every later one's too.
        while lo < len(shadow) and shadow[lo].ts_ms < fill.ts_ms - FILL_MATCH_WINDOW_MS:
            lo += 1
        cursor = lo
        horizon = fill.ts_ms + FILL_MATCH_WINDOW_MS
        while cursor < len(shadow) and shadow[cursor].ts_ms <= horizon:
            if shadow[cursor].match_key == fill.match_key:
                candidates[index].append(cursor)
                claims[cursor] += 1
            cursor += 1

    taken: set[int] = set()
    for index, fill in enumerate(paper):
        chosen = candidates[index]
        if len(chosen) == 1 and claims[chosen[0]] == 1:
            taken.add(chosen[0])
            _score(state, fill, shadow[chosen[0]])
        else:
            state.paper_only.append(fill)
    state.shadow_only.extend(
        record for cursor, record in enumerate(shadow) if cursor not in taken
    )


def _iter_fills(path: Path) -> Iterator[FillRecord]:
    """Stream the `FILL` events out of one run's `events.jsonl`.

    Line by line, and never `read_text`: a 48-hour session's log is hundreds of megabytes of
    which the fills are a thin slice, and loading it whole to compare a few thousand prices
    would make the parity report the heaviest thing the platform does.

    The `"FILL"` substring pre-filter skips the parse for most lines. It is matched on the
    bare word rather than on `'"kind":"FILL"'` deliberately: the compact separators the
    worker writes with are a formatting choice, and a pre-filter that depends on them would
    silently drop *every* fill from a log written with any other -- a false negative here
    reports perfect parity on a run that has none. Survivors are parsed and re-checked, which
    is what excludes `NO_QUOTE_FILL` and anything a strategy logged with the word in it.
    """
    if not path.exists():
        raise ParityInputMissing(
            f"{path} does not exist, so there are no fills to compare. A parity report "
            "needs two finished runs; check that this run completed and that its directory "
            "was not removed."
        )
    previous_ts: int | None = None
    increments: dict[str, int] = {}
    with path.open("r", encoding="utf-8") as handle:
        for number, line in enumerate(handle, start=1):
            if "FILL" not in line:
                continue
            try:
                entry = json.loads(line)
            except ValueError as exc:
                raise ParityInputMalformed(
                    f"{path} line {number} is not JSON: {exc}. The log is written to a "
                    "temporary name and renamed, so a torn line means the file was edited "
                    "or the disk lost it -- it is not a run this report can trust."
                ) from None
            if not isinstance(entry, dict) or entry.get("kind") != "FILL":
                continue
            record = _fill_record(entry, path, number, increments)
            if previous_ts is not None and record.ts_ms < previous_ts:
                raise ParityInputMalformed(
                    f"{path} line {number} holds a fill at {record.ts_ms} ms after one at "
                    f"{previous_ts} ms. Fills are compared in one streaming pass over both "
                    "logs, which is only correct while each log runs forwards; a log that "
                    "goes backwards has been reordered and its matching would be arbitrary."
                )
            previous_ts = record.ts_ms
            yield record


def _fill_record(
    entry: dict[str, Any], path: Path, number: int, increments: dict[str, int]
) -> FillRecord:
    """One `FILL` event, reduced to `FillRecord`.

    `increments` is the caller's running count of fills per order id, and it is threaded
    through rather than derived here because it is a property of the *log*, not of the line:
    an order's third increment is the third `FILL` this log wrote for it, and only something
    reading the log in order can say which one that is.

    The timestamp is the payload's own -- the instant the fill happened -- rather than the
    envelope's, which is when the log entry was written. The engine sets them from the same
    clock, so they agree today; taking the fill's own means they can stop agreeing without
    this report quietly matching on the wrong one.

    Quantity and price are quantised to the storage seam's eight decimals, which is the
    precision every price in the lake is held at. That makes the fallback match key
    insensitive to how a figure was rendered without making it insensitive to anything a run
    could really differ by.
    """
    payload = entry.get("payload")
    if not isinstance(payload, dict):
        raise ParityInputMalformed(
            f"{path} line {number} is a FILL event with no payload object; it was not "
            "written by this engine and its price cannot be compared."
        )
    qty = _exact_field(payload, "qty", path, number)
    price = _exact_field(payload, "price", path, number)
    if price <= _ZERO:
        raise ParityInputMalformed(
            f"{path} line {number} reports a fill at price {money_to_str(price)}. A "
            "deviation in basis points is measured against the paper price, and there is "
            "no such thing as a fill at or below zero."
        )
    side = str(payload.get("side", ""))
    if side not in _SIDE_DIRECTION:
        # Refused rather than defaulted to a direction. The deviation is signed against the
        # trader, so a side this module does not recognise would have its sign guessed, and
        # a guessed sign turns a systematically optimistic fill model into an average of
        # roughly zero -- a passing parity report on the exact run that should fail one.
        raise ParityInputMalformed(
            f"{path} line {number} reports a fill with side {side!r}; a parity report "
            f"signs each deviation against the trader and knows only "
            f"{sorted(_SIDE_DIRECTION)}."
        )
    ts = payload.get("ts_ms", entry.get("ts_ms"))
    if not isinstance(ts, int):
        raise ParityInputMalformed(
            f"{path} line {number} is a FILL event with no integer ts_ms ({ts!r}). Fills "
            "are matched inside a time window, so a fill with no instant cannot be "
            "compared against anything."
        )
    tag = payload.get("tag")
    order_id = str(payload.get("order_id", ""))
    fill_no = increments.get(order_id, 0)
    increments[order_id] = fill_no + 1
    return FillRecord(
        ts_ms=ts,
        seq=int(entry.get("seq", 0)),
        symbol=str(payload.get("symbol", "")),
        side=side,
        qty=qty,
        price=price,
        order_id=order_id,
        fill_no=fill_no,
        tag=None if tag is None else str(tag),
    )


def _exact_field(
    payload: dict[str, Any], key: str, path: Path, number: int
) -> Money:
    """One exact decimal out of a `FILL` payload, quantised to the storage seam.

    A JSON *number* is refused here for the same reason it is refused for `net_pnl`: by the
    time `json.load` has returned one it is a float, and a fill compared at a price that has
    already been rounded is precisely the divergence this report exists to detect.
    """
    value = payload.get(key)
    if not isinstance(value, str):
        raise ParityInputMalformed(
            f"{path} line {number} has {key}={value!r} in its FILL payload, not the exact "
            f"decimal string `Fill.to_json` writes. Regenerate the log rather than "
            f"comparing fills at a rounded {key}."
        )
    try:
        return quantize_money(parse_money(value))
    except ValueError as exc:
        raise ParityInputMalformed(
            f"{path} line {number} has {key}={value!r}, which is not a decimal number: "
            f"{exc}"
        ) from None


# ------------------------------------------------------------------------------ artefacts


def _read_json_object(path: Path) -> dict[str, Any]:
    """Read one small run artefact whole.

    `metrics.json` and `trades.json` are both JSON *documents*: they have to be parsed whole
    whatever this function does, and they are kilobytes. The streaming discipline applies to
    `events.jsonl`, which is the file that is not.
    """
    if not path.exists():
        raise ParityInputMissing(
            f"{path} does not exist. A parity report is built from two finished runs; this "
            "one may still be running, may have failed, or may have had its directory "
            "removed."
        )
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except ValueError as exc:
        raise ParityInputMalformed(f"{path} is not valid JSON: {exc}") from None
    if not isinstance(payload, dict):
        raise ParityInputMalformed(
            f"{path} holds {type(payload).__name__}, not the object a finished run writes."
        )
    return payload


def _bps_json(value: Money | None) -> str | None:
    """Render a basis-point figure, or `None` when there was nothing to average.

    Quantised to eight decimals for rendering only. The verdict was decided on the unrounded
    figure -- a mean of exact prices divides, and division is the one operation that runs a
    `Decimal` out to the full accounting precision.
    """
    return None if value is None else money_to_str(quantize_money(value))

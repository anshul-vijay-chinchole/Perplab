"""Leverage brackets, margin requirements, and liquidation price (spec 3.6, 3.7).

Binance charges maintenance margin on a **tiered** schedule: the larger your notional, the
higher the maintenance-margin rate and the lower the leverage you are permitted. The table
differs per symbol and changes over time, so spec 3.6 is explicit that it must be read from
`GET /fapi/v1/leverageBracket` and snapshotted -- never hardcoded. This module holds the
table and the arithmetic that reads it; it never fetches.

**The circularity, and why it needs a fixed point.** The bracket is selected by notional,
notional is quantity times mark price, and the liquidation price we are solving for *is* a
mark price. So the bracket that governs the solution depends on the solution. Spec 3.6
resolves this by iterating: solve under the bracket implied by the current mark, recompute
the notional at the resulting liquidation price, and re-solve if that lands in a different
bracket. `liquidation_price` implements exactly that, capped at
`MAX_BRACKET_ITERATIONS` and raising rather than returning a guess -- this was R6, and a
guess here is a liquidation price that is wrong in the direction of "you survived".

**Everything here is `Decimal`.** This module sits on the accounting side of the seam
described in `perplab.core.money`; `float` is not permitted anywhere in it. That includes
parsing: Binance publishes `maintMarginRatio` as a bare JSON *number*, so a plain
`json.loads` yields `0.004` as an IEEE-754 double before this module ever sees it. See
`brackets_from_payload` for why that one detail is load-bearing.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal, localcontext
from pathlib import Path
from typing import Any

from perplab.core.money import ACCOUNTING_CONTEXT, quantize_money

__all__ = [
    "MAX_BRACKET_ITERATIONS",
    "LeverageBracket",
    "BracketTable",
    "LiquidationSolution",
    "BracketConvergenceError",
    "brackets_from_payload",
    "bracket_symbols",
    "validate_bracket_document",
    "load_bracket_snapshot",
    "initial_margin",
    "maintenance_margin",
    "bankruptcy_price",
    "liquidation_price",
]

MAX_BRACKET_ITERATIONS = 8
"""Spec 3.6: "converges in <= 3 iterations in practice. Cap at 8 iterations; if it does
not converge, raise -- do not return a guess."

Non-convergence is not merely slow. Two adjacent brackets can each imply a liquidation
price that falls in the other's range, and the iteration then oscillates forever between
two self-consistent-looking answers. Returning either one would be picking a liquidation
price by coin flip."""


class BracketConvergenceError(RuntimeError):
    """The bracket/liquidation-price fixed point did not settle (spec 3.6, R6).

    Deliberately not a `ValueError`: the inputs were all well-formed. What failed is the
    solution procedure, and a caller that swallows bad input should not also swallow this.
    """


@dataclass(frozen=True, slots=True)
class LeverageBracket:
    """One tier of a symbol's maintenance-margin schedule.

    `notional_floor` and `notional_cap` are inclusive bounds on position notional, `mmr`
    is the maintenance margin *rate*, and `maintenance_amount` is the cumulative deduction
    Binance calls `cum` -- the constant that makes the piecewise-linear maintenance-margin
    curve continuous across tier boundaries. Dropping it (an easy thing to do, since it is
    zero in the first bracket and in every worked example in the spec) overstates
    maintenance margin on every position large enough to leave tier 1, which shows up as
    phantom liquidations in backtests of exactly the size you would actually trade.
    """

    bracket: int
    max_leverage: int
    notional_floor: Decimal
    notional_cap: Decimal
    mmr: Decimal
    maintenance_amount: Decimal

    def __post_init__(self) -> None:
        if self.notional_cap <= self.notional_floor:
            raise ValueError(
                f"bracket {self.bracket}: cap {self.notional_cap} must exceed "
                f"floor {self.notional_floor}"
            )
        if not (0 < self.mmr < 1):
            raise ValueError(
                f"bracket {self.bracket}: maintenance margin rate {self.mmr} is not a "
                "fraction between 0 and 1"
            )
        if self.max_leverage < 1:
            raise ValueError(f"bracket {self.bracket}: max leverage {self.max_leverage} < 1")


@dataclass(frozen=True, slots=True)
class BracketTable:
    """A symbol's full bracket schedule, at one point in time.

    "At one point in time" carries the same weight it does for `SymbolFilters`: brackets
    are versioned reference data (spec 3.2/3.6). A backtest over 2023 that resolves margin
    against today's table is reporting a liquidation that could not have happened.
    """

    symbol: str
    brackets: tuple[LeverageBracket, ...]
    snapshot_date: str = ""
    """ISO date of the snapshot these came from, or empty when constructed in-memory.
    Carried so a run's metadata can record which table it priced margin against."""

    def __post_init__(self) -> None:
        if not self.brackets:
            raise ValueError(f"{self.symbol}: bracket table is empty")

        ordered = sorted(self.brackets, key=lambda b: b.notional_floor)
        if list(ordered) != list(self.brackets):
            raise ValueError(f"{self.symbol}: brackets must be ordered by notional floor")

        # Contiguity matters more than it looks, and it is violated in two directions. A
        # *gap* between one bracket's cap and the next one's floor is a notional band with
        # no maintenance margin defined at all, and `resolve` would fall through to the
        # next tier up -- quietly applying a *higher* MMR to a mid-sized position. An
        # *overlap* fails the other way, which is worse: `resolve` returns the first tier
        # whose cap covers the notional, so every notional inside an overlap silently
        # takes the lower tier's MMR and solves a liquidation price further from the mark
        # than the exchange's -- wrong in the "you survived" direction, which is the one
        # nobody notices until the position that should have died reports a profit.
        # Real `leverageBracket` payloads are exactly contiguous (each tier's floor *is*
        # the previous tier's cap), so either defect is a malformed snapshot, and checking
        # here means it fails when the table is built rather than when a position happens
        # to land in the bad band months later.
        for lower, upper in zip(ordered, ordered[1:]):
            if upper.notional_floor > lower.notional_cap:
                raise ValueError(
                    f"{self.symbol}: gap between brackets {lower.bracket} and "
                    f"{upper.bracket}: {lower.notional_cap} .. {upper.notional_floor}"
                )
            if upper.notional_floor < lower.notional_cap:
                raise ValueError(
                    f"{self.symbol}: brackets {lower.bracket} and {upper.bracket} "
                    f"overlap: {upper.notional_floor} .. {lower.notional_cap} is claimed "
                    "by both tiers, and every notional in the overlap would resolve to "
                    "the lower maintenance rate"
                )

    def resolve(self, notional: Decimal) -> LeverageBracket:
        """The bracket governing a position of this notional (spec 3.6).

        Notional is `q * Pm` and is therefore always non-negative -- the sign of the
        position does not change which tier it sits in.

        A notional below the lowest tier's floor is refused rather than served the first
        tier. With exact contiguity enforced at construction, the first floor is the only
        floor a notional can fall outside of, and a table whose first floor is above zero
        is declaring that it defines no maintenance rate down there -- inventing one would
        be the same fiction as hardcoding the table, arriving from below. Every published
        Binance table starts at a floor of zero, so this branch is only reachable on a
        malformed or truncated snapshot.
        """
        if notional < 0:
            raise ValueError(f"notional must be non-negative, got {notional}")
        if notional < self.brackets[0].notional_floor:
            raise ValueError(
                f"{self.symbol}: notional {notional} is below the lowest bracket's "
                f"floor {self.brackets[0].notional_floor}; the table defines no "
                "maintenance rate there"
            )

        for bracket in self.brackets:
            if notional <= bracket.notional_cap:
                return bracket

        # Above the published top cap. Binance simply refuses the position at that size,
        # so applying the top bracket's rate is the closest honest answer -- and it is the
        # conservative one, since the top bracket carries the highest MMR.
        return self.brackets[-1]

    def max_leverage_for(self, notional: Decimal) -> int:
        return self.resolve(notional).max_leverage


_BAPI_FIELDS = {
    "bracket": "bracketSeq",
    "initialLeverage": "maxOpenPosLeverage",
    "notionalFloor": "bracketNotionalFloor",
    "notionalCap": "bracketNotionalCap",
    "maintMarginRatio": "bracketMaintenanceMarginRate",
    "cum": "cumFastMaintenanceAmount",
}
"""Documented `leverageBracket` field name -> its spelling on the public endpoint.

Two endpoints serve the same table under different names (see
`exchange.rest.BRACKETS_PUBLIC_URL` for why the public one is used at all). The mapping is
data rather than branching code so that both shapes converge on one parse path -- the
alternative is two constructors that can drift, and a bracket table parsed two subtly
different ways is a liquidation price that depends on where the snapshot came from.

`initialLeverage` maps to `maxOpenPosLeverage`, not `minOpenPosLeverage`: the documented
field is the highest leverage the tier permits, and the public payload splits that into a
min/max pair whose *max* is the same number.
"""


def _entries_from(payload: Any) -> list[Any]:
    """All per-symbol bracket entries in a payload, whichever shape it arrived in."""
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    # Public endpoint: {"code": "000000", "data": {"brackets": [...]}}
    body = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    entries = body.get("brackets", []) if isinstance(body, dict) else []
    return entries if isinstance(entries, list) else []


def validate_bracket_document(text: str, *, prefer: str = "BTCUSDT") -> tuple[str, ...]:
    """Prove a raw bracket document is usable, and report the symbols it carries.

    Takes **text** rather than a parsed object so the `parse_float=Decimal` decision cannot
    be made anywhere else. That matters beyond correctness: `Decimal` is confined by test
    to the accounting modules (spec 3.1), so a caller in the data layer that parsed this
    itself would either widen that seam or silently parse the rates as floats. Handing the
    text across the boundary keeps both properties without the caller needing to know why.

    "Usable" means more than "decodes": one symbol is put through the full
    `brackets_from_payload` path, so tier ordering, contiguity and the float guard all run.
    A payload that decodes but cannot produce a table is rejected here rather than at the
    point months later when a backtest tries to price margin against it.
    """
    try:
        payload = json.loads(text, parse_float=Decimal)
    except ValueError as exc:
        raise ValueError(
            f"bracket document is not decodable JSON ({len(text)} bytes): {exc}"
        ) from exc

    symbols = bracket_symbols(payload)
    if not symbols:
        raise ValueError(
            f"bracket document carries no symbol entries; the endpoint may have changed "
            f"shape. First 200 bytes: {text[:200]!r}"
        )

    brackets_from_payload(payload, prefer if prefer in symbols else symbols[0])
    return symbols


def bracket_symbols(payload: Any) -> tuple[str, ...]:
    """Every symbol carried by a leverage bracket payload, in payload order.

    Exists so a caller can check what a snapshot contains without reaching into the parse
    internals or guessing at the endpoint's envelope shape. `reference.snapshot_leverage_
    brackets` uses it to prove a fetched payload is usable before archiving it.
    """
    return tuple(
        str(e["symbol"])
        for e in _entries_from(payload)
        if isinstance(e, dict) and e.get("symbol")
    )


def _normalise_entry(entry: dict[str, Any]) -> list[dict[str, Any]]:
    """One symbol's tiers, renamed to the documented field names."""
    if "brackets" in entry:
        return list(entry["brackets"])
    return [
        {doc: tier[pub] for doc, pub in _BAPI_FIELDS.items()}
        for tier in entry.get("riskBrackets", [])
    ]


def brackets_from_payload(payload: Any, symbol: str) -> BracketTable:
    """Parse one symbol's entry out of a leverage bracket response.

    Accepts either the documented `GET /fapi/v1/leverageBracket` shape or the public
    endpoint's (`riskBrackets`, `bracketSeq`, ...). The two are normalised onto one parse
    path by `_BAPI_FIELDS` rather than handled by parallel constructors.

    **Pass a payload parsed with `parse_float=Decimal`.** Binance sends these fields as
    JSON numbers rather than the decimal strings it uses everywhere else in the futures
    API, so the default `json.loads` turns `0.004` into an IEEE-754 double *before* this
    function is reached, and no amount of `Decimal(...)` afterwards recovers the lost bits
    -- `Decimal(0.004)` is `0.004000000000000000083266726...`. That value then multiplies
    a six-figure notional inside every liquidation-price solve, and spec 3.10 forbids the
    epsilon that would be needed to paper over the result. `load_bracket_snapshot` does
    this correctly; anything hand-rolling the read must do the same.

    Floats are rejected rather than converted, for the same reason: by the time one
    arrives here the precision is already gone, and accepting it would make the failure
    invisible. The public endpoint made this guard earn its place -- it publishes rates
    like `0.0333`, which no binary float represents exactly.
    """
    entries = _entries_from(payload)

    chosen: Any = None
    for entry in entries:
        if isinstance(entry, dict) and entry.get("symbol") == symbol:
            chosen = entry
            break
    if chosen is None:
        # A single-symbol response (`?symbol=BTCUSDT`) is a bare object, not a list.
        if isinstance(payload, dict) and payload.get("symbol") == symbol:
            chosen = payload
        else:
            available = sorted(
                str(e.get("symbol")) for e in entries if isinstance(e, dict)
            )
            raise ValueError(
                f"{symbol} not present in leverageBracket payload "
                f"({len(available)} symbol(s) available)"
            )

    def number(raw: Any, field: str) -> Decimal:
        if isinstance(raw, float):
            raise ValueError(
                f"{symbol}.{field} arrived as a float ({raw!r}); parse the payload with "
                "json.loads(..., parse_float=Decimal) so the published precision survives"
            )
        return Decimal(str(raw))

    tiers = _normalise_entry(chosen)
    if not tiers:
        raise ValueError(f"{symbol}: bracket entry carries no tiers")

    brackets = tuple(
        LeverageBracket(
            bracket=int(b["bracket"]),
            max_leverage=int(b["initialLeverage"]),
            notional_floor=number(b["notionalFloor"], "notionalFloor"),
            notional_cap=number(b["notionalCap"], "notionalCap"),
            mmr=number(b["maintMarginRatio"], "maintMarginRatio"),
            maintenance_amount=number(b["cum"], "cum"),
        )
        for b in sorted(tiers, key=lambda b: int(b["bracket"]))
    )
    return BracketTable(symbol=symbol, brackets=brackets)


def load_bracket_snapshot(path: Path, symbol: str) -> BracketTable:
    """Read a dated `leverageBracket` snapshot from `userdata/reference/`.

    Note the `parse_float=Decimal`: see `brackets_from_payload` for why omitting it
    silently poisons every margin number downstream.
    """
    payload = json.loads(path.read_text(encoding="utf-8"), parse_float=Decimal)
    body = payload.get("payload", payload) if isinstance(payload, dict) else payload
    table = brackets_from_payload(body, symbol)
    return BracketTable(
        symbol=table.symbol, brackets=table.brackets, snapshot_date=path.stem
    )


# --------------------------------------------------------------------------- margin


def initial_margin(notional: Decimal, leverage: int) -> Decimal:
    """`IM = N_entry / L` (spec 3.6), quantised to the money seam's 8 decimal places.

    Notional here is the notional *at entry*, not at the current mark. Initial margin is
    posted once when the position is opened and does not float with price; only
    maintenance margin does.

    The quantisation is not cosmetic. This is a division, and on a leverage that does not
    divide the notional evenly it yields a value at the full 50-digit context precision.
    That figure reaches the wallet through a liquidation's realised loss, and an operand
    with forty-odd decimal places against a six-figure balance forces the next addition to
    round -- which surfaces as an I1 failure with a difference around 1e-23 and reads
    exactly like the float leak spec 3.10 warns about. Found by the property suite, which
    is what it is for.
    """
    if leverage < 1:
        raise ValueError(f"leverage must be at least 1, got {leverage}")
    if notional < 0:
        raise ValueError(f"notional must be non-negative, got {notional}")
    with localcontext(ACCOUNTING_CONTEXT):
        return quantize_money(notional / Decimal(leverage))


def maintenance_margin(notional: Decimal, bracket: LeverageBracket) -> Decimal:
    """`MM = N * MMR_i - MA_i` (spec 3.6).

    Clamped at zero. The maintenance amount is a deduction, and in the lowest bracket of
    some symbols it can exceed the maintenance charge on a very small position, which
    would otherwise produce a negative margin requirement -- an amount of margin the
    exchange supposedly owes you for holding a position. Nothing downstream is prepared
    for that, and the true answer is simply zero.
    """
    if notional < 0:
        raise ValueError(f"notional must be non-negative, got {notional}")
    with localcontext(ACCOUNTING_CONTEXT):
        mm = notional * bracket.mmr - bracket.maintenance_amount
    return mm if mm > 0 else Decimal(0)


# ---------------------------------------------------------------------- liquidation


def bankruptcy_price(qty: Decimal, entry_price: Decimal, margin: Decimal) -> Decimal:
    """`P_bank = Pe - W/Q` -- the mark price at which margin balance reaches zero (spec 3.7).

    Distinct from the liquidation price and always further away: liquidation fires while
    there is still maintenance margin left, bankruptcy is where there is nothing left at
    all. The gap between them is what the exchange's liquidation engine has to work with,
    and invariant I7 asserts the ordering holds.
    """
    if qty == 0:
        raise ValueError("bankruptcy price is undefined for a flat position")
    with localcontext(ACCOUNTING_CONTEXT):
        return entry_price - margin / qty


@dataclass(frozen=True, slots=True)
class LiquidationSolution:
    """The result of the spec 3.6 fixed-point solve.

    `bracket` and `iterations` are returned rather than discarded because they are the
    only evidence that the circularity was actually resolved: a liquidation price alone
    cannot be distinguished from one computed under the wrong tier.
    """

    price: Decimal
    bracket: LeverageBracket
    iterations: int

    @property
    def reachable(self) -> bool:
        """False when the solved price is non-positive.

        A position margined at or below 1x has a liquidation price below zero -- the mark
        price would have to become negative to trigger it. That is not an error and not a
        bug in the solve; it is a position that cannot be liquidated. Callers should skip
        the trigger check rather than compare against a negative price, and `Account`
        does.
        """
        return self.price > 0


def liquidation_price(
    *,
    qty: Decimal,
    entry_price: Decimal,
    margin: Decimal,
    mark_price: Decimal,
    table: BracketTable,
) -> LiquidationSolution:
    """Solve `P_liq` against the bracket table, resolving the circularity (spec 3.6/3.7).

    ```
    P_liq = (W - Q*Pe + MA) / (q*MMR - Q)
    ```

    `margin` is the isolated margin allocated to *this position* -- initial margin plus any
    manually added margin -- not the whole wallet. Passing the wallet balance is the single
    easiest way to compute a liquidation price that is far too optimistic, because it
    implies every other dollar in the account is defending this position, which under
    isolated margin it explicitly is not.

    The iteration is the spec 3.6 procedure verbatim: resolve the bracket at the current
    mark, solve, then re-resolve at the solved price and repeat if the tier moved.
    """
    if qty == 0:
        raise ValueError("liquidation price is undefined for a flat position")
    if mark_price <= 0:
        raise ValueError(f"mark price must be positive, got {mark_price}")

    q = abs(qty)

    with localcontext(ACCOUNTING_CONTEXT):
        notional = q * mark_price
        seen: list[int] = []

        for iteration in range(1, MAX_BRACKET_ITERATIONS + 1):
            bracket = table.resolve(notional)
            seen.append(bracket.bracket)

            denominator = q * bracket.mmr - qty
            if denominator == 0:
                # Only reachable for a long whose MMR is exactly 1, which the
                # `LeverageBracket` constructor already refuses. Kept as a guard because
                # the alternative is a DivisionByZero from inside a margin calculation,
                # which is a far worse thing to read in a crash log than this sentence.
                raise BracketConvergenceError(
                    f"{table.symbol}: degenerate liquidation denominator at bracket "
                    f"{bracket.bracket} (mmr={bracket.mmr}, qty={qty})"
                )

            price = (margin - qty * entry_price + bracket.maintenance_amount) / denominator

            # A non-positive solution is unreachable, so re-resolving the bracket "at" it
            # is meaningless -- notional would be a negative number. The tier that governs
            # the position at its current size is the answer.
            if price <= 0:
                return LiquidationSolution(price=price, bracket=bracket, iterations=iteration)

            settled = table.resolve(q * price)
            if settled.bracket == bracket.bracket:
                return LiquidationSolution(
                    price=price, bracket=bracket, iterations=iteration
                )
            notional = q * price

    raise BracketConvergenceError(
        f"{table.symbol}: bracket selection did not converge in "
        f"{MAX_BRACKET_ITERATIONS} iterations (visited brackets {seen}); "
        "refusing to return a guessed liquidation price"
    )

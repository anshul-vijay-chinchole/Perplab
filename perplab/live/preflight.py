"""Make the exchange agree with the ledger before a single order is sent.

PerpLab's ledger computes margin and liquidation prices from *its own* configuration: the
run's leverage, isolated margin, one position per symbol. Binance computes them from the
account's configuration, which is whatever it was last set to -- possibly by hand, in the
app, months ago. Nothing reconciles those two until a position exists, and by then the
disagreement has already priced a trade.

The gap this closes was real and silent. `SignedRestClient.set_leverage` existed, was
tested, was documented as idempotent -- and had **no callers anywhere in the platform**. A
run configured at 5x would have opened a position against an account still set to Binance's
default 20x: four times the intended size, and a liquidation price on the Live Monitor
computed from the 5x the ledger believed in rather than the 20x that would actually close
the position. Every number would have looked reasonable.

## Why this is a preflight rather than a step in the order path

Per-symbol configuration is account state, not order state. Sending it with each order
would be both wasteful and wrong -- `POST /fapi/v1/leverage` is not free against the weight
limit, and a leverage change *between* two orders of the same strategy is a state change
nothing in the ledger models. It belongs once, before the first order, where a failure
means "do not trade" rather than "this order failed".

## What it refuses, and why refusing is the point

Three of the four checks here can only fail in ways that make the ledger wrong rather than
the request wrong, so each one aborts the session instead of degrading it:

- **A margin mode the ledger cannot price.** `CROSSED` is refused before the request is
  sent, not after. See `core.types.MarginMode`.
- **An open position on the symbol.** Binance refuses a margin-type change while one
  exists (`-4048`), and it is right to: the position was opened under the old mode. A
  session that started anyway would be trading against a symbol configured differently
  from the rest.
- **An account whose position mode is not the run's.** The ledger supports both one-way and
  hedge (`core.account.Account`), but a run is in exactly one of them and the account has to
  agree: a one-way run against a hedge account has no single position to reconcile against,
  and a hedge run against a one-way account sends a `positionSide` the account rejects.
  `live.reconcile._assert_mode_matches` also catches this -- but only on the first
  reconciliation pass, which is up to sixty seconds *after* the first order. Catching it
  here moves the failure to before any exposure exists.
- **A leverage the exchange did not actually apply.** Checked by reading the echo rather
  than trusting the 200. See `_apply_leverage`.

`-4046` is the one error that is not a failure: Binance answers "No need to change margin
type" when the symbol already has the mode being requested. That is the *success* case for
an idempotent call, reported as an error, and treating it as one would make a correctly
configured account unable to trade.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Mapping, Sequence

from perplab.core.types import MarginMode
from perplab.exchange.rest import BinanceRestError

if TYPE_CHECKING:  # pragma: no cover - import cycle; the client is injected
    from perplab.exchange.signed import SignedRestClient

__all__ = [
    "PreflightError",
    "SymbolConfig",
    "PreflightReport",
    "configure_account",
    "MARGIN_TYPE_UNCHANGED",
    "MARGIN_TYPE_POSITION_OPEN",
]

MARGIN_TYPE_UNCHANGED = -4046
"""`No need to change margin type` -- the symbol already has the mode requested.

Success wearing an error's clothes. An idempotent call that finds nothing to do has done
its job, and this is the only code in this module that is caught and discarded."""

MARGIN_TYPE_POSITION_OPEN = -4048
"""`Cannot change margin type with open position`. A hard stop -- see the module docstring."""


class PreflightError(RuntimeError):
    """The exchange could not be brought into agreement with the ledger.

    Raised rather than returned. Every caller of `configure_account` is on the path to
    sending a real order, and a preflight whose result can be ignored is a preflight that
    will eventually be ignored.
    """


@dataclass(frozen=True, slots=True)
class SymbolConfig:
    """What was actually applied to one symbol, as the exchange confirmed it."""

    symbol: str
    leverage: int
    margin_mode: MarginMode
    margin_type_changed: bool
    """False when the symbol already had this margin mode (`-4046`).

    Recorded rather than collapsed into "applied" because the two are different facts about
    the account, and the run manifest is where someone reconstructs what the account looked
    like on the day."""
    max_notional: str | None = None
    """`maxNotionalValue` from the leverage response, as a string.

    Binance's own ceiling for this leverage on this symbol. Carried because it is the
    bracket boundary the ledger's own `margin.BracketTable` is asserting against, and a
    mismatch between the two is worth being able to see after the fact."""

    def to_json(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "leverage": self.leverage,
            "margin_mode": self.margin_mode.value,
            "margin_type_changed": self.margin_type_changed,
            "max_notional": self.max_notional,
        }


@dataclass(frozen=True, slots=True)
class PreflightReport:
    """Everything the preflight established, for the run manifest.

    Recorded because "the account was configured correctly" is a claim about a moment that
    has passed by the time anyone reads the run, and spec 12.1's reproducibility argument
    applies to account state as much as to data: a run that cannot say what leverage it
    traded at cannot be compared with one that can.
    """

    symbols: tuple[SymbolConfig, ...] = ()
    hedge_mode: bool = False
    warnings: tuple[str, ...] = field(default=())

    def to_json(self) -> dict[str, Any]:
        return {
            "symbols": [s.to_json() for s in self.symbols],
            "hedge_mode": self.hedge_mode,
            "warnings": list(self.warnings),
        }


def _error_code(exc: BinanceRestError) -> int | None:
    return exc.code


async def _assert_position_mode(client: SignedRestClient, *, hedge_mode: bool) -> bool:
    """Refuse an account whose position mode is not the one this run's ledger is in.

    Returns the account's `dualSidePosition` flag, having already raised if it disagrees.
    The return value exists so the report can record *that this was checked*, which is not
    the same fact as "it did not raise".

    **Both directions are refused, and neither is the obvious one.** A one-way run against a
    hedge account is the case this check was written for: the exchange keeps two positions
    per symbol, PerpLab would send orders with no `positionSide` (which Binance rejects) and
    reconcile against rows that describe half the exposure. The mirror -- a hedge run against
    a one-way account -- is quieter and just as wrong: every order would carry a
    `positionSide` the account cannot accept, so nothing would fill, and the ledger would
    hold two positions the exchange has never heard of.

    **PerpLab does not switch the mode itself.** `POST /fapi/v1/positionSide/dual` exists,
    and calling it here would be reconfiguring the operator's whole account -- affecting
    every other position and every order placed by hand -- as a side effect of starting a
    session. Binance also refuses the change while any position is open or any order is
    working, so the failure would be common and the recovery would be "close everything",
    which is not a decision a preflight gets to make.
    """
    payload = await client.position_mode()
    dual = payload.get("dualSidePosition")
    # Binance has served this as a JSON boolean and as the string "false"; both mean the
    # same thing and neither is worth a surprise at the point of the first order.
    hedge = dual is True or (isinstance(dual, str) and dual.strip().lower() == "true")
    if hedge == bool(hedge_mode):
        return hedge
    if hedge:
        raise PreflightError(
            "the Binance account is in hedge mode (dualSidePosition=true) and this run is "
            "configured one-way. In hedge mode the exchange keeps a long and a short "
            "position per symbol, so there is no single position to reconcile the ledger "
            "against and every order needs a positionSide this run does not set. Start the "
            "session with hedge mode on, or switch the account to one-way in the Binance "
            "app (it refuses the change while any position is open or any order is working)."
        )
    raise PreflightError(
        "this run is configured for hedge mode and the Binance account is one-way "
        "(dualSidePosition=false). Every order would carry a positionSide the account "
        "cannot accept, so nothing would fill while the ledger tracked two positions that "
        "do not exist. Turn hedge mode on for the account in the Binance app -- it refuses "
        "the change while any position is open or any order is working -- or start the "
        "session in one-way mode."
    )


async def _apply_margin_type(
    client: SignedRestClient, symbol: str, mode: MarginMode
) -> bool:
    """Set the symbol's margin type. Returns whether it actually changed.

    `-4046` is swallowed as success and `-4048` is re-raised with the reason spelled out;
    every other Binance error propagates untouched, because a preflight that guesses at
    unfamiliar error codes is how a real misconfiguration gets absorbed.
    """
    try:
        await client.set_margin_type(symbol, mode.value)
    except BinanceRestError as exc:
        code = _error_code(exc)
        if code == MARGIN_TYPE_UNCHANGED:
            return False
        if code == MARGIN_TYPE_POSITION_OPEN:
            raise PreflightError(
                f"{symbol} already has an open position, so its margin type cannot be "
                f"changed to {mode.value}. The position was opened under the account's "
                f"previous configuration and the ledger would price it under this run's. "
                f"Close the position on {symbol}, or start the session on a symbol that "
                f"is flat."
            ) from exc
        raise
    return True


async def _apply_leverage(client: SignedRestClient, symbol: str, leverage: int) -> str | None:
    """Set the symbol's leverage and **verify the exchange's echo**.

    The verification is the point. `POST /fapi/v1/leverage` answers with the leverage it
    applied, and a 200 alone does not promise that it equals the one requested -- Binance
    bounds leverage by the symbol's bracket table, which changes without notice. A silently
    reduced leverage is the same defect as never having sent the request: the ledger sizes
    and prices liquidation at a number the exchange is not using.

    A mismatch aborts rather than adopting the exchange's value. Adopting it would silently
    change the run's configuration out from under the strategy, and a walk-forward whose
    leverage moved mid-study is not comparable with itself.
    """
    payload = await client.set_leverage(symbol, leverage)
    echoed = payload.get("leverage")
    try:
        applied = int(echoed)
    except (TypeError, ValueError):
        raise PreflightError(
            f"{symbol}: the leverage response carried no usable leverage field "
            f"({echoed!r}), so there is no confirmation the exchange applied {leverage}x. "
            f"Refusing to trade on an unverified setting."
        ) from None

    if applied != int(leverage):
        raise PreflightError(
            f"{symbol}: requested {leverage}x leverage and the exchange applied {applied}x. "
            f"This run's margin and liquidation prices are computed at {leverage}x, so "
            f"trading now would price every position against a leverage the exchange is "
            f"not using. Binance bounds leverage by the symbol's bracket table -- lower "
            f"the run's leverage to {applied}x or less, or reduce the position size the "
            f"bracket is limiting."
        )

    max_notional = payload.get("maxNotionalValue")
    return None if max_notional is None else str(max_notional)


async def configure_account(
    client: SignedRestClient,
    symbols: Sequence[str],
    *,
    leverage: int,
    margin_mode: MarginMode = MarginMode.ISOLATED,
    hedge_mode: bool = False,
) -> PreflightReport:
    """Bring the exchange into agreement with this run's configuration, or refuse.

    Call once, before the first order of a session, with the same `leverage` and
    `margin_mode` the ledger was built with. Raises `PreflightError` if the account cannot
    be made to match; returns what was applied otherwise.

    Ordering is deliberate. The account-wide hedge check runs first, because it invalidates
    the whole session and there is no reason to reconfigure four symbols before discovering
    it. Then, per symbol, margin type before leverage: the margin-type call is the one that
    fails on an open position, and failing before the leverage of a symbol we are about to
    refuse has been changed leaves less of the account modified behind us.
    """
    if not margin_mode.is_implemented:
        raise PreflightError(
            f"margin mode {margin_mode.value} is not implemented. Under cross margin a "
            f"symbol's liquidation price depends on the unrealised PnL of every other open "
            f"position, so there is no closed form for it and PerpLab's ledger -- which "
            f"solves the isolated form in each position's own allocation -- would report a "
            f"liquidation price the exchange does not agree with. Use ISOLATED."
        )
    if not symbols:
        raise PreflightError(
            "the preflight was given no symbols, so nothing about the account was verified "
            "and the session would trade against an unchecked configuration."
        )
    if int(leverage) < 1:
        raise PreflightError(f"leverage {leverage} must be at least 1")

    hedge = await _assert_position_mode(client, hedge_mode=hedge_mode)

    applied: list[SymbolConfig] = []
    for symbol in symbols:
        changed = await _apply_margin_type(client, symbol, margin_mode)
        max_notional = await _apply_leverage(client, symbol, int(leverage))
        applied.append(
            SymbolConfig(
                symbol=symbol,
                leverage=int(leverage),
                margin_mode=margin_mode,
                margin_type_changed=changed,
                max_notional=max_notional,
            )
        )

    return PreflightReport(symbols=tuple(applied), hedge_mode=hedge)

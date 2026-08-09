"""Spec 6.7.3: ask Binance what the account holds, every 60 s, and halt if we disagree.

> *"Every 60 s during a live session, fetch account state from Binance and compare against
> PerpLab's internal accounting: wallet balance, position size, entry price, unrealised PnL,
> liquidation price. Any mismatch beyond tickSize/stepSize tolerance triggers the kill
> switch. This is the check that catches a missed fill or a dropped user-data-stream message
> before it becomes an unhedged position."*

Everything upstream of this is a *fast* path with no way to prove itself. The user-data
stream carries no sequence number, so a dropped `ORDER_TRADE_UPDATE` is invisible from the
socket alone (`exchange.userstream` says so in its own docstring). A POST that times out
leaves an order that may or may not exist (`exchange.signed.OrderOutcomeUnknown`). An order
placed by hand in the Binance app, or a leverage change made there, moves the account under
a session that never sees it. Each of those leaves PerpLab's ledger describing an account
that is not the one being held -- and every risk limit in spec 7 is then evaluated against
that description rather than against the position. This loop is the only thing in the system
that can notice.

**This code has never talked to a real Binance account.** That needs API credentials, and
none are available in this environment. It is tested against a fake signed client returning
hand-written payloads (`tests/unit/test_reconcile.py`), which pins the comparisons, the
tolerances and the halt path -- and pins nothing at all about what the exchange really puts
in those fields. Spec 13's Phase 8 exit criterion is a real round trip reconciled to the
cent; until that has happened, the field names and shapes read here come from Binance's
published documentation rather than from an observed response, and every place that matters
is called out where it occurs.

**"Could not check" is not "checked and disagreed", and conflating them is the failure mode
this loop is most likely to have.** A single REST timeout, a 5xx, a rate-limit refusal --
none of those say anything about the account. If they tripped the kill switch, a flaky link
would stop a healthy session, and an operator who has watched the kill switch fire for a
network blip will switch it off. A kill switch that has been switched off protects nothing,
so a fetch failure here is recorded as an outage against the reconciliation loop itself,
reported with the same DISCONNECT/RECONNECT vocabulary `data.rest_poller` uses, and never
compared against anything. The blindness it causes is real and is surfaced (`blind_for_ms`,
`consecutive_failures`) so an operator can act on it -- but the decision to stop is theirs,
not a timeout's.

**The halt goes through the engine, never around it.** `RiskEngine.observe_reconciliation`
decides whether a difference is a breach and trips the kill switch record; the engine then
performs the halt between events (`BacktestEngine.request_halt`), where the ledger is
consistent by construction. Halting from here would unwind the account halfway through
whatever the engine was doing.

All money is `Decimal` through `perplab.core.money`. Binance sends every balance, size and
price as a decimal *string* precisely so a client need not go via binary floating point, and
a reconciliation whose tolerance is one `tickSize` cannot be run in a representation where
`0.1 + 0.2 != 0.3`.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from perplab.core.account import position_label
from perplab.core.money import Money, accounting, from_scaled, money_to_str, parse_money
from perplab.core.types import CollectorEventKind, PositionSide

if TYPE_CHECKING:  # pragma: no cover - import cycle; the engine imports engine.transport
    from perplab.engine.backtest import BacktestEngine
    from perplab.exchange.signed import SignedRestClient

__all__ = [
    "DEFAULT_INTERVAL_S",
    "LIQUIDATION_TOLERANCE_FRACTION",
    "RECONCILED_FIELDS",
    "RECONCILE_LABEL",
    "WALLET_EPSILON",
    "FieldCheck",
    "ReconciliationPass",
    "Reconciler",
]

EventSink = Callable[[CollectorEventKind, str, str, int], None]

RECONCILE_LABEL = "reconcile"
"""Stream label on every event this module writes.

Not a dataset name, for the reason `userstream.USER_STREAM_LABEL` gives: spec 4.5's gap
detector matches records to datasets by this field, and a label naming a real dataset would
hand that dataset an alibi for a gap it did not earn.
"""

DEFAULT_INTERVAL_S = 60.0
"""Spec 6.7.3's own number.

Configurable, and the direction it should be moved is down rather than up: the interval is
the window in which a missed fill can become an unhedged position, and every extra second is
a second of trading on an account state nobody has checked. It costs weight 5 per symbol per
pass against a 2400/minute budget (`exchange.signed.WEIGHT_CEILING`), so 60 s is not the
number a rate limit forced.
"""

MAX_BLIND_MS = 600_000
"""Ten minutes -- ten consecutive failed passes -- before an unverified account halts (H7).

`_note_fetch_failure` refuses to halt on one failure, correctly: a timeout is weather, and
a kill switch that fires on a network blip is one an operator turns off. But *no* ceiling
meant a revoked key or a long venue outage left the strategy trading a ledger nobody had
verified, indefinitely, with `blind_for_ms` shown in a banner and compared against
nothing. Ten minutes is long enough that no transient survives it -- the retrying REST
client, the poller backoffs and nine full fresh passes have all had their chance -- and
short enough that the exposure it bounds (unverified fills, a frozen wallet, a dead fill
stream sharing the same cause) stays a nuisance rather than an incident.
"""

WALLET_EPSILON = parse_money("0.01")
"""Absolute tolerance on wallet balance and, as a floor, on unrealised PnL. One cent.

Neither side of this comparison is a rounded number in principle: PerpLab's ledger is exact
to `SCALE_EXP` decimal places and Binance reports eight. In practice they are two
*independent computations* over the same events -- our fee schedule against the exchange's
actual commission, our funding formula against the exchange's settlement -- and over a
48-hour session those accumulate thousands of terms each rounded at the exchange's own
precision. A tolerance of exactly zero would fire on the arithmetic rather than on the
account.

One cent is chosen because it is the smallest amount an operator could act on and because a
*real* divergence here is not small: the failure this check exists for is a missed fill, and
a missed fill of the smallest permitted BTCUSDT lot moves position size by a whole
`stepSize` -- caught by the position check, whose tolerance is *half* a step precisely so
that one whole step of divergence fails. (It was a full step for one release, at which the
smallest possible missed fill produced a delta exactly equal to the tolerance and passed --
the claim this paragraph makes was false while that held.) The wallet check is the
backstop, not the sharp edge, so it is allowed to be blunt.
"""

LIQUIDATION_TOLERANCE_FRACTION = parse_money("0.005")
"""Relative tolerance on liquidation price: half a percent, floored at one `tickSize`.

**The loosest of the five, and it has to be.** Spec 3.7's formula and Binance's are the same
algebra over different inputs. The exchange resolves the maintenance-margin bracket against
its own live table and rounds the maintenance amount per tier; PerpLab resolves it against a
`leverageBracket` snapshot that is dated (`core.margin`) and may be a day old, and spec 3.6's
fixed-point iteration settles on a tier boundary that the exchange may have moved. Requiring
agreement to a tick would be requiring agreement about a table we did not fetch this minute.

Half a percent is calibrated against what this check is for rather than against what the
formulas can achieve. A liquidation price sits tens of percent from the mark, so 0.5% of it
is well inside the noise of bracket rounding -- and every disagreement that matters is far
larger: a leverage change made in the Binance app during a session (which arrives as
`ACCOUNT_CONFIG_UPDATE` and invalidates every margin figure the engine computed) moves
`P_liq` by whole percent, and a position-size error moves it further still.

Floored at one `tickSize` so that a near-zero liquidation price -- which is what a heavily
over-margined position produces -- does not end up with a tolerance of nothing.
"""

RECONCILED_FIELDS = (
    "wallet_balance",
    "position_size",
    "entry_price",
    "unrealized_pnl",
    "liquidation_price",
)
"""The five quantities spec 6.7.3 names, in the order it names them.

The order is load-bearing rather than cosmetic. `RiskEngine._halt` records the *first*
breach and ignores the rest, so if several fields disagree at once -- and they will, because
a missed fill moves all five -- the one written into the run log is whichever is checked
first. Wallet balance and position size are the two an operator can act on directly; entry
price, unrealised PnL and liquidation price are all *derived* from position size, so leading
with one of them would report a symptom in place of its cause.
"""


@dataclass(frozen=True, slots=True)
class FieldCheck:
    """One quantity, compared. Frozen because it is a record of a measurement."""

    field: str
    symbol: str | None
    ours: Money
    theirs: Money
    tolerance: Money

    @property
    def delta(self) -> Money:
        with accounting():
            return abs(self.ours - self.theirs)

    @property
    def matched(self) -> bool:
        """A difference exactly equal to the tolerance passes.

        The rule `core.risk` states for every size limit, and the right one here: one tick or
        one step of rounding is precisely the disagreement a `tickSize`/`stepSize` tolerance
        exists to admit, and a check that halted on it would halt on every position the
        exchange rounded.
        """
        return self.delta <= self.tolerance

    def to_json(self) -> dict[str, Any]:
        return {
            "field": self.field,
            "symbol": self.symbol,
            "ours": money_to_str(self.ours),
            "theirs": money_to_str(self.theirs),
            "tolerance": money_to_str(self.tolerance),
            "delta": money_to_str(self.delta),
            "matched": self.matched,
        }


@dataclass(frozen=True, slots=True)
class ReconciliationPass:
    """The result of one comparison against the exchange.

    `fetched` is the field that keeps "could not check" separate from "checked and agreed".
    A pass with `fetched=False` carries no comparisons and proves nothing; without the flag
    it would be indistinguishable from a clean pass, and a reader counting clean passes would
    conclude the account had been verified when nobody had asked it anything.
    """

    ts_ms: int
    fetched: bool
    checks: tuple[FieldCheck, ...] = ()
    skipped: tuple[tuple[str, str], ...] = ()
    """`(field, why)` for each comparison that could not be made -- no mark price yet, no
    bracket table loaded, filters missing for a symbol. Recorded rather than counted as
    agreement, for the same reason `fetched` exists."""

    error: str = ""

    @property
    def mismatches(self) -> tuple[FieldCheck, ...]:
        return tuple(check for check in self.checks if not check.matched)

    def to_json(self) -> dict[str, Any]:
        return {
            "ts_ms": self.ts_ms,
            "fetched": self.fetched,
            "error": self.error,
            "checks": [check.to_json() for check in self.checks],
            "skipped": [{"field": f, "why": w} for f, w in self.skipped],
            "mismatches": [check.field for check in self.mismatches],
        }


class Reconciler:
    """Spec 6.7.3's loop: fetch, compare five quantities, halt on a mismatch.

    Reads its tolerances out of `engine.filters` rather than taking its own copy. That is
    deliberate: the whole check is "does the exchange agree with the engine", and a
    reconciler holding a different filter snapshot than the engine quantised against would
    be comparing two things that were never meant to be equal.
    """

    def __init__(
        self,
        engine: BacktestEngine,
        client: SignedRestClient,
        *,
        on_event: EventSink,
        interval_s: float = DEFAULT_INTERVAL_S,
        symbols: Sequence[str] | None = None,
        transport: Any | None = None,
    ) -> None:
        if interval_s <= 0:
            raise ValueError(
                f"interval_s must be positive, got {interval_s}. Spec 6.7.3 says 60 s; a "
                "non-positive interval would spin against a weight-limited endpoint and "
                "earn the IP ban that takes the session offline with positions open."
            )
        self._engine = engine
        self._client = client
        self._on_event = on_event
        self._transport = transport
        """The session's `ExchangeTransport`, when there is one. Two of this loop's jobs
        live on it rather than here because they need its route table: settling unknown
        order outcomes (C10) and diffing the venue's working-order set against the
        engine's book (H5). Typed `Any` to keep this module importable without the
        transport -- the tests build reconcilers against engines with no live stack."""
        self.interval_s = float(interval_s)
        self.symbols: tuple[str, ...] = tuple(
            symbols if symbols is not None else engine.config.symbols
        )

        self.passes = 0
        self.fetch_failures = 0
        self.consecutive_failures = 0
        self.mismatches = 0
        self.last_pass: ReconciliationPass | None = None
        self._blind_since_ms: int | None = None
        self._drift_warned = False
        """Whether the current clock-drift episode has been reported. Reset when the
        clock comes back inside the limit, so a machine that drifts, is re-synced, and
        drifts again warns once per episode rather than once per session."""

    # ------------------------------------------------------------------------ running

    async def run(self, stop: asyncio.Event) -> None:
        """Compare every `interval_s` until `stop` is set or the engine halts.

        The cadence is driven from a monotonic deadline rather than `sleep(interval)` after
        each pass, for the reason `RestPoller.run` gives: request latency added to every
        cycle accumulates, and here it accumulates against a window spec 6.7.3 wrote down.

        The loop ends once the engine has halted. There is nothing left to reconcile against
        -- the run has stopped trading -- and continuing would spend weight, and possibly
        raise a second breach, on an account the session is no longer driving.
        """
        next_at = time.monotonic() + self.interval_s
        while not stop.is_set():
            if await _sleep_unless_stopped(stop, max(0.0, next_at - time.monotonic())):
                return
            if self._engine.halted:
                return
            next_at = time.monotonic() + self.interval_s
            await self.check_once()

    async def check_once(self) -> ReconciliationPass:
        """One pass. Never raises: a failure to fetch is data, not an exception.

        Catches broadly and deliberately, exactly as `RestPoller._guarded` does. A
        reconciliation loop that dies on an unexpected exception takes spec 7's auto-trigger
        offline for the rest of the session, and by then nothing connects the missing checks
        to the exception -- which is strictly worse than the outage it would be reporting,
        because the session keeps trading with no verification at all and nothing says so.
        """
        ts_ms = self._now_ms()

        # The unknown-outcome sweep runs *before* the account fetch, so that an order the
        # venue accepted but never answered for is booked before the pass compares
        # positions -- otherwise the very fill this sweep recovers would first be reported
        # as a position mismatch and halt the run for a state the platform was one step
        # from explaining. Individually guarded: resolution failing must not cost the pass.
        if self._transport is not None:
            try:
                await self._transport.resolve_unknown_outcomes()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - the pass must still run
                self._event(
                    CollectorEventKind.STALE,
                    f"could not resolve unknown order outcomes this pass: "
                    f"{type(exc).__name__}: {exc}",
                )

        # **The drift is re-measured on the reconciler's cadence, not once at startup**
        # (H10). NTP walks: a clock 900 ms off at hour zero can be five seconds off at
        # hour thirty, at which point every signed request fails -1021, the rejection
        # streak trips the kill switch blaming the strategy, and nothing anywhere said
        # "your clock is wrong". One unsigned weight-1 request per pass keeps
        # `clock_drift_warning` describing the clock as it is now -- and when drift does
        # break signing, the blind-time ceiling halts within `MAX_BLIND_MS` while this
        # warning, already in the feed, names the actual cause.
        try:
            await self._client.server_time_ms()
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - the account fetch below reports the outage
            pass
        drift_warning = self._client.clock_drift_warning
        if drift_warning and not self._drift_warned:
            self._drift_warned = True
            self._event(CollectorEventKind.STALE, drift_warning)
            self._engine.warnings.append(drift_warning)
        elif not drift_warning:
            self._drift_warned = False

        try:
            account = await self._client.account()
            positions = {
                symbol: await self._client.position_risk(symbol)
                for symbol in self.symbols
            }
            open_orders = (
                None
                if self._transport is None
                else {
                    symbol: await self._client.open_orders(symbol)
                    for symbol in self.symbols
                }
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - see docstring
            return self._note_fetch_failure(ts_ms, exc)

        if self._transport is not None and open_orders is not None:
            # H5: the five account quantities cannot see a resting order. The diff lives
            # on the transport because only its route table knows which ids are ours.
            self._transport.reconcile_open_orders(open_orders)
        try:
            result = self._compare(ts_ms, account, positions)
        except (KeyError, ValueError, TypeError) as exc:
            # The payload did not have the shape this module reads. That is a *refusal*, not
            # a mismatch: comparing against a field that has moved would produce a halt whose
            # stated cause is wrong, and an operator sent to look at their position size when
            # the real fault is a renamed JSON key will not find anything.
            return self._note_fetch_failure(
                ts_ms, exc, phase="reading the exchange's account payload"
            )

        # **Recovered only once the payload has actually been read** (M17). Declaring it
        # right after the fetch meant a shape change alternated RECONNECT/DISCONNECT once
        # a minute and reset `blind_for_ms` on every pass -- so the one number that feeds
        # the blind-time ceiling never accumulated, precisely while the account was going
        # unverified. A pass counts as verification when it *compared*, not when bytes
        # arrived.
        self._note_fetch_recovered()
        self.passes += 1
        self.last_pass = result
        if result.mismatches:
            self._halt_on(result)
        return result

    # ---------------------------------------------------------------------- comparing

    def _compare(
        self,
        ts_ms: int,
        account: Mapping[str, Any],
        positions: Mapping[str, Sequence[Mapping[str, Any]]],
    ) -> ReconciliationPass:
        checks: list[FieldCheck] = []
        skipped: list[tuple[str, str]] = []

        checks.append(
            FieldCheck(
                field="wallet_balance",
                symbol=None,
                ours=self._engine.account.wallet,
                theirs=_wallet_balance(account),
                tolerance=WALLET_EPSILON,
            )
        )

        for symbol in self.symbols:
            self._compare_symbol(symbol, positions.get(symbol, ()), checks, skipped)

        # Spec 6.7.3's order, not the order the symbols happened to be iterated in. See
        # `RECONCILED_FIELDS`: only the first mismatch is recorded as the halt's cause.
        checks.sort(key=lambda check: RECONCILED_FIELDS.index(check.field))
        return ReconciliationPass(
            ts_ms=ts_ms,
            fetched=True,
            checks=tuple(checks),
            skipped=tuple(skipped),
        )

    def _compare_symbol(
        self,
        symbol: str,
        rows: Sequence[Mapping[str, Any]],
        checks: list[FieldCheck],
        skipped: list[tuple[str, str]],
    ) -> None:
        """The four per-symbol quantities, or a recorded reason for not comparing them.

        **Once per addressable position, not once per symbol.** In hedge mode Binance returns
        a `LONG` row and a `SHORT` row for the same symbol, and each is a separate position
        with its own entry price, unrealised PnL and liquidation price. Comparing the ledger's
        long against whichever row came first would reconcile against half the account and
        report agreement -- the exact failure spec 6.7.3 exists to catch, arriving through the
        check itself.
        """
        filters = self._engine.filters.get(symbol)
        if filters is None:
            skipped.append(
                (
                    "position_size",
                    f"{symbol}: no exchange filters loaded, so there is no stepSize or "
                    "tickSize to set a tolerance from",
                )
            )
            return

        _assert_mode_matches(symbol, rows, self._engine._sides)

        # **Half a step and half a tick, not a whole one.** Sizes and prices are quantised
        # to the same grid on both sides, so any *real* divergence is at least one full
        # step -- and `matched` is `delta <= tolerance`, so a tolerance of exactly one
        # step meant the smallest possible missed fill (one lot) produced a delta exactly
        # equal to the tolerance and passed, forever. The check's own docstring claimed
        # the opposite. Half the grid interval keeps what the tolerance was for --
        # representation dust below the grid, which cannot be a position -- while the
        # smallest divergence the venue can express now fails.
        with accounting():
            step = from_scaled(filters.step_size) / 2
            tick = from_scaled(filters.tick_size) / 2
        for side in self._engine._sides:
            self._compare_side(symbol, side, rows, step, tick, checks, skipped)

    def _compare_side(
        self,
        symbol: str,
        side: PositionSide,
        rows: Sequence[Mapping[str, Any]],
        step: Money,
        tick: Money,
        checks: list[FieldCheck],
        skipped: list[tuple[str, str]],
    ) -> None:
        """The four quantities for one `(symbol, position side)`."""
        label = position_label(symbol, side)
        row = _position_row(symbol, side, rows)
        if row is None:
            skipped.append(
                ("position_size", f"{label}: positionRisk returned no row for the position")
            )
            return

        ours_qty = self._engine.account.qty(symbol, side)
        theirs_qty = parse_money(str(row["positionAmt"]))
        checks.append(
            FieldCheck(
                field="position_size",
                symbol=label,
                ours=ours_qty,
                theirs=theirs_qty,
                tolerance=step,
            )
        )

        position = self._engine.account.position(symbol, side)
        if position is None and theirs_qty == 0:
            # Flat on both sides. Entry price, unrealised PnL and liquidation price are all
            # undefined here and the exchange reports them as literal zeros; comparing
            # against those would be comparing against a placeholder.
            return

        theirs_entry = parse_money(str(row["entryPrice"]))
        ours_entry = _ZERO if position is None else position.entry_price
        checks.append(
            FieldCheck(
                field="entry_price",
                symbol=label,
                ours=ours_entry,
                theirs=theirs_entry,
                tolerance=tick,
            )
        )

        self._compare_unrealized(
            symbol, label, side, row, position, ours_qty, tick, checks, skipped
        )
        self._compare_liquidation(symbol, label, side, row, tick, checks, skipped)

    def _compare_unrealized(
        self,
        symbol: str,
        label: str,
        side: PositionSide,
        row: Mapping[str, Any],
        position: Any,
        ours_qty: Money,
        tick: Money,
        checks: list[FieldCheck],
        skipped: list[tuple[str, str]],
    ) -> None:
        """Unrealised PnL, with the mark disagreement measured rather than assumed.

        `uPnL = Q x (Pm - Pe)` (spec 3.3), so the two sides can differ for exactly three
        reasons: `Q`, which is checked at `stepSize`; `Pe`, which is checked at `tickSize`;
        and `Pm`, which the two sides sample **at different instants** -- PerpLab's mark
        comes from a 1 Hz `premiumIndex` poll (`data.rest_poller.MarkPricePoller`, which
        exists because every `@markPrice` stream is suppressed on this endpoint) and the
        exchange's is whatever it held when it built the response.

        A fixed epsilon here would be wrong in both directions at once. On a 1 BTC position a
        single tick of mark drift is 0.10 of PnL, so a tight epsilon fires constantly on a
        mark that is doing exactly what spec 3.4 says it does; and on a small position a loose
        one admits a genuine error. So the mark difference is *measured* -- `positionRisk`
        reports the exchange's own `markPrice` in the same payload -- and the tolerance is
        the PnL that difference alone accounts for, plus one tick of entry-price rounding,
        plus `WALLET_EPSILON`. What is left over is a disagreement about `Q` or `Pe`, which
        is what this field is here to catch.

        The failure mode of that construction is a *stale* mark on our side: the tolerance
        grows with the drift, so a mark poller that died would make this check vacuous. It
        would not make it wrong -- position size and entry price are checked independently
        and neither depends on the mark -- and a dead mark poller is separately a
        `CollectorEventKind.STALE` event on the `markPrice` dataset.
        """
        ours_mark = self._engine.account.marks.get(symbol)
        if ours_mark is None:
            skipped.append(
                (
                    "unrealized_pnl",
                    f"{label}: no mark price has been recorded yet, so PerpLab's own "
                    "unrealised PnL is not defined (spec 3.4 forbids deriving one)",
                )
            )
            return
        theirs_mark = parse_money(str(row["markPrice"]))
        ours_pnl = _ZERO if position is None else position.unrealized_pnl(ours_mark)
        theirs_pnl = parse_money(str(row["unRealizedProfit"]))
        with accounting():
            tolerance = (
                abs(ours_qty) * abs(ours_mark - theirs_mark)
                + abs(ours_qty) * tick
                + WALLET_EPSILON
            )
        checks.append(
            FieldCheck(
                field="unrealized_pnl",
                symbol=label,
                ours=ours_pnl,
                theirs=theirs_pnl,
                tolerance=tolerance,
            )
        )

    def _compare_liquidation(
        self,
        symbol: str,
        label: str,
        side: PositionSide,
        row: Mapping[str, Any],
        tick: Money,
        checks: list[FieldCheck],
        skipped: list[tuple[str, str]],
    ) -> None:
        """Liquidation price, at the loosest tolerance of the five.

        See `LIQUIDATION_TOLERANCE_FRACTION` for why. Two cases are skipped rather than
        compared, and both are "we could not compute one" rather than "we agree":

        - PerpLab has no bracket table for the symbol, or the fixed-point iteration found no
          reachable price. `Account.liquidation_price` answers `None` for both, and `None`
          is not zero -- a sentinel compared against the exchange's figure would produce a
          halt whose cause is a missing snapshot.
        - Binance reports `0`, which is what it sends for a position it considers
          unliquidatable at the current margin. Comparing our real number against that zero
          would report a mismatch of the whole price.
        """
        try:
            ours_liq = self._engine.account.liquidation_price(symbol, side)
        except LookupError:
            ours_liq = None
        theirs_liq = parse_money(str(row["liquidationPrice"]))
        if ours_liq is None:
            skipped.append(
                (
                    "liquidation_price",
                    f"{label}: PerpLab has no liquidation price for this position -- no "
                    "bracket table loaded, or no reachable solution (spec 3.6)",
                )
            )
            return
        if theirs_liq == 0:
            skipped.append(
                (
                    "liquidation_price",
                    f"{symbol}: the exchange reports a liquidation price of 0, which is its "
                    "encoding for 'not liquidatable at this margin' rather than a price",
                )
            )
            return
        with accounting():
            tolerance = max(theirs_liq * LIQUIDATION_TOLERANCE_FRACTION, tick)
        checks.append(
            FieldCheck(
                field="liquidation_price",
                symbol=label,
                ours=ours_liq,
                theirs=theirs_liq,
                tolerance=tolerance,
            )
        )

    # ------------------------------------------------------------------------ verdicts

    def _halt_on(self, result: ReconciliationPass) -> None:
        """Hand each mismatch to the risk layer and ask the engine to halt on the first.

        Every mismatch is reported, not only the halting one, because a missed fill moves
        several of the five at once and the set of them is the diagnosis. Only the first
        produces a halt: `RiskEngine._halt` records one breach per incident on purpose, and
        `BacktestEngine.request_halt` performs the halt between events so that the ledger
        stays consistent. Nothing here halts anything directly.
        """
        self.mismatches += len(result.mismatches)
        requested = False
        for check in result.mismatches:
            breach = self._engine.risk.observe_reconciliation(
                result.ts_ms, check.field, check.ours, check.theirs, check.tolerance
            )
            self._event(
                CollectorEventKind.STALE,
                f"reconciliation mismatch on {check.field}"
                + (f" ({check.symbol})" if check.symbol else "")
                + f": ours {money_to_str(check.ours)}, exchange "
                f"{money_to_str(check.theirs)}, difference {money_to_str(check.delta)} "
                f"against a tolerance of {money_to_str(check.tolerance)}",
            )
            if breach is not None and not requested:
                requested = True
                self._engine.request_halt(breach)

    def _note_fetch_failure(
        self, ts_ms: int, exc: BaseException, *, phase: str = "fetching account state"
    ) -> ReconciliationPass:
        """Record an outage against the loop itself. **Nothing is compared and nothing halts.**

        See the module docstring: one timeout says nothing about the account, and a kill
        switch that fires for a network blip is a kill switch an operator turns off.
        """
        self.fetch_failures += 1
        self.consecutive_failures += 1
        if self._blind_since_ms is None:
            self._blind_since_ms = ts_ms
        detail = f"{phase} failed ({self.consecutive_failures} consecutive): " + (
            f"{type(exc).__name__}: {exc}"
        )
        self._event(CollectorEventKind.DISCONNECT, detail)
        result = ReconciliationPass(ts_ms=ts_ms, fetched=False, error=detail)
        self.last_pass = result
        # The ceiling on how long "not checked" may last (H7). One failure is weather;
        # ten minutes of failures is a broken credential or a broken venue, and either
        # way the ledger the strategy is sizing against is unverified. The halt goes
        # through the same request path a mismatch takes, so the settle sequence, the
        # kill-switch record and the run row all read identically.
        breach = self._engine.risk.observe_verification_outage(
            ts_ms, ts_ms - self._blind_since_ms, MAX_BLIND_MS
        )
        if breach is not None:
            self._engine.request_halt(breach)
        return result

    def _note_fetch_recovered(self) -> None:
        if not self.consecutive_failures:
            return
        downtime = self._now_ms() - (self._blind_since_ms or self._now_ms())
        self._event(
            CollectorEventKind.RECONNECT,
            f"reconciliation recovered after {self.consecutive_failures} consecutive "
            f"failure(s); the account was unverified for {downtime} ms",
            downtime_ms=downtime,
        )
        self.consecutive_failures = 0
        self._blind_since_ms = None

    # ------------------------------------------------------------------------- reading

    @property
    def blind_for_ms(self) -> int:
        """How long the account has gone unverified, or 0 while the loop is healthy.

        The number an operator needs in order to make the decision this module refuses to
        make for them. A session that has not been able to reach the exchange for ten
        minutes is not in breach of anything, and is also not being checked.
        """
        if self._blind_since_ms is None:
            return 0
        return max(0, self._now_ms() - self._blind_since_ms)

    def summary(self) -> dict[str, Any]:
        """A snapshot for the live monitor (spec 10.3)."""
        return {
            "interval_s": self.interval_s,
            "passes": self.passes,
            "mismatches": self.mismatches,
            "fetch_failures": self.fetch_failures,
            "consecutive_failures": self.consecutive_failures,
            "blind_for_ms": self.blind_for_ms,
            "last_pass": None if self.last_pass is None else self.last_pass.to_json(),
        }

    # ----------------------------------------------------------------------- internals

    def _now_ms(self) -> int:
        """The engine's clock, not the wall clock.

        Every breach in a run shares one timeline, and `PaperSession._observe_disconnect`
        already stamps spec 7's other live-only trigger this way. A `RiskBreach` stamped from
        `time.time()` would sort against event-log entries stamped from exchange event time,
        and the kill-switch record would land in the wrong place in the run log -- which is
        the one artefact whose ordering an incident review depends on.

        The cost is that on a quiet market the stamp can lag the moment the mismatch was
        observed by as long as the gap between market events. That is a known and bounded
        inaccuracy in a timestamp; mixing two clocks in one ordered log is neither.
        """
        return self._engine.runtime.now_ms

    def _event(
        self, kind: CollectorEventKind, detail: str, *, downtime_ms: int = 0
    ) -> None:
        self._on_event(kind, RECONCILE_LABEL, detail, downtime_ms)


_ZERO = parse_money("0")


def _wallet_balance(account: Mapping[str, Any]) -> Money:
    """`totalWalletBalance` out of a `GET /fapi/v2/account` payload.

    The same field `SignedRestClient.validate` shows at key entry, read the same way: as a
    decimal string, never through `float`, because this value is one side of a comparison
    whose tolerance is one cent.

    **Read as the single-asset figure it is for a USDT-margined account.** On an account with
    multi-assets mode enabled Binance reports this converted to USD across every collateral
    asset, and PerpLab's ledger is one wallet in the quote currency (`core.account`) -- so
    the two would be comparing different quantities and this check would fire every pass.
    Multi-assets mode is out of scope for v1 and arrives as an `ACCOUNT_CONFIG_UPDATE` if it
    is switched on mid-session, which is exactly the event `exchange.userstream` surfaces.
    This has not been verified against a real account; see the module docstring.
    """
    try:
        return parse_money(str(account["totalWalletBalance"]))
    except KeyError:
        raise KeyError(
            "the account payload carried no totalWalletBalance, so there is nothing to "
            "reconcile the wallet against. Refusing to fall back to availableBalance: it is "
            "a different quantity (wallet less allocated margin) and comparing it against "
            "the ledger's wallet would report a mismatch equal to the margin in use."
        ) from None


def _assert_mode_matches(
    symbol: str,
    rows: Sequence[Mapping[str, Any]],
    sides: Sequence[PositionSide],
) -> None:
    """Refuse to reconcile a run against an account in a different position mode.

    The mismatch is silent in both directions if it is not checked, and the silence is what
    makes it dangerous. A **one-way run against a hedge account** finds no `BOTH` row, so
    every per-side lookup returns `None` and the pass records five skips and zero
    disagreements -- a session trading against two exchange positions it does not model,
    reporting "nothing to compare" once a minute. A **hedge run against a one-way account**
    is the mirror: the single `BOTH` row matches neither leg, both legs skip, and the ledger's
    two positions are reconciled against nothing.

    `live.preflight.configure_account` catches this before the first order and is the right
    place for it -- but it runs once, and the account can be switched by hand in the Binance
    app mid-session. This is the standing check, and it raises rather than skipping so that
    `check_once` records an *unfetchable* pass with the reason attached, which is the same
    treatment a renamed JSON field gets and for the same reason: PerpLab cannot tell whether
    the account agrees, and saying so is not the same as saying it does.
    """
    seen = {
        _row_side(row, PositionSide.BOTH)
        for row in rows
        if str(row.get("symbol", symbol)) == symbol
    }
    if not seen:
        return
    expected = set(sides)
    if seen == expected or seen <= expected:
        return
    ours = "hedge" if PositionSide.BOTH not in expected else "one-way"
    theirs = "hedge" if PositionSide.BOTH not in seen else "one-way"
    raise ValueError(
        f"{symbol}: this run's ledger is in {ours} mode (position sides "
        f"{sorted(s.value for s in expected)}) and positionRisk reports "
        f"{sorted(s.value for s in seen)}, which is a {theirs} account. There is no "
        "correspondence between the two sets of positions, so nothing here can be "
        "reconciled -- and a pass that quietly compared nothing would report agreement. "
        "Stop the session and start one whose position mode matches the account."
    )


def _position_row(
    symbol: str, side: PositionSide, rows: Sequence[Mapping[str, Any]]
) -> Mapping[str, Any] | None:
    """The `positionRisk` row for one `(symbol, position side)`, or `None` if absent.

    Binance returns **one row per position side**: a single `BOTH` row for a one-way account
    and a `LONG` plus a `SHORT` row for a hedge one. Matching on the symbol alone and taking
    the first would reconcile a hedged ledger's long leg against whichever row the exchange
    happened to serialise first -- agreeing with half the account and reporting agreement,
    which is worse than not checking at all.

    Two rows for the *same* side is refused rather than resolved. That is not a shape Binance
    documents, so it means either the account is in a mode the ledger does not model or the
    response was misread, and both of those are "stop" rather than "pick one". The refusal is
    a `ValueError`, which `_compare_symbol`'s caller records as a skipped comparison with the
    reason attached rather than as an agreement.

    An empty result is a legitimate `None`: Binance omits rows for symbols the account has
    never held. The caller records that as a skip, so "no row" never reads as "matches".
    """
    matching = [
        row
        for row in rows
        if str(row.get("symbol", symbol)) == symbol
        and _row_side(row, side) is side
    ]
    if not matching:
        return None
    if len(matching) > 1:
        raise ValueError(
            f"positionRisk returned {len(matching)} rows for {symbol} on the "
            f"{side.value} side. Exactly one row per position side is the documented "
            "shape, so this response is either from an account mode PerpLab does not "
            "model or has been misread; refusing to reconcile against a guess."
        )
    return matching[0]


def _row_side(row: Mapping[str, Any], expected: PositionSide) -> PositionSide:
    """The row's `positionSide`, defaulting to `expected` when the field is absent.

    The default is deliberate and narrow. Binance has served `positionRisk` without a
    `positionSide` field on one-way accounts, where `BOTH` is the only possibility and the
    omission is unambiguous. Defaulting to the side being *asked for* rather than to `BOTH`
    keeps that working while making a hedge lookup against a field-less row fail to match --
    which is the safe direction, because it produces a recorded skip rather than a silent
    comparison of the long leg against an unlabelled row.
    """
    raw = row.get("positionSide")
    if raw in (None, ""):
        return expected
    try:
        return PositionSide(str(raw).strip().upper())
    except ValueError:
        # An unknown side is not `expected`. Returning a distinct value makes the row fail
        # to match anything, so the comparison is skipped with a reason rather than run
        # against a row whose meaning is unknown.
        return PositionSide.BOTH if expected is not PositionSide.BOTH else PositionSide.LONG


async def _sleep_unless_stopped(stop: asyncio.Event, delay: float) -> bool:
    """Wait `delay` seconds. Returns True if `stop` was set instead of the wait elapsing."""
    try:
        await asyncio.wait_for(stop.wait(), timeout=delay)
    except TimeoutError:
        return False
    return True

"""Spec 6.7.3's loop: five quantities, five tolerances, and one kill switch.

This is the check that decides whether a live session keeps trading, so the two ways of
getting it wrong are opposite and equally bad:

- **Too tight, and it cries wolf.** Every one of the five can legitimately differ from the
  exchange's figure -- by a rounding step, by a tick, by the fraction of a second between two
  mark samples, by whatever bracket table the exchange resolved against. A check that halts
  on those halts a healthy session, and an operator who has watched the kill switch fire for
  a rounding difference will switch it off. Every field is therefore tested **at** its
  tolerance as well as just past it, because a comparison written the wrong way round still
  fires on a gross breach and is invisible to a test that only uses gross breaches.
- **Too loose, and it misses the thing it exists for.** A missed fill or a dropped
  user-data-stream message is exactly what spec 6.7.3 names, and it moves position size by a
  whole `stepSize` at least.

And one distinction that is not about tolerance at all: **a failure to fetch is not a
mismatch.** One timeout says nothing about the account. Halting on it would be the same
cry-wolf failure arriving by a different route, so it is recorded as an outage against the
loop and compared against nothing.

**No network.** The signed client is a fake returning payloads written by hand in Binance's
own field names. Nothing here has spoken to a real account, and neither has the code it
tests -- see `perplab.live.reconcile`'s own docstring.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest

from perplab.core.money import parse_money, to_scaled
from perplab.core.risk import RiskLimits
from perplab.core.types import CollectorEventKind
from perplab.engine.backtest import BacktestConfig, BacktestEngine
from perplab.engine.clock import Event, EventKind
from perplab.engine.feed import MarkBar
from perplab.engine.latency import FixedLatency
from perplab.live.reconcile import (
    LIQUIDATION_TOLERANCE_FRACTION,
    RECONCILED_FIELDS,
    WALLET_EPSILON,
    Reconciler,
)
from perplab.strategy.base import Strategy
from perplab.strategy.context import FillTier
from tests.support import btcusdt_filters, single_bracket_table

START = 1_709_251_200_000
SYMBOL = "BTCUSDT"

WALLET = "1000000"
"""The ledger's opening balance, and the exchange's agreeing figure in every clean pass."""

ENTRY = "40000"
MARK = "40100"
QTY = "1"
"""One BTC bought at 40 000, marked at 40 100, so unrealised PnL is 1 x 100 = 100."""


def money(text: str) -> Decimal:
    return parse_money(text)


class Quiet(Strategy):
    requires = {
        "symbols": [SYMBOL],
        "timeframe": "1m",
        "history": 0,
        "datasets": ["klines", "bookTicker"],
    }


class NoSource:
    """A `MarketSource` supplying nothing, so the engine needs no lake."""

    def prepare(self, engine: BacktestEngine) -> Any:
        from perplab.engine.source import Prepared

        return Prepared(streams=(), data_start_ms=engine.config.start_ms, total_bars=0)

    def close(self) -> None:
        """Nothing to release."""


class FakeAccountClient:
    """Stands in for `SignedRestClient`, and reaches no network at all.

    Payload field names are Binance's own (`totalWalletBalance`, `positionAmt`, `entryPrice`,
    `markPrice`, `unRealizedProfit`, `liquidationPrice`), because those names are the part of
    this module that cannot be verified without an account -- so the test at least pins which
    names the code reads.
    """

    # The drift surface the reconciler re-measures each pass (H10). `None` models a
    # healthy clock; a test that wants the warning path assigns a string.
    clock_drift_warning: str | None = None

    def __init__(self, **overrides: str) -> None:
        self.wallet = overrides.pop("wallet", WALLET)
        self.position_amt = overrides.pop("positionAmt", QTY)
        self.entry_price = overrides.pop("entryPrice", ENTRY)
        self.mark_price = overrides.pop("markPrice", MARK)
        self.unrealized = overrides.pop("unRealizedProfit", "100")
        self.liquidation = overrides.pop("liquidationPrice", "36000")
        self.extra_rows: list[dict[str, Any]] = []
        self.time_calls = 0
        self.drop_wallet_field = False
        self.fail_next: BaseException | None = None
        self.account_calls = 0
        self.position_calls = 0
        assert not overrides, f"unknown payload override: {sorted(overrides)}"

    async def server_time_ms(self) -> int:
        self.time_calls += 1
        return 1_700_000_000_000

    def _maybe_fail(self) -> None:
        if self.fail_next is not None:
            error, self.fail_next = self.fail_next, None
            raise error

    async def account(self) -> dict[str, Any]:
        self.account_calls += 1
        self._maybe_fail()
        payload: dict[str, Any] = {"availableBalance": self.wallet}
        if not self.drop_wallet_field:
            payload["totalWalletBalance"] = self.wallet
        return payload

    async def position_risk(self, symbol: str) -> list[dict[str, Any]]:
        self.position_calls += 1
        self._maybe_fail()
        return [
            {
                "symbol": symbol,
                "positionAmt": self.position_amt,
                "entryPrice": self.entry_price,
                "markPrice": self.mark_price,
                "unRealizedProfit": self.unrealized,
                "liquidationPrice": self.liquidation,
                "positionSide": "BOTH",
            },
            *self.extra_rows,
        ]


class Events:
    def __init__(self) -> None:
        self.entries: list[tuple[CollectorEventKind, str, str, int]] = []

    def __call__(
        self, kind: CollectorEventKind, stream: str, detail: str, downtime_ms: int
    ) -> None:
        self.entries.append((kind, stream, detail, downtime_ms))

    def kinds(self) -> list[CollectorEventKind]:
        return [entry[0] for entry in self.entries]

    def details(self) -> str:
        return "\n".join(entry[2] for entry in self.entries)


def build(
    tmp_path: Path, *, flat: bool = False, **payload: str
) -> tuple[BacktestEngine, FakeAccountClient, Reconciler, Events]:
    """A real engine holding a real position, against a fake exchange that agrees with it.

    The engine is the genuine `BacktestEngine`, so `observe_reconciliation`,
    `request_halt` and the kill-switch record are the production ones. The position is
    established with a real `apply_fill` rather than by writing state, which is what makes
    `entry_price`, `unrealized_pnl` and `liquidation_price` derived numbers rather than
    fixtures agreeing with themselves.

    On the clean path the exchange's payload matches: 1 BTC at an entry of 40 000, marked at
    40 100, unrealised 100. The wallet is 1 000 000 less the 20 taker fee on the entry
    (40 000 x 0.0005), and the fake is told that figure, so a clean pass is genuinely clean.
    """
    strategy = Quiet()
    config = BacktestConfig(
        symbols=(SYMBOL,),
        timeframe="1m",
        start_ms=START,
        end_ms=START + 60_000,
        seed=1,
        opening_balance=money(WALLET),
        leverage=10,
        latency=FixedLatency(submit=10, cancel=10),
        fill_tier=FillTier.BOOK_TICKER,
        risk=RiskLimits.unlimited(),
    )
    engine = BacktestEngine(
        root=tmp_path,
        strategy=strategy,
        requirements=strategy.declared,
        config=config,
        filters={SYMBOL: btcusdt_filters()},
        brackets={SYMBOL: single_bracket_table(mmr=money("0.004"))},
        source=NoSource(),
    )
    engine.runtime.advance(START)
    engine.account.update_mark(START, SYMBOL, money(ENTRY))
    if not flat:
        engine.account.apply_fill(START, SYMBOL, money(QTY), money(ENTRY), is_maker=False)
    engine.account.update_mark(START, SYMBOL, money(MARK))

    defaults: dict[str, str] = {"wallet": str(engine.account.wallet)}
    if flat:
        defaults.update(
            {"positionAmt": "0", "entryPrice": "0", "unRealizedProfit": "0",
             "liquidationPrice": "0"}
        )
    else:
        liq = engine.account.liquidation_price(SYMBOL)
        assert liq is not None
        defaults["liquidationPrice"] = str(liq)
    defaults.update(payload)

    client = FakeAccountClient(**defaults)
    events = Events()
    return engine, client, Reconciler(engine, client, on_event=events), events


# ============================================================== the clean baseline


@pytest.mark.asyncio
async def test_an_account_that_agrees_produces_no_breach_and_no_halt(
    tmp_path: Path,
) -> None:
    """The baseline every other test is a perturbation of.

    Five comparisons are made -- wallet, position size, entry price, unrealised PnL and
    liquidation price -- all inside tolerance, so nothing is skipped, nothing mismatches and
    the kill switch stays untripped. Without this, a bug that skipped every field would make
    all the "does not halt" tests below pass for the wrong reason.
    """
    engine, _client, reconciler, events = build(tmp_path)
    result = await reconciler.check_once()

    assert result.fetched is True
    assert result.skipped == ()
    assert [check.field for check in result.checks] == list(RECONCILED_FIELDS)
    assert result.mismatches == ()
    assert engine.risk.halted is False
    assert engine.halted is False
    assert events.entries == []


@pytest.mark.asyncio
async def test_the_five_fields_are_compared_in_the_order_spec_673_names_them(
    tmp_path: Path,
) -> None:
    """`RiskEngine._halt` records the first breach only, so the order decides the diagnosis.

    A missed fill moves all five at once. Entry price, unrealised PnL and liquidation price
    are all *derived* from position size, so leading with one of them would write a symptom
    into the run log in place of its cause. Here every field is wrong at once and the breach
    that stops the session is the wallet, then position size -- the two an operator can act
    on -- rather than whichever happened to be built first.
    """
    engine, _client, reconciler, _ = build(
        tmp_path,
        wallet="900000",
        positionAmt="2",
        entryPrice="41000",
        unRealizedProfit="9999",
        liquidationPrice="20000",
    )
    result = await reconciler.check_once()

    assert [check.field for check in result.checks] == list(RECONCILED_FIELDS)
    assert len(result.mismatches) == 5
    assert engine.risk.halt_breach is not None
    assert "wallet_balance" in engine.risk.halt_breach.detail


# ======================================================== field by field, at the edge


@pytest.mark.asyncio
async def test_a_wallet_within_the_epsilon_is_not_a_mismatch(tmp_path: Path) -> None:
    """The ledger says 999 980; the exchange says 999 980.01. Difference 0.01, epsilon 0.01.

    A tolerance is a ceiling, so equality passes (`core.risk`'s rule for every size limit).
    Both sides compute this figure independently over thousands of fee and funding terms, so
    a tolerance of exactly zero would fire on the arithmetic rather than on the account.
    """
    engine, client, reconciler, _ = build(tmp_path)
    client.wallet = str(engine.account.wallet + WALLET_EPSILON)

    result = await reconciler.check_once()
    assert result.mismatches == ()
    assert engine.risk.halted is False


@pytest.mark.asyncio
async def test_a_wallet_one_cent_past_the_epsilon_halts_the_session(
    tmp_path: Path,
) -> None:
    """999 980 against 999 980.02 is 0.02, twice a 0.01 epsilon.

    The smallest disagreement past the ceiling has to fire, or the ceiling is decoration.
    """
    engine, client, reconciler, events = build(tmp_path)
    client.wallet = str(engine.account.wallet + money("0.02"))

    result = await reconciler.check_once()
    assert [check.field for check in result.mismatches] == ["wallet_balance"]
    assert engine.risk.halted is True
    assert engine.risk.kill_switch.trigger == "RECONCILIATION"
    assert "wallet_balance" in events.details()


@pytest.mark.asyncio
async def test_a_position_sub_step_residue_is_not_a_mismatch(tmp_path: Path) -> None:
    """Only representation dust below the grid passes -- and a whole step never does.

    The tolerance is *half* a `stepSize`. It was a whole one for one release, and since
    both sides quantise to the same grid, the smallest possible real divergence -- a
    missed fill of exactly one lot -- produced a delta exactly equal to the tolerance and
    passed forever (H4). Dust below the grid cannot be a position, so admitting it costs
    nothing; admitting a full step admitted the very failure the check exists for.
    """
    engine, _client, reconciler, _ = build(tmp_path, positionAmt="1.0002")
    result = await reconciler.check_once()

    assert result.mismatches == ()
    assert engine.risk.halted is False


@pytest.mark.asyncio
async def test_a_position_one_whole_step_off_halts_the_session(
    tmp_path: Path,
) -> None:
    """1.000 against 1.001 is one `stepSize`: the smallest missed fill the venue can express.

    This is the failure spec 6.7.3 is written for, at its minimum size -- the case the
    old one-step tolerance let through. The breach reports the *divergence* against the
    tolerance, because that is what was compared.
    """
    engine, _client, reconciler, _ = build(tmp_path, positionAmt="1.001")
    result = await reconciler.check_once()

    assert [check.field for check in result.mismatches] == ["position_size"]
    breach = engine.risk.halt_breach
    assert breach is not None
    assert breach.limit == "reconciliation"
    assert breach.observed == "0.00100000", "1.001 - 1.000: exactly one stepSize"
    assert breach.allowed == "0.00050000", "half a stepSize"
    assert engine.halted is False, "the engine halts between events, never from here"
    assert engine.perform_pending_halt() is True


@pytest.mark.asyncio
async def test_the_halt_a_reconciliation_asked_for_is_carried_out_exactly_once(
    tmp_path: Path,
) -> None:
    """`_halt_pending` is never cleared, so only `halted` stops the second run of it.

    The session loop calls `perform_pending_halt` after every drain, not once -- it exists
    precisely because a breach raised from outside dispatch has no event to ride, and the
    loop cannot know which pass will carry it. So the second call happens on a live session
    within a second of the first, every time.

    Performing it twice is not idempotent. `_perform_halt` cancels the book, runs the
    `on_cancel` notifications, force-flattens every open position when the switch is armed to
    and writes a `KILL_SWITCH` event. A second pass emits a second `KILL_SWITCH` -- two
    records of one trip, in the log that is hashed for reproducibility and rendered as the
    session's account of why it stopped -- and, with flatten armed, sends a second market
    order to close a position that is already closed, which on a live venue is a new position
    the opposite way.
    """
    engine, _client, reconciler, _ = build(tmp_path, positionAmt="1.002")
    await reconciler.check_once()

    assert engine.perform_pending_halt() is True
    kill_events = [event for event in engine.runtime.events if event.kind == "KILL_SWITCH"]
    assert len(kill_events) == 1
    warnings_after_first = list(engine.warnings)

    # The pending breach is still set -- nothing clears it -- so `halted` is the only guard.
    assert engine._halt_pending is not None
    assert engine.perform_pending_halt() is False
    assert [e for e in engine.runtime.events if e.kind == "KILL_SWITCH"] == kill_events
    assert engine.warnings == warnings_after_first


@pytest.mark.asyncio
async def test_a_halted_engine_refuses_to_dispatch_a_further_event(tmp_path: Path) -> None:
    """The frames already in the buffer when the switch trips must not be acted on.

    A halt is decided between events, and at that instant the session's reorder buffer is
    holding a window's worth of market data and the queue may hold order arrivals whose
    latency has elapsed. `PaperSession._drain_to_engine` pops until `step` says stop, so
    `step` refusing after a halt is the only thing standing between a tripped kill switch and
    a fill booked after it -- on a book the halt has just cancelled, against limits the risk
    layer has already declared breached.

    Asserted on the clock rather than on a return value alone: `_dispatch` advances
    `runtime.now_ms` before anything else, so an unchanged clock is proof the event was not
    merely handled quietly but never entered dispatch at all.
    """
    engine, _client, reconciler, _ = build(tmp_path, positionAmt="1.002")
    await reconciler.check_once()
    assert engine.perform_pending_halt() is True

    clock_at_halt = engine.runtime.now_ms
    events_at_halt = len(engine.runtime.events)

    mark = to_scaled("41000")
    later = Event(
        ts_ms=START + 30_000,
        kind=EventKind.MARK_PRICE_UPDATE,
        source_seq=1,
        dataset_id="markPrice:BTCUSDT",
        payload=MarkBar(
            symbol=SYMBOL, close_time=START + 30_000, high=mark, low=mark, close=mark
        ),
    )
    assert engine.step(later) is False
    assert engine.runtime.now_ms == clock_at_halt, "the halted engine advanced its clock"
    assert len(engine.runtime.events) == events_at_halt


@pytest.mark.asyncio
async def test_an_entry_price_within_half_a_tick_is_not_a_mismatch(tmp_path: Path) -> None:
    """40 000.0 against 40 000.05 is half of BTCUSDT's 0.10 `tickSize`.

    Entry price is a volume-weighted average each side rounds at its own precision, and
    the worst *rounding* can do is half a tick -- ours lands on the grid, theirs need
    not. A whole tick of disagreement is past anything rounding explains, so the old
    one-tick tolerance admitted a genuinely wrong entry (H4's twin): wrong realised PnL
    on every subsequent close, wrong liquidation price in the meantime.
    """
    engine, _client, reconciler, _ = build(tmp_path, entryPrice="40000.05")
    result = await reconciler.check_once()

    assert result.mismatches == ()
    assert engine.risk.halted is False


@pytest.mark.asyncio
async def test_an_entry_price_one_whole_tick_off_halts_the_session(
    tmp_path: Path,
) -> None:
    """40 000.0 against 40 000.1 is one full `tickSize` -- more than rounding can explain.

    This is the smallest entry-price divergence the venue's grid can express, and the
    exact case a one-tick tolerance waved through.
    """
    engine, _client, reconciler, _ = build(tmp_path, entryPrice="40000.1")
    result = await reconciler.check_once()

    assert "entry_price" in [check.field for check in result.mismatches]
    assert engine.risk.halted is True


@pytest.mark.asyncio
async def test_unrealised_pnl_absorbs_exactly_the_mark_drift_it_can_measure(
    tmp_path: Path,
) -> None:
    """The two sides sample the mark at different instants, and the payload says by how much.

    Ours: 1 BTC, mark 40 100, entry 40 000, so uPnL = 100. The exchange's mark is 40 100.50,
    half a dollar higher, so its own uPnL is 100.50. The tolerance is
    `|Q| x |mark drift| + |Q| x (tickSize / 2) + epsilon` = 1 x 0.50 + 0.05 + 0.01 = 0.56
    -- the entry-rounding term is half a tick, matching what the entry-price check itself
    now admits -- and the difference is 0.50, inside it. A fixed epsilon here would fire
    on a mark doing exactly what spec 3.4 says it does.
    """
    engine, _client, reconciler, _ = build(
        tmp_path, markPrice="40100.50", unRealizedProfit="100.50"
    )
    result = await reconciler.check_once()

    check = next(c for c in result.checks if c.field == "unrealized_pnl")
    assert check.ours == money("100")
    assert check.theirs == money("100.50")
    assert check.tolerance == money("0.56"), "1 x 0.50 + 1 x 0.05 + 0.01"
    assert check.matched is True
    assert engine.risk.halted is False


@pytest.mark.asyncio
async def test_unrealised_pnl_past_what_the_mark_drift_explains_halts_the_session(
    tmp_path: Path,
) -> None:
    """The same drift, and a PnL 0.57 away rather than 0.50.

    Tolerance is still 1 x 0.50 + 1 x 0.05 + 0.01 = 0.56, so 0.57 is the first value past it.
    What is left over once the mark difference is accounted for is a disagreement about
    quantity or entry price, which is what this field is here to catch.
    """
    engine, _client, reconciler, _ = build(
        tmp_path, markPrice="40100.50", unRealizedProfit="100.57"
    )
    result = await reconciler.check_once()

    check = next(c for c in result.checks if c.field == "unrealized_pnl")
    assert check.tolerance == money("0.56")
    assert check.delta == money("0.57")
    assert [c.field for c in result.mismatches] == ["unrealized_pnl"]
    assert engine.risk.halted is True


@pytest.mark.asyncio
async def test_a_liquidation_price_just_inside_half_a_percent_is_not_a_mismatch(
    tmp_path: Path,
) -> None:
    """The loosest of the five, and it has to be: two bracket tables, two roundings.

    Our own figure is the engine's real spec 3.7 solve on this position: 1 BTC entered at
    40 000 with 4 000 of isolated margin at a maintenance rate of 0.004, so
    `P_liq = (40 000 - 4 000) / (1 - 0.004) = 36 000 / 0.996 = 36 144.5783...`

    The exchange is told 36 326.20. Its tolerance is 36 326.20 x 0.005 = 181.6310 and the
    difference is 36 326.20 - 36 144.5783... = 181.6216..., which is inside it by 0.0093.

    **The boundary cannot be hit exactly here, and that is a property of the quantity rather
    than of the test.** `36 000 / 0.996` does not terminate, so no decimal `theirs` makes
    `|ours - theirs|` exactly `theirs x 0.005`. The next test takes the same construction
    0.80 to the other side of it -- 22 parts per million on a 36 000 price -- which brackets
    the ceiling as tightly as the arithmetic allows.
    """
    engine, _client, reconciler, _ = build(tmp_path, liquidationPrice="36326.20")
    result = await reconciler.check_once()

    check = next(c for c in result.checks if c.field == "liquidation_price")
    assert check.tolerance == money("36326.20") * LIQUIDATION_TOLERANCE_FRACTION
    assert check.tolerance == money("181.6310"), "36 326.20 x 0.005"
    assert check.delta < check.tolerance
    assert check.matched is True
    assert engine.risk.halted is False


@pytest.mark.asyncio
async def test_a_liquidation_price_just_past_half_a_percent_halts_the_session(
    tmp_path: Path,
) -> None:
    """The same construction, 0.80 further out, which crosses the ceiling.

    The exchange is told 36 327.00. Its tolerance is 36 327.00 x 0.005 = 181.6350 and the
    difference is 36 327.00 - 36 144.5783... = 182.4216..., which is past it. Widening
    `theirs` moves the difference at a rate of 1 and the tolerance at a rate of 0.005, so
    this is the direction the check actually bites in.

    What the field is for is the disagreement the other four cannot see: a leverage change
    made in the Binance app during a session moves `P_liq` by whole percent while wallet,
    size, entry and unrealised PnL all still agree.
    """
    engine, _client, reconciler, _ = build(tmp_path, liquidationPrice="36327.00")
    result = await reconciler.check_once()

    check = next(c for c in result.checks if c.field == "liquidation_price")
    assert check.tolerance == money("181.6350"), "36 327.00 x 0.005"
    assert check.delta > check.tolerance
    assert [c.field for c in result.mismatches] == ["liquidation_price"]
    assert engine.risk.halted is True
    assert engine.risk.kill_switch.trigger == "RECONCILIATION"


# =========================================================== what cannot be compared


@pytest.mark.asyncio
async def test_a_liquidation_price_the_exchange_reports_as_zero_is_skipped_not_matched(
    tmp_path: Path,
) -> None:
    """Binance sends `0` for a position it considers unliquidatable, not a price of zero.

    Comparing our real number against that zero would report a mismatch of the whole price
    and halt a session for a payload convention. Skipped and *recorded*, because "we did not
    compare this" and "we compared it and agreed" are different facts.
    """
    _engine, _client, reconciler, _ = build(tmp_path, liquidationPrice="0")
    result = await reconciler.check_once()

    assert [field for field, _why in result.skipped] == ["liquidation_price"]
    assert "liquidation_price" not in [check.field for check in result.checks]
    assert result.mismatches == ()


@pytest.mark.asyncio
async def test_an_unmarked_symbol_skips_unrealised_pnl_rather_than_guessing_one(
    tmp_path: Path,
) -> None:
    """Spec 3.4 forbids deriving a mark price, so with none recorded there is no uPnL.

    Comparing a zero against the exchange's figure would halt the session in the first
    seconds of a run, before the first mark sample has arrived.

    Liquidation price goes with it, and for the same reason: spec 3.7 solves `P_liq` against
    the mark, so with no mark there is no solution either. Two skips, not one -- and both
    recorded, because "we did not compare this" and "we compared it and agreed" are different
    facts. Wallet balance, position size and entry price are all still checked, which is what
    keeps an unmarked symbol from disabling the whole pass.
    """
    engine, _client, reconciler, _ = build(tmp_path)
    engine.account.marks.pop(SYMBOL)

    result = await reconciler.check_once()
    assert [field for field, _why in result.skipped] == [
        "unrealized_pnl",
        "liquidation_price",
    ]
    assert [check.field for check in result.checks] == [
        "wallet_balance",
        "position_size",
        "entry_price",
    ]
    assert result.mismatches == ()
    assert engine.risk.halted is False


@pytest.mark.asyncio
async def test_a_flat_account_that_the_exchange_agrees_is_flat_compares_only_the_wallet(
    tmp_path: Path,
) -> None:
    """Entry price, uPnL and `P_liq` are undefined when flat and Binance sends zeros.

    Comparing against those placeholders would halt a session that is doing nothing at all.
    Position size still is compared -- 0 against 0 -- because that is the field that would
    catch the exchange holding something we do not know about.
    """
    _engine, _client, reconciler, _ = build(tmp_path, flat=True)
    result = await reconciler.check_once()

    assert [check.field for check in result.checks] == ["wallet_balance", "position_size"]
    assert result.mismatches == ()


@pytest.mark.asyncio
async def test_a_position_the_exchange_holds_and_the_ledger_does_not_is_a_mismatch(
    tmp_path: Path,
) -> None:
    """The dangerous direction, and the one an unanswered POST produces.

    PerpLab is flat and the exchange holds 1 BTC -- which is exactly what happens after an
    `OrderOutcomeUnknown` where the order did in fact land. The 1.0 divergence is a thousand
    times a 0.001 `stepSize`.
    """
    engine, _client, reconciler, _ = build(
        tmp_path, flat=True, positionAmt="1", entryPrice="40000",
        unRealizedProfit="100", liquidationPrice="36000",
    )
    result = await reconciler.check_once()

    assert "position_size" in [check.field for check in result.mismatches]
    assert engine.risk.halted is True
    assert engine.risk.kill_switch.trigger == "RECONCILIATION"


@pytest.mark.asyncio
async def test_a_symbol_with_no_filters_is_skipped_rather_than_given_a_guessed_tolerance(
    tmp_path: Path,
) -> None:
    """No `stepSize` and no `tickSize` means no tolerance, and a guessed one is a wrong one.

    Zero would halt on any rounding; a made-up value would silently be either. The engine's
    own filter map is the source, so a reconciler that cannot find the symbol there is
    looking at a run the engine could not have quantised for either.
    """
    engine, _client, reconciler, _ = build(tmp_path)
    engine.filters.pop(SYMBOL)

    result = await reconciler.check_once()
    assert [field for field, _why in result.skipped] == ["position_size"]
    assert [check.field for check in result.checks] == ["wallet_balance"]


@pytest.mark.asyncio
async def test_a_hedge_mode_account_is_refused_rather_than_half_reconciled(
    tmp_path: Path,
) -> None:
    """`positionRisk` returns one row per position side, and `Account` is one-way only.

    Two rows for one symbol means silently taking the first would reconcile the ledger
    against half the exposure -- which would pass while the account held a net position
    nobody had checked. Refused, and refused as an *unfetchable* pass rather than a mismatch:
    the account may be perfectly consistent, and this module cannot tell.
    """
    _engine, client, reconciler, events = build(tmp_path)
    client.extra_rows = [
        {
            "symbol": SYMBOL,
            "positionAmt": "-0.5",
            "entryPrice": "40500",
            "markPrice": MARK,
            "unRealizedProfit": "-200",
            "liquidationPrice": "44000",
            "positionSide": "SHORT",
        }
    ]

    result = await reconciler.check_once()
    assert result.fetched is False
    assert "one-way mode" in result.error
    assert reconciler.fetch_failures == 1
    assert CollectorEventKind.DISCONNECT in events.kinds()


@pytest.mark.asyncio
async def test_an_account_payload_with_no_wallet_field_refuses_rather_than_substituting(
    tmp_path: Path,
) -> None:
    """`availableBalance` is wallet *less allocated margin* -- a different quantity.

    Falling back to it would report a mismatch equal to the margin in use on every pass with
    a position open, which is a halt whose stated cause is wrong. An operator sent to look at
    their wallet when the real fault is a renamed JSON key will not find anything.
    """
    engine, client, reconciler, _ = build(tmp_path)
    client.drop_wallet_field = True

    result = await reconciler.check_once()
    assert result.fetched is False
    assert "totalWalletBalance" in result.error
    assert engine.risk.halted is False, "a payload we cannot read is not a disagreement"


# ================================================= could not check vs checked and disagreed


@pytest.mark.asyncio
async def test_a_single_fetch_timeout_does_not_trip_the_kill_switch(
    tmp_path: Path,
) -> None:
    """One timeout says nothing about the account, and halting on it would be crying wolf.

    An operator who has watched the kill switch fire for a flaky link will switch it off, and
    a kill switch that has been switched off protects nothing. The pass is recorded as
    unfetched -- distinguishable from a clean pass, which is the whole point of the flag --
    and the outage is reported in the collector's own DISCONNECT vocabulary.
    """
    engine, client, reconciler, events = build(tmp_path)
    client.fail_next = httpx.ReadTimeout("timed out")

    result = await reconciler.check_once()
    assert result.fetched is False
    assert result.checks == ()
    assert engine.risk.halted is False
    assert engine.halted is False
    assert reconciler.fetch_failures == 1
    assert reconciler.consecutive_failures == 1
    assert reconciler.passes == 0, "a pass that fetched nothing is not a pass"
    assert events.kinds() == [CollectorEventKind.DISCONNECT]


@pytest.mark.asyncio
async def test_repeated_fetch_failures_still_do_not_halt_but_are_impossible_to_miss(
    tmp_path: Path,
) -> None:
    """Three consecutive failures are three minutes of an unverified account.

    That is a real problem and it is still not a *breach*: the account may be fine, and the
    decision to stop belongs to the operator rather than to a network. What the module owes
    them instead is the number -- `consecutive_failures` and `blind_for_ms` -- reported
    rather than inferred.
    """
    engine, client, reconciler, _ = build(tmp_path)
    for _ in range(3):
        client.fail_next = httpx.ConnectError("no route to host")
        await reconciler.check_once()

    assert reconciler.consecutive_failures == 3
    assert reconciler.fetch_failures == 3
    assert engine.risk.halted is False
    assert reconciler.summary()["consecutive_failures"] == 3


@pytest.mark.asyncio
async def test_a_recovered_fetch_reports_how_long_the_account_was_unverified(
    tmp_path: Path,
) -> None:
    """The RECONNECT half of the pair `data.rest_poller` writes, for the same reason.

    A DISCONNECT with no matching RECONNECT reads as an outage that never ended, so the
    recovery is data rather than a silent return to normal. `blind_for_ms` goes back to zero
    once the account is being checked again.
    """
    _engine, client, reconciler, events = build(tmp_path)
    client.fail_next = httpx.ReadTimeout("timed out")
    await reconciler.check_once()
    assert reconciler.blind_for_ms >= 0

    await reconciler.check_once()
    assert reconciler.consecutive_failures == 0
    assert reconciler.blind_for_ms == 0
    assert events.kinds() == [CollectorEventKind.DISCONNECT, CollectorEventKind.RECONNECT]
    assert "unverified" in events.details()


@pytest.mark.asyncio
async def test_a_timeout_followed_by_a_genuine_mismatch_still_halts(
    tmp_path: Path,
) -> None:
    """The two must not be confused in either direction.

    The point of separating them is not to make the check timid: once the fetch succeeds, a
    disagreement halts exactly as it would have without the outage before it.
    """
    engine, client, reconciler, _ = build(tmp_path, positionAmt="1.5")
    client.fail_next = httpx.ReadTimeout("timed out")
    await reconciler.check_once()
    assert engine.risk.halted is False

    await reconciler.check_once()
    assert engine.risk.halted is True
    assert engine.risk.kill_switch.trigger == "RECONCILIATION"


# ============================================================================ the loop


@pytest.mark.asyncio
async def test_the_loop_runs_on_its_interval_and_stops_when_asked(tmp_path: Path) -> None:
    """Spec 6.7.3 says every 60 s; the interval is configurable and 0.01 s here.

    What is pinned is that the loop actually reaches the exchange and actually stops, both of
    which a session depends on: a loop that never fired would leave the auto-trigger off, and
    one that never stopped would outlive the session it belongs to.
    """
    engine, client, _reconciler, events = build(tmp_path)
    reconciler = Reconciler(engine, client, on_event=events, interval_s=0.01)
    stop = asyncio.Event()
    task = asyncio.create_task(reconciler.run(stop))
    for _ in range(200):
        if reconciler.passes >= 2:
            break
        await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(task, timeout=2.0)

    assert reconciler.passes >= 2
    assert client.account_calls >= 2
    assert client.position_calls >= 2


@pytest.mark.asyncio
async def test_the_loop_stops_once_the_engine_has_halted(tmp_path: Path) -> None:
    """A halted run has stopped trading, so there is nothing left to reconcile against.

    Continuing would spend rate-limit weight on an account the session no longer drives, and
    would raise a second breach for an incident that already has one.
    """
    engine, client, _reconciler, events = build(tmp_path)
    reconciler = Reconciler(engine, client, on_event=events, interval_s=0.01)
    engine.halted = True
    stop = asyncio.Event()

    await asyncio.wait_for(reconciler.run(stop), timeout=2.0)
    assert reconciler.passes == 0
    assert client.account_calls == 0


def test_a_non_positive_interval_is_refused(tmp_path: Path) -> None:
    """Zero would spin against a weight-limited endpoint.

    The ban that earns takes the account offline with positions open, which is the one thing
    a risk check must never cause.
    """
    engine, client, _reconciler, events = build(tmp_path)
    with pytest.raises(ValueError, match="interval_s must be positive"):
        Reconciler(engine, client, on_event=events, interval_s=0)

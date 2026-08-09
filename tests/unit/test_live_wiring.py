"""Phase 8's wiring: what changes when the transport is a real venue, and what must not.

The seam itself (`ExchangeTransport`) is pinned in `test_exchange_transport.py`. What this
file pins is the *integration* around it -- the places where code written for a simulated
venue would quietly do the wrong thing against a real one:

- **A halt against a real venue books nothing locally.** `_perform_halt`'s simulated arm
  removes orders and prices exits from the engine's own models, which is correct when the
  book *is* the model and catastrophic when it is Binance's: a locally-booked exit leaves
  the ledger flat while the exchange holds the position, and the next thing to notice is
  the liquidation. The live arm must send cancels and real reduce-only exits through the
  transport and leave every outcome to the wire.
- **The live stack's status events never reach the market-disconnect trigger.** A
  user-stream drop or a reconciliation timeout says nothing about the market feed, and a
  session halted for a "disconnect" while its market data was healthy is spec 7's trigger
  firing on the wrong wire.
- **The worker's live setup refuses everything it cannot verify** -- a missing credential,
  a stale handoff, an unfunded wallet, a symbol that is not a clean slate -- *before* the
  account has been reconfigured, and adopts the venue's wallet as the ledger's opening
  balance, because spec 6.7.3 compares the two to the cent.

Nothing here reaches a network. The engine is the real `BacktestEngine` with the real
ledger; the venue is `FakeSignedClient` from the transport tests; the frames go through the
real `parse_user_frame`.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from perplab.core.risk import RiskAction
from perplab.core.types import CollectorEventKind, PositionSide
from perplab.strategy.context import OrderType
from perplab.engine.transport import SimulatedTransport
from perplab.exchange.keys import KeySession
from perplab.exchange.userstream import USER_STREAM_LABEL
from perplab.live import PRODUCTION_LIVE_ENABLED
from perplab.live import worker as worker_module
from perplab.live.exchange_transport import ExchangeTransport
from perplab.live.stack import LiveStack
from tests.unit.test_exchange_transport import (
    RUN_ID,
    START,
    SYMBOL,
    FakeSignedClient,
    build,
    money,
    preflight_for,
    report,
    submit,
)


def open_position(engine: Any, transport: Any, client: FakeSignedClient) -> str:
    """Open a real 1 BTC long through the wire: submit, drain, ack, fill report."""
    order_id = submit(engine, side="BUY", qty="1")
    asyncio.run(transport.drain())
    cid = client.orders[-1]["newClientOrderId"]
    transport.on_report(report(cid, status="FILLED", execution="TRADE"))
    assert engine.account.position(SYMBOL, PositionSide.BOTH) is not None
    return order_id


def trip_halt(engine: Any, *, flatten: bool) -> None:
    """Trip the kill switch through a real observation, the way a live session would."""
    engine.risk.kill_switch.flatten = flatten
    breach = engine.risk.observe_reconciliation(
        engine.runtime.now_ms, "wallet_balance", money("1"), money("2"), money("0.01")
    )
    assert breach is not None and breach.action is RiskAction.HALT
    engine.request_halt(breach)
    assert engine.perform_pending_halt()


# ==================================================================== the halt, live


def test_the_two_transports_declare_which_side_of_the_seam_they_are() -> None:
    """The halt path dispatches on this bit; a transport without it would be dispatched
    down the simulated arm and book local fills against a real account."""
    assert SimulatedTransport.simulated is True
    assert ExchangeTransport.simulated is False


def test_a_live_halt_with_flatten_books_nothing_and_sends_a_real_exit(
    tmp_path: Path,
) -> None:
    """The property the whole live arm exists for.

    After the halt: the ledger still holds the position (nothing was booked from a local
    price), a reduce-only market SELL for the full size is on the wire, and the
    KILL_SWITCH event says the exits are in flight rather than claiming a flatten that
    has not happened. When the venue's fill report then arrives, the position closes
    through `_book_fill` -- the same shared path every fill takes.
    """
    engine, client, transport, _events = build(tmp_path)
    open_position(engine, transport, client)
    fills_before = engine.counts["fills"]
    orders_on_wire = len(client.orders)

    trip_halt(engine, flatten=True)

    # Nothing booked locally: the position is still open and no fill was minted.
    assert engine.account.position(SYMBOL, PositionSide.BOTH) is not None
    assert engine.counts["fills"] == fills_before

    kill = next(e for e in engine.runtime.events if e.kind == "KILL_SWITCH")
    assert kill.payload["flattened"] == []
    assert len(kill.payload["exits_in_flight"]) == 1
    assert kill.payload["open_after"] != []

    # The exit is queued, not sent -- the session's settle path drains it.
    assert len(client.orders) == orders_on_wire
    asyncio.run(transport.drain())
    exit_params = client.orders[-1]
    assert exit_params["side"] == "SELL"
    assert exit_params["type"] == "MARKET"
    assert exit_params["reduceOnly"] is True
    assert exit_params["quantity"] == money("1")

    # The venue answers; the fill books through the shared path and the position closes.
    exit_cid = exit_params["newClientOrderId"]
    transport.on_report(report(exit_cid, status="FILLED", execution="TRADE", side="SELL"))
    assert engine.account.position(SYMBOL, PositionSide.BOTH) is None
    assert engine.counts["fills"] == fills_before + 1


def test_a_live_halt_cancels_working_orders_through_the_transport(
    tmp_path: Path,
) -> None:
    """The engine's own book keeps the order open until the venue says CANCELED.

    The simulated arm `_remove`s immediately, which against a real venue opens the window
    where a racing fill arrives, finds its order terminal, and is dropped as a duplicate.
    The live arm queues a DELETE and leaves the order working; the CANCELED report is what
    ends it.
    """
    engine, client, transport, _events = build(tmp_path)
    # 39 000 against the 40 000 mark: inside PERCENT_PRICE's 0.95 floor. The old 30 000
    # was a filter violation the live path used to let through unvalidated (C6) -- the
    # venue itself would have refused it, so the test's own premise depended on the bug.
    submit(engine, side="BUY", qty="1", order_type=OrderType.LIMIT, price="39000")
    asyncio.run(transport.drain())
    cid = client.orders[-1]["newClientOrderId"]
    transport.on_report(report(cid, status="NEW", execution="NEW", last_qty="0", cum_qty="0"))

    trip_halt(engine, flatten=False)

    order = next(iter(engine.orders.values()))
    assert order.is_open, "a live halt must not end the order before the venue does"
    asyncio.run(transport.drain())
    assert client.cancels and client.cancels[-1]["origClientOrderId"] == cid

    transport.on_report(
        report(cid, status="CANCELED", execution="CANCELED", last_qty="0", cum_qty="0")
    )
    assert not order.is_open


def test_a_transport_flatten_that_cannot_send_retires_the_order_loudly(
    tmp_path: Path,
) -> None:
    """A refused handoff must leave the book honest and the operator told.

    `place` raising (here: the stalled-queue guard) means the request never left, so the
    order is retired rather than left forever PENDING, and the warning says the exposure
    is still standing at the venue -- because it is.
    """
    engine, client, transport, _events = build(tmp_path)
    open_position(engine, transport, client)
    # Fill the queue to its ceiling so the exit's `place` refuses.
    from perplab.live.exchange_transport import MAX_UNSENT_REQUESTS, _Request

    transport._queue.extend(
        _Request("cancel", next(iter(engine.orders.values())))
        for _ in range(MAX_UNSENT_REQUESTS)
    )

    order_id = engine._transport_flatten(
        engine.runtime.now_ms, (SYMBOL, PositionSide.BOTH), reason="halt:test"
    )
    assert order_id is None
    assert engine.account.position(SYMBOL, PositionSide.BOTH) is not None
    assert any("still open at the venue" in w for w in engine.warnings)
    assert all(not o.is_open or o.id == "o1" for o in engine.orders.values())


def test_a_fill_booked_ahead_of_the_market_frontier_does_not_break_the_ledger(
    tmp_path: Path,
) -> None:
    """The two-clock hazard: execution reports run ahead of dispatched market data.

    A fill's exchange time is routinely ahead of the mark bar still working through the
    reorder window. Booking at the report's own time put the ledger's clock ahead of the
    next market event, and `Account._touch` then raised an I8 InvariantViolation on a
    perfectly healthy session -- which abandoned the run, skipped venue cleanup, and
    armed the kill switch against an account that never disagreed with anything. Fills
    book at the engine's own frontier instead, so nothing that follows can be behind.
    """
    engine, client, transport, _events = build(tmp_path)
    order_id = submit(engine, side="BUY", qty="1")
    asyncio.run(transport.drain())
    cid = client.orders[-1]["newClientOrderId"]

    # The venue's report is stamped 800 ms ahead of anything the engine has dispatched.
    transport.on_report(
        report(cid, status="FILLED", execution="TRADE", ts_ms=START + 800)
    )
    assert engine.account.position(SYMBOL, PositionSide.BOTH) is not None

    # The mark bar for the *earlier* minute now arrives -- late, as reality delivers it.
    # Booking at the report's own time made this raise InvariantViolation (I8).
    engine.account.update_mark(START + 100, SYMBOL, money("40100"))
    assert engine.account.marks[SYMBOL] == money("40100")


def test_the_final_taker_increment_of_a_split_fill_books_despite_admission_floors(
    tmp_path: Path,
) -> None:
    """A venue increment is a fact, not an application.

    A 0.003 BTC order filling 0.002-then-0.001 had its final increment (== remaining) run
    the whole-order `MIN_NOTIONAL` floor: 0.001 x 40 000 = 40 < 50 -> the order was
    marked REJECTED locally while FILLED at the venue, the ledger held 0.002 against the
    venue's 0.003, and every later report was dropped as a duplicate.
    """
    engine, client, transport, _events = build(tmp_path)
    submit(engine, side="BUY", qty="0.003")
    asyncio.run(transport.drain())
    cid = client.orders[-1]["newClientOrderId"]

    transport.on_report(
        report(cid, execution="TRADE", status="PARTIALLY_FILLED",
               last_qty="0.002", cum_qty="0.002")
    )
    transport.on_report(
        report(cid, execution="TRADE", status="FILLED",
               last_qty="0.001", cum_qty="0.003")
    )
    position = engine.account.position(SYMBOL, PositionSide.BOTH)
    assert position is not None and position.qty == money("0.003")
    assert transport.fills_booked == 2
    assert transport.rejections == 0


def test_the_simulated_liquidation_model_books_nothing_against_a_real_venue(
    tmp_path: Path,
) -> None:
    """The venue owns liquidation; the local model owns a warning.

    The probe used to close the position at a modelled trigger price with a modelled
    haircut and locally cancel the symbol's orders -- which were still working at the
    exchange. The ledger went flat on fiction while the venue held the position. Live,
    the crossing is reported loudly and nothing is booked.
    """
    engine, client, transport, _events = build(tmp_path)
    open_position(engine, transport, client)
    liq = engine.account.liquidation_price(SYMBOL, PositionSide.BOTH)
    assert liq is not None, "a 10x long has a liquidation price"

    # A mark bar whose range crosses the ledger's own P_liq, by a wide margin.
    below = liq - money("100")
    engine.pending_range = {SYMBOL: (below, money("40000"), money("40000"))}
    engine._on_liquidation_check(engine.runtime.now_ms + 60_000)

    assert engine.account.position(SYMBOL, PositionSide.BOTH) is not None, (
        "the live arm must not book a modelled liquidation"
    )
    assert engine.counts["liquidations"] == 0
    proximity = [e for e in engine.runtime.events if e.kind == "LIQUIDATION_PROXIMITY"]
    assert len(proximity) == 1
    assert any("venue decides" in w for w in engine.warnings)

    # Once per position, not once per minute: the same crossing again stays quiet.
    engine.pending_range = {SYMBOL: (below, money("40000"), money("40000"))}
    engine._on_liquidation_check(engine.runtime.now_ms + 120_000)
    assert (
        len([e for e in engine.runtime.events if e.kind == "LIQUIDATION_PROXIMITY"]) == 1
    )


# =============================================================== the session's sinks


def _quiet_session(tmp_path: Path, mode: str = "live") -> Any:
    """A real `PaperSession`, never run -- these tests exercise its sinks and guards."""
    from perplab.core.risk import RiskLimits
    from perplab.engine.backtest import BacktestConfig
    from perplab.engine.latency import FixedLatency
    from perplab.live.session import PaperSession, SessionConfig
    from perplab.strategy.context import FillTier
    from tests.support import btcusdt_filters, single_bracket_table
    from tests.unit.test_exchange_transport import Quiet

    strategy = Quiet()
    config = BacktestConfig(
        symbols=(SYMBOL,),
        timeframe="1m",
        start_ms=START,
        end_ms=START + 60_000,
        seed=1,
        opening_balance=money("1000000"),
        leverage=10,
        latency=FixedLatency(submit=10, cancel=10),
        fill_tier=FillTier.BOOK_TICKER,
        risk=RiskLimits.unlimited(),
    )
    tmp_path.mkdir(parents=True, exist_ok=True)
    return PaperSession(
        run_dir=tmp_path,
        strategy=strategy,
        requirements=strategy.declared,
        config=config,
        session=SessionConfig(run_id=RUN_ID, endpoint="testnet", mode=mode),
        filters={SYMBOL: btcusdt_filters()},
        brackets={SYMBOL: single_bracket_table(mmr=money("0.004"))},
    )


def test_live_stack_events_never_touch_the_market_disconnect_clock(
    tmp_path: Path,
) -> None:
    """Spec 7's disconnect trigger is about the market feed and nothing else.

    A user-stream drop, a transport event and a reconciliation outage all arrive as
    DISCONNECTs; feeding any of them into `_market_down_since` would halt a session whose
    market data is perfectly healthy -- the same conflation `POLLED_SOURCES` exists to
    prevent for the REST pollers, arriving through a third door.
    """
    session = _quiet_session(tmp_path)
    for stream in (USER_STREAM_LABEL, "orders", "reconcile"):
        session.exchange_event(CollectorEventKind.DISCONNECT, stream, "down", 0)
    assert session._market_down_since is None
    # The user stream is tracked on its own clock, and only the user stream.
    assert session._user_down_since is not None
    session.exchange_event(CollectorEventKind.RECONNECT, USER_STREAM_LABEL, "up", 5)
    assert session._user_down_since is None


def test_the_live_mode_guards_hold(tmp_path: Path) -> None:
    """A live session without a stack cannot run; a paper session cannot be given one."""
    live = _quiet_session(tmp_path / "live", mode="live")
    stop = asyncio.Event()
    with pytest.raises(RuntimeError, match="attach_live_stack"):
        asyncio.run(live.run(stop))

    paper = _quiet_session(tmp_path / "paper", mode="paper")
    fake_stack = object.__new__(LiveStack)
    with pytest.raises(ValueError, match="paper"):
        paper.attach_live_stack(fake_stack)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="expected 'paper' or 'live'"):
        _quiet_session(tmp_path / "bad", mode="shadow")


def test_the_tape_and_flags_carry_the_mode(tmp_path: Path) -> None:
    """The recording and the run flags must say which mode produced them: a tape whose
    meta reads `paper` for a session that traded a real account would hand the shadow --
    and every future reader -- a false statement about provenance."""
    live = _quiet_session(tmp_path / "live", mode="live")
    assert live.tape.meta_snapshot()["mode"] == "live"
    assert "LIVE" in live.engine.flags and "PAPER" not in live.engine.flags

    paper = _quiet_session(tmp_path / "paper", mode="paper")
    assert paper.tape.meta_snapshot()["mode"] == "paper"
    assert "PAPER" in paper.engine.flags


# ============================================================== the worker's live setup


class WorkerFakeClient:
    """What `_execute_live` needs from `SignedRestClient`, scripted per test."""

    clock_drift_warning: str | None = None
    """The drift surface the worker reads after `server_time_ms` (H10)."""

    def __init__(self, base: str, keys: KeySession, **_: Any) -> None:
        self.base = base
        self.keys = keys
        self.calls: list[str] = []
        self.wallet = "1234.50"
        self.positions: list[dict[str, Any]] = []
        self.working: list[dict[str, Any]] = []
        self.budget = object()
        """Stands in for `RateBudget`; the worker hands it to the feed (H9)."""

    async def __aenter__(self) -> "WorkerFakeClient":
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    async def server_time_ms(self) -> int:
        self.calls.append("server_time_ms")
        return 1_700_000_000_000

    async def account(self) -> dict[str, Any]:
        self.calls.append("account")
        return {"totalWalletBalance": self.wallet}

    async def position_risk(self, symbol: str) -> list[dict[str, Any]]:
        self.calls.append(f"position_risk:{symbol}")
        return list(self.positions)

    async def open_orders(self, symbol: str) -> list[dict[str, Any]]:
        self.calls.append(f"open_orders:{symbol}")
        return list(self.working)

    async def commission_rate(self, symbol: str) -> dict[str, Any]:
        # The testnet's published rates for a plain account -- and deliberately different
        # from `_spec()`'s requested 0.0002/0.0005, so the adoption tests can tell the
        # adopted schedule from the requested one by value alone.
        self.calls.append(f"commission_rate:{symbol}")
        return {
            "symbol": symbol,
            "makerCommissionRate": "0.000200",
            "takerCommissionRate": "0.000400",
        }


class _FeedStub:
    """The one attribute of `LiveFeed` the worker assigns: the shared weight budget."""

    budget: Any | None = None


class RecordingSession:
    """Stands in for a built `PaperSession` on `_execute_live`'s happy path."""

    def __init__(self, engine: Any) -> None:
        self.engine = engine
        self.attached: LiveStack | None = None
        self.ran = False
        self.feed = _FeedStub()

    def exchange_event(self, *args: Any, **kwargs: Any) -> None:
        pass

    def attach_live_stack(self, stack: LiveStack) -> None:
        self.attached = stack

    async def run(self, stop: asyncio.Event) -> None:
        self.ran = True


def _spec(**overrides: Any) -> Any:
    from perplab.engine.runspec import RunSpec

    base: dict[str, Any] = dict(
        strategy_id=1,
        version_id=1,
        version_no=1,
        strategy_name="Wobble",
        code="",
        class_name="Idea",
        params={},
        symbols=(SYMBOL,),
        timeframe="1m",
        start_ms=START,
        end_ms=START + 60_000,
        seed=0,
        opening_balance="10000",
        leverage=10,
        margin_mode="ISOLATED",
        hedge_mode=False,
        maker_rate="0.0002",
        taker_rate="0.0005",
        fee_source="test",
        latency={"model": "fixed", "submit_ms": 10, "cancel_ms": 10},
        fill_tier="BOOK_TICKER",
        fill_model={"tier": "BOOK_TICKER"},
        liquidation_recovery_pct="0",
        timeout_s=0.0,
        engine_version=1,
        risk_limits={},
        auto_flatten={},
        kill_switch_flatten=False,
        endpoint="testnet",
        reorder_buffer_ms=250,
        session_kind="live",
    )
    base.update(overrides)
    return RunSpec(**base)


def _run_execute_live(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    secrets: dict[str, Any],
    configure: Any = None,
    client_setup: Any = None,
) -> tuple[RecordingSession | None, list[Any], WorkerFakeClient | None]:
    """Drive `worker._execute_live` against fakes. Returns (session, build_calls, client)."""
    engine, _client, _transport, _events = build(tmp_path)
    # A fresh engine whose transport is still the simulator, as the worker would have it.
    engine.transport = SimulatedTransport(engine)

    made: dict[str, Any] = {}
    build_calls: list[Any] = []

    def fake_client_cls(base: str, keys: KeySession, **kw: Any) -> WorkerFakeClient:
        client = WorkerFakeClient(base, keys, **kw)
        if client_setup is not None:
            client_setup(client)
        made["client"] = client
        return client

    async def fake_configure(client: Any, symbols: Any, **kwargs: Any) -> Any:
        made["configure"] = {"symbols": tuple(symbols), **kwargs}
        return preflight_for(*symbols, leverage=kwargs["leverage"])

    # The worker's own KeySession copies, captured so every exit path -- refusals
    # included -- can be held to "the credential does not outlive the attempt".
    made_keys: list[KeySession] = []
    real_key_session = KeySession

    def capture_keys(*args: Any, **kwargs: Any) -> KeySession:
        keys = real_key_session(*args, **kwargs)
        made_keys.append(keys)
        return keys

    monkeypatch.setattr(worker_module, "KeySession", capture_keys)
    monkeypatch.setattr(worker_module, "SignedRestClient", fake_client_cls)
    monkeypatch.setattr(
        worker_module, "configure_account", configure or fake_configure
    )

    session = RecordingSession(engine)

    def build_session(opening_balance: Any, fees: Any = None) -> Any:
        build_calls.append({"opening_balance": opening_balance, "fees": fees})
        return session

    try:
        asyncio.run(
            worker_module._execute_live(
                build_session, _spec(), secrets, RUN_ID, asyncio.Event()
            )
        )
    finally:
        # Spec 11 on every path out of the worker, the refusals most of all: each one
        # spends the rest of the process formatting a traceback and writing the run row,
        # which is exactly the crash-dump window an unwiped credential would sit in.
        assert all(keys.wiped for keys in made_keys), (
            "the worker's credential copy survived an exit path un-wiped"
        )
    return session, build_calls, made.get("client")


def test_the_worker_refuses_a_live_run_with_no_credential(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    with pytest.raises(RuntimeError, match="stdin"):
        _run_execute_live(monkeypatch, tmp_path, secrets={"max_runtime_s": 0.0})


def test_the_worker_refuses_a_stale_credential_handoff(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A deadline already in the past means the parent's copy has been wiped."""
    with pytest.raises(RuntimeError, match="stale"):
        _run_execute_live(
            monkeypatch,
            tmp_path,
            secrets={"api_key": "k" * 10, "api_secret": "s" * 10, "keys_expire_ms": 1},
        )


def test_the_worker_refuses_an_unfunded_wallet(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def setup(client: WorkerFakeClient) -> None:
        client.wallet = "0"

    with pytest.raises(RuntimeError, match="fund the testnet account"):
        _run_execute_live(
            monkeypatch,
            tmp_path,
            secrets={"api_key": "k" * 10, "api_secret": "s" * 10},
            client_setup=setup,
        )


def test_the_worker_refuses_a_symbol_that_is_not_a_clean_slate(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A pre-existing position is the exact state spec 6.7.3 would halt on, sixty seconds
    in, after the account had already been reconfigured. Refused before either happens --
    and before `configure_account` has touched anything."""

    def setup(client: WorkerFakeClient) -> None:
        client.positions = [{"symbol": SYMBOL, "positionAmt": "0.5"}]

    with pytest.raises(RuntimeError, match="already holds a position"):
        _run_execute_live(
            monkeypatch,
            tmp_path,
            secrets={"api_key": "k" * 10, "api_secret": "s" * 10},
            client_setup=setup,
        )


def test_the_worker_refuses_pre_existing_working_orders(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def setup(client: WorkerFakeClient) -> None:
        client.working = [{"clientOrderId": "somebody-elses"}]

    with pytest.raises(RuntimeError, match="working order"):
        _run_execute_live(
            monkeypatch,
            tmp_path,
            secrets={"api_key": "k" * 10, "api_secret": "s" * 10},
            client_setup=setup,
        )


def test_the_worker_adopts_the_venue_wallet_and_wires_the_stack(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The happy path, end to end minus the network.

    The ledger opens at the venue's wallet (spec 6.7.3 compares the two to the cent), the
    fee schedule is adopted from the account's own commission rates (spec 3.8 -- the first
    live round trip halted on exactly the typed-rate/charged-rate difference), both
    adoptions are recorded as warnings because they differ from the requested figures, the
    preflight ran with the run's own leverage and margin mode, the stack reaches the
    session before `run`, and the worker's credential copy is wiped after.
    """
    session, build_calls, client = _run_execute_live(
        monkeypatch,
        tmp_path,
        secrets={"api_key": "k" * 10, "api_secret": "s" * 10},
    )
    assert session is not None and session.ran
    assert len(build_calls) == 1
    assert build_calls[0]["opening_balance"] == money("1234.50")
    fees = build_calls[0]["fees"]
    assert fees is not None, "a live session must adopt the account's commission rates"
    # The fake publishes taker 0.000400 against the spec's requested 0.0005 -- the exact
    # shape of the run-15 halt. Maker agrees numerically (0.000200 == 0.0002), which
    # also pins that agreement-by-value is not mistaken for disagreement-by-format.
    assert fees.taker_rate == money("0.0004")
    assert fees.maker_rate == money("0.0002")
    assert fees.source == f"commissionRate:{SYMBOL}"
    assert any("opened at the venue's wallet balance" in w for w in session.engine.warnings)
    assert any(
        "adopted from the account's own commission rates" in w
        for w in session.engine.warnings
    ), "a fee schedule that differs from the requested rates must say so"
    assert any(f"commission_rate:{SYMBOL}" in c for c in client.calls)

    stack = session.attached
    assert stack is not None
    assert stack.preflight.symbols[0].symbol == SYMBOL
    assert stack.transport.simulated is False
    assert stack.keys.wiped, "the worker's credential copy must not outlive the run"

    assert client is not None
    order = [c.split(":")[0] for c in client.calls]
    assert order.index("server_time_ms") < order.index("account"), (
        "drift is measured before the first signed request"
    )


def test_production_live_stays_disabled_until_the_phase8_criterion() -> None:
    """Flipping this is a deliberate, single-line decision -- see its docstring. This test
    exists so that the flip shows up as a failing test to be updated consciously rather
    than as a silent side effect of some other change."""
    assert PRODUCTION_LIVE_ENABLED is False

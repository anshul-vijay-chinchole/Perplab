"""The testnet/live order path: what goes on the wire, and what comes back off it.

`ExchangeTransport` is the half of spec 6.1's seam a backtest never exercises, and it is the
half where being wrong costs money rather than costing a number. Four properties are pinned
here because each has an obvious wrong implementation that a test asserting only "it
happened" would accept:

- **A POST that was sent and not answered is not a rejection.** Treating it as one flattens
  the engine's ledger while the exchange holds a position, and the next thing to notice is
  the liquidation. Two tests cover it: the `OrderOutcomeUnknown` the signed client is
  contracted to raise, and a raw `httpx.ReadTimeout` that something failed to classify.
- **A fill books through the engine's own `_book_fill`.** Asserted by looking at the
  *ledger* -- position size, wallet, fill count -- rather than at whether a method was
  called. A transport that reproduced the booking would pass a mock-based test and fail this
  one, which is the point.
- **A report for an id this session never issued is counted and reported.** It means the
  account is being traded by something else, and that is the state spec 6.7.3's
  reconciliation exists to catch. A silent drop removes the fastest evidence of it.
- **The client order id is the platform's.** A strategy-supplied `tag` or `client_id` is
  never validated against Binance's 36-character restricted charset anywhere in this
  codebase, so letting one reach `newClientOrderId` converts a typo into a rejection that
  feeds `observe_rejection` and can trip spec 7's kill switch.

**No network.** Every test runs against a fake signed client that records what it was asked
for and returns a payload written by hand, and against `ExchangeReport`s built by feeding
hand-written frames through the real `parse_user_frame`. Nothing here has ever spoken to
Binance, and neither has the code it tests.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest

from perplab.core.money import parse_money
from perplab.core.risk import RiskLimits
from perplab.core.types import CollectorEventKind, MarginMode
from perplab.engine.backtest import BacktestConfig, BacktestEngine
from perplab.engine.executor_base import OrderStatus
from perplab.engine.latency import FixedLatency
from perplab.exchange.rest import BinanceRestError
from perplab.exchange.signed import UNKNOWN_ORDER_CODE, OrderOutcomeUnknown
from perplab.exchange.userstream import parse_user_frame
from perplab.live.preflight import PreflightReport, SymbolConfig
from perplab.live.exchange_transport import (
    MAX_UNSENT_REQUESTS,
    ExchangeTransport,
    TransportStalled,
    client_order_id,
    engine_order_seq,
)
from perplab.strategy.base import Strategy
from perplab.strategy.context import FillTier, OrderIntent, OrderType, TimeInForce
from perplab.strategy.context import UnsupportedOrder
from tests.support import btcusdt_filters, single_bracket_table

START = 1_709_251_200_000
RUN_ID = 7
SYMBOL = "BTCUSDT"


def money(text: str) -> Decimal:
    return parse_money(text)


class Quiet(Strategy):
    """A strategy that never trades. The orders in these tests are submitted directly."""

    requires = {
        "symbols": [SYMBOL],
        "timeframe": "1m",
        "history": 0,
        "datasets": ["klines", "bookTicker"],
    }


class NoSource:
    """A `MarketSource` supplying nothing, so the engine needs no lake.

    The same shape `live.session._PushSource` uses for a live feed: there is no iterator
    over the future, and these tests push order reports rather than market data.
    """

    def prepare(self, engine: BacktestEngine) -> Any:
        from perplab.engine.source import Prepared

        return Prepared(streams=(), data_start_ms=engine.config.start_ms, total_bars=0)

    def close(self) -> None:
        """Nothing to release."""


class FakeSignedClient:
    """Stands in for `SignedRestClient`, and reaches no network at all.

    Records the exact parameter dictionaries it was handed, which is what lets a test assert
    what would have gone on the wire. `fail_next` makes the next call raise, so a failure
    path is exercised at the same seam a real one would arrive at.
    """

    def __init__(self) -> None:
        self.orders: list[dict[str, Any]] = []
        self.cancels: list[dict[str, Any]] = []
        self.cancel_alls: list[str] = []
        self.fail_next: BaseException | None = None
        self.next_order_id = 5_000

    def _maybe_fail(self) -> None:
        if self.fail_next is not None:
            error, self.fail_next = self.fail_next, None
            raise error

    async def new_order(self, **params: Any) -> dict[str, Any]:
        self.orders.append(dict(params))
        self._maybe_fail()
        self.next_order_id += 1
        return {
            "orderId": self.next_order_id,
            "clientOrderId": params["newClientOrderId"],
            "symbol": params["symbol"],
            "status": "NEW",
        }

    async def cancel_order(
        self,
        symbol: str,
        *,
        order_id: int | None = None,
        client_order_id: str | None = None,
    ) -> dict[str, Any]:
        self.cancels.append(
            {"symbol": symbol, "orderId": order_id, "origClientOrderId": client_order_id}
        )
        self._maybe_fail()
        return {"symbol": symbol, "status": "CANCELED"}

    async def cancel_all(self, symbol: str) -> dict[str, Any]:
        self.cancel_alls.append(symbol)
        self._maybe_fail()
        return {"code": 200, "msg": "The operation of cancel all open order is done."}


class Events:
    """Collects what the transport reported, in the collector's own event vocabulary."""

    def __init__(self) -> None:
        self.entries: list[tuple[CollectorEventKind, str, str, int]] = []

    def __call__(
        self, kind: CollectorEventKind, stream: str, detail: str, downtime_ms: int
    ) -> None:
        self.entries.append((kind, stream, detail, downtime_ms))

    def details(self) -> str:
        return "\n".join(entry[2] for entry in self.entries)


def preflight_for(*symbols: str, leverage: int) -> PreflightReport:
    """A report standing for a completed `configure_account` against these symbols."""
    return PreflightReport(
        symbols=tuple(
            SymbolConfig(
                symbol=symbol,
                leverage=leverage,
                margin_mode=MarginMode.ISOLATED,
                margin_type_changed=False,
            )
            for symbol in symbols
        )
    )


def build(tmp_path: Path) -> tuple[BacktestEngine, FakeSignedClient, ExchangeTransport, Events]:
    """A real engine with a real ledger, wired to a fake exchange.

    The engine is the genuine `BacktestEngine` -- same order lifecycle, same `Account`, same
    `RiskEngine` -- because the property under test is that the transport routes *into* it
    rather than reproducing it. `start()` is not called: it exists to load market data, and
    these tests supply the one price the ledger needs directly.
    """
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
    engine.account.update_mark(START, SYMBOL, money("40000"))

    client = FakeSignedClient()
    events = Events()
    # Assigned after construction because the transport needs the engine it feeds, which is
    # the same order `SimulatedTransport` is built in inside `BacktestEngine.__init__`.
    # The preflight report has to agree with the engine's own leverage or the transport
    # refuses to exist -- see `ExchangeTransport._check_preflight`.
    transport = ExchangeTransport(
        engine,
        client,
        run_id=RUN_ID,
        on_event=events,
        preflight=preflight_for(SYMBOL, leverage=10),
    )
    engine.transport = transport
    return engine, client, transport, events


def submit(
    engine: BacktestEngine,
    *,
    side: str = "BUY",
    qty: str = "1",
    order_type: OrderType = OrderType.MARKET,
    price: str | None = None,
    stop_price: str | None = None,
    callback_rate: str | None = None,
    tif: TimeInForce = TimeInForce.GTC,
    tag: str | None = None,
    client_id: str | None = None,
) -> str:
    """Push one order through the engine's own `_submit`, which is where a strategy enters."""
    return engine.runtime.submit(
        OrderIntent(
            symbol=SYMBOL,
            side=side,
            qty=money(qty),
            type=order_type,
            price=None if price is None else money(price),
            stop_price=None if stop_price is None else money(stop_price),
            callback_rate=None if callback_rate is None else money(callback_rate),
            tif=tif,
            tag=tag,
            client_id=client_id,
        )
    )


_NEXT_TRADE_ID = 987


def order_frame(
    cid: str,
    *,
    status: str = "FILLED",
    execution: str = "TRADE",
    last_qty: str = "1",
    last_price: str = "40000",
    cum_qty: str = "1",
    side: str = "BUY",
    is_maker: bool = False,
    ts_ms: int = START,
    exchange_order_id: int = 5_001,
    commission: str = "0",
    commission_asset: str = "USDT",
    trade_id: int | None = None,
) -> dict[str, Any]:
    """One raw `ORDER_TRADE_UPDATE` frame, in Binance's own field names.

    `trade_id` defaults to a fresh id per frame, as the venue's do -- every execution is
    its own trade. A test exercising the duplicate guard passes the same id twice
    explicitly, which is also how a genuine redelivery looks.
    """
    global _NEXT_TRADE_ID
    if trade_id is None:
        _NEXT_TRADE_ID += 1
        trade_id = _NEXT_TRADE_ID
    return {
        "e": "ORDER_TRADE_UPDATE",
        "E": ts_ms,
        "T": ts_ms,
        "o": {
            "s": SYMBOL,
            "c": cid,
            "S": side,
            "o": "MARKET",
            "f": "GTC",
            "q": "1",
            "p": "0",
            "ap": last_price,
            "sp": "0",
            "x": execution,
            "X": status,
            "i": exchange_order_id,
            "l": last_qty,
            "z": cum_qty,
            "L": last_price,
            "n": commission,
            "N": commission_asset,
            "T": ts_ms,
            "t": trade_id,
            "m": is_maker,
            "R": False,
            "wt": "MARK_PRICE",
            "ot": "MARKET",
            "ps": "BOTH",
            "cp": False,
            "rp": "0",
        },
    }


def report(cid: str, **kwargs: Any):
    """A frame put through the real parser, so the field mapping is under test too."""
    return parse_user_frame(order_frame(cid, **kwargs))


# =============================================================== the client order id


def test_the_client_order_id_names_the_run_and_the_engines_own_order_sequence() -> None:
    """`pl{run_id}-{order_seq}` -- run 7, order `o12`, therefore `pl7-12`.

    Both numbers are the platform's. Reusing the engine's own sequence rather than counting
    separately is what keeps `pl7-12` and engine order `o12` obviously the same order in a
    run log; a separate counter would drift the first time an order was refused by the risk
    layer before reaching the transport, and the drift would be silent.
    """
    assert client_order_id(7, 12) == "pl7-12"
    assert engine_order_seq("o12") == 12


def test_a_run_id_that_is_not_a_number_is_refused_rather_than_formatted() -> None:
    """An id built from an arbitrary object is one reconciliation cannot look an order up by.

    `newClientOrderId` is the handle `openOrders` and `userTrades` are queried with after an
    unanswered POST (`exchange.signed`), so an id that is merely *a string* is not enough.
    """
    with pytest.raises(ValueError, match="run_id must be a non-negative int"):
        client_order_id("seven", 12)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="order_seq must be a non-negative int"):
        client_order_id(7, -1)


def test_an_order_id_the_engine_did_not_mint_is_refused_rather_than_numbered() -> None:
    """`BacktestEngine._submit` names orders `o{n}`; anything else means the format moved.

    Guessing a sequence would produce a well-formed client order id belonging to no order,
    which is worse than a crash: the POST would succeed and the report would come back
    unroutable.
    """
    with pytest.raises(ValueError, match="cannot read an order sequence"):
        engine_order_seq("order-12")


@pytest.mark.asyncio
async def test_a_strategy_supplied_tag_never_becomes_a_client_order_id(
    tmp_path: Path,
) -> None:
    """A tag is free text and Binance's id charset is not.

    The tag here contains a space and a `#`, neither of which is in
    `^[.A-Za-z0-9:/_-]{1,36}$`. Nothing in this codebase validates a strategy-supplied string
    against that, so an id built from one would earn a `-1100` -- which arrives on the
    rejection path, feeds `observe_rejection`, and can trip spec 7's kill switch on what is
    really a typo. The order is the first of the run, so the id must be exactly `pl7-1`.
    """
    engine, client, transport, _ = build(tmp_path)
    order_id = submit(engine, tag="momo #3 entry", client_id="whatever-the-strategy-said")
    await transport.drain()

    sent = client.orders[0]
    assert sent["newClientOrderId"] == "pl7-1"
    assert "momo" not in sent["newClientOrderId"]
    assert "whatever-the-strategy-said" not in sent["newClientOrderId"]
    # Carried, not discarded: it still labels the order for the operator.
    assert transport.route_for("pl7-1").tag == "momo #3 entry"
    assert engine.orders[order_id].intent.tag == "momo #3 entry"


# ======================================================================= what is sent


@pytest.mark.asyncio
async def test_a_market_order_is_posted_with_the_quantity_the_engine_quantised(
    tmp_path: Path,
) -> None:
    """1.0005 BTC against a 0.001 `stepSize` is 1.0 -- floored by the engine, not here.

    `BacktestEngine._submit` quantises into `order.remaining` at submission, and the
    transport sends that. Re-quantising here would be the duplication spec 6.1 forbids, and
    sending the raw 1.0005 would earn a `LOT_SIZE` refusal for a size the engine had already
    corrected.
    """
    engine, client, transport, _ = build(tmp_path)
    submit(engine, qty="1.0005")
    await transport.drain()

    sent = client.orders[0]
    assert sent["symbol"] == SYMBOL
    assert sent["side"] == "BUY"
    assert sent["type"] == "MARKET"
    assert sent["quantity"] == money("1")
    assert "timeInForce" not in sent, "a MARKET order carries no time in force"


@pytest.mark.asyncio
async def test_a_limit_order_carries_its_price_and_its_time_in_force(
    tmp_path: Path,
) -> None:
    """A resting order without `timeInForce` is a different order.

    `GTX` is post-only, which is how maker fees are guaranteed and how orders silently fail
    to enter (spec 6.5). Dropping it would turn a post-only quote into an ordinary one that
    pays the taker fee whenever it crosses.
    """
    engine, client, transport, _ = build(tmp_path)
    submit(engine, order_type=OrderType.LIMIT, price="39999.9", tif=TimeInForce.GTX)
    await transport.drain()

    sent = client.orders[0]
    assert sent["type"] == "LIMIT"
    assert sent["price"] == money("39999.9")
    assert sent["timeInForce"] == "GTX"


@pytest.mark.asyncio
async def test_a_trailing_stop_converts_the_fraction_into_binances_percent(
    tmp_path: Path,
) -> None:
    """`OrderIntent.callback_rate` is a fraction; `callbackRate` on the wire is a percent.

    0.01 here is Binance's 1.0, and its own docstring says so. Sending 0.01 unconverted asks
    for a 0.01% callback -- a stop a hundred times tighter than the author wrote, which stops
    out on noise and looks like a bad strategy rather than a bad unit.
    """
    engine, client, transport, _ = build(tmp_path)
    submit(
        engine,
        order_type=OrderType.TRAILING_STOP_MARKET,
        callback_rate="0.01",
        stop_price="41000",
    )
    await transport.drain()

    sent = client.orders[0]
    assert sent["type"] == "TRAILING_STOP_MARKET"
    assert sent["callbackRate"] == money("1"), "0.01 x 100"
    assert sent["activationPrice"] == money("41000")
    assert sent["workingType"] == "MARK_PRICE"


@pytest.mark.asyncio
async def test_a_stop_order_carries_its_trigger_price_and_working_type(
    tmp_path: Path,
) -> None:
    """Spec 6.4: a stop triggers against **mark price** by default, matching Binance.

    Omitting `workingType` would let the exchange's default decide, and a stop measured
    against contract price can be fired by a single wick on one venue -- which is a different
    order from the one the backtest modelled.
    """
    engine, client, transport, _ = build(tmp_path)
    submit(engine, side="SELL", order_type=OrderType.STOP_MARKET, stop_price="39000")
    await transport.drain()

    sent = client.orders[0]
    assert sent["stopPrice"] == money("39000")
    assert sent["workingType"] == "MARK_PRICE"


# ================================================== an outcome that never came back


@pytest.mark.asyncio
async def test_a_timed_out_submit_is_not_reported_as_a_rejection(tmp_path: Path) -> None:
    """`OrderOutcomeUnknown` means the POST arrived and the answer did not. Not a failure.

    The engine's ledger must stay exactly where it was: no fill, no rejection, no position.
    Booking a rejection would leave PerpLab flat while the exchange held 1 BTC, and the next
    thing to notice would be the liquidation. The order stays `PENDING` -- neither working
    nor dead -- which is the only honest encoding of "we do not know".
    """
    engine, client, transport, events = build(tmp_path)
    order_id = submit(engine)
    client.fail_next = OrderOutcomeUnknown(
        symbol=SYMBOL, client_order_id="pl7-1", detail="ReadTimeout"
    )
    await transport.drain()

    assert engine.counts["fills"] == 0
    assert engine.counts["rejects"] == 0, "an unknown outcome is not a rejection"
    assert engine.account.qty(SYMBOL) == money("0")
    assert engine.orders[order_id].status is OrderStatus.PENDING
    assert transport.unknown_outcomes == 1
    assert list(transport.unresolved) == ["pl7-1"]
    assert "Do NOT resubmit it" in transport.unresolved["pl7-1"]
    assert "outcome is unknown" in events.details()
    assert "NOT been treated as a rejection" in events.details()


@pytest.mark.asyncio
async def test_a_read_timeout_nothing_classified_is_still_not_a_rejection(
    tmp_path: Path,
) -> None:
    """The same rule one layer out, for an exception the signed client did not convert.

    `SignedRestClient` is contracted to turn every ambiguous order-path failure into
    `OrderOutcomeUnknown`, so a bare `httpx.ReadTimeout` reaching the transport means
    something outside that model went wrong. "I do not know what this exception means" on a
    POST that may have arrived has exactly one safe reading, and it is not "rejected".
    """
    engine, client, transport, _ = build(tmp_path)
    order_id = submit(engine)
    client.fail_next = httpx.ReadTimeout("timed out")
    await transport.drain()

    assert engine.counts["rejects"] == 0
    assert engine.counts["fills"] == 0
    assert engine.orders[order_id].status is OrderStatus.PENDING
    assert transport.unknown_outcomes == 1
    assert engine.risk.halted is False


@pytest.mark.asyncio
async def test_an_unknown_outcome_that_the_stream_later_explains_stops_being_unresolved(
    tmp_path: Path,
) -> None:
    """The order did exist. The user-data stream is what settles it, and it clears the flag.

    This is the whole reason an unknown is not a rejection: the exchange answers the question
    a moment later, on the channel that always answers it. A transport that had already
    rejected the order would have nothing left to route the report to.
    """
    engine, client, transport, _ = build(tmp_path)
    submit(engine)
    client.fail_next = OrderOutcomeUnknown(
        symbol=SYMBOL, client_order_id="pl7-1", detail="HTTP 503"
    )
    await transport.drain()
    assert transport.unresolved

    transport.on_report(report("pl7-1", status="NEW", execution="NEW", last_qty="0",
                               cum_qty="0"))
    assert transport.unresolved == {}
    assert engine.orders["o1"].status is OrderStatus.WORKING


# ================================================================ genuine refusals


@pytest.mark.asyncio
async def test_an_exchange_refusal_with_a_code_goes_through_the_engines_reject_path(
    tmp_path: Path,
) -> None:
    """A `-1013` is the exchange saying no, which is data about the strategy's sizing.

    Routed to `BacktestEngine._reject` rather than reproduced: that is what counts the
    rejection, raises `ORDERS_REJECTED`, writes the `REJECT` event and feeds spec 7's
    consecutive-rejection trigger. One rejection here, so `rejects` is 1.
    """
    engine, client, transport, _ = build(tmp_path)
    order_id = submit(engine)
    client.fail_next = BinanceRestError(400, -1013, "Filter failure: MIN_NOTIONAL")
    await transport.drain()

    assert engine.counts["rejects"] == 1
    assert engine.orders[order_id].status is OrderStatus.REJECTED
    assert "MIN_NOTIONAL" in engine.orders[order_id].reason
    assert "ORDERS_REJECTED" in engine.flags
    assert transport.unknown_outcomes == 0, "a coded refusal is not an unknown"


@pytest.mark.asyncio
async def test_a_rejection_report_from_the_stream_takes_the_same_path(
    tmp_path: Path,
) -> None:
    """A refusal can arrive on either channel and must mean the same thing on both.

    Here the POST is accepted and the exchange rejects the order afterwards, which is what a
    `PERCENT_PRICE` breach looks like from the stream side.
    """
    engine, _client, transport, _ = build(tmp_path)
    order_id = submit(engine)
    await transport.drain()
    transport.on_report(
        report("pl7-1", status="REJECTED", execution="REJECTED", last_qty="0", cum_qty="0")
    )

    assert engine.counts["rejects"] == 1
    assert engine.orders[order_id].status is OrderStatus.REJECTED


# ========================================================== fills reach the ledger


@pytest.mark.asyncio
async def test_a_fill_report_moves_the_ledger_through_the_engines_own_booking_path(
    tmp_path: Path,
) -> None:
    """1 BTC bought at 40 000 on a 1 000 000 wallet at the 5 bps default taker rate.

    Notional 1 x 40 000 = 40 000; fee 40 000 x 0.0005 = 20; wallet 1 000 000 - 20 = 999 980.
    Position 1 BTC at an entry of 40 000.

    Asserted on the *ledger*, not on whether `_book_fill` was called. A transport that had
    its own copy of the booking would satisfy a mock and fail this, which is exactly the
    distinction spec 6.1 draws: the transport delivers, the engine accounts.
    """
    engine, _client, transport, _ = build(tmp_path)
    order_id = submit(engine)
    await transport.drain()
    transport.on_report(report("pl7-1"))

    assert engine.account.qty(SYMBOL) == money("1")
    assert engine.account.position(SYMBOL).entry_price == money("40000")
    assert engine.account.wallet == money("999980"), "1 000 000 - 40 000 x 0.0005"
    assert engine.counts["fills"] == 1
    assert engine.orders[order_id].status is OrderStatus.FILLED
    assert engine.orders[order_id].filled_price == money("40000")
    assert transport.fills_booked == 1


@pytest.mark.asyncio
async def test_a_maker_fill_is_booked_at_the_maker_rate_the_report_declares(
    tmp_path: Path,
) -> None:
    """`o.m` says whether *we* were the maker, and the fee follows it.

    The default `FeeSchedule.all_taker(0.0005)` charges the taker rate on both sides, so the
    fee is 20 either way -- what this pins is that the flag survives the trip and reaches the
    ledger as a maker fill, which is what `counts["maker_fills"]` counts and what spec 6.7's
    parity report compares. Inverting it turns a rebate into a fee in that report, by an
    amount small enough to look like model error.
    """
    engine, _client, transport, _ = build(tmp_path)
    submit(engine, order_type=OrderType.LIMIT, price="40000")
    await transport.drain()
    transport.on_report(report("pl7-1", is_maker=True))

    assert engine.counts["fills"] == 1
    assert engine.counts["maker_fills"] == 1
    assert engine.account.wallet == money("999980")


@pytest.mark.asyncio
async def test_a_partial_fill_leaves_the_order_working_with_the_remainder(
    tmp_path: Path,
) -> None:
    """0.4 of a 1.0 order fills, so 0.6 is still working and the position is 0.4.

    Spec 6.5: `on_fill` fires per increment and the position updates per increment, not
    batched at the end. A transport that waited for `FILLED` before booking would leave the
    engine flat while the account held 0.4 BTC for as long as the rest took to fill.
    """
    engine, _client, transport, _ = build(tmp_path)
    order_id = submit(engine)
    await transport.drain()
    transport.on_report(
        report("pl7-1", status="PARTIALLY_FILLED", last_qty="0.4", cum_qty="0.4")
    )

    assert engine.account.qty(SYMBOL) == money("0.4")
    assert engine.orders[order_id].status is OrderStatus.WORKING
    assert engine.orders[order_id].remaining == 60_000_000, "0.6 scaled by 10^8"
    assert engine.counts["partial_fills"] == 1


@pytest.mark.asyncio
async def test_a_redelivered_fill_report_is_counted_and_not_booked_twice(
    tmp_path: Path,
) -> None:
    """The same frame twice must not become two fills.

    The exchange's cumulative `z` has not moved between the two, so the second is a repeat.
    Booked twice, the ledger would hold 2 BTC against an account holding 1 -- which spec
    6.7.3's reconciliation would then halt the session for, correctly and far too late.
    """
    engine, _client, transport, _ = build(tmp_path)
    submit(engine, qty="2")
    await transport.drain()
    frame = report("pl7-1", status="PARTIALLY_FILLED", last_qty="1", cum_qty="1")
    transport.on_report(frame)
    transport.on_report(frame)

    assert engine.account.qty(SYMBOL) == money("1")
    assert engine.counts["fills"] == 1
    assert transport.duplicate_reports == 1


@pytest.mark.asyncio
async def test_a_dropped_execution_report_is_detected_from_the_cumulative_quantity(
    tmp_path: Path,
) -> None:
    """The socket carries no sequence number, so `z` is the only evidence a frame was lost.

    Two increments of 1 arrive as one frame: the first is delivered (`z` = 1), the second is
    lost, and the third reports `l` = 1 with `z` = 3. Booking `l` would leave the ledger at
    2 against an account holding 3. Booking the difference -- 3 - 1 = 2 -- keeps the quantity
    right at this report's price, and says loudly that the entry price is now uncertain.
    """
    engine, _client, transport, events = build(tmp_path)
    submit(engine, qty="3")
    await transport.drain()
    transport.on_report(report("pl7-1", status="PARTIALLY_FILLED", last_qty="1", cum_qty="1"))
    transport.on_report(
        report("pl7-1", status="FILLED", last_qty="1", cum_qty="3", last_price="40010")
    )

    assert engine.account.qty(SYMBOL) == money("3")
    assert transport.dropped_frames == 1
    assert "execution report was dropped" in events.details()


@pytest.mark.asyncio
async def test_a_report_that_is_not_a_trade_books_nothing(tmp_path: Path) -> None:
    """`o.x` is the execution type, and only `TRADE` moved any quantity.

    A `CALCULATED` or `AMENDMENT` report can carry `X = FILLED` with `l = 0`. Booking it
    would send a zero-quantity fill into `validate_order`, which refuses a non-positive
    quantity -- so the order would be *rejected*, by us, for a message that reported nothing
    wrong.
    """
    engine, _client, transport, _ = build(tmp_path)
    submit(engine)
    await transport.drain()
    transport.on_report(
        report("pl7-1", status="FILLED", execution="CALCULATED", last_qty="0", cum_qty="0")
    )

    assert engine.counts["fills"] == 0
    assert engine.counts["rejects"] == 0


# ================================================ reports this session did not cause


@pytest.mark.asyncio
async def test_a_report_for_an_id_this_session_never_issued_is_counted_and_reported(
    tmp_path: Path,
) -> None:
    """Never silently dropped: it means something else is trading the account.

    Another process, a hand-placed order in the Binance app, a stale session -- each leaves
    PerpLab's ledger describing a position that is not the one being held, and every spec 7
    limit is then evaluated against that description. Spec 6.7.3's reconciliation is what
    stops the session; this is the fastest evidence of why.
    """
    engine, _client, transport, events = build(tmp_path)
    transport.on_report(report("someone-elses-order", status="FILLED"))

    assert transport.foreign_reports == 1
    assert transport.foreign_client_order_ids == ("someone-elses-order",)
    assert engine.counts["fills"] == 0, "nothing unroutable may reach the ledger"
    assert "never issued" in events.details()
    assert transport.summary()["foreign_reports"] == 1


@pytest.mark.asyncio
async def test_an_exchange_minted_autoclose_id_is_counted_apart_from_a_foreign_order(
    tmp_path: Path,
) -> None:
    """A liquidation fill arrives with an `autoclose-` id the exchange minted itself.

    Counting it as "somebody else is trading this account" would be wrong in a way that
    matters: nobody else touched the account, it was force-closed, and an operator's response
    to the two is different. One report of each here, so the two counters read 1 and 1.
    """
    _engine, _client, transport, events = build(tmp_path)
    transport.on_report(report("autoclose-1712345678901", status="FILLED"))
    transport.on_report(report("hand-placed", status="FILLED"))

    assert transport.exchange_closures == 1
    assert transport.foreign_reports == 1
    assert "liquidated or ADL-closed" in events.details()


@pytest.mark.asyncio
async def test_an_account_report_is_ignored_and_counted_rather_than_routed(
    tmp_path: Path,
) -> None:
    """`ACCOUNT_UPDATE` reports no order, so there is no order for it to be about.

    Counted so that "the transport saw nothing" and "the transport saw nothing it owned" stay
    distinguishable -- a session whose user-data stream is delivering only account events has
    a different problem from one whose stream is dead.
    """
    _engine, _client, transport, _ = build(tmp_path)
    transport.on_report(
        parse_user_frame({"e": "ACCOUNT_UPDATE", "E": START, "a": {"m": "ORDER"}})
    )

    assert transport.ignored_reports == 1
    assert transport.reports == 0
    assert transport.foreign_reports == 0


# ============================================================ cancels are a race


@pytest.mark.asyncio
async def test_a_cancel_request_does_not_mark_the_order_cancelled_by_itself(
    tmp_path: Path,
) -> None:
    """Spec 6.3 and R19: a cancel is a race it can lose.

    The DELETE goes out and is acknowledged, and the order is still `WORKING` -- because
    nothing has said it stopped working. A transport that set the status here would turn a
    race the strategy can lose into a veto it always wins, which is exactly the backtest
    optimism spec 6.3 names.
    """
    engine, client, transport, _ = build(tmp_path)
    order_id = submit(engine, order_type=OrderType.LIMIT, price="39000")
    await transport.drain()
    transport.on_report(report("pl7-1", status="NEW", execution="NEW", last_qty="0",
                               cum_qty="0"))

    engine.runtime.cancel(order_id)
    await transport.drain()

    assert client.cancels == [
        {"symbol": SYMBOL, "orderId": None, "origClientOrderId": "pl7-1"}
    ]
    assert engine.orders[order_id].status is OrderStatus.WORKING


@pytest.mark.asyncio
async def test_a_cancel_report_removes_the_order_through_the_engines_own_path(
    tmp_path: Path,
) -> None:
    """The exchange saying `CANCELED` is what ends the order, and `_remove` is what ends it.

    `_remove` is the shared code: it discards from the resting book, releases any flatten
    latch, queues the `on_cancel` notification and writes the removal event. Reproducing any
    of that here would be a second copy of the order lifecycle.
    """
    engine, _client, transport, _ = build(tmp_path)
    order_id = submit(engine, order_type=OrderType.LIMIT, price="39000")
    await transport.drain()
    transport.on_report(
        report("pl7-1", status="CANCELED", execution="CANCELED", last_qty="0", cum_qty="0")
    )

    assert engine.orders[order_id].status is OrderStatus.CANCELLED
    assert engine.orders[order_id].is_open is False


@pytest.mark.asyncio
async def test_an_expired_post_only_order_is_recorded_as_expired_not_cancelled(
    tmp_path: Path,
) -> None:
    """Spec 6.5's named hazard: a post-only order that silently fails to enter.

    `EXPIRED` means the order retired itself; `CANCELLED` means the strategy asked. A
    requoting strategy that waits for `on_cancel` needs to hear about the first, and a run
    that reported it as the second would hide the fact that the quote never entered.
    """
    engine, _client, transport, _ = build(tmp_path)
    order_id = submit(
        engine, order_type=OrderType.LIMIT, price="40000", tif=TimeInForce.GTX
    )
    await transport.drain()
    transport.on_report(
        report("pl7-1", status="EXPIRED", execution="EXPIRED", last_qty="0", cum_qty="0")
    )

    assert engine.orders[order_id].status is OrderStatus.EXPIRED
    assert "GTX" in engine.orders[order_id].reason


@pytest.mark.asyncio
async def test_a_cancel_the_exchange_says_it_never_had_is_not_a_failure(
    tmp_path: Path,
) -> None:
    """`-2011 Unknown order sent` is the expected answer to a cancel that already worked.

    `exchange.signed` documents it: the DELETE is idempotent in its effect and not in its
    error. Counting it as a failure would report a successful cancel as an error, and during
    a halt that is the difference between "the book is clear" and "something went wrong".
    """
    engine, client, transport, _ = build(tmp_path)
    order_id = submit(engine, order_type=OrderType.LIMIT, price="39000")
    await transport.drain()
    engine.runtime.cancel(order_id)
    client.fail_next = BinanceRestError(400, UNKNOWN_ORDER_CODE, "Unknown order sent.")
    await transport.drain()

    assert transport.cancel_failures == 0
    assert len(transport.latency_samples()["cancel"]) == 1, (
        "the cancel is treated as acknowledged, so it produced exactly one latency sample"
    )
    assert engine.orders[order_id].status is OrderStatus.WORKING, (
        "'already gone' is not this transport's evidence that the order ended -- the "
        "user-data stream reports that, and here it has not"
    )


@pytest.mark.asyncio
async def test_a_cancel_that_failed_for_another_reason_leaves_the_order_working(
    tmp_path: Path,
) -> None:
    """A `-1021` on a cancel says nothing about the order, which may still be resting.

    Marking it cancelled would be inventing an outcome; the honest state is "still working,
    and we could not pull it", which is what an operator needs to see.
    """
    engine, client, transport, events = build(tmp_path)
    order_id = submit(engine, order_type=OrderType.LIMIT, price="39000")
    await transport.drain()
    engine.runtime.cancel(order_id)
    client.fail_next = BinanceRestError(400, -1021, "Timestamp for this request is outside")
    await transport.drain()

    assert transport.cancel_failures == 1
    assert engine.orders[order_id].status is OrderStatus.WORKING
    assert "may still be working" in events.details()


# ================================================================= latency, measured


@pytest.mark.asyncio
async def test_the_ack_records_a_submit_latency_the_session_can_read(
    tmp_path: Path,
) -> None:
    """Spec 6.3's `empirical` model is "sampled from latencies measured during your own
    sessions", and this is the measurement.

    Two places have to see it: the transport's own pool, and `order.arrival_ts`, which is
    what `PaperSession.latency_samples` derives its pool from. The order is submitted under
    `FixedLatency(submit=10)`, so `arrival_ts` starts at `submit_ts + 10` -- a *modelled*
    figure that means nothing once a real exchange is answering. After the ack it must equal
    `submit_ts` plus the observed measurement, whatever that turned out to be: keeping the
    larger of the two would report the model whenever the network beat it, which is exactly
    the case the model is worst at. The fake client returns immediately and the clock is
    monotonic, so the value itself can only be asserted to be a non-negative whole number of
    milliseconds.
    """
    engine, _client, transport, _ = build(tmp_path)
    order_id = submit(engine)
    order = engine.orders[order_id]
    assert order.arrival_ts == order.submit_ts + 10, "the modelled latency, before any ack"

    await transport.drain()

    samples = transport.latency_samples()
    assert len(samples["submit"]) == 1
    assert samples["submit"][0] >= 0
    assert order.arrival_ts == order.submit_ts + samples["submit"][0]


@pytest.mark.asyncio
async def test_a_second_ack_for_the_same_order_does_not_restate_the_latency(
    tmp_path: Path,
) -> None:
    """The POST response and the stream's `NEW` say the same thing and either can be first.

    A second sample would measure a message that was already on its way, so one order
    produces exactly one submit-latency sample however many acks arrive.
    """
    engine, _client, transport, _ = build(tmp_path)
    submit(engine)
    await transport.drain()
    transport.on_report(report("pl7-1", status="NEW", execution="NEW", last_qty="0",
                               cum_qty="0"))

    assert len(transport.latency_samples()["submit"]) == 1
    assert transport.acks == 1


@pytest.mark.asyncio
async def test_an_ack_arriving_after_the_fill_does_not_reopen_a_finished_order(
    tmp_path: Path,
) -> None:
    """The user-data stream can beat the POST response, and a market order can fill first.

    Promoting a `FILLED` order back to `WORKING` would put a finished order into
    `ctx.open_orders()` for the rest of the session, where a strategy waiting for its book to
    clear would wait forever.
    """
    engine, _client, transport, _ = build(tmp_path)
    order_id = submit(engine)
    transport.on_report(report("pl7-1"))
    assert engine.orders[order_id].status is OrderStatus.FILLED

    await transport.drain()
    assert engine.orders[order_id].status is OrderStatus.FILLED


# ============================================================== refusals and limits


def test_amending_a_working_order_is_refused_rather_than_guessed_at(
    tmp_path: Path,
) -> None:
    """The engine's amend rules are ledger-side, and a transport cannot reach them.

    Queue priority, the overtaken-amendment refusal, the already-filled-past-the-new-size
    refusal -- all of `BacktestEngine._apply_modify`. Silently sending a cancel and a fresh
    order instead would *always* lose queue position, so a strategy's reprice would quote
    from the back of the book while it believed it had held its place. The refusal names the
    alternative and its cost.
    """
    engine, _client, _transport, _ = build(tmp_path)
    order_id = submit(engine, order_type=OrderType.LIMIT, price="39000")

    with pytest.raises(UnsupportedOrder, match="Cancel the order and submit a new one"):
        engine.runtime.modify(order_id, money("39100"), None)


def test_queued_requests_past_the_ceiling_refuse_rather_than_pile_up(
    tmp_path: Path,
) -> None:
    """A session that never scheduled `run` would otherwise queue orders silently forever.

    The engine's ledger counts those orders as working, so a run would believe it was in the
    market while nothing had been sent -- the worst state this transport can be in, and one
    nothing else in the system would notice. `MAX_UNSENT_REQUESTS` orders are accepted and
    the next is refused, so exactly that many sit in the queue.
    """
    engine, _client, transport, _ = build(tmp_path)
    for _ in range(MAX_UNSENT_REQUESTS):
        submit(engine)

    assert transport.summary()["queued"] == MAX_UNSENT_REQUESTS
    with pytest.raises(TransportStalled, match="queued and unsent"):
        submit(engine)


@pytest.mark.asyncio
async def test_an_order_that_cannot_be_expressed_is_refused_before_it_is_sent(
    tmp_path: Path,
) -> None:
    """A LIMIT order with no price cannot become a Binance order at all.

    Refused locally rather than sent for the exchange to refuse, because the exchange's
    answer would be a `-1102` naming a query parameter the strategy author never wrote.
    """
    engine, client, transport, _ = build(tmp_path)
    order_id = submit(engine, order_type=OrderType.LIMIT, price="39000")
    # Reach past the intent to produce the shape the engine cannot itself produce, which is
    # what makes this a guard against a future call site rather than against today's.
    from dataclasses import replace

    engine.orders[order_id].intent = replace(engine.orders[order_id].intent, price=None)
    await transport.drain()

    assert client.orders == [], "nothing was sent"
    assert engine.orders[order_id].status is OrderStatus.REJECTED
    assert "a LIMIT order needs a price" in engine.orders[order_id].reason


@pytest.mark.asyncio
async def test_the_kill_switch_can_clear_the_book_at_the_exchange(tmp_path: Path) -> None:
    """Spec 7.3 item 2: *"cancels all open orders on the exchange"*.

    `BacktestEngine._cancel_all` removes engine-initiated cancels from its *own* book without
    going through a transport, which is right for a simulated venue and leaves the real
    orders working. This is the call a session's halt path makes instead.
    """
    _engine, client, transport, _ = build(tmp_path)
    assert await transport.cancel_all_at_exchange(SYMBOL) is True
    assert client.cancel_alls == [SYMBOL]


@pytest.mark.asyncio
async def test_a_failed_kill_switch_cancel_reports_rather_than_raises(
    tmp_path: Path,
) -> None:
    """A halt that raised would abandon the rest of the halt path.

    The operator needs to be told the book may still be live at the exchange, which is a
    sentence in an event, not a traceback in a worker.
    """
    _engine, client, transport, events = build(tmp_path)
    client.fail_next = BinanceRestError(500, None, "Internal error")
    assert await transport.cancel_all_at_exchange(SYMBOL) is False
    assert "cancel them by hand" in events.details()


@pytest.mark.asyncio
async def test_the_sender_loop_stops_when_asked_and_flushes_what_it_had(
    tmp_path: Path,
) -> None:
    """`run(stop)` is the task a session owns, and it must both send and stop.

    One order is queued before the loop starts; the loop is given long enough to drain it and
    is then stopped. Without the drain the order would never reach the exchange; without the
    stop the session could not shut down.
    """
    engine, client, transport, _ = build(tmp_path)
    submit(engine)
    stop = asyncio.Event()
    task = asyncio.create_task(transport.run(stop))
    for _ in range(100):
        if client.orders:
            break
        await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(task, timeout=2.0)

    assert len(client.orders) == 1
    assert transport.summary()["queued"] == 0


# ------------------------------------------------------------------ the preflight gate


def test_the_transport_refuses_a_symbol_the_preflight_never_configured(
    tmp_path: Path,
) -> None:
    """A symbol the engine can trade but the account was never configured for.

    Its leverage and margin type are then whatever someone last set by hand in the Binance
    app. This is the shape of the original defect -- nothing sends the configuration, and
    nothing notices -- so the transport refuses to exist rather than trading on it.
    """
    engine, client, _, events = build(tmp_path)
    with pytest.raises(ValueError, match="did not configure"):
        ExchangeTransport(
            engine,
            client,
            run_id=RUN_ID,
            on_event=events,
            preflight=preflight_for("ETHUSDT", leverage=10),
        )


def test_the_transport_refuses_a_leverage_the_ledger_does_not_share(
    tmp_path: Path,
) -> None:
    """The ledger prices at 10x, the exchange was configured at 5x.

    Both numbers are individually plausible and every displayed figure would look
    reasonable. What makes it dangerous is the direction: the ledger's liquidation price
    sits further away than the real one, so the position dies before the platform says it
    can.
    """
    engine, client, _, events = build(tmp_path)
    with pytest.raises(ValueError, match="computing margin at 10x"):
        ExchangeTransport(
            engine,
            client,
            run_id=RUN_ID,
            on_event=events,
            preflight=preflight_for(SYMBOL, leverage=5),
        )


def test_a_position_mode_mismatch_refuses_the_transport(tmp_path: Path) -> None:
    """The ledger's mode and the account's mode must agree, or nothing may be sent.

    `configure_account` checks this too, and it is the *earlier* check -- but it runs once,
    and the account can be switched by hand in the Binance app between the preflight and the
    session's first order. This is the guard that cannot be skipped: `ExchangeTransport` takes
    the report as a required argument and refuses to exist unless it matches.

    The failure it prevents is quiet in both directions. A one-way ledger against a hedge
    account sends orders with no `positionSide`, which Binance rejects -- and "rejected" is
    indistinguishable downstream from "did not fill". A hedge ledger against a one-way account
    sends a `positionSide` the account cannot take, while the ledger tracks two positions that
    do not exist.
    """
    engine, client, _transport, events = build(tmp_path)
    assert engine.account.hedge_mode is False

    hedged_report = PreflightReport(
        symbols=preflight_for(SYMBOL, leverage=10).symbols,
        hedge_mode=True,
    )
    with pytest.raises(ValueError, match="hedge.*one-way|one-way.*hedge"):
        ExchangeTransport(
            engine, client, run_id=RUN_ID, on_event=events, preflight=hedged_report
        )

    # The matching report still constructs, so the guard is about the mismatch rather than
    # about the flag being set at all.
    ExchangeTransport(
        engine,
        client,
        run_id=RUN_ID,
        on_event=events,
        preflight=preflight_for(SYMBOL, leverage=10),
    )


# ------------------------------------------------------------------- wire parameters


def test_a_hedge_mode_order_carries_its_position_side_on_the_wire() -> None:
    """C7: `positionSide` is mandatory on a hedge-mode order and was never sent.

    `ctx` documents the field as "sent verbatim... a mismatch is rejected outright
    (-4061)" -- and `_order_params` omitted it, so a hedge-mode session had every order
    refused -4061, the rejection streak tripped the kill switch, and the halt blamed the
    strategy. The three cases below are the whole contract: a hedged intent names its
    leg, a one-way intent stays silent (the account default is BOTH, and one more
    parameter is one more thing to be refused for an unrelated reason), and the hedge
    exit is the opposite side routed to the *same* leg with no `reduceOnly` -- the flag
    is rejected alongside `positionSide` at the exchange, and the engine's own
    `OrderIntent.__post_init__` refuses the combination before it could get here.
    """
    from perplab.core.money import to_scaled
    from perplab.core.types import PositionSide
    from perplab.engine.executor_base import Order
    from perplab.live.exchange_transport import _order_params

    def wire(intent: OrderIntent) -> dict[str, Any]:
        order = Order(
            id="o1",
            intent=intent,
            submit_ts=START,
            arrival_ts=START,
            reference_price=money("40000"),
            remaining=to_scaled("0.5"),
        )
        return _order_params(order, "pl-1-1")

    entry = wire(
        OrderIntent(
            symbol=SYMBOL,
            side="BUY",
            qty=money("0.5"),
            type=OrderType.MARKET,
            position_side=PositionSide.LONG,
        )
    )
    assert entry["positionSide"] == "LONG"
    assert "reduceOnly" not in entry

    # The exit of the long leg: a SELL routed to the same side. Without `positionSide`
    # this exact request is an instruction to open a short.
    exit_long = wire(
        OrderIntent(
            symbol=SYMBOL,
            side="SELL",
            qty=money("0.5"),
            type=OrderType.MARKET,
            position_side=PositionSide.LONG,
        )
    )
    assert exit_long["positionSide"] == "LONG"
    assert "reduceOnly" not in exit_long

    one_way = wire(
        OrderIntent(symbol=SYMBOL, side="BUY", qty=money("0.5"), type=OrderType.MARKET)
    )
    assert "positionSide" not in one_way


@pytest.mark.asyncio
async def test_an_overlapping_drain_cannot_send_a_cancel_before_its_own_place(
    tmp_path: Path,
) -> None:
    """C11: two drainers on one deque broke the FIFO the module promises.

    The session keeps `run(stop)` alive through the end sequence while `_settle_until`
    calls `drain()` directly, so two coroutines could pop the same queue. A halt queues
    `place(exit)` then `cancel(resting)`; drainer A pops the place and parks on its POST,
    drainer B pops the cancel and sends it *now* -- the cancel lands first, its -2011
    reads as "already gone", then the place lands and rests at the venue believed
    cancelled. This test parks the first POST on an event, fires a second drain mid-park,
    and requires that the cancel still leaves after the place returns.
    """
    engine, client, transport, _ = build(tmp_path)

    chronology: list[str] = []
    gate = asyncio.Event()

    original_new_order = client.new_order
    original_cancel_order = client.cancel_order

    async def slow_new_order(**params: Any) -> dict[str, Any]:
        chronology.append("place")
        await gate.wait()
        return await original_new_order(**params)

    async def recording_cancel(*args: Any, **kwargs: Any) -> dict[str, Any]:
        chronology.append("cancel")
        return await original_cancel_order(*args, **kwargs)

    client.new_order = slow_new_order  # type: ignore[method-assign]
    client.cancel_order = recording_cancel  # type: ignore[method-assign]

    order_id = submit(engine)
    first = asyncio.create_task(transport.drain())
    for _ in range(100):
        if chronology:
            break
        await asyncio.sleep(0.01)
    assert chronology == ["place"], "the first drainer must be parked on the POST"

    # The halt's second instruction arrives while the first is still on the wire.
    transport.cancel(engine.orders[order_id], reason="halt")
    second = asyncio.create_task(transport.drain())
    await asyncio.sleep(0.05)
    assert chronology == ["place"], (
        "an overlapping drain sent the cancel while the place was still in flight"
    )

    gate.set()
    await asyncio.wait_for(asyncio.gather(first, second), timeout=2.0)
    assert chronology == ["place", "cancel"]


def test_the_live_path_refuses_the_same_bad_orders_the_backtest_refuses(
    tmp_path: Path,
) -> None:
    """C6, as a differential: the same intent must be refused on both sides of the seam.

    For one release `validate_order` ran only on the simulated arrival path, which a real
    transport never takes -- so an off-tick limit the backtest refused locally was signed
    and sent, Binance answered -1111, the streak tripped the kill switch, and a healthy
    session halted. Worse than the halt was the parity break: backtest and live were
    refusing *different order sets*. Each case below submits through `_submit` with the
    live transport wired and asserts the order dies as REJECTED with the same
    exchange-vocabulary reason the simulated arrival would produce -- and that nothing
    reached the wire.
    """
    engine, client, transport, _ = build(tmp_path)
    assert engine.transport.simulated is False

    # An off-tick limit: BTCUSDT's tick is 0.10, so 42123.456789 cannot print.
    off_tick = submit(
        engine, order_type=OrderType.LIMIT, price="42123.456789", qty="0.5"
    )
    order = engine.orders[off_tick]
    assert order.status is OrderStatus.REJECTED
    assert "PRICE_FILTER" in order.reason
    assert client.orders == [], "a locally-refused order must never reach the wire"

    # Below MIN_NOTIONAL: 0.001 x 40000 = 40 against the fixture's floor of 50.
    thin = submit(engine, order_type=OrderType.LIMIT, price="40000", qty="0.001")
    assert engine.orders[thin].status is OrderStatus.REJECTED
    assert "MIN_NOTIONAL" in engine.orders[thin].reason
    assert client.orders == []

    # A market order carries no price, so no price-anchored filter may fire -- the mark
    # here (40000) is deliberately tick-aligned-or-not irrelevant: the order is queued.
    fine = submit(engine, order_type=OrderType.MARKET, qty="0.5")
    assert engine.orders[fine].status is OrderStatus.PENDING
    assert transport.summary()["queued"] == 1


# ============================================== settling the unknown (C10) and H5


@pytest.mark.asyncio
async def test_a_never_landed_unknown_outcome_is_retired_by_the_query(
    tmp_path: Path,
) -> None:
    """C10, branch one: the venue has no record, so the POST never arrived.

    `signed.py` promises unknown outcomes are "settled by reconciliation, never by
    assumption", and for one release nothing settled them -- `unresolved` was rendered in
    the UI and read by no code. Here the query answers -2013 and the order is retired
    loudly: there is nothing at the venue to double-fill against, so PENDING-forever
    would only park capital against a ghost.
    """
    from perplab.exchange.signed import ORDER_DOES_NOT_EXIST_CODE

    engine, client, transport, events = build(tmp_path)
    order_id = submit(engine)
    client.fail_next = OrderOutcomeUnknown(
        symbol=SYMBOL, client_order_id="pl7-1", detail="ReadTimeout"
    )
    await transport.drain()
    assert list(transport.unresolved) == ["pl7-1"]

    async def get_order(symbol: str, *, client_order_id: str) -> dict[str, Any]:
        raise BinanceRestError(400, ORDER_DOES_NOT_EXIST_CODE, "Order does not exist.")

    client.get_order = get_order  # type: ignore[attr-defined]
    resolved = await transport.resolve_unknown_outcomes()

    assert resolved == 1
    assert transport.unresolved == {}
    assert transport.unknown_resolved == 1
    assert engine.orders[order_id].status is OrderStatus.EXPIRED
    assert "never landed" in engine.orders[order_id].reason
    assert engine.account.qty(SYMBOL) == money("0")


@pytest.mark.asyncio
async def test_a_filled_unknown_outcome_is_booked_through_the_shared_ledger_path(
    tmp_path: Path,
) -> None:
    """C10, branch two: the POST landed and filled while we were not listening.

    The resolution replays the venue's snapshot through `on_report`, so the fill books
    via the very `_book` a stream frame uses -- asserted at the ledger, exactly as the
    stream tests assert, because a resolution path with its own booking code would be a
    second opinion about the account.
    """
    engine, client, transport, _ = build(tmp_path)
    order_id = submit(engine)
    client.fail_next = OrderOutcomeUnknown(
        symbol=SYMBOL, client_order_id="pl7-1", detail="ReadTimeout"
    )
    await transport.drain()

    async def get_order(symbol: str, *, client_order_id: str) -> dict[str, Any]:
        return {
            "symbol": symbol,
            "orderId": 9_001,
            "clientOrderId": client_order_id,
            "status": "FILLED",
            "executedQty": "1",
            "avgPrice": "40100.0",
            "updateTime": START + 5_000,
        }

    client.get_order = get_order  # type: ignore[attr-defined]
    resolved = await transport.resolve_unknown_outcomes()

    assert resolved == 1
    assert transport.unresolved == {}
    assert engine.counts["fills"] == 1
    assert engine.account.qty(SYMBOL) == money("1")
    assert engine.account.position(SYMBOL) is not None
    assert engine.account.position(SYMBOL).entry_price == money("40100.0")
    assert engine.orders[order_id].status is OrderStatus.FILLED

    # Running it again finds nothing unknown -- and cannot double-book: the report path's
    # cumulative bookkeeping treats a replay as the duplicate it is.
    assert await transport.resolve_unknown_outcomes() == 0
    assert engine.counts["fills"] == 1


@pytest.mark.asyncio
async def test_the_open_order_diff_reports_orphans_and_two_pass_absences(
    tmp_path: Path,
) -> None:
    """H5: the five account quantities cannot see a resting order; the diff can.

    Three shapes, each reported once rather than once per pass: a venue order this
    session never issued (foreign flow before its fill arrives as such); an engine order
    the venue stops listing, alarmed only on the second consecutive pass so a fill ack
    in flight does not cry wolf; and nothing at all when the two books agree.
    """
    engine, client, transport, events = build(tmp_path)
    order_id = submit(
        engine, order_type=OrderType.LIMIT, price="39000", qty="1", tif=TimeInForce.GTC
    )
    await transport.drain()
    # Ack the placement so the engine's order counts as venue-known.
    frame = order_frame("pl7-1", status="NEW", execution="NEW", last_qty="0", cum_qty="0")
    report = parse_user_frame(frame)
    transport.on_report(report)
    assert engine.orders[order_id].is_open

    ours = {"clientOrderId": "pl7-1", "orderId": 5001}
    foreign = {"clientOrderId": "web_abc123", "orderId": 7777}

    # Pass 1: both books agree on ours; a foreign order appears -> reported once.
    transport.reconcile_open_orders({SYMBOL: [ours, foreign]})
    assert transport.open_order_mismatches == 1
    transport.reconcile_open_orders({SYMBOL: [ours, foreign]})
    assert transport.open_order_mismatches == 1, "a foreign order is reported once, not per pass"
    assert "never issued" in events.details()

    # Ours vanishes from the venue: pass one is grace, pass two is the alarm.
    transport.reconcile_open_orders({SYMBOL: [foreign]})
    assert transport.open_order_mismatches == 1
    transport.reconcile_open_orders({SYMBOL: [foreign]})
    assert transport.open_order_mismatches == 2
    assert "has not listed it for two passes" in events.details()


# ================================================= the venue's own commission (H6)


def test_a_fill_books_the_venues_actual_commission_not_the_modelled_fee(
    tmp_path: Path,
) -> None:
    """H6: the report's `n` field is the bill; the fee schedule is a guess about it.

    The platform's first live round trip halted on exactly this: the model charged
    0.05% taker while the venue charged 0.04%, and reconciliation refused the wallet by
    the difference. The commission is parsed off every execution report, and for one
    release it was then discarded. Here a 1 BTC fill at 40 000 would be modelled at
    40 000 x taker rate; the venue says 16.0, and 16.0 is what the wallet moves by.
    """
    engine, client, transport, _ = build(tmp_path)
    submit(engine)
    asyncio.run(transport.drain())
    cid = client.orders[-1]["newClientOrderId"]

    wallet_before = engine.account.wallet
    transport.on_report(report(cid, commission="16.0"))

    assert engine.counts["fills"] == 1
    with_fee = wallet_before - engine.account.wallet
    assert with_fee == money("16.0"), (
        f"the wallet must move by the venue's exact commission, not the model's; "
        f"moved {with_fee}"
    )
    assert engine.account.total_fees == money("16.0")


def test_a_non_usdt_commission_falls_back_to_the_model(tmp_path: Path) -> None:
    """A BNB-discounted fee is real but not in the ledger's currency.

    Booking its number as USDT would charge ~0.0004 BNB as 0.0004 dollars. The model
    stands in, and the wallet reconciliation measures the discount as the drift it
    genuinely is -- an account trading with BNB fee payment enabled needs the operator
    to know their wallet will not reconcile to the cent, not a silently wrong ledger.
    """
    engine, client, transport, _ = build(tmp_path)
    submit(engine)
    asyncio.run(transport.drain())
    cid = client.orders[-1]["newClientOrderId"]

    wallet_before = engine.account.wallet
    transport.on_report(report(cid, commission="0.0004", commission_asset="BNB"))

    assert engine.counts["fills"] == 1
    fee_taken = wallet_before - engine.account.wallet
    # The default schedule's taker rate applied to 1 x 40 000 -- whatever it is, it is
    # not the BNB number.
    assert fee_taken != money("0.0004")
    assert fee_taken == engine.config.fees.taker_rate * money("40000")


def test_a_redelivered_frame_with_no_cumulative_is_still_deduplicated(
    tmp_path: Path,
) -> None:
    """M13: the cumulative (`z`) dedup is disabled exactly when `z` is absent or zero.

    This module has never seen a real Binance response, so "z is always present" is an
    assumption -- and the trade id is the second, independent lock: a redelivered frame
    redelivers its own `t`. Two identical partial-fill frames with z=0 must book once.
    """
    engine, client, transport, _ = build(tmp_path)
    submit(engine)
    asyncio.run(transport.drain())
    cid = client.orders[-1]["newClientOrderId"]

    frame = report(
        cid,
        status="PARTIALLY_FILLED",
        last_qty="0.4",
        cum_qty="0",  # the falsy-z shape the audit named
        trade_id=4242,
    )
    transport.on_report(frame)
    assert engine.account.qty(SYMBOL) == money("0.4")

    transport.on_report(
        report(
            cid,
            status="PARTIALLY_FILLED",
            last_qty="0.4",
            cum_qty="0",
            trade_id=4242,  # the same execution, redelivered
        )
    )
    assert engine.account.qty(SYMBOL) == money("0.4"), "a redelivery must not book twice"
    assert transport.duplicate_reports == 1


# ============================================== reduce-only is clamped before the wire (M7)


def test_a_reduce_only_order_is_clamped_to_the_position_before_the_wire(
    tmp_path: Path,
) -> None:
    """M7: the venue must never see a reduce-only order for more than the position.

    The risk layer exempts reduce-only orders from every size limit -- correctly, they can
    only shrink exposure -- and the simulator clamps them at its modelled arrival. The live
    path had neither: `_submit` handed the unclamped intent to `transport.place`, so a
    `ctx.close()` sized before a partial exit went out for the stale full quantity. The
    position here is 0.4 long and the exit asks for 1: what rides the wire must be 0.4,
    and the engine's own book must agree with the wire (`order.remaining == 0.4`).
    """
    engine, client, transport, _ = build(tmp_path)
    engine.account.apply_fill(START, SYMBOL, money("0.4"), money("40000"))
    assert engine.account.qty(SYMBOL) == money("0.4")

    order_id = submit(engine, side="SELL", qty="1", tag="exit")
    asyncio.run(transport.drain())

    # submit() builds a plain order; rebuild the reduce-only version explicitly.
    assert client.orders[-1]["quantity"] == money("1"), "control: non-reduce-only is unclamped"

    order_id = engine.runtime.submit(
        OrderIntent(symbol=SYMBOL, side="SELL", qty=money("1"), reduce_only=True)
    )
    asyncio.run(transport.drain())
    sent = client.orders[-1]
    assert sent["reduceOnly"] is True
    assert sent["quantity"] == money("0.4"), "the wire quantity must be the position"
    assert engine.orders[order_id].remaining == 40_000_000, (
        "the ledger's working size must agree with the wire (0.4 scaled by 10^8)"
    )


def test_a_reduce_only_order_against_a_flat_position_never_reaches_the_wire(
    tmp_path: Path,
) -> None:
    """The wire twin of `_clamp`'s flat case: nothing to reduce, nothing to send.

    Cancelled rather than rejected, exactly as the simulated arrival treats it -- a bracket
    outliving its position is ordinary, not a sizing mistake -- and cancelled *locally*: a
    real venue answers a flat reduce-only with `-2022 ReduceOnly Order is rejected`, which
    would feed `observe_rejection` and the kill switch's streak for an order that means
    nothing is wrong.
    """
    engine, client, transport, _ = build(tmp_path)
    assert engine.account.qty(SYMBOL) == 0

    order_id = engine.runtime.submit(
        OrderIntent(symbol=SYMBOL, side="SELL", qty=money("1"), reduce_only=True)
    )
    asyncio.run(transport.drain())

    assert client.orders == [], "nothing may reach the venue"
    order = engine.orders[order_id]
    assert order.status is OrderStatus.CANCELLED
    assert "already flat" in order.reason


# ===================================================== the route table is bounded (M19)


def _fill_and_retire(transport, client, engine, count: int) -> list[str]:
    """Place `count` market orders, fill each at the venue, return their client ids."""
    cids = []
    for _ in range(count):
        submit(engine)
        asyncio.run(transport.drain())
        cid = client.orders[-1]["newClientOrderId"]
        transport.on_report(report(cid))
        cids.append(cid)
    return cids


def test_terminal_routes_drop_after_the_retention_window(tmp_path: Path) -> None:
    """M19: `_routes`/`_by_order_id` grew one entry per order for the life of a session.

    Three filled orders retire; the first sweep stamps them, a sweep one second past the
    retention window evicts all three, and `summary()` reports the tombstone count. A late
    duplicate frame for an evicted id still books as a *duplicate* -- the `pl{run}-` prefix
    says it is ours -- never as foreign flow, which would read as "something else is
    trading this account".
    """
    import time as _time

    from perplab.live.exchange_transport import ROUTE_RETENTION_S

    engine, client, transport, _ = build(tmp_path)
    cids = _fill_and_retire(transport, client, engine, 3)
    assert len(transport._routes) == 3

    now = _time.monotonic()
    transport._evict_expired(now)  # stamps retired_at; nothing may go yet
    assert len(transport._routes) == 3
    transport._evict_expired(now + ROUTE_RETENTION_S + 1.0)
    assert transport._routes == {}
    assert transport._by_order_id == {}
    assert transport.summary()["routes_evicted"] == 3

    before_foreign = transport.foreign_reports
    transport.on_report(report(cids[0]))
    assert transport.foreign_reports == before_foreign, "our own late frame is not foreign"
    assert transport.duplicate_reports == 1
    assert engine.counts["fills"] == 3, "nothing may book against an evicted route"


def test_an_open_or_unresolved_route_is_never_evicted(tmp_path: Path) -> None:
    """The retention rule's hard floor: eviction may only forget finished business.

    An order whose POST was never answered is exactly what `resolve_unknown_outcomes`
    exists to settle, and an open order's route is its only link to the wire. Both must
    survive any amount of clock.
    """
    import time as _time

    from perplab.live.exchange_transport import ROUTE_RETENTION_S

    engine, client, transport, _ = build(tmp_path)

    client.fail_next = OrderOutcomeUnknown(
        symbol=SYMBOL, client_order_id="pl7-1", detail="read timeout after POST"
    )
    submit(engine)
    asyncio.run(transport.drain())
    unknown_cid = client.orders[-1]["newClientOrderId"]
    assert list(transport.unresolved) == [unknown_cid]
    assert "read timeout after POST" in transport.unresolved[unknown_cid]

    submit(engine, order_type=OrderType.LIMIT, price="39000")
    asyncio.run(transport.drain())
    open_cid = client.orders[-1]["newClientOrderId"]
    transport.on_report(report(open_cid, status="NEW", execution="NEW", last_qty="0", cum_qty="0"))

    transport._evict_expired(_time.monotonic())
    transport._evict_expired(_time.monotonic() + 100 * ROUTE_RETENTION_S)
    assert set(transport._routes) == {unknown_cid, open_cid}
    assert transport.summary()["routes_evicted"] == 0


def test_the_once_per_id_report_guards_are_pruned_each_pass(tmp_path: Path) -> None:
    """M19's other leak: `_venue_orphans_reported` grew per distinct foreign id forever.

    A foreign order reported on one pass and gone from the venue's set on the next has no
    business being remembered: the guard exists to avoid re-reporting a *persisting*
    condition, and pruning to the venue's own working set bounds the memory by the venue's
    open orders rather than by two days of someone else's trading.
    """
    engine, client, transport, _ = build(tmp_path)

    row = {"clientOrderId": "someone-elses-1"}
    transport.reconcile_open_orders({SYMBOL: [row]})
    assert transport.open_order_mismatches == 1
    assert "someone-elses-1" in transport._venue_orphans_reported

    # Same id still working: guarded, no second report.
    transport.reconcile_open_orders({SYMBOL: [row]})
    assert transport.open_order_mismatches == 1

    # Gone from the venue: the guard entry is pruned with it.
    transport.reconcile_open_orders({SYMBOL: []})
    assert "someone-elses-1" not in transport._venue_orphans_reported

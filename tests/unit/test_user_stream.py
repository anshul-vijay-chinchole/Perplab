"""Tests for the user-data stream (spec 6.7, spec 11).

This socket is how PerpLab's ledger learns about a fill inside the 60 s before spec 6.7's
reconciliation pass compares it against the exchange and fires the kill switch on any
mismatch. Everything below is testing one of the two ways that goes wrong: a frame that is
translated incorrectly, or a feed that stops delivering without saying so.

The second is the one worth the most attention, and it has a specific shape here.
`listenKeyExpired` does not close the socket. The connection stays up, `StreamManager` sees
a healthy link forever, heartbeats keep being written, and every fill after that instant is
simply never mentioned. A design that routed it into the ordinary disconnect path would
report an outage that self-heals -- the socket reconnects fine, to a dead key -- and leave
a live session sitting on a connection that will never speak again. So it is surfaced as
its own report kind, recorded as STALE rather than DISCONNECT, and acted on by re-keying.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any

import pytest

from perplab.core.types import CollectorEventKind, Side
from perplab.exchange import userstream
from perplab.exchange.userstream import (
    KEEPALIVE_INTERVAL_S,
    LISTEN_KEY_TTL_S,
    USER_STREAM_LABEL,
    ExchangeReport,
    ReportKind,
    UnhandledUserEvent,
    UserDataStream,
    parse_user_frame,
)

SYMBOL = "BTCUSDT"


def order_frame(**overrides: Any) -> dict[str, Any]:
    """One `ORDER_TRADE_UPDATE`: a 0.020 BTC buy filling at 63000.10, maker side.

    Values chosen so every scaled figure below is derivable by hand at the fixed 10^8
    scale (`core.money`): 0.020 -> 2_000_000, 63000.10 -> 6_300_010_000_000,
    0.050 -> 5_000_000, 62999.50 -> 6_299_950_000_000, 0.50400080 -> 50_400_080,
    -1.25 -> -125_000_000.
    """
    order: dict[str, Any] = {
        "s": SYMBOL,
        "c": "perplab-7f3a",
        "S": "BUY",
        "o": "LIMIT",
        "f": "GTC",
        "q": "0.100",
        "p": "63000.10",
        "x": "TRADE",
        "X": "PARTIALLY_FILLED",
        "i": 8_886_774,
        "l": "0.020",
        "z": "0.050",
        "L": "63000.10",
        "N": "USDT",
        "n": "0.50400080",
        "T": 1_568_879_465_650,
        "t": 42,
        "m": True,
        "R": False,
        "ap": "62999.50",
        "rp": "-1.25",
    }
    order.update(overrides)
    return {"e": "ORDER_TRADE_UPDATE", "E": 1_568_879_465_651, "T": 1_568_879_465_650, "o": order}


ACCOUNT_FRAME: dict[str, Any] = {
    "e": "ACCOUNT_UPDATE",
    "E": 1_564_745_798_939,
    "T": 1_564_745_798_938,
    "a": {
        "m": "ORDER",
        "B": [{"a": "USDT", "wb": "122624.12345678", "cw": "100.12345678", "bc": "50.12345678"}],
        "P": [
            {
                "s": SYMBOL,
                "pa": "0.050",
                "ep": "62999.50",
                "cr": "200",
                "up": "-1.166074",
                "mt": "cross",
                "iw": "0",
                "ps": "BOTH",
            }
        ],
    },
}

LEVERAGE_FRAME: dict[str, Any] = {
    "e": "ACCOUNT_CONFIG_UPDATE",
    "E": 1_611_646_737_479,
    "T": 1_611_646_737_476,
    "ac": {"s": SYMBOL, "l": 25},
}

MULTI_ASSETS_FRAME: dict[str, Any] = {
    "e": "ACCOUNT_CONFIG_UPDATE",
    "E": 1_611_646_737_480,
    "T": 1_611_646_737_477,
    "ai": {"j": True},
}

MARGIN_CALL_FRAME: dict[str, Any] = {
    "e": "MARGIN_CALL",
    "E": 1_587_727_187_525,
    "cw": "3.16812045",
    "p": [
        {
            "s": SYMBOL,
            "ps": "LONG",
            "pa": "1.327",
            "mt": "CROSSED",
            "iw": "0",
            "mp": "187.17127",
            "up": "-1.166074",
            "mm": "1.614445",
        }
    ],
}

EXPIRED_FRAME: dict[str, Any] = {"e": "listenKeyExpired", "E": 1_576_653_824_250}


class Recorder:
    """Collects what the stream emits, standing in for the live session."""

    def __init__(self) -> None:
        self.reports: list[ExchangeReport] = []
        self.events: list[tuple[CollectorEventKind, str, str, int]] = []

    def on_report(self, report: ExchangeReport) -> None:
        self.reports.append(report)

    def on_event(
        self, kind: CollectorEventKind, stream: str, detail: str, downtime_ms: int
    ) -> None:
        self.events.append((kind, stream, detail, downtime_ms))

    def kinds(self) -> list[CollectorEventKind]:
        return [k for k, _, _, _ in self.events]

    def details(self, kind: CollectorEventKind) -> list[str]:
        return [d for k, _, d, _ in self.events if k is kind]


class FakeRest:
    """The three listen-key endpoints of `SignedRestClient`, and a log of the calls.

    The call log is ordered rather than counted because the ordering is load-bearing: a
    close issued *after* the replacement key was created would close the key the new socket
    had just connected with, since `POST /fapi/v1/listenKey` returns the currently active
    key rather than minting a second one.
    """

    def __init__(self) -> None:
        self.ops: list[str] = []
        self.create_error: Exception | None = None
        self.keepalive_error: Exception | None = None
        self._created = 0

    async def listen_key_create(self) -> str:
        if self.create_error is not None:
            raise self.create_error
        self._created += 1
        key = f"listen-key-{self._created}"
        self.ops.append(f"create:{key}")
        return key

    async def listen_key_keepalive(self) -> None:
        if self.keepalive_error is not None:
            raise self.keepalive_error
        self.ops.append("keepalive")

    async def listen_key_close(self) -> None:
        self.ops.append("close")

    @property
    def keepalives(self) -> int:
        return self.ops.count("keepalive")

    @property
    def creates(self) -> list[str]:
        return [op.split(":", 1)[1] for op in self.ops if op.startswith("create:")]


class FakeManager:
    """A `StreamManager` shaped just enough to hold a session open and deliver frames.

    It never opens a socket. What it preserves is the part the stream depends on: the raw
    path it was constructed with, the `on_message` callback, and a `run` that lasts until
    its stop event is set.
    """

    def __init__(
        self,
        streams: Any,
        on_message: Callable[[str, dict[str, Any], int], None],
        on_event: Callable[[CollectorEventKind, str, str, int], None],
        *,
        base_url: str,
        raw_path: str | None = None,
        label: str | None = None,
        **_: Any,
    ) -> None:
        self.streams = list(streams)
        self.on_message = on_message
        self.on_event = on_event
        self.base_url = base_url
        self.raw_path = raw_path
        self.label = label
        self.finished = False
        self.end_immediately = False

    async def run(self, stop: asyncio.Event) -> None:
        if not self.end_immediately:
            await stop.wait()
        self.finished = True

    def deliver(self, payload: dict[str, Any]) -> None:
        """Hand a frame to the stream the way raw mode does: `e` as the label, whole payload."""
        self.on_message(str(payload.get("e") or ""), payload, 0)


def install_fake_manager(
    monkeypatch: pytest.MonkeyPatch, *, ending_early: frozenset[int] = frozenset()
) -> list[FakeManager]:
    """Replace `StreamManager` with a socketless stand-in, and collect what gets built.

    `ending_early` names managers, by order of construction, whose `run` returns on its own
    instead of lasting until its stop event -- the one thing the real manager only does
    when it hits a failure it cannot absorb.
    """
    made: list[FakeManager] = []

    def factory(*args: Any, **kwargs: Any) -> FakeManager:
        manager = FakeManager(*args, **kwargs)
        manager.end_immediately = len(made) in ending_early
        made.append(manager)
        return manager

    monkeypatch.setattr(userstream, "StreamManager", factory)
    return made


async def until(predicate: Callable[[], bool], timeout: float = 2.0) -> None:
    """Wait for `predicate`, or fail the test.

    Polled rather than event-driven because the conditions being waited on are the
    stream's own internal transitions -- a second manager appearing, a keepalive landing --
    and adding hooks for the test to await would be adding production surface to observe
    the thing under test.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.005)
    raise AssertionError("condition was not reached within the timeout")


class TestFrameParsing:
    def test_an_order_trade_update_carries_the_fill_as_scaled_integers(self) -> None:
        """Prices and quantities are scaled int64 at the fixed 10^8 scale (spec 3.1).

        0.020 * 10^8 = 2_000_000; 63000.10 * 10^8 = 6_300_010_000_000;
        0.050 * 10^8 = 5_000_000; 62999.50 * 10^8 = 6_299_950_000_000;
        0.50400080 * 10^8 = 50_400_080. `float("63000.10")` is not 63000.10, which is the
        whole reason the exchange sends these as strings and this parses them as such.
        """
        report = parse_user_frame(order_frame())

        assert report.kind is ReportKind.ORDER
        assert report.ts_ms == 1_568_879_465_651
        assert report.symbol == SYMBOL
        assert report.order_id == 8_886_774
        assert report.client_order_id == "perplab-7f3a"
        assert report.status == "PARTIALLY_FILLED"
        assert report.reason == "TRADE"
        assert report.side is Side.BUY
        assert report.last_filled_qty == 2_000_000
        assert report.last_filled_price == 6_300_010_000_000
        assert report.cum_filled_qty == 5_000_000
        assert report.avg_price == 6_299_950_000_000
        assert report.commission == 50_400_080
        assert report.commission_asset == "USDT"
        assert all(
            isinstance(value, int)
            for value in (
                report.last_filled_qty,
                report.last_filled_price,
                report.cum_filled_qty,
                report.avg_price,
                report.commission,
                report.realized_pnl,
            )
        )

    def test_a_realized_loss_keeps_its_sign(self) -> None:
        """-1.25 * 10^8 = -125_000_000.

        A sign dropped here turns every losing fill into a winning one, and the equity
        curve stays perfectly plausible while being exactly wrong.
        """
        assert parse_user_frame(order_frame()).realized_pnl == -125_000_000

    def test_the_maker_flag_is_carried_through_unchanged(self) -> None:
        """Inverting it turns a maker rebate into a taker fee in the parity report.

        The per-fill discrepancy is small enough to read as fill-model error rather than as
        a sign flip, which is what makes it worth pinning rather than trusting.
        """
        assert parse_user_frame(order_frame(m=True)).is_maker is True
        assert parse_user_frame(order_frame(m=False)).is_maker is False

    def test_reduce_only_survives_parsing(self) -> None:
        assert parse_user_frame(order_frame(R=True)).reduce_only is True
        assert parse_user_frame(order_frame()).reduce_only is False

    def test_a_null_commission_asset_reads_as_absent_rather_than_as_a_string(self) -> None:
        """Binance sends `N: null` on an event that charged no commission."""
        report = parse_user_frame(order_frame(N=None, n=None))

        assert report.commission_asset == ""
        assert report.commission == 0

    def test_a_liquidation_fill_arrives_with_an_id_we_never_submitted(self) -> None:
        """The exchange mints the client order id for a forced close.

        Recognising the prefix is how spec 7's `halt_on_liquidation` learns it happened at
        all, so the field has to be carried verbatim rather than normalised or blanked
        because it matches no order in the engine's book.
        """
        report = parse_user_frame(order_frame(c="autoclose-1568879465651", x="CALCULATED"))

        assert report.client_order_id == "autoclose-1568879465651"
        assert report.reason == "CALCULATED"

    def test_a_missing_price_field_is_refused_rather_than_read_as_zero(self) -> None:
        """A report built around a guess is a fill with a plausible wrong price.

        The ledger has no way to notice that; it does notice a refusal, which lands in the
        event log as a malformed frame.
        """
        frame = order_frame()
        del frame["o"]["L"]

        with pytest.raises(KeyError):
            parse_user_frame(frame)

    def test_an_account_update_names_its_reason_and_keeps_its_payload(self) -> None:
        """Balances and positions stay in `raw` on purpose.

        Spec 6.7 reconciles against `GET /fapi/v2/account`, which is authoritative. A
        second account snapshot with the same field names from a weaker source reads fine
        right up until the two disagree, and then nobody can say which one the ledger was
        built from.
        """
        report = parse_user_frame(ACCOUNT_FRAME)

        assert report.kind is ReportKind.ACCOUNT
        assert report.ts_ms == 1_564_745_798_939
        assert report.reason == "ORDER"
        assert report.raw["a"]["P"][0]["s"] == SYMBOL
        assert report.symbol == ""

    def test_a_funding_payment_is_an_account_update_with_its_own_reason(self) -> None:
        """`FUNDING_FEE` moves the wallet balance with no order behind it (spec 3.5)."""
        frame = {**ACCOUNT_FRAME, "a": {**ACCOUNT_FRAME["a"], "m": "FUNDING_FEE"}}

        assert parse_user_frame(frame).reason == "FUNDING_FEE"

    def test_a_leverage_change_names_the_symbol_it_applies_to(self) -> None:
        """A leverage change made in the Binance app invalidates every margin figure the
        engine computed (spec 3.6). Without this event the first symptom is spec 6.7's
        reconciliation firing the kill switch on a `P_liq` mismatch nobody can explain.
        """
        report = parse_user_frame(LEVERAGE_FRAME)

        assert report.kind is ReportKind.ACCOUNT_CONFIG
        assert report.symbol == SYMBOL
        assert report.reason == "LEVERAGE"
        assert report.raw["ac"]["l"] == 25

    def test_a_multi_assets_mode_change_is_the_same_kind_with_no_symbol(self) -> None:
        """It is an account-wide setting, so naming a symbol would be inventing one."""
        report = parse_user_frame(MULTI_ASSETS_FRAME)

        assert report.kind is ReportKind.ACCOUNT_CONFIG
        assert report.symbol == ""
        assert report.reason == "MULTI_ASSETS_MODE"

    def test_a_config_update_that_says_nothing_changed_is_refused(self) -> None:
        """Refuse loudly over guessing quietly: reporting a configuration change without
        saying what changed would leave the engine's margin state unchanged and a record
        claiming otherwise.
        """
        with pytest.raises(KeyError, match="neither 'ac'"):
            parse_user_frame({"e": "ACCOUNT_CONFIG_UPDATE", "E": 1})

    def test_a_margin_call_is_its_own_kind_and_keeps_every_position(self) -> None:
        """`p` can carry several symbols, so no single `symbol` is named.

        A field that is sometimes populated and sometimes not is one every consumer has to
        special-case, and the positions -- with their mark prices and maintenance margins
        -- are all in `raw`.
        """
        report = parse_user_frame(MARGIN_CALL_FRAME)

        assert report.kind is ReportKind.MARGIN_CALL
        assert report.ts_ms == 1_587_727_187_525
        assert report.symbol == ""
        assert report.raw["p"][0]["mm"] == "1.614445"

    def test_an_unknown_event_name_is_a_refusal_and_not_a_default_branch(self) -> None:
        """Binance adds events to this stream without notice.

        `UnhandledUserEvent` rather than a catch-all report kind: a report the caller has
        no way to act on is worse than a counted absence, because it looks like coverage.
        """
        with pytest.raises(UnhandledUserEvent, match="TRADE_LITE"):
            parse_user_frame({"e": "TRADE_LITE", "E": 1})

    def test_a_report_cannot_be_edited_after_delivery(self) -> None:
        """This is the ledger's input. A fill whose price can be rewritten by whoever reads
        it second is not a record of anything.
        """
        report = parse_user_frame(order_frame())

        with pytest.raises(AttributeError):
            report.last_filled_price = 1


class TestListenKeyExpiry:
    def test_a_listen_key_expiry_is_not_reported_as_a_disconnect(self) -> None:
        """The socket stays open and simply stops delivering.

        Reported as STALE, which `CollectorEventKind` defines as exactly this state -- a
        subscribed stream stopped delivering while the connection stayed up. Calling it a
        DISCONNECT would be wrong twice: spec 4.5's gap detector would treat the silence as
        explained by a drop that never happened, and spec 7's disconnect auto-trigger would
        start counting downtime on a socket that is still connected.
        """
        recorder = Recorder()
        stream = UserDataStream(FakeRest(), recorder.on_report, recorder.on_event)

        stream._on_frame("listenKeyExpired", dict(EXPIRED_FRAME), 0)

        assert [r.kind for r in recorder.reports] == [ReportKind.LISTEN_KEY_EXPIRED]
        assert recorder.kinds() == [CollectorEventKind.STALE]
        assert CollectorEventKind.DISCONNECT not in recorder.kinds()
        assert "listen key expired" in recorder.details(CollectorEventKind.STALE)[0]
        assert stream.listen_key_expiries == 1

    def test_an_expiry_frame_never_carries_the_key_into_the_report(self) -> None:
        """The listen key is a bearer token for the account's own order flow.

        `raw` is the field most likely to be written into a run record, so the key is
        dropped once at the parser rather than at every consumer -- the same reasoning
        `ws.StreamManager._sanitise` applies to its own event text.
        """
        report = parse_user_frame({**EXPIRED_FRAME, "listenKey": "s3cr3t-listen-key"})

        assert report.kind is ReportKind.LISTEN_KEY_EXPIRED
        assert "listenKey" not in report.raw
        assert "s3cr3t-listen-key" not in repr(report)
        assert "s3cr3t-listen-key" not in str(report.raw)

    def test_an_expiry_frame_missing_its_timestamp_is_still_acted_on(self) -> None:
        """The one place a missing field is tolerated, and the reason is asymmetric.

        Refusing this frame counts a malformed frame and moves on, leaving the session
        attached to a socket that has already stopped delivering -- the exact failure the
        event exists to prevent. A report with `ts_ms=0` still re-keys the stream.
        """
        report = parse_user_frame({"e": "listenKeyExpired"})

        assert report.kind is ReportKind.LISTEN_KEY_EXPIRED
        assert report.ts_ms == 0


class TestBadFrames:
    def test_a_malformed_frame_does_not_kill_the_stream(self) -> None:
        """`StreamManager` puts no handler around `on_message`.

        An exception raised while parsing escapes the read loop, is caught by the reconnect
        arm and reported as a disconnect -- so one unfamiliar frame shape would become a
        permanent reconnect loop against a socket that is working perfectly.
        """
        recorder = Recorder()
        stream = UserDataStream(FakeRest(), recorder.on_report, recorder.on_event)
        broken = order_frame()
        del broken["o"]["L"]

        stream._on_frame("ORDER_TRADE_UPDATE", broken, 0)
        stream._on_frame("ORDER_TRADE_UPDATE", order_frame(), 0)

        assert stream.malformed_frames == 1
        assert [r.kind for r in recorder.reports] == [ReportKind.ORDER]
        assert "malformed user-data frame" in recorder.details(CollectorEventKind.STALE)[0]

    def test_an_unhandled_event_name_is_counted_separately_from_a_malformed_one(
        self,
    ) -> None:
        """An event that is newer than us, and a field we depend on having moved, are
        different faults.

        Counting them together would let the second hide inside the first, and the second
        is the one that means a fill was silently not recorded.
        """
        recorder = Recorder()
        stream = UserDataStream(FakeRest(), recorder.on_report, recorder.on_event)

        stream._on_frame("STRATEGY_UPDATE", {"e": "STRATEGY_UPDATE", "E": 1}, 0)

        assert stream.unhandled_frames == 1
        assert stream.malformed_frames == 0
        assert recorder.reports == []

    def test_bad_frames_are_reported_at_one_ten_and_a_hundred(self) -> None:
        """A frame shape we do not understand arrives at the rate the ones we do arrive at.

        Ten bad frames produce two records -- at the 1st and the 10th -- rather than ten.
        An event per occurrence buries the fault inside its own description, and this event
        stream is also the gap-detection input (spec 4.5), so flooding it is not cosmetic.
        """
        recorder = Recorder()
        stream = UserDataStream(FakeRest(), recorder.on_report, recorder.on_event)

        for _ in range(10):
            stream._on_frame("TRADE_LITE", {"e": "TRADE_LITE", "E": 1}, 0)

        assert stream.unhandled_frames == 10
        assert len(recorder.events) == 2


class TestKeepalive:
    def test_the_keepalive_interval_leaves_half_the_key_lifetime_as_margin(self) -> None:
        """1800 s is half of 3600 s, so the first PUT lands with 30 minutes still on the key.

        At the 60 s retry spacing that is 1800 / 60 = 30 further attempts before the key
        expires. An interval of, say, 3300 s would meet the documented requirement and
        leave one failed request as the difference between a live fill feed and a dead one.
        """
        assert LISTEN_KEY_TTL_S == 3600
        assert KEEPALIVE_INTERVAL_S == 1800
        assert LISTEN_KEY_TTL_S - KEEPALIVE_INTERVAL_S == 1800

    def test_an_interval_past_the_key_lifetime_is_refused_at_construction(self) -> None:
        """It guarantees the stream goes silent once an hour, on a healthy-looking socket."""
        recorder = Recorder()

        with pytest.raises(ValueError, match="keepalive_interval_s"):
            UserDataStream(
                FakeRest(),
                recorder.on_report,
                recorder.on_event,
                keepalive_interval_s=LISTEN_KEY_TTL_S,
            )

    @pytest.mark.asyncio
    async def test_the_keepalive_fires_repeatedly_on_its_own_schedule(self) -> None:
        """Extending the key is the only thing standing between a session and a silent feed."""
        recorder, rest = Recorder(), FakeRest()
        stream = UserDataStream(
            rest,
            recorder.on_report,
            recorder.on_event,
            keepalive_interval_s=0.01,
        )
        stop = asyncio.Event()

        task = asyncio.create_task(stream._keepalive_loop(stop))
        await until(lambda: rest.keepalives >= 3)
        stop.set()
        await asyncio.wait_for(task, timeout=2.0)

        assert stream.keepalives >= 3

    @pytest.mark.asyncio
    async def test_a_failing_keepalive_records_the_failure_and_keeps_the_loop_alive(
        self,
    ) -> None:
        """A keepalive that dies takes the whole fill feed with it 60 minutes later.

        By then nothing in the session connects the silence to the exception, which is why
        this is guarded the way `RestPoller._guarded` guards a poll: every failure becomes a
        recorded event and the loop keeps its schedule.
        """
        recorder, rest = Recorder(), FakeRest()
        rest.keepalive_error = RuntimeError("listenKey endpoint on fire")
        stream = UserDataStream(
            rest,
            recorder.on_report,
            recorder.on_event,
            keepalive_interval_s=0.01,
            keepalive_retry_s=0.01,
        )
        stop = asyncio.Event()

        task = asyncio.create_task(stream._keepalive_loop(stop))
        await until(lambda: len(recorder.details(CollectorEventKind.DISCONNECT)) >= 3)
        assert not task.done(), "the keepalive loop exited instead of retrying"

        rest.keepalive_error = None
        await until(lambda: rest.keepalives >= 1)
        stop.set()
        await asyncio.wait_for(task, timeout=2.0)

        failures = recorder.details(CollectorEventKind.DISCONNECT)
        assert "keepalive failed (1 consecutive)" in failures[0]
        assert "listenKey endpoint on fire" in failures[0]
        assert "3 consecutive" in failures[2]
        assert any(
            "recovered after" in detail
            for detail in recorder.details(CollectorEventKind.RECONNECT)
        )

    @pytest.mark.asyncio
    async def test_cancellation_is_not_swallowed_by_the_guard(self) -> None:
        """Catching broadly must not extend to the shutdown signal.

        Swallowing `CancelledError` would make the stream unkillable and hang every clean
        stop -- and the kill switch (spec 7) is one of the callers.
        """
        recorder = Recorder()
        stream = UserDataStream(FakeRest(), recorder.on_report, recorder.on_event)

        async def cancelled() -> None:
            raise asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await stream._guarded(cancelled, "keepalive")


class TestTheSessionLifecycle:
    @pytest.mark.asyncio
    async def test_the_socket_connects_on_the_listen_key_in_raw_mode(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Raw mode is not optional here.

        The combined-stream demultiplexer drops any frame without a `stream` wrapper, and
        every frame on this socket is bare -- so a combined-mode manager would silently
        discard every fill report.
        """
        made = install_fake_manager(monkeypatch)
        recorder, rest = Recorder(), FakeRest()
        stream = UserDataStream(
            rest,
            recorder.on_report,
            recorder.on_event,
            keepalive_interval_s=60.0,
        )
        stop = asyncio.Event()

        task = asyncio.create_task(stream.run(stop))
        await until(lambda: bool(made))
        stop.set()
        await asyncio.wait_for(task, timeout=2.0)

        assert made[0].raw_path == "listen-key-1"
        assert made[0].streams == []
        assert made[0].label == USER_STREAM_LABEL
        assert rest.ops == ["create:listen-key-1", "close"]

    @pytest.mark.asyncio
    async def test_a_frame_delivered_by_the_socket_reaches_the_caller(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        made = install_fake_manager(monkeypatch)
        recorder, rest = Recorder(), FakeRest()
        stream = UserDataStream(
            rest,
            recorder.on_report,
            recorder.on_event,
            keepalive_interval_s=60.0,
        )
        stop = asyncio.Event()

        task = asyncio.create_task(stream.run(stop))
        await until(lambda: bool(made))
        made[0].deliver(order_frame())
        stop.set()
        await asyncio.wait_for(task, timeout=2.0)

        assert [r.kind for r in recorder.reports] == [ReportKind.ORDER]
        assert recorder.reports[0].last_filled_price == 6_300_010_000_000

    @pytest.mark.asyncio
    async def test_an_expiry_re_keys_the_stream_instead_of_reconnecting_to_a_dead_key(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The key is embedded in the socket's own URL, so a new key means a new connection.

        There is nothing to renew in place: a manager told to reconnect would reconnect to
        the dead key forever, which is why the expiry tears the session down rather than
        being handled inside it. The old key is closed *before* the replacement is created,
        because `POST /fapi/v1/listenKey` returns the currently active key rather than
        minting a second one -- closing afterwards would kill the key the new socket had
        just connected with.
        """
        made = install_fake_manager(monkeypatch)
        recorder, rest = Recorder(), FakeRest()
        stream = UserDataStream(
            rest,
            recorder.on_report,
            recorder.on_event,
            keepalive_interval_s=60.0,
        )
        stop = asyncio.Event()

        task = asyncio.create_task(stream.run(stop))
        await until(lambda: bool(made))
        made[0].deliver(dict(EXPIRED_FRAME))
        await until(lambda: len(made) == 2)
        stop.set()
        await asyncio.wait_for(task, timeout=2.0)

        assert rest.creates == ["listen-key-1", "listen-key-2"]
        assert rest.ops == [
            "create:listen-key-1",
            "close",
            "create:listen-key-2",
            "close",
        ]
        assert made[1].raw_path == "listen-key-2"
        assert made[0].finished, "the socket on the dead key was left running"

    @pytest.mark.asyncio
    async def test_a_socket_that_ends_on_its_own_does_not_park_the_session(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The session waits on the socket task as well as on stop and re-key.

        `StreamManager.run` absorbs its own connection failures, so it ending at all means
        something it could not absorb -- and if the session were waiting only on two events
        that nothing was left to set, it would park there forever on a feed that has
        stopped, with no error anywhere. Silence is the one failure mode this module exists
        to refuse, so the socket ending is treated as a reason to take a fresh key.
        """
        made = install_fake_manager(monkeypatch, ending_early=frozenset({0}))
        recorder, rest = Recorder(), FakeRest()
        stream = UserDataStream(
            rest,
            recorder.on_report,
            recorder.on_event,
            keepalive_interval_s=60.0,
        )
        stop = asyncio.Event()

        task = asyncio.create_task(stream.run(stop))
        await until(lambda: len(made) == 2)
        stop.set()
        await asyncio.wait_for(task, timeout=2.0)

        assert rest.creates == ["listen-key-1", "listen-key-2"]
        assert made[1].raw_path == "listen-key-2"

    @pytest.mark.asyncio
    async def test_a_failing_key_creation_backs_off_instead_of_exiting(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The run loop must survive an endpoint that is down when the session starts.

        Exiting would leave a live session with no fill feed and nothing scheduled to bring
        one back, which reads from outside exactly like a market with no fills.
        """
        made = install_fake_manager(monkeypatch)
        recorder, rest = Recorder(), FakeRest()
        rest.create_error = RuntimeError("listenKey endpoint on fire")
        stream = UserDataStream(
            rest,
            recorder.on_report,
            recorder.on_event,
            keepalive_interval_s=60.0,
        )
        stop = asyncio.Event()

        task = asyncio.create_task(stream.run(stop))
        await until(lambda: bool(recorder.details(CollectorEventKind.DISCONNECT)))
        assert not task.done()
        assert made == []

        rest.create_error = None
        await until(lambda: bool(made))
        stop.set()
        await asyncio.wait_for(task, timeout=2.0)

        assert "listen key create failed" in recorder.details(CollectorEventKind.DISCONNECT)[0]

    @pytest.mark.asyncio
    async def test_the_listen_key_never_reaches_an_event_message(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Spec 11's rule, applied to the token that stands in for the credential here.

        The api secret never touches this module at all -- the listen-key endpoints are
        authorised by the key header alone -- but the listen key itself authorises the
        account's whole order flow, and an `httpx` error is perfectly capable of carrying it
        in a URL.
        """
        made = install_fake_manager(monkeypatch)
        recorder, rest = Recorder(), FakeRest()
        stream = UserDataStream(
            rest,
            recorder.on_report,
            recorder.on_event,
            keepalive_interval_s=0.01,
            keepalive_retry_s=0.01,
        )
        stop = asyncio.Event()

        task = asyncio.create_task(stream.run(stop))
        await until(lambda: bool(made))
        rest.keepalive_error = RuntimeError(
            "GET wss://host/ws/listen-key-1 failed for listen-key-1"
        )
        await until(lambda: bool(recorder.details(CollectorEventKind.DISCONNECT)))
        stop.set()
        await asyncio.wait_for(task, timeout=2.0)

        detail = recorder.details(CollectorEventKind.DISCONNECT)[0]
        assert "listen-key-1" not in detail
        assert "<listen-key>" in detail

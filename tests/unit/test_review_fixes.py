"""Regression tests for the 2026-08-02 platform review.

One test per confirmed defect, each naming what broke and how it was demonstrated. They
live together rather than being scattered into the per-module files for one reason: every
one of these survived a green suite, so the interesting property is not "does this module
work" but "would the suite have noticed". Reading them in sequence is the shortest account
of where the previous coverage was thin.

Findings that were **not** fixed, deliberately, and why:

- `writer.py` does not `fsync` before renaming, so a power cut can make the rename durable
  ahead of the data. Real, and not reproducible without crash injection; fixing it costs a
  synchronous flush per partition on the collector's hot path. Recorded in
  `docs/DATA_AVAILABILITY.md` rather than changed on a suspicion.
- `secrets.compare_digest` versus `==` in the password middleware is a **proven-equivalent**
  mutation: both accept the right token and reject the wrong one, and the difference is
  timing-attack resistance, which no functional test can observe. The constant-time
  comparison stays; there is nothing to assert about it.
"""

from __future__ import annotations

import asyncio
import io
import json
import zipfile
from decimal import Decimal
from pathlib import Path

import pytest

from perplab.core.money import MAX_MONEY_EXPONENT, money_to_str, parse_money
from perplab.core.types import CollectorEventKind
from perplab.data.gaps import Gap, GapKind, explain_gaps
from perplab.data.rest_poller import AggTradePoller
from perplab.data.schemas import SCHEMAS
from perplab.data.writer import ParquetBufferedWriter
from perplab.strategy.library import MAX_CODE_BYTES, LibraryError, StrategyLibrary
from perplab.strategy.params import ParamError, parse_param_specs
from perplab.strategy.scan import Diagnostic
from perplab.strategy.validate import validate_code

SYMBOL = "BTCUSDT"
T0 = 1_704_067_200_000


def strategy(body: str, *, requires: str | None = None) -> str:
    declared = requires or '{"symbols": ["BTCUSDT"], "timeframe": "1h", "history": 1}'
    return (
        "from perplab import Strategy\n\n\n"
        "class S(Strategy):\n"
        f"    requires = {declared}\n\n"
        "    def on_bar(self, ctx, bar):\n" + body
    )


def error_codes(code: str, *, sandbox: bool = False) -> set[str]:
    result = validate_code(code, run_sandbox=sandbox)
    return {d.code for d in result.diagnostics if d.severity == "error"}


# ------------------------------------------------------------------- the data layer


class TestAggTradeCursor:
    """A mid-catch-up REST failure used to skip trades permanently."""

    class _Client:
        """Serves a dense id sequence, failing on the Nth page."""

        def __init__(self, head: int, last: int, fail_on: int | None = None) -> None:
            self.head, self.last, self.fail_on = head, last, fail_on
            self.pages = 0

        async def agg_trades(self, symbol, *, from_id=None, limit=1000):
            if from_id is None:
                return [_agg(self.head)]
            self.pages += 1
            if self.fail_on is not None and self.pages == self.fail_on:
                raise ConnectionError("429 from the exchange")
            ids = range(from_id, min(from_id + limit, self.last + 1))
            return [_agg(i) for i in ids]

    def _poller(self, client, sink, events):
        return AggTradePoller(
            symbol=SYMBOL,
            client=client,
            on_rows=lambda dataset, rows: sink.extend(rows),
            on_event=lambda kind, stream, detail, downtime: events.append(kind),
        )

    def test_a_failure_mid_catchup_loses_nothing_already_fetched(self) -> None:
        """The cursor may only move past trades that have been handed to the writer.

        Accumulating pages into a local list and returning it at the end meant a failure on
        page three discarded pages one and two **while `_next_id` had already moved past
        them**. Those trades became unreachable: the next poll asks from the cursor, the
        ids below it are never requested again, and because the cursor moved consistently
        the id-jump check has nothing to notice. A 429 partway through a twenty-page
        catch-up — 400 weight in one tick — silently lost thousands of trades.
        """
        rows: list[dict] = []
        events: list[CollectorEventKind] = []
        client = self._Client(head=5000, last=8500, fail_on=3)
        poller = self._poller(client, rows, events)

        ok = asyncio.run(poller._guarded(poller._poll_and_emit, "poll"))

        assert ok is False  # the failure is still reported
        delivered = [row["agg_id"] for row in rows]
        assert delivered == list(range(5001, 5001 + len(delivered)))
        assert len(delivered) == 2000  # the two pages that succeeded, not zero
        # The cursor names the first id not yet delivered — nothing between is lost.
        assert poller._next_id == delivered[-1] + 1

    def test_a_clean_catchup_is_still_gapless_and_duplicate_free(self) -> None:
        rows: list[dict] = []
        client = self._Client(head=5000, last=7500)
        poller = self._poller(client, rows, [])
        asyncio.run(poller._poll_and_emit())
        ids = [row["agg_id"] for row in rows]
        assert ids == list(range(5001, 7501))
        assert len(set(ids)) == len(ids)


def _agg(agg_id: int) -> dict:
    return {
        "a": agg_id,
        "T": T0 + agg_id,
        "p": "60000.0",
        "q": "0.01",
        "f": agg_id,
        "l": agg_id,
        "m": bool(agg_id % 2),
    }


class TestWriterFlush:
    def test_a_partition_that_fails_does_not_duplicate_the_ones_before_it(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Retrying a partial flush must not rewrite what already landed.

        The buffer was cleared only after the whole loop, so a failure on the third of four
        partitions left the first two both on disk *and* still buffered — and the documented
        retry then wrote them a second time. One transient disk error became permanent
        silent duplication, in tick datasets, where `Coverage.duplicate_rows` is not
        computed (finding F9) and nothing downstream would ever have reported it.
        """
        writer = ParquetBufferedWriter(
            tmp_path, "klines", SCHEMAS["klines"], symbol=SYMBOL, max_rows=10_000
        )
        # Two *months* apart: klines partition by year/month (spec 4.3), so a same-day
        # split would produce one partition and the loop this test exercises would run once.
        month = 40 * 86_400_000
        for index in range(3):
            writer.append(_kline(T0 + index * 60_000))
        for index in range(2):
            writer.append(_kline(T0 + month + index * 60_000))

        real = writer._write_partition
        calls: list[tuple] = []

        def flaky(components, rows):
            calls.append(components)
            if len(calls) == 2:
                raise OSError("disk full")
            return real(components, rows)

        monkeypatch.setattr(writer, "_write_partition", flaky)
        with pytest.raises(OSError):
            writer.flush()

        monkeypatch.setattr(writer, "_write_partition", real)
        writer.flush()

        import duckdb

        rows = duckdb.connect().execute(
            "SELECT open_time FROM read_parquet('"
            f"{(tmp_path / 'klines').as_posix()}/**/*.parquet')"
        ).fetchall()
        timestamps = [row[0] for row in rows]
        assert len(timestamps) == 5
        assert len(set(timestamps)) == 5


def _kline(ts_ms: int) -> dict:
    row = {name: 0 for name in SCHEMAS["klines"].names}
    if "symbol" in row:
        row["symbol"] = SYMBOL
    row["open_time"] = ts_ms
    row["close_time"] = ts_ms + 59_999
    return row


class TestGapExplanation:
    GAP = Gap("aggTrades", SYMBOL, T0 + 10 * 60_000, T0 + 20 * 60_000, GapKind.TICK_SILENCE, "x")

    def test_a_bar_still_forming_is_not_reported_missing(self, tmp_path: Path) -> None:
        """A bar exists only once it closes.

        `cmd_gaps` clamps its end to the present, which leaves the range ending part-way
        through a bar — and expecting every bar whose *open* time falls inside the range
        then expects the one still forming. `gaps` reported one missing bar, on every run,
        for ever, and exited non-zero on a lake with nothing wrong with it.
        """
        from perplab.data.gaps import detect_kline_gaps

        writer = ParquetBufferedWriter(
            tmp_path, "klines", SCHEMAS["klines"], symbol=SYMBOL, max_rows=10_000
        )
        for index in range(10):
            writer.append(_kline(T0 + index * 60_000))
        writer.flush()

        # Range ends 30 s into the 11th bar: ten closed bars, one forming.
        gaps, coverage = detect_kline_gaps(
            tmp_path, SYMBOL, T0, T0 + 10 * 60_000 + 30_000
        )
        assert gaps == ()
        assert coverage.expected == 10

    def test_a_restart_written_after_the_range_still_explains_its_gap(
        self, tmp_path: Path
    ) -> None:
        """A RESTART is written when the collector comes *back*.

        The event window was widened by 30 s at each end, but the record that accounts for
        an overnight crash arrives hours later — so `--end yesterday`, the exact query the
        Phase 1b criterion is checked with, reported a fully explained outage as
        unexplained. The explanation existed and matched; it was never loaded.
        """
        from perplab.data.gaps import load_collector_events

        writer = ParquetBufferedWriter(
            tmp_path,
            "collectorEvents",
            SCHEMAS["collectorEvents"],
            symbol=None,
            max_rows=10_000,
        )
        writer.append(
            {
                "ts_ms": T0 + 2 * 3_600_000,
                "kind": "RESTART",
                "stream": "collector",
                "detail": "previous run ended without clean shutdown",
                "downtime_ms": 2 * 3_600_000,
            }
        )
        writer.flush()

        events = load_collector_events(tmp_path, T0 - 3_600_000, T0)
        assert [e.kind for e in events] == [CollectorEventKind.RESTART]

    def test_a_twenty_second_recovery_does_not_explain_a_ten_minute_gap(self) -> None:
        """Matching on overlap alone let an instant account for any duration.

        A REST poller writes DISCONNECT when one request fails and RECONNECT when the next
        succeeds, passing `downtime_ms = 0`. Under pure overlap that twenty-second incident
        was recorded as accounting for a day of missing trades — which is how genuinely
        lost data came to read as explained.
        """
        brief = (
            _event(self.GAP.start_ms, CollectorEventKind.DISCONNECT, stream="aggTrades"),
            _event(
                self.GAP.start_ms + 20_000,
                CollectorEventKind.RECONNECT,
                stream="aggTrades",
                downtime_ms=20_000,
            ),
        )
        assert not explain_gaps((self.GAP,), brief)[0].explained

    def test_an_unclosed_disconnect_still_explains_everything_after_it(self) -> None:
        """The other direction, so the fix does not overshoot: a DISCONNECT with no
        recovery yet means "down from here", and legitimately covers an open-ended gap."""
        opened = (_event(self.GAP.start_ms, CollectorEventKind.DISCONNECT, stream="aggTrades"),)
        assert explain_gaps((self.GAP,), opened)[0].explained


def _event(ts_ms, kind, *, stream="collector", downtime_ms=0):
    from perplab.data.gaps import CollectorEventRecord

    return CollectorEventRecord(
        ts_ms=ts_ms, kind=kind, stream=stream, detail="", downtime_ms=downtime_ms
    )


# --------------------------------------------------------------------- the strategy API


class TestScannerScope:
    def test_a_local_does_not_disable_the_rule_for_the_rest_of_the_file(self) -> None:
        """Bindings were tracked in one flat, permanent set.

        An ordinary `open = bar.open` in one method turned the `open()` rule off for every
        later method — and a scanner that silently stops scanning is worse than none,
        because it is still trusted.
        """
        code = (
            "from perplab import Strategy\n"
            "class S(Strategy):\n"
            '    requires = {"symbols": ["BTCUSDT"], "timeframe": "1h", "history": 1}\n'
            "    def on_bar(self, ctx, bar):\n"
            "        open = bar.open\n"
            "        self.first = open\n"
            "    def on_stop(self, ctx):\n"
            "        handle = open('dump.csv', 'w')\n"
        )
        assert "filesystem" in error_codes(code)

    def test_a_loop_variable_does_not_disable_a_module_alias(self) -> None:
        code = (
            "import time as t\n"
            "from perplab import Strategy\n"
            "class S(Strategy):\n"
            '    requires = {"symbols": ["BTCUSDT"], "timeframe": "1h", "history": 1}\n'
            "    def on_bar(self, ctx, bar):\n"
            "        for t in range(3):\n            pass\n"
            "    def on_stop(self, ctx):\n"
            "        self.started = t.time()\n"
        )
        assert "wall-clock" in error_codes(code)

    def test_reading_an_alias_inside_a_target_does_not_unbind_it(self) -> None:
        """`_shadow` walked the whole assignment target, so `self.buf[np.float64(x)] = 1`
        bound `np` as a local — `np` appears in the target, in a *Load* context."""
        code = (
            "import numpy as np\n"
            "from perplab import Strategy\n"
            "class S(Strategy):\n"
            '    requires = {"symbols": ["BTCUSDT"], "timeframe": "1h", "history": 1}\n'
            "    def on_bar(self, ctx, bar):\n"
            "        self.buf = {}\n"
            "        self.buf[np.float64(bar.close)] = 1\n"
            "        x = np.random.normal()\n"
        )
        assert "unseeded-random" in error_codes(code)

    @pytest.mark.parametrize(
        "body",
        [
            "        opens = [open for open in (bar.open, bar.close)]\n",
            "        try:\n            x = 1\n        except ValueError as open:\n            x = open\n",
            "        f = lambda open: open + 1\n        self.v = f(bar.open)\n",
            "        with ctx.log as open:\n            pass\n",
        ],
        ids=["comprehension", "except-as", "lambda-arg", "with-as"],
    )
    def test_bindings_other_than_assignment_are_recognised(self, body: str) -> None:
        """Only `=`, annotations and `for` targets counted as bindings, so a comprehension
        variable, a `with ... as`, an `except ... as` or a lambda parameter named after a
        rejected builtin was reported as a violation of a rule it did not break."""
        assert "filesystem" not in error_codes(strategy(body))

    def test_an_import_binding_a_rejected_name_is_not_then_flagged(self) -> None:
        assert "filesystem" not in error_codes(
            "import json as open\n" + strategy("        self.v = open.dumps({})\n")
        )

    def test_class_level_names_do_not_leak_into_methods(self) -> None:
        """A class body is a scope that nested functions do not inherit — as in Python."""
        code = (
            "from perplab import Strategy\n"
            "class S(Strategy):\n"
            '    requires = {"symbols": ["BTCUSDT"], "timeframe": "1h", "history": 1}\n'
            "    open = 1\n"
            "    def on_bar(self, ctx, bar):\n"
            "        handle = open('x')\n"
        )
        assert "filesystem" in error_codes(code)


class TestDeterminismProbe:
    def test_a_two_element_string_set_is_caught(self) -> None:
        """The shape that used to slip through, and the most likely one in this domain.

        The probe compared exactly two hash seeds. For a two-element set those two agree
        about half the time, and `{"long", "short"}` was one of the agreeing pairs — so a
        strategy looping over it passed cleanly and produced two different equity curves in
        production. Now every candidate seed is compared, and the static rule below catches
        the shape independently.
        """
        result = validate_code(
            strategy('        for side in {"long", "short"}:\n            ctx.log.info(side)\n')
        )
        assert not result.ok
        assert "nondeterministic" in {d.code for d in result.diagnostics}

    def test_the_static_rule_names_the_shape_without_running_anything(self) -> None:
        result = validate_code(
            strategy('        for side in {"long", "short"}:\n            ctx.log.info(side)\n'),
            run_sandbox=False,
        )
        warning = next(d for d in result.diagnostics if d.code == "set-iteration")
        assert warning.severity == "warning"  # order-independent bodies are legitimate
        assert "sorted()" in warning.message

    @pytest.mark.parametrize(
        "body",
        [
            '        for s in sorted({"a", "b"}):\n            ctx.log.info(s)\n',
            '        for s in ["a", "b"]:\n            ctx.log.info(s)\n',
            '        if bar.symbol in {"BTCUSDT"}:\n            ctx.log.info("hit")\n',
        ],
        ids=["sorted", "list", "membership"],
    )
    def test_order_free_uses_of_a_set_are_not_flagged(self, body: str) -> None:
        """Building a set, testing membership, or sorting one are all order-free. Only
        iteration makes the order observable."""
        result = validate_code(strategy(body), run_sandbox=False)
        assert "set-iteration" not in {d.code for d in result.diagnostics}


class TestSmokeRunCoverage:
    def test_a_warmup_past_the_bar_cap_is_an_error_not_a_pass(self) -> None:
        """`MAX_SMOKE_BARS` caps the bar count; the warm-up gate does not.

        A strategy declaring `history: 6000` ran 5 000 bars, never went warm, placed no
        orders, and passed — with the determinism probe then comparing two event logs
        containing nothing the strategy decided. `ok=True` stood for "nothing was tested".
        """
        result = validate_code(
            strategy(
                "        if not ctx.warm:\n            return\n"
                '        ctx.buy(qty=ctx.money("0.01"))\n',
                requires='{"symbols": ["BTCUSDT"], "timeframe": "1h", "history": 6000}',
            )
        )
        assert not result.ok
        assert "warmup-exceeds-smoke-run" in {d.code for d in result.diagnostics}

    def test_the_warmup_message_names_the_gate_that_blocked_the_order(self) -> None:
        """It quoted the indicator set's warm-up, which is zero when there are no
        indicators — telling an author who declared `history: 50` that they needed 0 bars
        while refusing their order, and blaming indicators that do not exist."""
        result = validate_code(
            strategy(
                '        ctx.buy(qty=ctx.money("0.01"))\n',
                requires='{"symbols": ["BTCUSDT"], "timeframe": "1h", "history": 50}',
            )
        )
        failure = next(d for d in result.diagnostics if d.code == "runtime-failure")
        assert "of 50 bars" in failure.message
        assert "requires['history']" in failure.message


class TestParams:
    def test_nan_is_refused_on_every_float_path(self) -> None:
        """NaN passes *both* bound checks — `nan < min` and `nan > max` are each False — so
        a param declared with a min and a max accepted it and it went on to multiply a
        notional. The `decimal` branch already refused it; the sibling did not."""
        with pytest.raises(ParamError, match="NaN"):
            parse_param_specs(
                {"r": {"type": "float", "default": float("nan"), "min": 0.0, "max": 1.0}}
            )

        from perplab.strategy.params import bind_params

        specs = parse_param_specs({"r": {"type": "float", "default": 0.5, "min": 0.0, "max": 1.0}})
        with pytest.raises(ParamError, match="NaN"):
            bind_params(specs, {"r": "nan"})
        with pytest.raises(ParamError, match="infinite"):
            bind_params(specs, {"r": "inf"})

    def test_a_keyword_is_not_a_usable_param_name(self) -> None:
        """`"class".isidentifier()` is True and `self.p.class` is a SyntaxError, so the
        check did not achieve the thing its message claimed."""
        for name in ("class", "lambda", "None", "await"):
            with pytest.raises(ParamError, match="keyword"):
                parse_param_specs({name: {"type": "int", "default": 1}})

    def test_an_absurd_exponent_is_refused_rather_than_expanded(self) -> None:
        """`money_to_str` forces fixed-point notation so a tick renders as `0.00000001`
        rather than `1E-8`. That is right for every real value and catastrophic for an
        unreal one: `Decimal("1E+999999999")` parses in nine characters and renders in a
        billion, inside the server process, from a strategy param."""
        with pytest.raises(ValueError, match="exponent outside"):
            parse_money("1E+200000")
        with pytest.raises(ValueError, match="exponent outside"):
            parse_money("1E-200000")

        ok = parse_money(f"1E+{MAX_MONEY_EXPONENT}")
        assert len(money_to_str(ok)) < 100
        assert parse_money("0") == 0  # zero has no meaningful exponent


class TestLibraryHardening:
    @pytest.fixture
    def library(self, tmp_path: Path) -> StrategyLibrary:
        with StrategyLibrary(tmp_path) as lib:
            yield lib

    def test_a_zip_bomb_is_refused_before_it_is_expanded(self) -> None:
        """The router's limit was on the *compressed* upload and the 1 MB source limit only
        applied after the member had been materialised. A 204 KB bundle decompressed to
        200 MB and peaked at 459 MB of allocation."""
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("strategy.py", "#" * (MAX_CODE_BYTES * 4))
        assert len(buffer.getvalue()) < 100_000
        with pytest.raises(LibraryError, match="expands to"):
            StrategyLibrary.read_bundle(buffer.getvalue())

    def test_a_non_utf8_member_is_a_library_error_not_a_crash(self) -> None:
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr("strategy.py", b"\xff\xfe# not utf-8\n")
        with pytest.raises(LibraryError, match="could not be read"):
            StrategyLibrary.read_bundle(buffer.getvalue())

    def test_a_long_name_is_suffixed_rather_than_refused(
        self, library: StrategyLibrary
    ) -> None:
        """`_unique_name` gave up at 77 characters — the exact refusal its own docstring
        rules out, and it hit hardest on imports where the name came from a file the author
        did not choose."""
        name = "M" + "e" * 76
        library.create(name, code="x = 1\n")
        data = library.export_bundle(library.get_by_name(name).id)
        outcome, _ = library.import_bundle(data, filename=f"{name}.perplab")
        assert outcome.strategy.name != name
        assert outcome.strategy.name.endswith(" (2)")
        assert len(outcome.strategy.name) <= 80

    def test_like_wildcards_in_a_search_are_literal(self, library: StrategyLibrary) -> None:
        library.create("Alpha", code="x = 1\n")
        library.create("a_b", code="y = 1\n")
        library.create("axb", code="z = 1\n")
        assert library.list(search="%") == ()
        assert [s.name for s in library.list(search="a_b")] == ["a_b"]

    def test_the_same_version_exports_byte_identically(
        self, library: StrategyLibrary
    ) -> None:
        """The manifest carried an `exported_ms`, which made the byte-identical claim in
        the code's own comment false. A bundle whose bytes change on every write cannot be
        checksummed, so "is this the same strategy I sent you" had no cheap answer."""
        created = library.create("Repeatable", code="x = 1\n").strategy
        assert library.export_bundle(created.id) == library.export_bundle(created.id)

    def test_the_exported_manifest_still_identifies_the_version(
        self, library: StrategyLibrary
    ) -> None:
        created = library.create("Named", code="x = 1\n").strategy
        with zipfile.ZipFile(io.BytesIO(library.export_bundle(created.id))) as archive:
            manifest = json.loads(archive.read("manifest.json"))
        assert manifest["name"] == "Named"
        assert manifest["version_no"] == 1
        assert "created_ms" in manifest


class TestAccountingReview:
    def test_a_losing_flip_the_wallet_cannot_fund_is_refused(self) -> None:
        """Cross-referenced with the golden case in `tests/golden/test_funding.py`, which
        carries the hand-computed arithmetic. Repeated here because this file is the
        review's index and the finding was the most serious one in it."""
        from perplab.core.account import Account, FeeSchedule, InsufficientMargin
        from tests.support import btcusdt_filters, single_bracket_table

        account = Account(
            opening_balance=Decimal("6000"),
            fees=FeeSchedule.all_taker(Decimal(0)),
            brackets={SYMBOL: single_bracket_table()},
            filters={SYMBOL: btcusdt_filters()},
        )
        account.set_leverage(SYMBOL, 10)
        account.apply_fill(T0, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.update_mark(T0 + 1, SYMBOL, Decimal("45300.0"))
        with pytest.raises(InsufficientMargin):
            account.apply_fill(T0 + 2, SYMBOL, Decimal("-3"), Decimal("45300.0"))
        assert account.available_balance >= 0

    def test_margin_cannot_be_added_free_to_an_overdrawn_allocation(self) -> None:
        """`reserved_margin` floors at zero, so while `isolated_margin` was negative --
        which funding does routinely -- adding margin moved the liquidation price without
        touching `available_balance`. The same money was spendable and pledged at once."""
        from perplab.core.account import Account, FeeSchedule, InsufficientMargin
        from tests.support import btcusdt_filters, single_bracket_table

        account = Account(
            opening_balance=Decimal("10000"),
            fees=FeeSchedule.all_taker(Decimal(0)),
            brackets={SYMBOL: single_bracket_table()},
            filters={SYMBOL: btcusdt_filters()},
        )
        account.set_leverage(SYMBOL, 100)
        account.apply_fill(T0, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.update_mark(T0 + 1, SYMBOL, Decimal("50000.0"))
        account.apply_funding(T0 + 1, SYMBOL, Decimal("0.02"))

        position = account.position(SYMBOL)
        assert position is not None and position.isolated_margin < 0
        before = account.available_balance
        with pytest.raises(InsufficientMargin, match="overdrawn"):
            account.add_margin(T0 + 2, SYMBOL, Decimal("400"))
        assert account.available_balance == before

    def test_a_liquidations_clearance_penalty_is_not_charged_to_the_price_edge(
        self,
    ) -> None:
        """A liquidation books one realised figure mixing the price move and the clearance
        penalty. Folded into `price_pnl`, a position liquidated *while in profit* reported a
        price leg of zero for a price leg that was positive."""
        from perplab.core.account import Account, FeeSchedule
        from tests.support import btcusdt_filters, single_bracket_table

        account = Account(
            opening_balance=Decimal("10000"),
            fees=FeeSchedule.all_taker(Decimal(0)),
            brackets={SYMBOL: single_bracket_table()},
            filters={SYMBOL: btcusdt_filters()},
        )
        account.set_leverage(SYMBOL, 100)
        account.apply_fill(T0, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.update_mark(T0 + 1, SYMBOL, Decimal("50000.0"))
        account.apply_funding(T0 + 1, SYMBOL, Decimal("0.011"))
        account.update_mark(T0 + 2, SYMBOL, Decimal("50100.0"))

        assert account.check_liquidations(T0 + 2)
        split = account.attribution()
        assert split["price_pnl"] == Decimal("100.00")
        # The clearance penalty is the margin balance remaining after the close:
        # allocation 500 - funding 550 + price leg 100 = 50, all confiscated. The
        # earlier pin of -100 dated from the revision that booked the whole outcome as
        # -reserved_margin (zero here) and derived the memo as loss - price_leg; the
        # wallet then showed -550 for a round trip whose true result is -500
        # (funding -550, price +100, clearance -50).
        assert split["liquidation_cost"] == Decimal("-50.00")
        # The decomposition still adds up, which is what makes the split safe to make.
        assert (
            split["price_pnl"]
            + split["funding_pnl"]
            + split["liquidation_cost"]
            - split["fees"]
            == split["net_pnl"]
        )
        account.reconcile()


class TestApiSurface:
    def test_an_unknown_api_path_is_a_json_404_not_the_app_shell(self, tmp_path: Path) -> None:
        """The SPA catch-all answered any unmatched route with `200 text/html`, so a client
        calling a renamed endpoint got a page and then a JSON parse error — the exact
        failure the typed error handlers exist to avoid."""
        import warnings

        warnings.filterwarnings(
            "ignore", message=".*httpx.*starlette.testclient.*", category=DeprecationWarning
        )
        from fastapi.testclient import TestClient

        from perplab.api.app import create_app

        with TestClient(create_app(tmp_path)) as client:
            response = client.get("/api/nope")
            assert response.status_code == 404
            assert response.headers["content-type"].startswith("application/json")
            assert "no such endpoint" in response.json()["detail"]

    def test_a_401_carries_cors_headers_so_the_browser_can_read_it(
        self, tmp_path: Path
    ) -> None:
        """`add_middleware` prepends, so the password gate ended up *outside* CORS and its
        401 went back without CORS headers. The frontend then saw an opaque network error
        and could not tell a wrong password from a server that was down."""
        import warnings

        warnings.filterwarnings(
            "ignore", message=".*httpx.*starlette.testclient.*", category=DeprecationWarning
        )
        from fastapi.testclient import TestClient

        from perplab.api.app import DEV_ORIGINS, create_app

        app = create_app(tmp_path, host="0.0.0.0", password="hunter2")
        with TestClient(app) as client:
            response = client.get("/api/strategies", headers={"Origin": DEV_ORIGINS[0]})
            assert response.status_code == 401
            assert response.headers.get("access-control-allow-origin") == DEV_ORIGINS[0]

    def test_health_does_not_leak_the_data_path_when_exposed(self, tmp_path: Path) -> None:
        """Health is the one route the password gate exempts, so a client can tell "wrong
        password" from "not running". Returning an absolute filesystem path from it, to
        anyone who can reach the port, is not what that exemption is for."""
        import warnings

        warnings.filterwarnings(
            "ignore", message=".*httpx.*starlette.testclient.*", category=DeprecationWarning
        )
        from fastapi.testclient import TestClient

        from perplab.api.app import create_app

        with TestClient(create_app(tmp_path, host="0.0.0.0", password="pw")) as client:
            payload = client.get("/api/health").json()
        assert payload["status"] == "ok"
        assert "root" not in payload

        with TestClient(create_app(tmp_path)) as local:
            assert "root" in local.get("/api/health").json()

    def test_the_reload_factory_applies_the_same_settings_and_the_same_refusal(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`serve --reload` handed uvicorn a bare factory, which it calls with no arguments.

        The reloaded app therefore took `create_app`'s defaults: `--root` ignored, and
        `--password` producing an app with **no authentication middleware at all** while
        still bound to the requested interface. The spec-11 check passed on the app that was
        then thrown away.
        """
        from perplab.api.app import app_from_env

        monkeypatch.setenv("PERPLAB_ROOT", str(tmp_path))
        monkeypatch.setenv("PERPLAB_HOST", "0.0.0.0")
        monkeypatch.setenv("PERPLAB_PASSWORD", "hunter2")
        app = app_from_env()
        assert app.state.exposed
        assert app.state.root == tmp_path
        assert any("Password" in type(m.cls).__name__ or "Password" in getattr(m.cls, "__name__", "")
                   for m in app.user_middleware)

        monkeypatch.delenv("PERPLAB_PASSWORD")
        with pytest.raises(ValueError, match="password is mandatory"):
            app_from_env()


def test_every_review_finding_has_a_named_test() -> None:
    """A checklist, kept honest by being executable.

    Not a coverage metric — it asserts that this module still contains a test for each
    finding, so removing one has to be a deliberate edit rather than a deletion nobody
    notices. The two findings deliberately left unfixed are named in the module docstring.
    """
    source = Path(__file__).read_text(encoding="utf-8")
    for marker in (
        "test_a_failure_mid_catchup_loses_nothing_already_fetched",
        "test_a_partition_that_fails_does_not_duplicate_the_ones_before_it",
        "test_a_bar_still_forming_is_not_reported_missing",
        "test_a_restart_written_after_the_range_still_explains_its_gap",
        "test_a_twenty_second_recovery_does_not_explain_a_ten_minute_gap",
        "test_a_local_does_not_disable_the_rule_for_the_rest_of_the_file",
        "test_bindings_other_than_assignment_are_recognised",
        "test_a_two_element_string_set_is_caught",
        "test_a_warmup_past_the_bar_cap_is_an_error_not_a_pass",
        "test_nan_is_refused_on_every_float_path",
        "test_a_zip_bomb_is_refused_before_it_is_expanded",
        "test_a_losing_flip_the_wallet_cannot_fund_is_refused",
        "test_margin_cannot_be_added_free_to_an_overdrawn_allocation",
        "test_an_unknown_api_path_is_a_json_404_not_the_app_shell",
        "test_the_reload_factory_applies_the_same_settings_and_the_same_refusal",
    ):
        assert f"def {marker}" in source, f"the regression test for {marker} is gone"

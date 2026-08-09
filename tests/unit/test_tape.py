"""The session tape (spec 6.7.1).

The round-trip test is the centre of this file. Spec 6.7.1 makes the shadow backtest replay a
paper session's *exact* market-data inputs, so what has to hold is not "the JSON matches" but
"the reconstructed payload object equals the one the session consumed" -- a tape that loses a
field or re-dates a payload produces a parity report about a market that never happened.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from perplab.core.types import Bar, DepthSnapshot
from perplab.engine.clock import Event, EventKind
from perplab.engine.feed import BarStep, FundingPoint, MarkBar
from perplab.engine.tape import (
    BOOK_LADDER,
    BOOK_TOP,
    EXCHANGE_NAME,
    MARKET_NAME,
    META_NAME,
    TAPE_DIRNAME,
    TapeError,
    TapeReader,
    TapeWriter,
)
from perplab.engine.ticks import TopOfBook, TradePrint

SCALE = 10**8


def mark_event() -> Event:
    return Event(
        ts_ms=1_700_000_059_999,
        kind=EventKind.MARK_PRICE_UPDATE,
        source_seq=0,
        dataset_id="markPrice:BTCUSDT",
        payload=MarkBar(
            symbol="BTCUSDT",
            close_time=1_700_000_059_999,
            high=61_000 * SCALE,
            low=59_500 * SCALE,
            close=60_250 * SCALE,
        ),
    )


def funding_event() -> Event:
    return Event(
        ts_ms=1_700_000_060_000,
        kind=EventKind.FUNDING_SETTLEMENT,
        source_seq=0,
        dataset_id="markPrice:BTCUSDT",
        payload=FundingPoint(symbol="BTCUSDT", ts_ms=1_700_000_060_000, rate=10_000),
    )


def top_event() -> Event:
    return Event(
        ts_ms=1_700_000_060_100,
        kind=EventKind.BOOK_UPDATE,
        source_seq=7,
        dataset_id="bookTicker:BTCUSDT",
        payload=TopOfBook(
            symbol="BTCUSDT",
            ts_ms=1_700_000_060_100,
            bid_px=60_249 * SCALE,
            bid_qty=3 * SCALE,
            ask_px=60_251 * SCALE,
            ask_qty=5 * SCALE,
        ),
    )


def ladder_event() -> Event:
    return Event(
        ts_ms=1_700_000_060_200,
        kind=EventKind.BOOK_UPDATE,
        source_seq=9,
        dataset_id="depth20:BTCUSDT",
        payload=DepthSnapshot(
            symbol="BTCUSDT",
            ts_ms=1_700_000_060_200,
            recv_ms=1_700_000_060_240,
            last_update_id=4242,
            bid_px=(60_249 * SCALE, 60_248 * SCALE),
            bid_qty=(3 * SCALE, 11 * SCALE),
            ask_px=(60_251 * SCALE, 60_252 * SCALE),
            ask_qty=(5 * SCALE, 8 * SCALE),
        ),
    )


def trade_event() -> Event:
    return Event(
        ts_ms=1_700_000_060_300,
        kind=EventKind.TRADE,
        source_seq=11,
        dataset_id="aggTrades:BTCUSDT",
        payload=TradePrint(
            symbol="BTCUSDT",
            ts_ms=1_700_000_060_300,
            price_scaled=60_250 * SCALE,
            qty_scaled=SCALE // 4,
            is_buyer_maker=True,
            agg_id=98_765_432,
        ),
    )


def bar_event() -> Event:
    step = BarStep(
        close_time=1_700_000_119_999,
        bars=(
            Bar(
                symbol="BTCUSDT",
                open_time=1_700_000_060_000,
                close_time=1_700_000_119_999,
                open=60_000 * SCALE,
                high=60_500 * SCALE,
                low=59_900 * SCALE,
                close=60_250 * SCALE,
                volume=12 * SCALE,
                quote_volume=723_000 * SCALE,
                trades=418,
            ),
            Bar(
                symbol="ETHUSDT",
                open_time=1_700_000_060_000,
                close_time=1_700_000_119_999,
                open=3_000 * SCALE,
                high=3_020 * SCALE,
                low=2_990 * SCALE,
                close=3_010 * SCALE,
                volume=140 * SCALE,
                quote_volume=421_400 * SCALE,
                trades=96,
            ),
        ),
    )
    return Event(
        ts_ms=1_700_000_119_999,
        kind=EventKind.BAR_CLOSE,
        source_seq=3,
        dataset_id="klines:BTCUSDT",
        payload=step,
    )


ALL_EVENTS = [
    mark_event(),
    funding_event(),
    top_event(),
    ladder_event(),
    trade_event(),
    bar_event(),
]


# ------------------------------------------------------------------------- the round trip


def test_every_market_event_kind_round_trips_to_an_equal_payload_object(
    tmp_path: Path,
) -> None:
    """Spec 6.7.1's whole requirement, asserted on the objects rather than on the JSON.

    Six rows -- one per payload shape, including both book variants -- are written and read
    back. Equality is on the reconstructed `MarkBar`, `FundingPoint`, `TopOfBook`,
    `DepthSnapshot`, `TradePrint` and `BarStep`, which are frozen dataclasses, so a dropped
    field or a re-dated timestamp fails here rather than surfacing as an unexplained parity
    divergence weeks later.

    Every row is offered at `ts_ms + 40`, which is also the `recv_ms` the fixture's
    `DepthSnapshot` carries -- the reader rebuilds a snapshot's `recv_ms` from the row's `r`
    rather than from a duplicate inside the payload, so the two have to be the same number for
    the ladder to compare equal.
    """
    with TapeWriter(tmp_path) as writer:
        for event in ALL_EVENTS:
            writer.append_market(event, recv_ms=event.ts_ms + 40)
        writer.seal()

    read_back = list(TapeReader(tmp_path).events())
    assert len(read_back) == len(ALL_EVENTS)
    for original, restored in zip(ALL_EVENTS, read_back, strict=True):
        assert restored.payload == original.payload
        assert type(restored.payload) is type(original.payload)
        assert restored == original


def test_a_market_row_reconstructs_the_spec_6_2_total_order_key_verbatim(
    tmp_path: Path,
) -> None:
    """`t`, `k`, `s`, `d` are `(ts_ms, kind_priority, source_seq, dataset_id)` in order.

    Asserted against the raw row as well as the rebuilt event, because the point of the key
    names is that a reader can rebuild the ordering key without understanding any payload.
    """
    event = trade_event()
    with TapeWriter(tmp_path) as writer:
        writer.append_market(event, recv_ms=event.ts_ms + 40)
        writer.seal()

    line = (tmp_path / TAPE_DIRNAME / MARKET_NAME).read_text(encoding="utf-8").splitlines()[0]
    row = json.loads(line)
    assert (row["t"], row["k"], row["s"], row["d"]) == event.key
    assert list(TapeReader(tmp_path).events())[0].key == event.key


def test_top_of_book_rides_book_update_with_a_discriminator(tmp_path: Path) -> None:
    """Spec 6.2 fixes the priority table, so top of book gets no EventKind of its own.

    Both book rows are written at priority 3 -- `EventKind.BOOK_UPDATE` -- and are told apart
    by `p.w`. Inventing a tenth priority would renumber the kinds after it and silently
    reorder every run already stored.
    """
    with TapeWriter(tmp_path) as writer:
        writer.append_market(top_event(), recv_ms=1)
        writer.append_market(ladder_event(), recv_ms=2)
        writer.seal()

    rows = [
        json.loads(line)
        for line in (tmp_path / TAPE_DIRNAME / MARKET_NAME)
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    assert [row["k"] for row in rows] == [int(EventKind.BOOK_UPDATE)] * 2
    assert [row["p"]["w"] for row in rows] == [BOOK_TOP, BOOK_LADDER]

    restored = list(TapeReader(tmp_path).events())
    assert isinstance(restored[0].payload, TopOfBook)
    assert isinstance(restored[1].payload, DepthSnapshot)


def test_a_depth_snapshots_recv_ms_comes_from_the_row_rather_than_the_payload(
    tmp_path: Path,
) -> None:
    """The row already carries the arrival instant as `r`, so `p` does not repeat it.

    The ladder is written with `recv_ms=1_700_000_060_240` on the row, and the reconstructed
    `DepthSnapshot.recv_ms` is that same number. Two copies of one fact is two facts that can
    disagree, and a snapshot whose own `recv_ms` contradicted its row would make the tape's
    latency numbers depend on which of them a reader happened to read.
    """
    ladder = ladder_event()
    assert isinstance(ladder.payload, DepthSnapshot)
    with TapeWriter(tmp_path) as writer:
        writer.append_market(ladder, recv_ms=ladder.payload.recv_ms)
        writer.seal()

    row = json.loads(
        (tmp_path / TAPE_DIRNAME / MARKET_NAME).read_text(encoding="utf-8").splitlines()[0]
    )
    assert "recv_ms" not in row["p"] and "r" not in row["p"]
    restored = list(TapeReader(tmp_path).events())[0]
    assert restored.payload.recv_ms == 1_700_000_060_240


def test_a_bar_close_step_round_trips_every_symbol_in_its_original_order(
    tmp_path: Path,
) -> None:
    """`BarStep.bars` is ordered, and the order is what `on_bar` sees.

    Two bars go in as BTCUSDT then ETHUSDT and must come back in that order: `feed.load_bars`
    builds the tuple in the caller's symbol order, and a reordered step would hand a pairs
    strategy its legs the other way round.
    """
    with TapeWriter(tmp_path) as writer:
        writer.append_market(bar_event(), recv_ms=1)
        writer.seal()

    restored = list(TapeReader(tmp_path).events())[0].payload
    assert isinstance(restored, BarStep)
    assert [bar.symbol for bar in restored.bars] == ["BTCUSDT", "ETHUSDT"]
    assert restored == bar_event().payload


def test_the_aggressor_flag_survives_the_round_trip(tmp_path: Path) -> None:
    """`is_buyer_maker=True` must not come back False.

    Spec 6.4 builds the entire limit-order queue model on this one boolean, so an inversion
    here reverses every queue decision in the shadow backtest while leaving prices and
    quantities looking perfectly correct.
    """
    with TapeWriter(tmp_path) as writer:
        writer.append_market(trade_event(), recv_ms=1)
        writer.seal()

    restored = list(TapeReader(tmp_path).events())[0].payload
    assert isinstance(restored, TradePrint)
    assert restored.is_buyer_maker is True
    assert restored.consumes_bids is True


# -------------------------------------------------------------------------- crash safety


def test_a_trailing_partial_line_is_tolerated(tmp_path: Path) -> None:
    """A session killed mid-write loses exactly one row, and the rest still reads.

    Three rows are written and the file is then truncated to the length of the first two plus
    eleven bytes of the third -- a line with no terminating newline, which is precisely what a
    crash between `write` and the next flush leaves. The reader stops there and yields two
    events, which is the property that made JSONL the format over a Parquet writer whose
    footer would have been missing entirely.
    """
    with TapeWriter(tmp_path) as writer:
        for event in (mark_event(), trade_event(), bar_event()):
            writer.append_market(event, recv_ms=1)
        writer.seal()

    path = tmp_path / TAPE_DIRNAME / MARKET_NAME
    raw = path.read_bytes()
    lines = raw.split(b"\n")
    keep = len(lines[0]) + 1 + len(lines[1]) + 1
    path.write_bytes(raw[: keep + 11])

    restored = list(TapeReader(tmp_path).events())
    assert [event.kind for event in restored] == [EventKind.MARK_PRICE_UPDATE, EventKind.TRADE]


def test_a_complete_line_that_will_not_parse_is_refused_rather_than_skipped(
    tmp_path: Path,
) -> None:
    """Corruption in the middle of a tape is not a crash and must not be treated as one.

    Skipping it would let the shadow backtest replay a market with a hole in it and report
    the result as parity.
    """
    with TapeWriter(tmp_path) as writer:
        writer.append_market(mark_event(), recv_ms=1)
        writer.seal()

    path = tmp_path / TAPE_DIRNAME / MARKET_NAME
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write('{"t":1,"k":0,\n')

    with pytest.raises(TapeError, match="not valid JSON"):
        list(TapeReader(tmp_path).events())


def test_a_tape_that_was_never_sealed_says_the_session_did_not_finish(
    tmp_path: Path,
) -> None:
    """`meta.json` exists from the moment the tape opens, saying `sealed: false`.

    Three states have to be distinguishable: no tape at all, a tape from a session that died,
    and a complete recording. Writing the metadata only at seal would collapse the first two.
    """
    writer = TapeWriter(tmp_path)
    writer.append_market(mark_event(), recv_ms=1)
    writer.close()

    reader = TapeReader(tmp_path)
    assert reader.sealed is False
    assert list(reader.events())[0].payload == mark_event().payload


def test_sealing_records_the_row_counts_the_writer_observed(tmp_path: Path) -> None:
    """Two market rows and one exchange row are counted by the writer, not by the caller.

    The caller's own tallies -- here a reorder buffer's `late_dropped` -- are merged beside
    them, so a parity report can read both without parsing the tape.
    """
    with TapeWriter(tmp_path) as writer:
        writer.append_market(mark_event(), recv_ms=1)
        writer.append_market(trade_event(), recv_ms=2)
        writer.append_exchange({"e": "ORDER_TRADE_UPDATE", "i": 1})
        writer.seal(late_dropped=3, window_ms=250)

    meta = TapeReader(tmp_path).meta()
    assert meta["sealed"] is True
    assert meta["market_rows"] == 2
    assert meta["exchange_rows"] == 1
    assert meta["late_dropped"] == 3
    assert meta["window_ms"] == 250
    assert meta["sealed_ms"] >= meta["created_ms"]


def test_seal_refuses_to_overwrite_a_count_the_writer_observed(tmp_path: Path) -> None:
    """A caller-supplied `market_rows` would silently replace the counted one.

    Refused rather than merged, because the discrepancy between a caller's tally and the
    writer's is exactly what the metadata exists to settle.
    """
    with TapeWriter(tmp_path) as writer:
        writer.append_market(mark_event(), recv_ms=1)
        with pytest.raises(TapeError, match="market_rows"):
            writer.seal(market_rows=99)


def test_a_sealed_tape_takes_no_further_rows(tmp_path: Path) -> None:
    """Sealing is the end of the recording, so a later append is a bug, not a no-op."""
    writer = TapeWriter(tmp_path)
    writer.seal()
    with pytest.raises(TapeError, match="sealed"):
        writer.append_market(mark_event(), recv_ms=1)


def test_a_run_directory_that_already_holds_a_recording_is_refused(tmp_path: Path) -> None:
    """Two sessions in one tape would interleave two markets into one file.

    The shadow backtest would then replay a market that never existed at any instant, which
    is worse than having no tape at all -- it would look like data.
    """
    with TapeWriter(tmp_path) as writer:
        writer.append_market(mark_event(), recv_ms=1)
        writer.seal()

    with pytest.raises(TapeError, match="already holds a recording"):
        TapeWriter(tmp_path)


# ---------------------------------------------------------------------------- durability


def test_fsync_is_called_at_most_once_every_five_seconds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every row is flushed, but the platter is forced on a five-second cadence.

    Three rows are written at monotonic times 100.0, 101.0 and 106.0 against the default
    5.0 s interval. The first two are within five seconds of the writer's own opening
    timestamp of 100.0 and force nothing; the third is 6.0 s past it and forces both handles,
    which is 2 `fsync` calls. An `fsync` per aggregate trade would put a disk seek on the
    critical path of a live session.
    """
    clock = iter([100.0, 100.0, 101.0, 106.0])
    calls: list[int] = []

    def fake_monotonic() -> float:
        return next(clock)

    def fake_fsync(fd: int) -> None:
        calls.append(fd)

    monkeypatch.setattr("perplab.engine.tape.time.monotonic", fake_monotonic)
    monkeypatch.setattr("perplab.engine.tape.os.fsync", fake_fsync)

    writer = TapeWriter(tmp_path)
    writer.append_market(mark_event(), recv_ms=1)
    assert calls == []
    writer.append_market(trade_event(), recv_ms=2)
    assert calls == []
    writer.append_market(bar_event(), recv_ms=3)
    assert len(calls) == 2
    writer.close()


def test_every_row_is_flushed_so_a_process_crash_loses_nothing(tmp_path: Path) -> None:
    """The rows are readable from another handle before the writer is closed.

    This is the half of the durability story that does not need `fsync`: a flush hands the
    bytes to the operating system, which survives the process dying. `fsync` is only what
    survives the machine losing power.
    """
    writer = TapeWriter(tmp_path)
    writer.append_market(mark_event(), recv_ms=1)
    assert len(list(TapeReader(tmp_path).events())) == 1
    writer.close()


def test_rows_are_written_with_unix_line_endings_on_every_platform(tmp_path: Path) -> None:
    """A tape recorded on Windows and replayed on Linux must be the same bytes.

    Text mode would translate to the platform's line ending, making the artefact's contents a
    property of the machine that produced it -- which is a reproducibility problem (spec 12.1)
    rather than a cosmetic one.
    """
    with TapeWriter(tmp_path) as writer:
        writer.append_market(mark_event(), recv_ms=1)
        writer.seal()

    for name in (MARKET_NAME, META_NAME):
        assert b"\r\n" not in (tmp_path / TAPE_DIRNAME / name).read_bytes()


# ------------------------------------------------------------------------------ refusals


def test_an_engine_derived_event_kind_is_refused_by_the_market_tape(tmp_path: Path) -> None:
    """The tape records observations; decisions are re-derived by the shadow backtest.

    Recording an `ORDER_ARRIVAL` would make the parity report compare the engine against a
    recording of itself, which can only ever say the two agreed.
    """
    event = Event(
        ts_ms=1,
        kind=EventKind.ORDER_ARRIVAL,
        source_seq=0,
        dataset_id="engine",
        payload=None,
    )
    with TapeWriter(tmp_path) as writer:
        with pytest.raises(TapeError, match="not market data"):
            writer.append_market(event, recv_ms=1)


def test_a_payload_whose_timestamp_disagrees_with_the_event_is_refused(
    tmp_path: Path,
) -> None:
    """The instant is stored once, as `t`, and handed back to the payload constructor.

    A `MarkBar` whose `close_time` is 1_700_000_059_000 inside an event stamped
    1_700_000_059_999 would round-trip into a bar silently re-dated by 999 ms -- corruption
    that looks like clean data on the way out.
    """
    event = Event(
        ts_ms=1_700_000_059_999,
        kind=EventKind.MARK_PRICE_UPDATE,
        source_seq=0,
        dataset_id="markPrice:BTCUSDT",
        payload=MarkBar(
            symbol="BTCUSDT",
            close_time=1_700_000_059_000,
            high=1,
            low=1,
            close=1,
        ),
    )
    with TapeWriter(tmp_path) as writer:
        with pytest.raises(TapeError, match="close_time"):
            writer.append_market(event, recv_ms=1)


def test_a_book_update_with_an_unknown_payload_type_is_refused(tmp_path: Path) -> None:
    """Only the two book observations ride priority 3, and the tape must rebuild them."""
    event = Event(
        ts_ms=1,
        kind=EventKind.BOOK_UPDATE,
        source_seq=0,
        dataset_id="depth20:BTCUSDT",
        payload={"bid": 1},
    )
    with TapeWriter(tmp_path) as writer:
        with pytest.raises(TapeError, match="TopOfBook"):
            writer.append_market(event, recv_ms=1)


def test_a_book_row_with_an_unknown_discriminator_is_refused_on_read(tmp_path: Path) -> None:
    """`w` is the only thing distinguishing the two book shapes, so it must be one of two."""
    with TapeWriter(tmp_path) as writer:
        writer.append_market(top_event(), recv_ms=1)
        writer.seal()

    path = tmp_path / TAPE_DIRNAME / MARKET_NAME
    row = json.loads(path.read_text(encoding="utf-8").splitlines()[0])
    row["p"]["w"] = "ladder20"
    path.write_text(json.dumps(row) + "\n", encoding="utf-8", newline="\n")

    with pytest.raises(TapeError, match="BOOK_UPDATE"):
        list(TapeReader(tmp_path).events())


def test_the_writer_refuses_a_run_directory_that_does_not_exist(tmp_path: Path) -> None:
    """The tape lives beside a run's other artefacts and does not invent the run.

    Creating the parent silently would put a session's only market-data record in a directory
    nothing else knows about.
    """
    with pytest.raises(TapeError, match="not an existing directory"):
        TapeWriter(tmp_path / "no-such-run")


def test_the_reader_refuses_a_run_directory_with_no_tape(tmp_path: Path) -> None:
    """Only paper and live sessions record one; a backtest replays the lake."""
    with pytest.raises(TapeError, match="no session tape"):
        TapeReader(tmp_path)


def test_the_reader_refuses_a_tape_directory_with_no_metadata(tmp_path: Path) -> None:
    """A directory called `tape/` that TapeWriter did not produce is not a tape."""
    (tmp_path / TAPE_DIRNAME).mkdir()
    with pytest.raises(TapeError, match="missing"):
        TapeReader(tmp_path).meta()


# ------------------------------------------------------------------------- exchange side


def test_exchange_reports_round_trip_as_the_venue_sent_them(tmp_path: Path) -> None:
    """Kept as the caller's own JSON rather than normalised into a typed record.

    Spec 6.7.3's reconciliation is investigated by reading exactly what arrived, so an
    interpretation of an exchange message stored in place of the message would remove the
    evidence the check exists to produce.
    """
    reports = [
        {"e": "ORDER_TRADE_UPDATE", "o": {"i": 111, "X": "FILLED", "z": "0.250"}},
        {"e": "ACCOUNT_UPDATE", "a": {"B": [{"a": "USDT", "wb": "9987.4"}]}},
    ]
    with TapeWriter(tmp_path) as writer:
        for report in reports:
            writer.append_exchange(report)
        writer.seal()

    assert list(TapeReader(tmp_path).exchange_reports()) == reports


def test_the_two_halves_of_the_tape_are_separate_files(tmp_path: Path) -> None:
    """Market data and exchange reports never interleave in one file.

    The shadow backtest streams the market half and never has to skip past rows belonging to
    the other, which is what keeps a replay of a million-row session from parsing every
    exchange message it will not use.
    """
    with TapeWriter(tmp_path) as writer:
        writer.append_market(mark_event(), recv_ms=1)
        writer.append_exchange({"e": "ACCOUNT_UPDATE"})
        writer.seal()

    directory = tmp_path / TAPE_DIRNAME
    assert sorted(path.name for path in directory.iterdir()) == [
        EXCHANGE_NAME,
        MARKET_NAME,
        META_NAME,
    ]
    assert len((directory / MARKET_NAME).read_text(encoding="utf-8").splitlines()) == 1
    assert len((directory / EXCHANGE_NAME).read_text(encoding="utf-8").splitlines()) == 1

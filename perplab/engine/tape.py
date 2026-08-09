"""The session tape: the market data a paper or live session actually saw (spec 6.7.1).

Spec 6.7.1 is one sentence and it is the whole reason this module exists: *"Every paper/live
session records its exact market-data inputs."* The shadow backtest re-runs the session's
window with the same strategy, seed and params, and attaches a parity report -- fill-count
delta, average fill-price delta, final-PnL delta. That report only measures the *engine* if
both halves read the same market. Re-reading the lake instead would not: the lake is
backfilled, de-duplicated and re-ingested behind the collector, so a shadow backtest run an
hour later reads a slightly different market and reports the difference as a fill-model
divergence. The parity threshold in spec 6.7.2 would then be firing on data drift, and the
feedback loop that is supposed to make the fill models more honest over time would instead be
chasing its own tail.

So the session writes down what it consumed, verbatim, and the shadow backtest replays that.

**JSONL, not Parquet, and it is a crash-safety argument rather than a taste one.**
`pq.write_table` cannot append, so a Parquet tape means either buffering the session in
memory -- losing everything if it dies, which is exactly when the tape is most wanted -- or a
long-lived `ParquetWriter`. A `ParquetWriter` killed mid-row-group leaves a file with no
footer, and a Parquet file with no footer is not a partially readable file, it is an
unreadable one. JSONL truncated mid-line loses exactly one line, and `TapeReader` is written
to expect that line and stop cleanly at it.

**Only market data goes on the market tape.** The four engine-derived kinds --
`LIQUIDATION_CHECK`, `ORDER_FILL_CHECK`, `ORDER_SUBMIT`, `ORDER_ARRIVAL` -- are refused.
Recording them would mean the shadow backtest replayed the live engine's decisions rather
than re-deriving them from the same inputs, and a parity report between an engine and a
recording of itself is a report that can only ever say "identical".

**An unsealed tape means the session crashed.** `meta.json` is written at open with
`sealed: false` and rewritten at `seal` with `sealed: true`, so the three states -- no tape,
a tape from a session that died, a complete tape -- are three distinct things on disk rather
than two ambiguous ones.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

from perplab.core.types import Bar, DepthSnapshot
from perplab.engine.clock import Event, EventKind
from perplab.engine.feed import BarStep, FundingPoint, MarkBar
from perplab.engine.ticks import TopOfBook, TradePrint

__all__ = [
    "BOOK_LADDER",
    "BOOK_TOP",
    "EXCHANGE_NAME",
    "FSYNC_INTERVAL_S",
    "MARKET_NAME",
    "META_NAME",
    "TAPE_DIRNAME",
    "TAPE_VERSION",
    "TapeError",
    "TapeReader",
    "TapeWriter",
]

TAPE_SOURCE_PREFIX = "tape:"
"""How a `RunSpec` says "replay run N's recording" rather than "query the lake".

Lives here rather than beside the shadow runner so that `engine` never has to import from
`live`: the tape is an engine artefact, the shadow is one of its readers, and a dependency
in that direction would make the backtest worker unimportable without the live package.

A string rather than a boolean, because the worker has to know *which* tape and because spec
12.1 wants a run's inputs to be a closed, readable list -- `source: "tape:41"` tells someone
re-deriving a parity report a year later where these numbers came from.
"""

TAPE_DIRNAME = "tape"
META_NAME = "meta.json"
MARKET_NAME = "market.jsonl"
EXCHANGE_NAME = "exchange.jsonl"

TAPE_VERSION = 1
"""Bumped when a row's shape changes in a way an older reader would misread."""

FSYNC_INTERVAL_S = 5.0
"""How often the tape is forced to the platter.

Every row is `flush`ed to the operating system as it is written, so a process crash -- the
overwhelmingly likely failure -- loses nothing. `fsync` is what survives a machine losing
power, and it is a disk seek: paying one per aggregate trade would put the tape on the
critical path of a live session. Five seconds bounds what a power cut costs to the tail of
the recording, which the shadow backtest can see is missing because the tape is unsealed.
"""

BOOK_TOP = "top"
BOOK_LADDER = "ladder"
"""Discriminators for the two book observations that share `EventKind.BOOK_UPDATE`.

Spec 6.2 fixes the priority table, and top of book has no number in it. Inventing one would
renumber the events after it and silently reorder every run already stored, so `bookTicker`
and `depth20` both ride priority 3 -- which is what `ticks.py` already does in the lake -- and
the row says which of the two it is. See `ticks`' module docstring: the two are separated by
`dataset_id`, the last component of the total-order key, so sharing a priority is not a tie.
"""

_RESERVED_META = frozenset(
    {"sealed", "tape_version", "created_ms", "sealed_ms", "market_rows", "exchange_rows"}
)


class TapeError(RuntimeError):
    """The tape cannot be written, or cannot be read back into the events it recorded."""


# ------------------------------------------------------------------------------- writing


def _require_ts(event: Event, payload_ts: int, field: str) -> None:
    """Refuse a payload whose own timestamp disagrees with the event's.

    The row schema stores the instant once, as `t`, and the reader hands it back to the
    payload constructor. That is only sound while the two agree, and nothing upstream
    guarantees they do -- an event assembled with the wrong timestamp would round-trip into a
    payload silently re-dated to the event's instant, which is a market-data corruption that
    looks like clean data on the other side.
    """
    if payload_ts != event.ts_ms:
        raise TapeError(
            f"{event.kind.name} at ts_ms={event.ts_ms} carries a payload whose {field} is "
            f"{payload_ts}. The tape stores the instant once and reconstructs the payload "
            f"from it, so the two must agree; fix whichever of them is wrong before "
            f"recording."
        )


def _market_payload(event: Event) -> dict[str, Any]:
    """The `p` object for one market event, keyed for compactness.

    Short keys because this file is the highest-volume artefact a session produces and a
    live `aggTrades` feed writes millions of rows: `{"sym","px","qty","m","id"}` against
    `{"symbol","price_scaled",...}` is roughly a third of the bytes. Every price and quantity
    stays a scaled int64 exactly as it arrived, so no rounding happens between the session and
    the shadow backtest of it.
    """
    kind = event.kind
    payload = event.payload

    if kind is EventKind.MARK_PRICE_UPDATE:
        if not isinstance(payload, MarkBar):
            raise TapeError(_wrong_payload(kind, "feed.MarkBar", payload))
        _require_ts(event, payload.close_time, "close_time")
        return {"sym": payload.symbol, "c": payload.close, "h": payload.high, "l": payload.low}

    if kind is EventKind.FUNDING_SETTLEMENT:
        if not isinstance(payload, FundingPoint):
            raise TapeError(_wrong_payload(kind, "feed.FundingPoint", payload))
        _require_ts(event, payload.ts_ms, "ts_ms")
        # `next` is the exchange's `nextFundingTime`. Recorded because spec 3.5 rule 3
        # forbids assuming an 8-hour schedule -- Binance has changed the interval on existing
        # symbols -- and a live session is the only place the *upcoming* settlement time is
        # observable at all; the lake's `funding` dataset holds settlements after the fact and
        # has nowhere to put it. `FundingPoint` does not carry it either, so a recorder that
        # observed one passes a payload that extends `FundingPoint` with the field. Zero means
        # "not observed", which is what a replay of lake rows honestly records.
        return {
            "sym": payload.symbol,
            "rate": payload.rate,
            "next": _next_funding_ms(payload),
        }

    if kind is EventKind.BOOK_UPDATE:
        return _book_payload(event, payload)

    if kind is EventKind.TRADE:
        if not isinstance(payload, TradePrint):
            raise TapeError(_wrong_payload(kind, "ticks.TradePrint", payload))
        _require_ts(event, payload.ts_ms, "ts_ms")
        return {
            "sym": payload.symbol,
            "px": payload.price_scaled,
            "qty": payload.qty_scaled,
            # The aggressor flag. `True` means the buyer was the maker, so the trade was
            # sell-aggressive and consumed bid-side queue; inverting it reverses every queue
            # decision in the shadow backtest (spec 6.4).
            "m": bool(payload.is_buyer_maker),
            "id": payload.agg_id,
        }

    if kind is EventKind.BAR_CLOSE:
        if not isinstance(payload, BarStep):
            raise TapeError(_wrong_payload(kind, "feed.BarStep", payload))
        _require_ts(event, payload.close_time, "close_time")
        return {
            "bars": [
                {
                    "sym": bar.symbol,
                    "ot": bar.open_time,
                    "ct": bar.close_time,
                    "o": bar.open,
                    "h": bar.high,
                    "l": bar.low,
                    "c": bar.close,
                    "v": bar.volume,
                    "qv": bar.quote_volume,
                    "n": bar.trades,
                }
                for bar in payload.bars
            ]
        }

    raise TapeError(
        f"{kind.name} is not market data and has no place on the market tape. The tape "
        f"records what the session *observed*; the engine-derived kinds "
        f"(LIQUIDATION_CHECK, ORDER_FILL_CHECK, ORDER_SUBMIT, ORDER_ARRIVAL) are decisions "
        f"the shadow backtest has to re-derive, and recording them would make the parity "
        f"report compare the engine against a recording of itself (spec 6.7.1)."
    )


def _next_funding_ms(payload: FundingPoint) -> int:
    """`nextFundingTime` for a funding row, or 0 when the session did not observe one."""
    return int(getattr(payload, "next_funding_ms", 0) or 0)


def _book_payload(event: Event, payload: Any) -> dict[str, Any]:
    if isinstance(payload, TopOfBook):
        _require_ts(event, payload.ts_ms, "ts_ms")
        return {
            "sym": payload.symbol,
            "w": BOOK_TOP,
            "bp": payload.bid_px,
            "bq": payload.bid_qty,
            "ap": payload.ask_px,
            "aq": payload.ask_qty,
        }
    if isinstance(payload, DepthSnapshot):
        _require_ts(event, payload.ts_ms, "ts_ms")
        # `recv_ms` is not repeated in `p`: the row already carries the arrival instant as
        # `r`, and a snapshot that stored its own copy could disagree with it. The reader
        # rebuilds the snapshot from `r`, which is the same number the collector would have
        # written into the lake's `depth20.recv_ms` column.
        return {
            "sym": payload.symbol,
            "w": BOOK_LADDER,
            "u": payload.last_update_id,
            "bp": list(payload.bid_px),
            "bq": list(payload.bid_qty),
            "ap": list(payload.ask_px),
            "aq": list(payload.ask_qty),
        }
    raise TapeError(
        _wrong_payload(EventKind.BOOK_UPDATE, "ticks.TopOfBook or core.types.DepthSnapshot", payload)
    )


def _wrong_payload(kind: EventKind, expected: str, payload: Any) -> str:
    return (
        f"{kind.name} must carry a {expected} payload to be taped, got "
        f"{type(payload).__name__}. The tape reconstructs typed payloads on the way out, so "
        f"it cannot record one it does not know how to rebuild."
    )


class TapeWriter:
    """Records a session's market-data inputs and exchange reports into a run directory.

    The layout is `tape/meta.json`, `tape/market.jsonl` and `tape/exchange.jsonl` inside an
    existing run directory, alongside the run's other artefacts. Nothing is buffered across
    rows: each append is one `write` of one complete line followed by a `flush`, so a process
    that dies between two rows leaves a file whose last line is either wholly there or wholly
    absent.

    Refuses to open over a tape that already holds rows. Appending to one would interleave two
    sessions' market data in a single file, and the shadow backtest would then replay a market
    that never existed at any instant.
    """

    __slots__ = (
        "_directory",
        "_fsync_interval_s",
        "_market",
        "_exchange",
        "_market_rows",
        "_exchange_rows",
        "_last_fsync",
        "_created_ms",
        "_closed",
        "_sealed",
        "_supplied_meta",
    )

    def __init__(
        self,
        run_dir: Path | str,
        *,
        meta: Mapping[str, Any] | None = None,
        fsync_interval_s: float = FSYNC_INTERVAL_S,
    ) -> None:
        run_path = Path(run_dir)
        if not run_path.is_dir():
            raise TapeError(
                f"{run_path} is not an existing directory. The tape lives beside a run's "
                f"other artefacts; create the run directory first."
            )
        directory = run_path / TAPE_DIRNAME
        directory.mkdir(exist_ok=True)
        for name in (MARKET_NAME, EXCHANGE_NAME):
            existing = directory / name
            if existing.exists() and existing.stat().st_size > 0:
                raise TapeError(
                    f"{existing} already holds a recording. A tape belongs to exactly one "
                    f"session; appending would interleave two sessions' market data in one "
                    f"file. Start a new run directory."
                )

        self._directory = directory
        self._fsync_interval_s = fsync_interval_s
        # **Written before the first row, not only at seal.** A shadow backtest reads the
        # endpoint, the fill tier, the reorder window and the data start from here; if they
        # were only written when the session stopped cleanly, a crashed session would leave
        # a tape full of events that nothing could interpret. Refused keys are checked the
        # same way `seal` checks them, so the writer's own counts can never be overwritten.
        supplied = dict(meta or {})
        clashes = sorted(set(supplied) & _RESERVED_META)
        if clashes:
            raise TapeError(
                f"the tape's metadata cannot set {clashes}: the writer counts those itself "
                f"and a caller-supplied value would silently replace an observed one."
            )
        self._supplied_meta = supplied
        self._market_rows = 0
        self._exchange_rows = 0
        self._closed = False
        self._sealed = False
        self._created_ms = int(time.time() * 1000)
        # `newline="\n"` on both the writer and the reader. Text mode would otherwise
        # translate to the platform's line ending, so the same session recorded on Windows
        # and on Linux would produce different bytes -- and a tape whose bytes depend on the
        # machine is a reproducibility artefact that is not reproducible (spec 12.1).
        self._market = (directory / MARKET_NAME).open("a", encoding="utf-8", newline="\n")
        self._exchange = (directory / EXCHANGE_NAME).open("a", encoding="utf-8", newline="\n")
        self._last_fsync = time.monotonic()
        self._write_meta(sealed=False)

    # ------------------------------------------------------------------------ appending

    @property
    def directory(self) -> Path:
        """The `tape/` directory this writer owns."""
        return self._directory

    @property
    def market_rows(self) -> int:
        return self._market_rows

    @property
    def exchange_rows(self) -> int:
        return self._exchange_rows

    def append_market(self, event: Event, recv_ms: int) -> None:
        """Record one market event and the instant it reached us.

        The row's keys are chosen so spec 6.2's total-order key is reconstructible verbatim:
        `t`, `k`, `s` and `d` are `(ts_ms, kind_priority, source_seq, dataset_id)` in order,
        so a reader rebuilds the key without having to understand the payload at all.
        """
        self._require_open()
        row = {
            "t": int(event.ts_ms),
            "k": int(event.kind),
            "s": int(event.source_seq),
            "d": event.dataset_id,
            "r": int(recv_ms),
            "p": _market_payload(event),
        }
        self._write(self._market, row)
        self._market_rows += 1

    def append_exchange(self, report_json: Mapping[str, Any]) -> None:
        """Record one report from the exchange -- an order update, a reconciliation, a fill.

        Kept as the caller's own JSON rather than a typed record. This half of the tape is
        evidence about what the venue said, and normalising it here would mean the stored
        artefact is this module's interpretation of an exchange message rather than the
        message; a reconciliation mismatch (spec 6.7.3) is investigated by reading exactly
        what arrived.
        """
        self._require_open()
        self._write(self._exchange, report_json)
        self._exchange_rows += 1

    def _write(self, handle: Any, row: Any) -> None:
        line = json.dumps(row, separators=(",", ":"), ensure_ascii=False, default=str) + "\n"
        # One `write` for the whole line including its newline, so a truncated file can only
        # ever be missing the newline -- which is precisely what `TapeReader` detects. Two
        # writes would let a crash land between them and produce a complete-looking line.
        handle.write(line)
        handle.flush()
        now = time.monotonic()
        if now - self._last_fsync >= self._fsync_interval_s:
            self._fsync()
            self._last_fsync = now

    def _fsync(self) -> None:
        os.fsync(self._market.fileno())
        os.fsync(self._exchange.fileno())

    def _require_open(self) -> None:
        if self._sealed:
            raise TapeError(
                "this tape has been sealed and cannot take more rows. A sealed tape is a "
                "complete record of one session; open a new run directory."
            )
        if self._closed:
            raise TapeError("this tape is closed")

    # -------------------------------------------------------------------------- closing

    def meta_snapshot(self) -> dict[str, Any]:
        """The metadata as it stands, for a manifest written beside a live run.

        A copy rather than the live mapping: the manifest is the run's permanent record and
        a reader holding a reference that a later `seal` mutated would find its fingerprint
        had changed after it was published.
        """
        return self._meta(sealed=self._sealed)

    def seal(self, **counts: Any) -> None:
        """Finish the tape: force it to disk and mark `meta.json` sealed.

        `counts` are merged into the metadata as the session's own tallies -- released and
        dropped frame counts from `reorder.ReorderBuffer.stats`, order counts, whatever the
        session wants the parity report to be able to read without parsing the tape itself.
        A key that would overwrite one of this module's own is refused rather than silently
        winning, because a `market_rows` supplied by a caller and a `market_rows` counted by
        the writer disagreeing is the exact discrepancy the metadata exists to settle.
        """
        self._require_open()
        clashes = sorted(set(counts) & _RESERVED_META)
        if clashes:
            raise TapeError(
                f"seal() cannot set {clashes}: the writer counts those itself and a "
                f"caller-supplied value would silently replace an observed one. Rename the "
                f"key(s) you are passing."
            )
        self._fsync()
        self._write_meta(sealed=True, extra=counts)
        self._sealed = True
        self.close()

    def close(self) -> None:
        """Release the file handles. Leaves the tape unsealed unless `seal` was called.

        That asymmetry is the design: a session that exits without sealing -- because it
        crashed, or was killed -- leaves a tape whose metadata says so, and the shadow
        backtest can report that it is replaying an incomplete recording rather than
        reporting a parity divergence it cannot explain.
        """
        if self._closed:
            return
        self._closed = True
        try:
            self._fsync()
        finally:
            self._market.close()
            self._exchange.close()

    def __enter__(self) -> TapeWriter:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _meta(
        self, *, sealed: bool, extra: Mapping[str, Any] | None = None
    ) -> dict[str, Any]:
        """The metadata document. Caller-supplied keys first, so the writer's own win."""
        meta: dict[str, Any] = {
            **self._supplied_meta,
            **(dict(extra) if extra else {}),
            "sealed": sealed,
            "tape_version": TAPE_VERSION,
            "created_ms": self._created_ms,
            "sealed_ms": int(time.time() * 1000) if sealed else None,
            "market_rows": self._market_rows,
            "exchange_rows": self._exchange_rows,
        }
        return meta

    def _write_meta(self, *, sealed: bool, extra: Mapping[str, Any] | None = None) -> None:
        meta = self._meta(sealed=sealed, extra=extra)
        path = self._directory / META_NAME
        # Temp-and-rename, like every other artefact this platform writes: a metadata file
        # truncated mid-write would parse as neither sealed nor unsealed.
        tmp = path.parent / f".{path.name}.tmp"
        tmp.write_text(
            json.dumps(meta, indent=2, default=str) + "\n", encoding="utf-8", newline="\n"
        )
        tmp.replace(path)


# ------------------------------------------------------------------------------- reading


def _bars_from_rows(rows: Any) -> tuple[Bar, ...]:
    return tuple(
        Bar(
            symbol=row["sym"],
            open_time=row["ot"],
            close_time=row["ct"],
            open=row["o"],
            high=row["h"],
            low=row["l"],
            close=row["c"],
            volume=row["v"],
            quote_volume=row["qv"],
            trades=row["n"],
        )
        for row in rows
    )


def _event_from_row(row: Mapping[str, Any], *, line_no: int) -> Event:
    """Rebuild one `clock.Event`, payload type and all, from a market row."""
    try:
        ts_ms = int(row["t"])
        kind = EventKind(int(row["k"]))
        source_seq = int(row["s"])
        dataset_id = str(row["d"])
        recv_ms = int(row["r"])
        payload: Mapping[str, Any] = row["p"]
    except (KeyError, TypeError, ValueError) as exc:
        raise TapeError(
            f"market.jsonl line {line_no} is not a tape row: {exc}. A row needs "
            f"t/k/s/d/r/p; this file may have been written by a different tool."
        ) from exc

    built: Any
    if kind is EventKind.MARK_PRICE_UPDATE:
        built = MarkBar(
            symbol=payload["sym"],
            close_time=ts_ms,
            high=payload["h"],
            low=payload["l"],
            close=payload["c"],
        )
    elif kind is EventKind.FUNDING_SETTLEMENT:
        # `next` is recorded but has no home on `FundingPoint`; see `_market_payload`. It is
        # read back by anything auditing the settlement interval, not by the engine.
        built = FundingPoint(symbol=payload["sym"], ts_ms=ts_ms, rate=payload["rate"])
    elif kind is EventKind.BOOK_UPDATE:
        built = _book_from_payload(payload, ts_ms=ts_ms, recv_ms=recv_ms, line_no=line_no)
    elif kind is EventKind.TRADE:
        built = TradePrint(
            symbol=payload["sym"],
            ts_ms=ts_ms,
            price_scaled=payload["px"],
            qty_scaled=payload["qty"],
            is_buyer_maker=bool(payload["m"]),
            agg_id=payload["id"],
        )
    elif kind is EventKind.BAR_CLOSE:
        built = BarStep(close_time=ts_ms, bars=_bars_from_rows(payload["bars"]))
    else:
        raise TapeError(
            f"market.jsonl line {line_no} carries {kind.name}, which is not market data. "
            f"The market tape holds observations only (spec 6.7.1)."
        )

    return Event(
        ts_ms=ts_ms,
        kind=kind,
        source_seq=source_seq,
        dataset_id=dataset_id,
        payload=built,
    )


def _book_from_payload(
    payload: Mapping[str, Any], *, ts_ms: int, recv_ms: int, line_no: int
) -> Any:
    which = payload.get("w")
    if which == BOOK_TOP:
        return TopOfBook(
            symbol=payload["sym"],
            ts_ms=ts_ms,
            bid_px=payload["bp"],
            bid_qty=payload["bq"],
            ask_px=payload["ap"],
            ask_qty=payload["aq"],
        )
    if which == BOOK_LADDER:
        return DepthSnapshot(
            symbol=payload["sym"],
            ts_ms=ts_ms,
            # From the row's own arrival stamp rather than a duplicate inside the payload,
            # so the two can never disagree. See `_book_payload`.
            recv_ms=recv_ms,
            last_update_id=payload["u"],
            bid_px=tuple(payload["bp"]),
            bid_qty=tuple(payload["bq"]),
            ask_px=tuple(payload["ap"]),
            ask_qty=tuple(payload["aq"]),
        )
    raise TapeError(
        f"market.jsonl line {line_no} is a BOOK_UPDATE with w={which!r}; expected "
        f"{BOOK_TOP!r} or {BOOK_LADDER!r}. Top of book has no EventKind of its own -- spec "
        f"6.2 fixes the priority table -- so the discriminator is how the two are told apart."
    )


class TapeReader:
    """Streams a sealed (or crashed) tape back into typed events.

    Line by line rather than whole: a day of live `aggTrades` is millions of rows, and the
    shadow backtest consumes them through `EventQueue`, which is itself lazy. Materialising
    the tape to start replaying it would make a session's own parity check the heaviest thing
    the machine does.
    """

    __slots__ = ("_directory",)

    def __init__(self, run_dir: Path | str) -> None:
        directory = Path(run_dir) / TAPE_DIRNAME
        if not directory.is_dir():
            raise TapeError(
                f"{directory} does not exist, so this run has no session tape. Only paper "
                f"and live sessions record one (spec 6.7.1); a backtest replays the lake."
            )
        self._directory = directory

    @property
    def directory(self) -> Path:
        return self._directory

    def meta(self) -> dict[str, Any]:
        """The tape's metadata, including whether the session sealed it."""
        path = self._directory / META_NAME
        if not path.exists():
            raise TapeError(
                f"{path} is missing. A tape writes its metadata when it opens, so a tape "
                f"directory without one was not produced by TapeWriter."
            )
        return json.loads(path.read_text(encoding="utf-8"))

    @property
    def sealed(self) -> bool:
        """Whether the session finished. `False` means it crashed mid-recording."""
        return bool(self.meta().get("sealed", False))

    def events(self) -> Iterator[Event]:
        """Every recorded market event, in the order the session processed it."""
        for line_no, row in self._rows(MARKET_NAME):
            yield _event_from_row(row, line_no=line_no)

    def exchange_reports(self) -> Iterator[dict[str, Any]]:
        """Every recorded exchange report, in arrival order."""
        for _, row in self._rows(EXCHANGE_NAME):
            yield row

    def _rows(self, name: str) -> Iterator[tuple[int, dict[str, Any]]]:
        """Parsed lines, stopping cleanly at a truncated final one.

        A row and its newline are written by a single `write`, so a line without a trailing
        newline is by construction the last thing in the file and is incomplete -- the
        session died between the two. It is dropped without complaint, which is the whole
        reason this is JSONL: the cost of a crash is one row, not the file.

        A *complete* line that will not parse is a different thing entirely and raises. That
        would mean something other than `TapeWriter` wrote here, and silently skipping it
        would let a shadow backtest replay a partial market and call the result parity.
        """
        path = self._directory / name
        if not path.exists():
            raise TapeError(
                f"{path} is missing; this tape directory is incomplete. Both market.jsonl "
                f"and exchange.jsonl are created when the tape opens, even if empty."
            )
        with path.open("r", encoding="utf-8", newline="\n") as handle:
            for line_no, raw in enumerate(handle, start=1):
                if not raw.endswith("\n"):
                    return
                line = raw.strip()
                if not line:
                    continue
                try:
                    yield line_no, json.loads(line)
                except json.JSONDecodeError as exc:
                    raise TapeError(
                        f"{path} line {line_no} is not valid JSON: {exc}. The line is "
                        f"complete -- it ends in a newline -- so this is corruption rather "
                        f"than a crash, and replaying past it would produce a shadow "
                        f"backtest over a market with a hole in it."
                    ) from exc

"""Bounded artifact writers/readers, also accepting the original JSON/Parquet files."""
from __future__ import annotations
import itertools
import json
import math
import os
import sqlite3
import threading
from pathlib import Path
from collections.abc import Iterator, Sequence
from typing import Any
import pyarrow as pa
import pyarrow.parquet as pq
from perplab.resources import checkpoint, _atomic


def write_columns(path: Path, columns: dict[str, Sequence], types: dict[str, pa.DataType]) -> None:
    tmp = path.with_name("." + path.name + ".tmp")
    schema = pa.schema([(name, types[name]) for name in columns])
    streams = [iter(series) for series in columns.values()]
    with pq.ParquetWriter(tmp, schema, compression="zstd", compression_level=3) as writer:
        while True:
            batch = [list(itertools.islice(stream, 4096)) for stream in streams]
            if not batch[0]:
                break
            if len({len(values) for values in batch}) != 1:
                raise ValueError("artifact series lengths differ")
            checkpoint("artifact_serialization")
            writer.write_table(pa.Table.from_arrays([pa.array(values, type=types[name]) for name, values in zip(columns, batch)], schema=schema))
    os.replace(tmp, path)


def write_trades(path: Path, trades: Sequence) -> None:
    tmp = path.with_name("." + path.name + ".tmp")
    lines_path = path.with_suffix(".jsonl")
    lines_tmp = lines_path.with_name("." + lines_path.name + ".tmp")
    index_path = path.with_name("trades-index.sqlite3")
    con = sqlite3.connect(index_path)
    try:
        con.execute("CREATE TABLE IF NOT EXISTS trades (i INTEGER PRIMARY KEY, entry_ms INTEGER, net_pnl REAL, mae REAL, mfe REAL, duration_ms INTEGER, data TEXT)")
        con.execute("DELETE FROM trades")
        with tmp.open("w", encoding="utf-8") as handle, lines_tmp.open("w", encoding="utf-8") as lines:
            handle.write('{"trades":[')
            for i, trade in enumerate(trades):
                if i % 4096 == 0:
                    checkpoint("trade_serialization")
                payload = trade.to_json() if hasattr(trade, "to_json") else trade
                data = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
                handle.write(("," if i else "") + data)
                lines.write(data + "\n")
                con.execute("INSERT INTO trades VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (payload["index"], payload["entry_ms"], float(payload["net_pnl"]), float(payload["mae"]), float(payload["mfe"]), payload.get("duration_ms"), data))
            handle.write("]}\n")
        con.commit()
        os.replace(lines_tmp, lines_path)
        os.replace(tmp, path)
    finally:
        con.close()


def publish_trades(path: Path, trades: Sequence, closed_count: int) -> None:
    """Append newly closed trades and replace the small active snapshot.

    The index is authoritative during a session; the JSON is a bounded preview.
    Finalization still writes the complete original-format artifact.
    """
    con = sqlite3.connect(path.with_name("trades-index.sqlite3"), timeout=10)
    try:
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA cache_size=-2048")
        con.execute("CREATE TABLE IF NOT EXISTS trades (i INTEGER PRIMARY KEY, entry_ms INTEGER, net_pnl REAL, mae REAL, mfe REAL, duration_ms INTEGER, data TEXT)")
        con.execute("CREATE TABLE IF NOT EXISTS progress (key TEXT PRIMARY KEY, data TEXT)")
        previous = con.execute("SELECT data FROM progress WHERE key='closed'").fetchone()
        start = int(previous[0]) if previous else 0
        old_open = con.execute("SELECT data FROM progress WHERE key='open'").fetchone()
        for ident in json.loads(old_open[0]) if old_open else ():
            con.execute("DELETE FROM trades WHERE i=?", (ident,))
        active = []
        for position in range(start, len(trades)):
            trade = trades[position]
            payload = trade.to_json() if hasattr(trade, "to_json") else trade
            data = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
            con.execute("INSERT OR REPLACE INTO trades VALUES (?, ?, ?, ?, ?, ?, ?)",
                (payload["index"], payload["entry_ms"], float(payload["net_pnl"]), float(payload["mae"]), float(payload["mfe"]), payload.get("duration_ms"), data))
            if position >= closed_count:
                active.append(payload["index"])
        con.execute("INSERT OR REPLACE INTO progress VALUES ('closed', ?)", (str(closed_count),))
        con.execute("INSERT OR REPLACE INTO progress VALUES ('open', ?)", (json.dumps(active),))
        con.commit()
        recent = [json.loads(r[0]) for r in con.execute("SELECT data FROM trades ORDER BY i DESC LIMIT 200")]
        _atomic(path, {"trades": list(reversed(recent)), "preview": True})
    finally:
        con.close()


def iter_trades(path: Path) -> Iterator[dict[str, Any]]:
    index_path = path.with_name("trades-index.sqlite3")
    if index_path.exists():
        con = sqlite3.connect(f"file:{index_path.as_posix()}?mode=ro", uri=True)
        try:
            con.execute("PRAGMA cache_size=-2048")
            for (data,) in con.execute("SELECT data FROM trades ORDER BY i"):
                yield json.loads(data)
        finally:
            con.close()
        return
    lines = path.with_suffix(".jsonl")
    if lines.exists():
        with lines.open(encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    yield json.loads(line)
        return
    # Legacy artifacts have one object containing a trades array. Decode one
    # object at a time and discard consumed bytes; strings may contain brackets.
    decoder = json.JSONDecoder()
    with path.open(encoding="utf-8") as handle:
        buffer = ""
        while "[" not in buffer:
            more = handle.read(65536)
            if not more:
                return
            buffer += more
        buffer = buffer.split("[", 1)[1]
        while True:
            buffer = buffer.lstrip(" \n\r\t,")
            if buffer.startswith("]"):
                return
            try:
                value, end = decoder.raw_decode(buffer)
            except json.JSONDecodeError:
                more = handle.read(65536)
                if not more:
                    raise ValueError("truncated legacy trades artifact") from None
                buffer += more
                continue
            yield value
            buffer = buffer[end:]


def trade_page(path: Path, offset: int, limit: int, sort: str = "index", descending: bool = False) -> dict[str, Any]:
    index_path = path.with_name("trades-index.sqlite3")
    allowed = {"index": "i", "entry_ms": "entry_ms", "net_pnl": "net_pnl", "mae": "mae", "mfe": "mfe", "duration_ms": "duration_ms"}
    if sort not in allowed:
        raise ValueError("unknown trade sort field")
    if not index_path.exists():
        # Build a bounded disk index for old artifacts so all sort orders apply
        # to the complete archive, rather than merely the current page.
        tmp = index_path.with_name(f".{index_path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
        con = sqlite3.connect(tmp)
        try:
            con.execute("PRAGMA cache_size=-2048")
            con.execute("CREATE TABLE trades (i INTEGER PRIMARY KEY, entry_ms INTEGER, net_pnl REAL, mae REAL, mfe REAL, duration_ms INTEGER, data TEXT)")
            for row in iter_trades(path):
                con.execute("INSERT INTO trades VALUES (?, ?, ?, ?, ?, ?, ?)", (row["index"], row["entry_ms"], float(row["net_pnl"]), float(row["mae"]), float(row["mfe"]), row.get("duration_ms"), json.dumps(row)))
            con.commit()
        finally:
            con.close()
        try:
            if not index_path.exists():
                os.replace(tmp, index_path)
        finally:
            tmp.unlink(missing_ok=True)
    if index_path.exists():
        con = sqlite3.connect(f"file:{index_path.as_posix()}?mode=ro", uri=True)
        try:
            con.execute("PRAGMA cache_size=-2048")
            con.execute("PRAGMA temp_store=FILE")
            total = con.execute("SELECT count(*) FROM trades").fetchone()[0]
            rows = con.execute(f"SELECT data FROM trades ORDER BY {allowed[sort]} {'DESC' if descending else 'ASC'}, i LIMIT ? OFFSET ?", (limit, offset))
            page = [json.loads(r[0]) for r in rows]
        finally:
            con.close()
    return {"trades": page, "total": total, "offset": offset, "limit": limit}


def equity_chart(path: Path, points: int = 4000) -> dict[str, Any]:
    parquet = pq.ParquetFile(path)
    total = parquet.metadata.num_rows
    width = max(1, math.ceil(total / max(1, (points - 2) // 3)))
    selected: dict[int, tuple[int, float, float | None]] = {}
    extrema: list[Any] = [None, None, None]
    bucket_count = 0
    peak = 0.0
    index = 0

    def flush() -> None:
        if extrema[0] is not None:
            for row in extrema:
                selected[row[0]] = row[1:]
            extrema[:] = [None, None, None]

    for batch in parquet.iter_batches(batch_size=4096):
        cols = batch.to_pydict()
        for ts, value, low, high in zip(cols["ts_ms"], cols["equity"], cols.get("equity_low", cols["equity"]), cols.get("equity_high", cols["equity"])):
            peak = max(peak, value if index == 0 else peak, high)
            drawdown = low / peak - 1 if peak > 0 else None
            row = (index, ts, value, drawdown)
            if index in (0, total - 1):
                selected[index] = row[1:]
            if extrema[0] is None or row[2] < extrema[0][2]:
                extrema[0] = row
            if extrema[1] is None or row[2] > extrema[1][2]:
                extrema[1] = row
            if extrema[2] is None or (row[3] if row[3] is not None else 0) < (extrema[2][3] if extrema[2][3] is not None else 0):
                extrema[2] = row
            bucket_count += 1
            if bucket_count == width:
                flush()
                bucket_count = 0
            index += 1
    flush()
    rows = [selected[i] for i in sorted(selected)]
    return {"ts": [r[0] for r in rows], "equity": [r[1] for r in rows],
            "drawdown": [r[2] for r in rows], "samples": total,
            "returned": len(rows), "partial": False}


def price_chart(root: Path, timeframe: str, symbol: str, start: int, end: int, points: int = 4000) -> dict[str, Any]:
    from perplab.data.query import derive_timeframe
    bucket_count = max(1, (points - 2) // 2)
    width = max(1, math.ceil((end-start)/bucket_count))
    bins = {}
    first = last = None
    bars = 0
    week = 7 * 86_400_000
    lo = start
    while lo < end:
        checkpoint("price_summary")
        hi = min(end, (lo//week+1)*week)
        table = derive_timeframe(root, timeframe, symbol=symbol, start_ms=lo, end_ms=hi)
        for ts, value in zip(table.column("close_time").to_pylist(), table.column("close").to_pylist()):
            row = (ts, value/1e8)
            first = first or row
            last = row
            bars += 1
            slot = bins.setdefault(min(bucket_count-1, max(0, (ts-start)//width)), [row, row])
            if row[1] < slot[0][1]:
                slot[0] = row
            if row[1] > slot[1][1]:
                slot[1] = row
        lo = hi
    selected = {r[0]: r[1] for pair in bins.values() for r in pair}
    if first:
        selected[first[0]], selected[last[0]] = first[1], last[1]
    rows = sorted(selected.items())
    return {"symbol": symbol, "timeframe": timeframe, "ts": [r[0] for r in rows], "close": [r[1] for r in rows], "bars": bars}


class ParquetColumn(Sequence):
    """Lazy full-resolution column with one Arrow row group and one Python page."""
    def __init__(self, path: Path, name: str):
        self.parquet, self.name = pq.ParquetFile(path), name
        self.starts = [0]
        for group in range(self.parquet.metadata.num_row_groups):
            self.starts.append(self.starts[-1] + self.parquet.metadata.row_group(group).num_rows)
        self._group, self._arrow = -1, None
        self._page_start, self._page = -1, []

    def __len__(self):
        return self.starts[-1]

    def __getitem__(self, index):
        from bisect import bisect_right
        from perplab.engine.history import Slice
        if isinstance(index, slice):
            return Slice(self, range(*index.indices(len(self))))
        if index < 0:
            index += len(self)
        if not 0 <= index < len(self):
            raise IndexError(index)
        group = bisect_right(self.starts, index) - 1
        if group != self._group:
            self._arrow = self.parquet.read_row_group(group, columns=[self.name]).column(self.name)
            self._group, self._page_start = group, -1
        local = index - self.starts[group]
        start = local // 4096 * 4096
        if start != self._page_start:
            self._page = self._arrow.slice(start, 4096).to_pylist()
            self._page_start = start
        return self._page[local - start]

    def __iter__(self):
        for batch in self.parquet.iter_batches(batch_size=4096, columns=[self.name]):
            yield from batch.column(0).to_pylist()

    def close(self):
        self._arrow, self._page = None, []
        self.parquet.close()

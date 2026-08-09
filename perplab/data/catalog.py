"""Parquet footer catalog and reusable sorted inputs; archives are never rewritten."""
from __future__ import annotations
import hashlib
import json
import os
import sqlite3
from pathlib import Path
from collections.abc import Iterator, Sequence
from typing import Any
import pyarrow as pa
import pyarrow.parquet as pq

from perplab.resources import checkpoint, ResourceLimitExceeded, MiB, process_memory, cache_reservation

_catalogs: dict[tuple[int, str, str], list[dict[str, Any]]] = {}
HOUR_MS = 3_600_000


def catalog(root: Path, dataset: str) -> list[dict[str, Any]]:
    key = (os.getpid(), str(root.resolve()), dataset)
    paths = sorted((root / dataset).rglob("*.parquet"))
    current = [(p, p.stat()) for p in paths]
    signature = [(p.resolve().as_posix(), s.st_size, s.st_mtime_ns) for p, s in current]
    if key in _catalogs and signature == [(r["path"], r["size"], r["mtime"]) for r in _catalogs[key]]:
        return _catalogs[key]
    directory = root / "_replay"
    directory.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(directory / "catalog.sqlite3", timeout=30)
    try:
        con.execute("CREATE TABLE IF NOT EXISTS files_v3 (path TEXT PRIMARY KEY, size INTEGER, mtime INTEGER, metadata TEXT)")
        result = []
        for path, stat in current:
            checkpoint("catalog")
            absolute = path.resolve().as_posix()
            saved = con.execute("SELECT metadata FROM files_v3 WHERE path=? AND size=? AND mtime=?", (absolute, stat.st_size, stat.st_mtime_ns)).fetchone()
            if saved:
                row = json.loads(saved[0])
            else:
                metadata = pq.read_metadata(path)
                extent = []
                unknown = False
                clocks = {}
                for clock in ("ts_ms", "recv_ms", "open_time", "close_time", "calc_time"):
                    index = metadata.schema.names.index(clock) if clock in metadata.schema.names else -1
                    stats = [metadata.row_group(g).column(index).statistics for g in range(metadata.num_row_groups)] if index >= 0 else []
                    valid = [s for s in stats if s is not None and s.has_min_max]
                    incomplete = index >= 0 and any(s is None or not s.has_min_max and s.null_count != metadata.row_group(g).num_rows for g, s in enumerate(stats))
                    if incomplete:
                        unknown = True
                    if valid:
                        bounds = (min(s.min for s in valid), max(s.max for s in valid))
                        extent.append(bounds)
                        if not incomplete:
                            clocks[clock] = bounds
                    elif index >= 0 and not incomplete:
                        clocks[clock] = (None, None)
                row = {"path": absolute, "size": stat.st_size, "mtime": stat.st_mtime_ns,
                       "rows": metadata.num_rows, "clocks": clocks,
                       "min": None if unknown else min((r[0] for r in extent), default=None),
                       "max": None if unknown else max((r[1] for r in extent), default=None)}
                con.execute("INSERT OR REPLACE INTO files_v3 VALUES (?, ?, ?, ?)", (absolute, stat.st_size, stat.st_mtime_ns, json.dumps(row)))
            result.append(row)
        con.commit()
        _catalogs[key] = result
        return result
    finally:
        con.close()


def inventory(root: Path, dataset: str, symbol: str, clock: str, *,
              end_clock: str | None = None, days: bool = False) -> dict[str, Any]:
    """Exact row bounds/counts and optional observed days, with bounded fallback.

    Hive symbol partitions let footer counts answer tick coverage without scanning
    trades. Missing statistics and files without a symbol partition use Arrow
    batches instead. Bar day coverage reads only its clock column and is cached by
    source fingerprint; a monthly directory never implies all days are present.
    """
    end_clock = end_clock or clock
    rows = [r for r in catalog(root, dataset)
            if not any(p.startswith("symbol=") for p in Path(r["path"]).parts)
            or f"symbol={symbol}" in Path(r["path"]).parts]
    signature = [(r["path"], r["size"], r["mtime"]) for r in rows]
    key = hashlib.sha256(json.dumps([signature, symbol, clock, end_clock, days]).encode()).hexdigest()
    con = sqlite3.connect(root / "_replay" / "catalog.sqlite3", timeout=30)
    try:
        con.execute("CREATE TABLE IF NOT EXISTS inventories (key TEXT PRIMARY KEY, value TEXT)")
        saved = con.execute("SELECT value FROM inventories WHERE key=?", (key,)).fetchone()
        if saved:
            return json.loads(saved[0])
        count, lo, hi, observed = 0, None, None, set()
        for row in rows:
            checkpoint("coverage")
            partitioned = f"symbol={symbol}" in Path(row["path"]).parts
            clocks = row["clocks"]
            footer = partitioned and clock in clocks and end_clock in clocks
            if footer:
                count += row["rows"]
                first, last = clocks[clock][0], clocks[end_clock][1]
                lo = first if lo is None else lo if first is None else min(lo, first)
                hi = last if hi is None else hi if last is None else max(hi, last)
            if footer and not days:
                continue
            with pq.ParquetFile(row["path"]) as file:
                columns = list(dict.fromkeys([clock] if footer else [clock, end_clock]))
                if not partitioned:
                    columns.append("symbol")
                for batch in file.iter_batches(batch_size=16_384, columns=columns):
                    checkpoint("coverage")
                    values = batch.to_pydict()
                    for i, first in enumerate(values[clock]):
                        if not partitioned and values["symbol"][i] != symbol:
                            continue
                        if not footer:
                            count += 1
                            last = values[end_clock][i]
                            if first is not None:
                                lo = first if lo is None else min(lo, first)
                            if last is not None:
                                hi = last if hi is None else max(hi, last)
                        if days and first is not None:
                            observed.add(first // 86_400_000)
        result = {"rows": count, "start_ms": lo, "end_ms": None if hi is None else hi + 1,
                  "days": sorted(observed)}
        con.execute("INSERT OR REPLACE INTO inventories VALUES (?, ?)", (key, json.dumps(result)))
        con.commit()
        return result
    finally:
        con.close()


def source_fingerprint(root: Path, datasets: Sequence[str]) -> str:
    # Unlike the process catalog, validation needs a fresh stat on every call: a
    # collector may append a part between sweep points.
    digest = hashlib.sha256()
    for dataset in sorted(datasets):
        for path in sorted((root / dataset).rglob("*.parquet")):
            stat = path.stat()
            digest.update(json.dumps((path.relative_to(root).as_posix(), stat.st_size, stat.st_mtime_ns), separators=(",", ":")).encode())
    return digest.hexdigest()


def clean_abandoned(root: Path) -> None:
    boundary = (root / "_duckdb_tmp").resolve()
    for directory in boundary.glob("*"):
        if directory.is_dir() and directory.name.isdigit() and not process_memory(int(directory.name)):
            import shutil
            target = directory.resolve()
            if not target.is_relative_to(boundary) or target == boundary:
                raise ResourceLimitExceeded("spill cleanup escaped its temporary directory")
            try:
                shutil.rmtree(target)
            except FileNotFoundError:
                pass  # another admitted worker already removed this dead process
    for path in (root / "_replay" / "sorted").glob("*.tmp-*"):
        if not process_memory(int(path.name.rsplit("-", 1)[-1])):
            path.unlink(missing_ok=True)
    for path in (root / "_replay" / "prepared").glob(".*.tmp-*.sqlite3*"):
        pid = int(path.name.split(".tmp-", 1)[1].split(".", 1)[0])
        if not process_memory(pid):
            path.unlink(missing_ok=True)


def stream_windows(root: Path | str, sql: str, *, datasets: Sequence[str],
                   params: Sequence[Any], window_ms: int = HOUR_MS) -> Iterator[pa.RecordBatch]:
    """Last two parameters are a half-open clock range, as in every tick loader.

    Window on the queried clock; footer pruning conservatively includes both venue
    and receive clocks, including arrivals crossing midnight. No stream sequence resets.
    """
    from perplab.data.query import STREAM_BATCH_ROWS
    root = Path(root)
    clean_abandoned(root)
    records = {dataset: catalog(root, dataset) for dataset in datasets}
    start, end = int(params[-2]), int(params[-1])
    for lo in range(start, end, window_ms):
        checkpoint("replay_query")
        hi = min(end, lo + window_ms)
        files = {dataset: [r["path"] for r in rows if r["min"] is None or r["max"] >= lo and r["min"] < hi]
                 for dataset, rows in records.items()}
        if not any(files.values()):
            continue
        fingerprint = [[(r["path"], r["size"], r["mtime"]) for r in rows if r["path"] in files[dataset]] for dataset, rows in records.items()]
        key = hashlib.sha256(json.dumps([sql, list(params[:-2]), lo, hi, fingerprint, 1]).encode()).hexdigest()
        cache_dir = root / "_replay" / "sorted"
        cache_dir.mkdir(parents=True, exist_ok=True)
        target = cache_dir / (key + ".parquet")
        if target.exists():
            with pq.ParquetFile(target) as file:
                for batch in file.iter_batches(batch_size=STREAM_BATCH_ROWS):
                    checkpoint("replay_cache")
                    yield batch
            continue
        # Reserve the remainder among active research processes; DuckDB's own
        # per-instance spill cap is additionally set at connection creation.
        from perplab.resources import reserve_spill
        reserve_spill(root / "_duckdb_tmp" / str(os.getpid()))
        with cache_reservation(root, 256 * MiB, target=target) as quota:
            yield from _build_window(root, sql, datasets, params, files, lo, hi, target, quota)


def _build_window(root, sql, datasets, params, files, lo, hi, target, quota):
    from perplab.data.query import connect, _arrow_reader, STREAM_BATCH_ROWS
    con = None
    tmp = target.with_name(target.name + f".tmp-{os.getpid()}")
    writer = None
    complete = False
    try:
        con = connect(root, datasets=datasets, include_unscaled=False, files=files)
        cursor = con.execute(sql, [*params[:-2], lo, hi])
        for batch in _arrow_reader(cursor, STREAM_BATCH_ROWS):
            checkpoint("replay")
            if writer is None:
                writer = pq.ParquetWriter(tmp, batch.schema, compression="zstd")
            writer.write_batch(batch)
            if tmp.stat().st_size > quota:
                raise ResourceLimitExceeded("spill_limit: sorted replay cache reached shared disk allowance")
            yield batch
        complete = True
    finally:
        try:
            if writer:
                writer.close()
        except BaseException:
            complete = False
            raise
        finally:
            if con is not None:
                con.close()
            if complete and tmp.exists():
                if tmp.stat().st_size > quota:
                    tmp.unlink(missing_ok=True)
                    raise ResourceLimitExceeded("spill_limit: sorted replay cache exceeded its disk allowance")
                if target.exists():
                    tmp.unlink(missing_ok=True)
                else:
                    try:
                        os.replace(tmp, target)
                    except PermissionError:
                        if not target.exists():
                            raise
                        tmp.unlink(missing_ok=True)
            else:
                tmp.unlink(missing_ok=True)

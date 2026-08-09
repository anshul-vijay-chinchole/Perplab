"""Buffered, partitioned, crash-safe Parquet writer.

Three properties matter more than throughput here, because this is the last stage before
data becomes permanent:

**Atomicity.** Every file is written to a `.tmp` name, **fsynced**, and then renamed into
place. `os.replace` is atomic on both NTFS and POSIX, so a reader never observes a partial
file. Writing directly to the final path would leave a truncated Parquet footer after a
crash, and DuckDB reads truncated files as *short* rather than *broken* -- a silent data
loss that surfaces months later as an unexplained gap. The fsync closes the half of that
story the rename alone cannot: rename orders the *name*, not the *bytes*, so on a power
cut the published name could point at pages the OS never flushed and the "atomic" publish
delivered exactly the truncated-footer corruption it was built to prevent. The kernel's
write-back cache made that window minutes wide, not milliseconds. Durability of the
*directory entry* is weaker on purpose: `os.fsync` on a directory handle is POSIX-only
(Windows cannot open a directory for writing), so the rename itself is fsynced only where
the platform allows -- see `_write_partition` -- and a power cut can still cost the *last*
published name on NTFS, which is one flush interval of loss, the bound already accepted
below, never a torn file.

**Bounded loss.** Buffered rows live in memory until flushed, so a crash loses at most
one flush interval. That window is deliberately short (60 s by default), and any loss it
does cause is bracketed by a `RESTART` record in the collector event stream, which makes
it an *explained* gap rather than a mysterious one (spec 4.5).

**Partition safety.** Rows are grouped by partition before writing, so a buffer that spans
a boundary splits correctly instead of filing an hour of the new day under the old one.
The layout and the timestamp column both come from `schemas.PARTITION_LAYOUT`, because
the two are a single decision: a dataset that partitions by `open_time` must also *group*
by `open_time`, and letting the writer assume `ts_ms` would file bulk klines under
whatever unrelated column happened to be first.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from perplab.data.schemas import layout_for, normalise_symbol, partition_components

__all__ = ["ParquetBufferedWriter"]

_COMPRESSION = "zstd"
_COMPRESSION_LEVEL = 3
"""Spec 4.3. Better ratio than Snappy at comparable decode speed; the collector is
nowhere near CPU-bound, so the extra compression is free in practice."""


class ParquetBufferedWriter:
    """Accumulates rows in memory and flushes them as partitioned Parquet files.

    Files are named `part-<epoch_ms>-<pid>-<seq>.parquet` rather than the single
    `data.parquet` of spec 4.3. A live collector cannot produce one file per day
    atomically without holding the entire day in memory, so it writes many part-files
    which a later compaction step merges. Hive globbing reads a multi-file partition
    identically, so nothing downstream needs to know the difference.

    The pid is in the stem because two processes legitimately share one partition:
    `perplab collect` and `perplab macro` both flush `collectorEvents` into the same
    dated directory, and each keeps a private `_seq` counter starting at 1. Without the
    pid, two flushes landing in the same millisecond produced the same stem, and the
    second `os.replace` -- the very call that makes publishing atomic -- silently
    replaced the first process's file with the second's. Same-millisecond collisions are
    exactly what a shared flush interval manufactures; the pid makes the stems disjoint
    per process, so the collision cannot occur rather than being unlikely to.
    """

    def __init__(
        self,
        root: Path,
        dataset: str,
        schema: pa.Schema,
        *,
        symbol: str | None = None,
        max_rows: int = 100_000,
        flush_interval_s: float = 60.0,
    ) -> None:
        self._root = Path(root)
        self._dataset = dataset
        self._schema = schema
        # Canonicalised and validated at the boundary. `symbol` becomes a literal path
        # component (`symbol=<value>`), and this constructor used to accept it verbatim:
        # `--symbol 'BTC/USDT'` created a partition no reader could parse, and a value
        # like `../..` walked *out of the lake root* before writing. `normalise_symbol`
        # rejects every separator and quote outright, so nothing that could escape the
        # partition tree survives to reach `_partition_dir` (finding M29).
        self._symbol = None if symbol is None else normalise_symbol(symbol)
        self._max_rows = max_rows
        self._flush_interval_s = flush_interval_s
        # Resolved once, at construction, so an unregistered dataset name fails when the
        # writer is built rather than on the first flush -- by which point the collector
        # is already running and the rows are already in memory.
        self._time_column = layout_for(dataset).time_column

        self._buffer: dict[tuple[str, ...], list[dict[str, Any]]] = {}
        self._buffered_rows = 0
        self._last_flush = time.monotonic()
        self._seq = 0
        self.rows_written = 0
        self.files_written = 0

    def append(self, row: dict[str, Any]) -> None:
        """Buffer one row. Cheap by design -- this runs on the WebSocket read path.

        No disk or network work happens here. Blocking the read path would stall the
        socket drain, fill Binance's send buffer, and get the connection dropped, turning
        a slow writer into a data gap.
        """
        key = partition_components(self._dataset, row[self._time_column])
        self._buffer.setdefault(key, []).append(row)
        self._buffered_rows += 1

    def maybe_flush(self) -> None:
        """Flush if the row or time threshold has been reached."""
        if self._buffered_rows == 0:
            return
        if (
            self._buffered_rows >= self._max_rows
            or time.monotonic() - self._last_flush >= self._flush_interval_s
        ):
            self.flush()

    def flush(self) -> None:
        """Write buffered rows, dropping each partition from the buffer as it lands.

        Retry on failure is the point: a transient disk error must not lose rows. But the
        retry has to be *per partition*, and an earlier version cleared the buffer only
        after the whole loop. A failure on the third of four partitions then left the first
        two both on disk **and** still buffered, so the next flush wrote them a second time
        — turning one transient error into permanent silent duplication. Tick datasets are
        where those duplicates land, and `Coverage.duplicate_rows` is not computed for them
        (finding F9), so nothing downstream would ever have reported it.

        Dropping each partition as soon as its file is published makes the retry
        idempotent: what is on disk is out of the buffer, and what is in the buffer is not
        on disk. The first failure still propagates — the caller decides whether a disk
        error is fatal — and the partitions after it stay buffered for the next attempt.
        """
        if not self._buffer:
            return

        for components, rows in sorted(self._buffer.items()):
            if rows:
                self._write_partition(components, rows)
            # Only reached if the write above succeeded, so the buffer and the lake never
            # both hold the same rows.
            del self._buffer[components]
            self._buffered_rows -= len(rows)

        self._buffer.clear()
        self._buffered_rows = 0
        self._last_flush = time.monotonic()

    def _write_partition(
        self, components: tuple[str, ...], rows: list[dict[str, Any]]
    ) -> None:
        directory = self._partition_dir(components)
        directory.mkdir(parents=True, exist_ok=True)

        table = pa.Table.from_pylist(rows, schema=self._schema)

        self._seq += 1
        stem = f"part-{int(time.time() * 1000)}-{os.getpid()}-{self._seq:06d}"
        tmp = directory / f".{stem}.parquet.tmp"
        final = directory / f"{stem}.parquet"

        pq.write_table(
            table,
            tmp,
            compression=_COMPRESSION,
            compression_level=_COMPRESSION_LEVEL,
        )
        # Durability before visibility. `os.replace` orders the *name*; it says nothing
        # about the *bytes*, which after `write_table` may still live entirely in the
        # OS write-back cache. Publishing a name whose bytes are not on stable storage
        # is how a power cut yields the truncated-footer file the module docstring
        # describes -- under the atomic-looking rename. So the data is fsynced first,
        # through a reopened handle because pyarrow has already closed its own. The
        # handle is opened read-write ("r+b", which never truncates): Windows implements
        # `os.fsync` as `_commit`, which demands a writable descriptor and fails EBADF
        # on a read-only one.
        with open(tmp, "r+b") as handle:
            os.fsync(handle.fileno())
        # Atomic publish. Until this line the file is invisible to readers (leading dot
        # plus .tmp suffix), and after it the file is complete. There is no in-between.
        os.replace(tmp, final)
        # Persist the rename itself where the platform can. On POSIX a directory is
        # fsyncable and this pins the new directory entry; Windows cannot open a
        # directory handle for fsync (`os.open` on a directory raises PermissionError),
        # so there the entry rides the next metadata flush -- losing, at worst, the last
        # published *name* after a power cut, never a partial file. Documented in the
        # module docstring; not silently skipped.
        if os.name == "posix":
            fd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)

        self.rows_written += len(rows)
        self.files_written += 1

    def _partition_dir(self, components: tuple[str, ...]) -> Path:
        base = self._root / self._dataset
        if self._symbol is not None:
            base = base / f"symbol={self._symbol}"
        for component in components:
            base = base / component
        return base

    def __enter__(self) -> ParquetBufferedWriter:
        return self

    def __exit__(self, *exc: object) -> None:
        self.flush()

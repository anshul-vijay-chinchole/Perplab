"""Append-only histories with bounded pages and lazy slices, including exact Money.

JSON is decoded against types registered by this process when appending, never imported
or executed from an artifact. Legacy/small engines retain their ordinary lists.
"""
from __future__ import annotations
import json
import sqlite3
import contextlib
import threading
from dataclasses import fields, is_dataclass
from enum import Enum
from pathlib import Path
from collections.abc import Iterator, Sequence
from typing import Any
from perplab.core.money import Money, parse_money
from perplab.resources import ResourceLimitExceeded

_types: dict[str, type] = {}
_scopes = threading.local()


def track_handle(handle: Any) -> None:
    """Close an artifact reader with its owner's history scope, when present."""
    handles = getattr(_scopes, "handles", None)
    if handles is not None:
        handles.append(handle)


@contextlib.contextmanager
def history_scope():
    previous = getattr(_scopes, "handles", None)
    _scopes.handles = []
    try:
        yield
    finally:
        for handle in _scopes.handles:
            handle.close()
        _scopes.handles = previous


def _encode(value: Any) -> Any:
    if isinstance(value, Money):
        return {"$money": str(value)}
    if isinstance(value, Enum) or is_dataclass(value) and not isinstance(value, type):
        cls = type(value)
        name = cls.__module__ + "." + cls.__qualname__
        _types[name] = cls
        if isinstance(value, Enum):
            return {"$enum": name, "value": value.value}
        return {"$type": name, "fields": {f.name: _encode(getattr(value, f.name)) for f in fields(value) if f.init}}
    if isinstance(value, dict) or hasattr(value, "items"):
        return {"$map": {str(k): _encode(v) for k, v in value.items()}}
    if isinstance(value, tuple):
        return {"$tuple": [_encode(v) for v in value]}
    if isinstance(value, (list, set, frozenset)):
        return {"$" + type(value).__name__: [_encode(v) for v in value]}
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise ResourceLimitExceeded(f"history serialization refused unsupported value {type(value).__name__}")


def _decode(value: Any) -> Any:
    if not isinstance(value, dict):
        return value
    if "$money" in value:
        return parse_money(value["$money"])
    if "$map" in value:
        return {k: _decode(v) for k, v in value["$map"].items()}
    if "$enum" in value:
        return _types[value["$enum"]](value["value"])
    if "$type" in value:
        return _types[value["$type"]](**{k: _decode(v) for k, v in value["fields"].items()})
    for kind, factory in (("tuple", tuple), ("list", list), ("set", set), ("frozenset", frozenset)):
        if "$" + kind in value:
            return factory(_decode(v) for v in value["$" + kind])
    return {k: _decode(v) for k, v in value.items()}


class Slice(Sequence):
    def __init__(self, source: Sequence, indices: range):
        self.source, self.indices = source, indices

    def __len__(self) -> int:
        return len(self.indices)

    def __getitem__(self, index: int | slice) -> Any:
        if isinstance(index, slice):
            return Slice(self.source, self.indices[index])
        return self.source[self.indices[index]]

    def __iter__(self) -> Iterator[Any]:
        if isinstance(self.source, DiskSequence) and self.indices.step == 1:
            yield from self.source.iter_range(self.indices.start, self.indices.stop)
        else:
            for i in self.indices:
                yield self.source[i]


class DiskSequence(Sequence):
    PAGE = 256

    def __init__(self, path: Path, *, readonly: bool = False, event_sequence: bool = False):
        import hashlib
        self._event_digest = hashlib.sha256(b"[") if event_sequence else None
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.con = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True) if readonly else sqlite3.connect(path)
        self.closed = False
        if getattr(_scopes, "handles", None) is not None:
            _scopes.handles.append(self)
        if readonly:
            self.con.execute("PRAGMA cache_size=-1024")
            self.length = int(self.con.execute("SELECT count(*) FROM items").fetchone()[0])
            self._page_at, self._page = -1, []
            return
        self.con.execute("PRAGMA journal_mode=TRUNCATE")
        self.con.execute("PRAGMA cache_size=-1024")
        self.con.execute("CREATE TABLE IF NOT EXISTS items (i INTEGER PRIMARY KEY, data TEXT NOT NULL)")
        self.con.execute("DELETE FROM items")
        self.con.commit()
        self.length = 0
        self._page_at, self._page = -1, []

    def append(self, item: Any) -> None:
        data = json.dumps(_encode(item), separators=(",", ":"), ensure_ascii=True)
        if len(data) > 16 * 1024 ** 2:
            raise ResourceLimitExceeded("history_limit: a single history entry exceeds 16 MiB")
        if self._event_digest is not None:
            from perplab.strategy.dryrun import _canonical
            if self.length:
                self._event_digest.update(b",")
            self._event_digest.update(json.dumps({"seq": item.seq, "ts_ms": item.ts_ms,
                "kind": item.kind, "payload": _canonical(dict(item.payload))},
                sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8"))
        self.con.execute("INSERT INTO items VALUES (?, ?)", (self.length, data))
        self.length += 1
        self._page_at = -1
        if self.length % self.PAGE == 0:
            self.con.commit()

    def __len__(self) -> int:
        return self.length

    def __getitem__(self, index: int | slice) -> Any:
        if isinstance(index, slice):
            return Slice(self, range(*index.indices(self.length)))
        if index < 0:
            index += self.length
        if not 0 <= index < self.length:
            raise IndexError(index)
        start = index // self.PAGE * self.PAGE
        if start != self._page_at:
            self._page = list(self.iter_range(start, min(start + self.PAGE, self.length)))
            self._page_at = start
        return self._page[index - start]

    def iter_range(self, start: int, end: int) -> Iterator[Any]:
        cursor = self.con.execute("SELECT data FROM items WHERE i>=? AND i<? ORDER BY i", (start, end))
        while rows := cursor.fetchmany(self.PAGE):
            for (data,) in rows:
                yield _decode(json.loads(data))

    def __iter__(self) -> Iterator[Any]:
        yield from self.iter_range(0, self.length)

    def flush(self) -> None:
        self.con.commit()

    def canonical_hash(self) -> str | None:
        if self._event_digest is None:
            return None
        digest = self._event_digest.copy()
        digest.update(b"]")
        return digest.hexdigest()

    def close(self) -> None:
        if not self.closed:
            self.flush()
            self.con.close()
            self.closed = True

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


class Concat(Sequence):
    def __init__(self, first: Sequence, second: Sequence):
        self.first, self.second = first, second

    def __len__(self) -> int:
        return len(self.first) + len(self.second)

    def __getitem__(self, index: int | slice) -> Any:
        if isinstance(index, slice):
            return Slice(self, range(*index.indices(len(self))))
        if index < 0:
            index += len(self)
        return self.first[index] if index < len(self.first) else self.second[index - len(self.first)]

    def __iter__(self) -> Iterator[Any]:
        yield from self.first
        yield from self.second


def freeze(series: Sequence) -> Sequence:
    if isinstance(series, DiskSequence):
        series.flush()
        return series
    return tuple(series)


def register_types(*classes: type) -> None:
    for cls in classes:
        _types[cls.__module__ + "." + cls.__qualname__] = cls


class BoundedMessages(list):
    def __init__(self, values=(), limit=4096):
        super().__init__(values)
        self.limit, self.dropped = limit, 0

    def append(self, value):
        if len(self) >= self.limit:
            del self[64]
            self.dropped += 1
        super().append(value)

    def extend(self, values):
        for value in values:
            self.append(value)


class ArchivedOrders(dict):
    """All active orders and a recent terminal window; older lookups use disk."""
    def __init__(self, path: Path):
        super().__init__()
        self.con = sqlite3.connect(path)
        self.closed = False
        if getattr(_scopes, "handles", None) is not None:
            _scopes.handles.append(self)
        self.con.execute("PRAGMA cache_size=-1024")
        self.con.execute("CREATE TABLE IF NOT EXISTS orders (id TEXT PRIMARY KEY, data TEXT)")
        self.con.execute("DELETE FROM orders")
        self.con.commit()

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __getitem__(self, key):
        if dict.__contains__(self, key):
            return dict.__getitem__(self, key)
        row = self.con.execute("SELECT data FROM orders WHERE id=?", (key,)).fetchone()
        if row is None:
            raise KeyError(key)
        order = _decode(json.loads(row[0]))
        self[key] = order
        return order

    def prune(self, recent=1000):
        terminal = [key for key, order in self.items() if not order.is_open]
        for key in terminal[:-recent]:
            self.con.execute("INSERT OR REPLACE INTO orders VALUES (?, ?)", (key, json.dumps(_encode(dict.__getitem__(self, key)))))
            dict.__delitem__(self, key)
        self.con.commit()

    def close(self):
        if not self.closed:
            self.con.commit()
            self.con.close()
            self.closed = True

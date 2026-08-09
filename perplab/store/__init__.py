"""SQLite metadata store — strategies, versions, tags, runs (spec 2.2).

Everything that is *not* market data lives here: one file, no server process, no ops
burden. Market data stays in the Parquet lake, because SQLite is the wrong shape for
billions of rows and Parquet is the wrong shape for a row you edit.
"""

from __future__ import annotations

from perplab.store.db import DB_FILENAME, SCHEMA_VERSION, connect, database_path

__all__ = ["DB_FILENAME", "SCHEMA_VERSION", "connect", "database_path"]

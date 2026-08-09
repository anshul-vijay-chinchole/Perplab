"""The SQLite schema and connection factory (spec 2.2, 2.4).

One file at `<root>/perplab.db`. Four decisions here are worth reading before changing
anything:

**Foreign keys are enforced, and that is not the default.** SQLite ships with
`PRAGMA foreign_keys = OFF` for backwards compatibility, so a schema full of `REFERENCES`
clauses does nothing at all unless every connection turns them on. The reference that
matters is `runs.version_id`: spec 5.6 says deleting a strategy is *blocked* while a
non-archived run references it, because runs must stay reproducible. With foreign keys off
that rule would be enforced only by the Python check above it, and the day someone deletes
a row from a shell is the day the run history stops meaning anything.

**WAL, with `synchronous = NORMAL`.** The API server reads while a job worker writes; in
the default rollback-journal mode the reader blocks. WAL lets them proceed concurrently.
`NORMAL` rather than `FULL` because losing the last few milliseconds of metadata on a power
cut is recoverable -- the strategy code is still in the editor -- while an fsync per commit
is not worth paying for on every keystroke-triggered save.

**Timestamps are integer epoch milliseconds**, matching spec 3.1 and the rest of the
codebase. Not SQLite's `datetime()` text, which is local-time-shaped and invites exactly
the timezone contamination `core.types` refuses.

**Migrations are forward-only and numbered.** `schema_version` holds one row. A database
from a newer PerpLab is refused rather than opened, because the alternative is a silent
partial read of a schema this build does not understand.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

__all__ = [
    "DB_FILENAME",
    "SCHEMA_VERSION",
    "SCHEMA",
    "SCHEMA_V7",
    "SCHEMA_V8",
    "database_path",
    "connect",
]

DB_FILENAME = "perplab.db"
SCHEMA_VERSION = 8

SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS strategies (
    id            INTEGER PRIMARY KEY,
    name          TEXT    NOT NULL,
    notes         TEXT    NOT NULL DEFAULT '',
    created_ms    INTEGER NOT NULL,
    updated_ms    INTEGER NOT NULL,
    archived_ms   INTEGER,
    head_version_id INTEGER REFERENCES strategy_versions(id) ON DELETE SET NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS strategies_name_unique
    ON strategies(name COLLATE NOCASE);

CREATE TABLE IF NOT EXISTS strategy_versions (
    id            INTEGER PRIMARY KEY,
    strategy_id   INTEGER NOT NULL REFERENCES strategies(id) ON DELETE CASCADE,
    version_no    INTEGER NOT NULL,
    code          TEXT    NOT NULL,
    code_sha256   TEXT    NOT NULL,
    created_ms    INTEGER NOT NULL,
    message       TEXT    NOT NULL DEFAULT '',
    class_name    TEXT,
    params_json   TEXT    NOT NULL DEFAULT '[]',
    requires_json TEXT,
    valid         INTEGER NOT NULL DEFAULT 0,
    diagnostics_json TEXT NOT NULL DEFAULT '[]'
);

CREATE UNIQUE INDEX IF NOT EXISTS strategy_versions_seq
    ON strategy_versions(strategy_id, version_no);

CREATE INDEX IF NOT EXISTS strategy_versions_hash
    ON strategy_versions(strategy_id, code_sha256);

CREATE TABLE IF NOT EXISTS strategy_tags (
    strategy_id INTEGER NOT NULL REFERENCES strategies(id) ON DELETE CASCADE,
    tag         TEXT    NOT NULL,
    PRIMARY KEY (strategy_id, tag)
);

CREATE TABLE IF NOT EXISTS runs (
    id          INTEGER PRIMARY KEY,
    strategy_id INTEGER NOT NULL REFERENCES strategies(id),
    version_id  INTEGER NOT NULL REFERENCES strategy_versions(id),
    mode        TEXT    NOT NULL,
    status      TEXT    NOT NULL,
    created_ms  INTEGER NOT NULL,
    archived_ms INTEGER
);

CREATE INDEX IF NOT EXISTS runs_by_strategy ON runs(strategy_id, archived_ms);
"""
"""Schema v1, kept verbatim as the base every database starts from.

`runs` was created in v1 although Phase 4 owns it. Two reasons, both concrete: spec 5.6's
delete rule ("blocked if any non-archived run references it") needs a table to query, and a
foreign key added later cannot be enforced retroactively over rows that already exist --
SQLite would accept the `ALTER TABLE` and quietly not check the history. Phase 4's columns
arrive by migration below rather than by editing this string, so a v1 database on disk and
a freshly created one converge on exactly the same shape.

`strategies.name` is unique **case-insensitively**. `EMACross` and `emacross` as two
strategies is a filing mistake, not a feature, and the export bundle's filename would
collide on any case-insensitive filesystem, which is all of Windows and most of macOS.
"""

RUN_COLUMNS_V2: tuple[tuple[str, str], ...] = (
    ("label", "TEXT NOT NULL DEFAULT ''"),
    ("symbols_json", "TEXT NOT NULL DEFAULT '[]'"),
    ("timeframe", "TEXT NOT NULL DEFAULT ''"),
    ("start_ms", "INTEGER"),
    ("end_ms", "INTEGER"),
    ("seed", "INTEGER"),
    ("engine_version", "INTEGER"),
    ("fill_tier", "TEXT"),
    ("params_sha", "TEXT"),
    ("started_ms", "INTEGER"),
    ("finished_ms", "INTEGER"),
    ("event_hash", "TEXT"),
    ("flags_json", "TEXT NOT NULL DEFAULT '[]'"),
    ("warnings_json", "TEXT NOT NULL DEFAULT '[]'"),
    ("error", "TEXT"),
    ("pid", "INTEGER"),
    ("progress_bars", "INTEGER NOT NULL DEFAULT 0"),
    ("progress_total", "INTEGER NOT NULL DEFAULT 0"),
    ("heartbeat_ms", "INTEGER"),
    # A worker killed outright -- OOM, a hard kill, the machine losing power -- never gets
    # to write its own failure, so a `running` row can outlive the process behind it. The
    # heartbeat is how that is distinguishable from a run that is simply slow, without the
    # reader having to guess at a process id that may have been recycled.
    # Denormalised headline figures. The authoritative copies live in the run directory's
    # `metrics.json`; these exist so the Runs table can sort and filter thousands of rows
    # without opening thousands of files, which is the difference between a list that
    # renders and one that hangs.
    ("net_pnl", "TEXT"),
    ("sharpe", "REAL"),
    ("max_drawdown", "REAL"),
    ("round_trips", "INTEGER"),
    ("fills", "INTEGER"),
)
"""Columns migration 2 adds to `runs`. Declared as data so the migration and the
fresh-database path cannot drift: `_migrate` applies exactly this list either way."""

SCHEMA_V2 = """
CREATE TABLE IF NOT EXISTS strategy_trials (
    strategy_id INTEGER NOT NULL REFERENCES strategies(id) ON DELETE CASCADE,
    params_sha  TEXT    NOT NULL,
    params_json TEXT    NOT NULL,
    evaluations INTEGER NOT NULL DEFAULT 0,
    best_sharpe REAL,
    best_run_id INTEGER,
    PRIMARY KEY (strategy_id, params_sha)
);

CREATE INDEX IF NOT EXISTS runs_by_status ON runs(status, created_ms);
"""
"""Schema v2 -- the multiple-testing counter of spec 8.5.

One row per *distinct parameter combination*, not per run, and that is the statistically
meaningful unit. Spec 8.5's warning is that "the best result from N trials is upward-biased
by roughly sqrt(2 ln N) standard deviations under the null"; re-running an identical
combination does not produce a new draw from the null, so counting evaluations rather than
combinations would inflate N and overstate the correction. `evaluations` is kept alongside
because it answers a different and also useful question -- how much compute went into this
strategy -- and it costs one integer.
"""


RUN_COLUMNS_V3: tuple[tuple[str, str], ...] = (
    ("requested_fill_tier", "TEXT"),
    ("tier_reason", "TEXT"),
)
"""Columns migration 3 adds to `runs` -- spec 4.2's "never let a downgrade happen invisibly".

`fill_tier` alone cannot keep that promise. A run showing `BOOK_TICKER` might have asked for
it, or might have asked for `BOOK_WALK` over a range that predates the collector; those are
a preference and a data limitation and they need different reactions from the reader. The
requested tier and one sentence of explanation make the Runs list able to say which without
opening the run's manifest.
"""


RUN_COLUMNS_V4: tuple[tuple[str, str], ...] = (
    ("shadow_of_run_id", "INTEGER"),
    ("endpoint", "TEXT"),
    ("session_kind", "TEXT"),
)
"""Columns migration 4 adds to `runs` -- Phase 7's session and shadow bookkeeping.

`shadow_of_run_id` is what turns two unrelated rows into a paper session and its replay, so
the Runs list can offer the parity report of spec 6.7.1 without opening either directory.
`endpoint` matters because testnet prices diverge from production and two sessions on
different venues are not comparable. `session_kind` distinguishes a live session from its
shadow, which decides whether spec 8.5's trials counter should move.

Deliberately **not** foreign keys. `strategy_trials.best_run_id` is declared the same way
and `delete` has to clear it by hand for exactly that reason -- but adding a constraint here
would mean deleting a paper run cascaded into or blocked deleting its shadow, and which of
those is right depends on why it is being deleted.
"""


SCHEMA_V4 = """
CREATE TABLE IF NOT EXISTS kill_switch_trips (
    id             INTEGER PRIMARY KEY,
    armed_ms       INTEGER NOT NULL,
    trigger_source TEXT    NOT NULL,
    detail         TEXT    NOT NULL DEFAULT '',
    run_id         INTEGER,
    flattened      INTEGER NOT NULL DEFAULT 0,
    cleared_ms     INTEGER,
    cleared_by     TEXT
);

CREATE INDEX IF NOT EXISTS kill_switch_trips_open
    ON kill_switch_trips(cleared_ms, armed_ms);
"""
"""Schema v4 -- spec 7.6's un-arm requirement, made to survive a restart.

The kill switch's armed state lived in one process's memory, so restarting the API server
disarmed it. That is the exact inverse of what spec 7.6 promises: *"requires an explicit
un-arm action before any live session can start again"*. A guarantee a crash can clear is
not a guarantee, and the crash and the trip are correlated -- the thing that tripped the
switch is often the thing that killed the process.

**A log, not a single current-state row.** Each trip is a row; un-arming stamps
`cleared_ms`/`cleared_by` on it rather than deleting it. The armed state is then "the most
recent row with no `cleared_ms`", and the history comes free -- and history is the first
thing anyone wants after an incident, starting with whether this has happened before and
who cleared it last time.

**`run_id` deliberately carries no foreign key**, which is the one place in this schema that
is true. The trip is a fact about the operator's account, not a child of the run: it has to
outlive `RunStore.delete`, and it must never be the reason a delete fails. A `REFERENCES
runs(id)` would give the opposite of both -- and `ON DELETE SET NULL` would quietly erase
which session was running when the platform stopped itself, which is the first question the
record exists to answer.

`trigger_source` rather than `trigger`, matching spec 7.5's own words and avoiding a column
named for a SQL keyword.
"""


SCHEMA_V5 = """
CREATE TABLE IF NOT EXISTS lab_jobs (
    id             INTEGER PRIMARY KEY,
    run_id         INTEGER NOT NULL,
    tool           TEXT    NOT NULL,
    status         TEXT    NOT NULL,
    label          TEXT    NOT NULL DEFAULT '',
    config_json    TEXT    NOT NULL DEFAULT '{}',
    created_ms     INTEGER NOT NULL,
    started_ms     INTEGER,
    finished_ms    INTEGER,
    heartbeat_ms   INTEGER,
    pid            INTEGER,
    progress_done  INTEGER NOT NULL DEFAULT 0,
    progress_total INTEGER NOT NULL DEFAULT 0,
    summary_json   TEXT,
    error          TEXT
);

CREATE INDEX IF NOT EXISTS lab_jobs_by_run ON lab_jobs(run_id, created_ms);
"""
LAB_JOB_COLUMNS_V6: tuple[tuple[str, str], ...] = (
    ("cancel_requested_ms", "INTEGER"),
)
"""Columns migration 6 adds to `lab_jobs`.

`cancel_requested_ms` exists so the UI has something to render the instant Cancel is
pressed. Cancellation is cooperative and only the walk-forward polls for it, so without
this the row read `running` for however long the current fold took -- a button that
appears to do nothing gets pressed again.
"""

SCHEMA_V7 = """
CREATE TABLE IF NOT EXISTS symbol_claims (
    run_id       INTEGER NOT NULL,
    symbol       TEXT    NOT NULL,
    leverage     INTEGER NOT NULL,
    margin_mode  TEXT    NOT NULL,
    hedge_mode   INTEGER NOT NULL,
    endpoint     TEXT    NOT NULL,
    claimed_ms   INTEGER NOT NULL,
    released_ms  INTEGER,
    PRIMARY KEY (run_id, symbol)
);

CREATE INDEX IF NOT EXISTS symbol_claims_open
    ON symbol_claims(endpoint, symbol, released_ms);
"""
"""Schema v7 -- which running session has configured which symbol at the exchange.

**This table exists because leverage is account state, not order state.** `POST
/fapi/v1/leverage` takes a symbol and applies to the whole account; there is no per-strategy
and no per-position-side scope, and Binance offers none. So two strategies trading BTCUSDT
concurrently *share* one leverage whether or not either of them knows it, and the platform's
job is to make that constraint visible rather than to pretend it away.

Without this table the failure is silent and one-directional. Session B's preflight would
reconfigure the symbol under session A, A's `ExchangeTransport` preflight check has already
run and never runs again, and A would go on sizing positions and solving `P_liq` at a
leverage the venue stopped using -- with the displayed liquidation further from the mark than
the real one, which is the wrong direction to be wrong in.

**In SQLite rather than in process memory**, because the sessions are separate processes and
the API server that would hold the dict is restartable while they keep trading. A claim
outlives the process that made it, and `released_ms` is what says a session is done with the
symbol -- `NULL` means still held.

`endpoint` is part of the identity: testnet and production are different accounts, and a
testnet session must not block a production one. Not a foreign key to `runs`, deliberately:
`RunStore.delete` should not have to reason about claims, and a claim whose run row is gone
is stale by definition -- `SymbolClaims` prunes on read against the live run status rather
than relying on referential integrity to have noticed.
"""


SCHEMA_V8 = """
CREATE TABLE IF NOT EXISTS ingest_jobs (
    id                  INTEGER PRIMARY KEY,
    kind                TEXT    NOT NULL,
    symbol              TEXT    NOT NULL,
    status              TEXT    NOT NULL,
    created_ms          INTEGER NOT NULL,
    started_ms          INTEGER,
    finished_ms         INTEGER,
    heartbeat_ms        INTEGER,
    pid                 INTEGER,
    progress_done       INTEGER NOT NULL DEFAULT 0,
    progress_total      INTEGER NOT NULL DEFAULT 0,
    summary_json        TEXT,
    error               TEXT,
    cancel_requested_ms INTEGER
);

CREATE INDEX IF NOT EXISTS ingest_jobs_recent ON ingest_jobs(created_ms, id);
"""
"""Schema v8 -- one row per "bring my data up to date" click.

**The row exists so that a refresh survives the request that started it.** A top-up of
`aggTrades` is tens of minutes of transfer; answering the HTTP call synchronously would
hold a browser open across it, and a browser that times out mid-download leaves the
operator with no way to ask what happened. The row is the thing that can be polled, and
`summary_json` is what makes the answer readable after the fact rather than only while the
progress bar is on screen.

**Columns mirror `lab_jobs` deliberately**, down to `heartbeat_ms`, `pid` and
`cancel_requested_ms`. `store.runs._reap`'s reasoning about workers that died without
saying so applies to a refresh worker verbatim, and a second lifecycle shape would mean a
second set of dead-worker bugs to find. The one semantic difference is what the heartbeat
means here: a refresh worker can spend five minutes retrying a single archive with nothing
to report, so its heartbeat comes from a wall clock rather than from progress -- see
`data.refresh_worker`.

**No foreign key and no `symbol` uniqueness.** A refresh is a fact about an *attempt*, not
about a symbol: two finished jobs for the same symbol are ordinary history, and the thing
that actually prevents two *concurrent* refreshes is `data.refresh.refresh_lock`, which is
cross-process and therefore also covers the CLI. A uniqueness constraint here would claim
an exclusion the database cannot enforce against a process that never opens it.
"""


"""Schema v5 -- Phase 9's Lab jobs (spec 9).

One row per Lab tool invocation, linked to its source run by `run_id`. **Deliberately not
a foreign key**, but for the opposite reason to `shadow_of_run_id`: spec 9 says Lab
artefacts are "linked back to the source run permanently", so `RunStore.delete` refuses
to delete a run that still has Lab jobs -- the guard lives in code, where it can say
*why* and name the jobs, rather than as a bare constraint error. A cascade would have
been silent destruction of exactly the analysis the permanence promise protects.

The columns mirror `runs` where they answer the same questions -- status lifecycle,
heartbeat, pid, progress -- because `_reap`'s reasoning about dead and orphaned workers
applies to a Lab worker verbatim. `summary_json` is the denormalised headline (fold
count, aggregate WFE) so the Lab tab can list fifty jobs without opening fifty result
files, the same argument as the runs table's metric columns.
"""


class SchemaTooNew(RuntimeError):
    """The database was written by a newer PerpLab than this one."""


def database_path(root: Path | str) -> Path:
    """`<root>/perplab.db` (spec 2.4)."""
    return Path(root) / DB_FILENAME


def connect(root: Path | str, *, create: bool = True) -> sqlite3.Connection:
    """Open the metadata database, applying migrations if needed.

    `check_same_thread=False` because FastAPI serves requests from a thread pool and the
    connection is guarded by a lock at the library level. `Row` factory so callers read
    columns by name -- positional indexing into a schema that grows a column is how a query
    starts returning `created_ms` where it meant `updated_ms`.
    """
    path = database_path(root)
    if create:
        path.parent.mkdir(parents=True, exist_ok=True)
    elif not path.exists():
        raise FileNotFoundError(f"no PerpLab database at {path}")

    connection = sqlite3.connect(path, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA synchronous = NORMAL")
    # Without a busy timeout, a concurrent writer makes the other connection raise
    # "database is locked" immediately rather than waiting out a commit that takes
    # microseconds.
    connection.execute("PRAGMA busy_timeout = 5000")
    try:
        _migrate(connection)
    except BaseException:
        # A refused database must also be a *released* one. `SchemaTooNew` tells the reader
        # to upgrade PerpLab, and on Windows the process that raised it was still holding the
        # file open -- so the database could not be moved or replaced while the server it had
        # just refused to start was running.
        connection.close()
        raise
    return connection


def _migrate(connection: sqlite3.Connection) -> None:
    """Apply the base schema, then every migration the database has not seen.

    Migrations are idempotent by construction rather than by bookkeeping: `_apply_v2` adds
    only the columns `PRAGMA table_info` says are missing, so running it against a database
    that already has them is a no-op. That matters because the version row and the actual
    shape can disagree -- a crash between the `ALTER` and the version bump would otherwise
    leave a database that no longer migrates and no longer works.

    **The whole migration runs under one write lock.** Idempotence makes a *sequential*
    re-run safe; it does nothing for two processes running it at once, and two do: the API
    server opens the store at startup and every worker subprocess opens it again. Read
    `PRAGMA table_info`, have another process `ALTER` the same column, then `ALTER` it
    yourself, and SQLite raises `duplicate column name` -- measured at 10 failures in 60
    concurrent opens of a fresh database. The worker's open happens *outside* the `try` that
    turns a failed run into data, so the outcome was a run that vanished without recording
    why. `BEGIN IMMEDIATE` takes the write lock up front and the existing `busy_timeout`
    makes the loser wait rather than fail.

    `executescript` is deliberately not used: it commits any open transaction before running,
    which would drop the lock this depends on.
    """
    connection.execute("BEGIN IMMEDIATE")
    try:
        # The version is read *before* anything is written, so a database from a newer
        # PerpLab is refused rather than half-created and then refused. "Refused rather than
        # opened" has to mean refused rather than written to.
        row = connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'"
        ).fetchone()
        found = SCHEMA_VERSION
        if row is not None:
            stored = connection.execute("SELECT version FROM schema_version").fetchone()
            if stored is not None:
                found = int(stored["version"])
        if found > SCHEMA_VERSION:
            raise SchemaTooNew(
                f"database schema is version {found} but this build understands "
                f"{SCHEMA_VERSION}. Opening it would read a schema this code does not "
                "know, which is worse than refusing. Upgrade PerpLab."
            )

        for statement in _statements(SCHEMA):
            connection.execute(statement)
        row = connection.execute("SELECT version FROM schema_version").fetchone()
        _apply_v2(connection)
        _apply_v3(connection)
        _apply_v4(connection)
        _apply_v5(connection)
        _apply_v6(connection)
        _apply_v7(connection)
        _apply_v8(connection)
        if row is None:
            connection.execute(
                "INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
            )
        elif found < SCHEMA_VERSION:
            connection.execute("UPDATE schema_version SET version = ?", (SCHEMA_VERSION,))
    except BaseException:
        connection.rollback()
        raise
    connection.commit()


def _statements(script: str) -> list[str]:
    """Split a DDL script into statements, so it can run inside a transaction.

    `executescript` would be shorter and commits before it starts, which discards the write
    lock the migration is holding. Every statement in these scripts is a single
    `CREATE ... ;` with no embedded semicolons -- no triggers, no string literals containing
    one -- so splitting on `;` is exact here rather than merely usually right.
    """
    return [part.strip() for part in script.split(";") if part.strip()]


def _apply_v2(connection: sqlite3.Connection) -> None:
    """Phase 4's additions: run metadata columns and the spec 8.5 trials counter."""
    _add_columns(connection, RUN_COLUMNS_V2)
    for statement in _statements(SCHEMA_V2):
        connection.execute(statement)


def _apply_v3(connection: sqlite3.Connection) -> None:
    """Phase 5's addition: what tier the run *asked* for, beside what it got."""
    _add_columns(connection, RUN_COLUMNS_V3)


def _apply_v4(connection: sqlite3.Connection) -> None:
    """Phase 7's additions: session/shadow columns, and the kill switch's armed state."""
    _add_columns(connection, RUN_COLUMNS_V4)
    for statement in _statements(SCHEMA_V4):
        connection.execute(statement)


def _apply_v5(connection: sqlite3.Connection) -> None:
    """Phase 9's addition: the Lab jobs table."""
    for statement in _statements(SCHEMA_V5):
        connection.execute(statement)


def _apply_v6(connection: sqlite3.Connection) -> None:
    """Phase 10's addition: a Lab job's cancel-requested stamp."""
    _add_columns(connection, LAB_JOB_COLUMNS_V6, table="lab_jobs")


def _apply_v7(connection: sqlite3.Connection) -> None:
    """The per-symbol exchange-configuration claims held by running sessions."""
    for statement in _statements(SCHEMA_V7):
        connection.execute(statement)


def _apply_v8(connection: sqlite3.Connection) -> None:
    """The data-refresh job table, so a top-up outlives the request that started it."""
    for statement in _statements(SCHEMA_V8):
        connection.execute(statement)


def _add_columns(
    connection: sqlite3.Connection,
    columns: tuple[tuple[str, str], ...],
    *,
    table: str = "runs",
) -> None:
    # `table` is a literal from this module, never caller input -- see the note on the
    # ALTER below about DDL not accepting parameter binding.
    existing = {
        str(row["name"]) for row in connection.execute(f"PRAGMA table_info({table})")
    }
    for name, declaration in columns:
        if name not in existing:
            # No parameter binding is possible in DDL, and no value here comes from
            # anywhere but the module-level tuples above -- a fixed list of literals, not
            # caller input.
            connection.execute(f"ALTER TABLE {table} ADD COLUMN {name} {declaration}")

"""The kill switch's armed state has to outlive the process that armed it (spec 7.6).

Spec 7's sixth kill-switch obligation is that the platform *"requires an explicit un-arm
action before any live session can start again"*. `core.risk.KillSwitch` keeps that state in
memory, and memory is precisely what the trip destroys: the events that arm the switch -- an
invariant failure (spec 3.10), a reconciliation mismatch (spec 6.7.3), a socket down over an
open position -- are the same events that end with somebody restarting the API server, and
often with the process dying on its own. A restart re-created the switch clear,
`require_clear()` returned, and a live session opened against the account whose state had
just been declared untrustworthy. It did so in exactly the words a switch that had never
fired would have used, which is why this is worse than having no mechanism at all.

`test_the_armed_state_survives_a_fresh_store_instance` is the test this file exists for.
Everything else pins the record an incident is reconstructed from afterwards: spec 7.5's
timestamp and trigger source, whether the account was left flat or cancel-only (spec 7.3),
which session was live when it happened, and -- because "explicit" means a person decided --
whose name is on the un-arm.
"""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pytest

from perplab.core.risk import KillSwitchArmed
from perplab.store import db
from perplab.store.killswitch import KillSwitchStore, KillSwitchTrip

ARMED_MS = 1_700_000_000_000
"""A fixed trip timestamp, well in the past, so a `cleared_ms` taken from the wall clock
cannot be confused with it."""

RUN_ID = 42


@pytest.fixture()
def store(tmp_path: Path):
    handle = KillSwitchStore(tmp_path)
    yield handle
    handle.close()


def trip_rows(handle: KillSwitchStore) -> list[sqlite3.Row]:
    """Every row in the log, newest last. The store reports only the open trip, so the
    append-only claim can only be checked against the table itself."""
    return list(
        handle._connection.execute("SELECT * FROM kill_switch_trips ORDER BY id ASC")
    )


def open_trip(handle: KillSwitchStore) -> KillSwitchTrip:
    """`state()` where the switch is known to be armed.

    Turns the `None` case into the assertion that actually failed, rather than into an
    `AttributeError` on the next line that says nothing about which property broke.
    """
    trip = handle.state()
    assert trip is not None, "the kill switch should be armed at this point"
    return trip


# ------------------------------------------------------------------------- migration


def make_v3_database(root: Path) -> None:
    """A schema v3 database, built the way Phase 5 left it: no `kill_switch_trips`.

    Written statement by statement rather than copied from a fixture file so that it tracks
    `db.RUN_COLUMNS_V2`/`V3` -- a hand-written v3 schema would drift from the real one and
    the test would then be migrating a database that never existed.
    """
    legacy = sqlite3.connect(root / db.DB_FILENAME)
    legacy.executescript(db.SCHEMA)
    legacy.executescript(db.SCHEMA_V2)
    for name, declaration in (*db.RUN_COLUMNS_V2, *db.RUN_COLUMNS_V3):
        legacy.execute(f"ALTER TABLE runs ADD COLUMN {name} {declaration}")
    legacy.execute("INSERT INTO schema_version (version) VALUES (3)")
    legacy.execute("INSERT INTO strategies (name, created_ms, updated_ms) VALUES ('S', 0, 0)")
    legacy.execute(
        """
        INSERT INTO strategy_versions
            (strategy_id, version_no, code, code_sha256, created_ms)
        VALUES (1, 1, 'x', 'abc', 0)
        """
    )
    legacy.execute(
        "INSERT INTO runs (strategy_id, version_id, mode, status, created_ms) "
        "VALUES (1, 1, 'BACKTEST', 'done', 0)"
    )
    legacy.commit()
    legacy.close()


def test_a_v3_database_upgrades_in_place_without_losing_rows(tmp_path: Path) -> None:
    """Phase 5 databases exist on disk and hold run history somebody paid compute for.

    The fixture writes one strategy, one version and one run, so after the migration the
    counts are 1, 1 and 1 -- and the version row reads 4 rather than the 3 it was written
    with. A migration that recreated the file instead of altering it would satisfy the
    kill-switch half of this and silently discard the other three rows.
    """
    make_v3_database(tmp_path)

    connection = db.connect(tmp_path)
    assert connection.execute("SELECT count(*) FROM runs").fetchone()[0] == 1
    assert connection.execute("SELECT count(*) FROM strategies").fetchone()[0] == 1
    assert connection.execute("SELECT count(*) FROM strategy_versions").fetchone()[0] == 1
    assert (
        connection.execute("SELECT version FROM schema_version").fetchone()[0]
        == db.SCHEMA_VERSION
    )
    assert (
        connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='kill_switch_trips'"
        ).fetchone()
        is not None
    )
    connection.close()


def test_the_v4_migration_is_idempotent_and_keeps_the_trips_already_recorded(
    tmp_path: Path,
) -> None:
    """A crash between `CREATE TABLE` and the version bump leaves the table present and the
    version row saying 3, and the next open must not fail on either count.

    That state is forced here by rolling the version back to 3 with one trip already
    recorded. Re-opening must leave exactly the 1 row that was armed -- a migration that
    dropped and recreated the table would be idempotent in shape and would have thrown away
    the armed state, which is the one thing this table exists to keep.
    """
    first = KillSwitchStore(tmp_path)
    first.arm(ts_ms=ARMED_MS, trigger="RECONCILIATION", detail="position size disagrees")
    first.close()

    connection = db.connect(tmp_path)
    connection.execute("UPDATE schema_version SET version = 3")
    connection.commit()
    connection.close()

    reopened = KillSwitchStore(tmp_path)
    assert len(trip_rows(reopened)) == 1
    state = reopened.state()
    assert state is not None
    assert state.trigger == "RECONCILIATION"
    reopened.close()

    # And again, from a database that is already at 4: the second open is the ordinary case
    # -- every worker subprocess does it -- and must be a no-op rather than an error.
    third = KillSwitchStore(tmp_path)
    assert len(trip_rows(third)) == 1
    third.close()


# ----------------------------------------------------------------------------- arming


def test_arming_records_the_trigger_detail_timestamp_run_id_and_whether_it_flattened(
    store: KillSwitchStore,
) -> None:
    """Spec 7.5 asks for the timestamp and the trigger source; spec 7.3 makes cancel-only
    the default, so whether the account was actually flattened is the other half.

    Everything asserted here is what this test passed in: ts 1_700_000_000_000, trigger
    INVARIANT, that detail string, run 42 and flattened True. It is read back through
    `state()` rather than off the returned object, because a record that only exists in the
    caller's hand is the memory-only switch this module replaced.
    """
    returned = store.arm(
        ts_ms=ARMED_MS,
        trigger="INVARIANT",
        detail="cash conservation failed by 0.01",
        run_id=RUN_ID,
        flattened=True,
    )
    assert returned.armed is True

    trip = store.state()
    assert trip is not None
    assert trip.id == returned.id
    assert trip.armed_ms == ARMED_MS
    assert trip.trigger == "INVARIANT"
    assert trip.detail == "cash conservation failed by 0.01"
    assert trip.run_id == RUN_ID
    assert trip.flattened is True
    assert trip.cleared_ms is None
    assert trip.cleared_by is None


def test_a_manual_trip_with_nothing_running_records_a_null_run_id(
    store: KillSwitchStore,
) -> None:
    """The red button is reachable from every tab (spec 7), including with no session live.

    `run_id` must come back as `None` and not as 0: the column is nullable precisely so the
    record can say "no session was running", and a 0 would name a run that either does not
    exist or, worse, belongs to somebody else's incident.
    """
    store.arm(ts_ms=ARMED_MS, trigger="MANUAL", detail="operator pressed the red button")
    trip = store.state()
    assert trip is not None
    assert trip.run_id is None
    assert trip.flattened is False  # spec 7.3: cancel-only is the default


def test_arming_without_a_trigger_source_is_refused(store: KillSwitchStore) -> None:
    """Spec 7.5 makes the trigger source part of the record, so a trip that cannot say what
    fired it is not a record of anything. Refusing loudly beats writing a row that makes the
    incident unreadable, and the error names the values that are acceptable."""
    with pytest.raises(ValueError, match="trigger source"):
        store.arm(ts_ms=ARMED_MS, trigger="")
    assert trip_rows(store) == []


# ---------------------------------------------------------------------- require_clear


def test_require_clear_raises_while_armed_and_passes_once_unarmed(
    store: KillSwitchStore,
) -> None:
    """Spec 7.6's guarantee, in one test: no live session while the switch is armed.

    The exception is `core.risk.KillSwitchArmed` -- the same one the in-memory switch raises
    -- because a session-start path that catches one and not the other is a path for which
    this whole module does nothing. The message has to carry the trigger and whether the
    account was left flat, since the operator deciding whether to un-arm reads that message
    before deciding whether to reconcile against the exchange first.
    """
    store.require_clear()  # clear to begin with

    store.arm(ts_ms=ARMED_MS, trigger="DISCONNECT", detail="socket down 45s, position open")
    with pytest.raises(KillSwitchArmed) as raised:
        store.require_clear()
    message = str(raised.value)
    assert "DISCONNECT" in message
    assert "socket down 45s, position open" in message
    assert str(ARMED_MS) in message
    # Cancel-only was the default here, so the message must not claim the account is flat.
    assert "cancel-only" in message

    store.unarm("anshul")
    store.require_clear()  # must not raise


# ------------------------------------------------------------------------ persistence


def test_the_armed_state_survives_a_fresh_store_instance(tmp_path: Path) -> None:
    """The reason this module exists, and the property a memory-only switch cannot have.

    The first store arms and closes, which is a process ending -- the ordinary way a trip
    ends, since whatever tripped the switch is often what killed the process. A second store
    opened on the same root is a restarted API server, and it must read back the same one
    trip, with the same trigger, and still refuse to let a session start.

    One row, not two: the second store re-reads the log rather than starting its own.
    """
    first = KillSwitchStore(tmp_path)
    first.arm(
        ts_ms=ARMED_MS,
        trigger="LIQUIDATION",
        detail="BTCUSDT liquidated",
        run_id=RUN_ID,
        flattened=True,
    )
    first.close()

    restarted = KillSwitchStore(tmp_path)
    try:
        trip = restarted.state()
        assert trip is not None
        assert trip.trigger == "LIQUIDATION"
        assert trip.detail == "BTCUSDT liquidated"
        assert trip.armed_ms == ARMED_MS
        assert trip.run_id == RUN_ID
        assert trip.flattened is True
        assert len(trip_rows(restarted)) == 1
        with pytest.raises(KillSwitchArmed):
            restarted.require_clear()
    finally:
        restarted.close()


def test_un_arming_survives_a_restart_too(tmp_path: Path) -> None:
    """The converse, and it is not free: a restart must not re-arm a switch a person cleared.

    A store that re-read the log without honouring `cleared_ms` would refuse every session
    forever after the first trip, and an operator who cannot start a session after clearing
    the switch learns to bypass the switch.
    """
    first = KillSwitchStore(tmp_path)
    first.arm(ts_ms=ARMED_MS, trigger="INVARIANT", detail="ledger disagreed with itself")
    first.unarm("anshul")
    first.close()

    restarted = KillSwitchStore(tmp_path)
    try:
        assert restarted.state() is None
        restarted.require_clear()  # must not raise
        # The trip itself is still on disk, with the name of whoever cleared it.
        rows = trip_rows(restarted)
        assert len(rows) == 1
        assert rows[0]["cleared_by"] == "anshul"
    finally:
        restarted.close()


# ------------------------------------------------------------------------------ unarm


def test_unarm_records_who_cleared_it_and_returns_the_trip_it_cleared(
    store: KillSwitchStore,
) -> None:
    """Spec 7.6 asks for an *explicit* action, and an audit row that cannot say who took it
    is not evidence that anybody did.

    `cleared_ms` is the wall clock rather than the trip's own timestamp: the trip is caused
    by an event and carries that event's time, while the un-arm is a person pressing a button
    now. The trip here is stamped 1_700_000_000_000, roughly November 2023, so a `cleared_ms`
    bracketed by two `time.time()` readings taken around the call proves the two timestamps
    come from different sources rather than one being copied from the other.
    """
    armed = store.arm(ts_ms=ARMED_MS, trigger="MANUAL", detail="operator", run_id=RUN_ID)

    before_ms = int(time.time() * 1000)
    cleared = store.unarm("anshul")
    after_ms = int(time.time() * 1000)

    assert cleared is not None
    assert cleared.id == armed.id
    assert cleared.trigger == "MANUAL"
    assert cleared.run_id == RUN_ID
    assert cleared.cleared_by == "anshul"
    assert cleared.cleared_ms is not None
    assert before_ms <= cleared.cleared_ms <= after_ms
    assert cleared.armed is False
    assert store.state() is None


def test_un_arming_a_switch_that_is_already_clear_is_a_no_op_returning_none(
    store: KillSwitchStore,
) -> None:
    """A second click on the un-arm button is not a condition worth raising over, and an
    exception there would push every call site into catching one it should not have to.

    `None` rather than a record, and the log stays empty -- 0 rows, because un-arming a clear
    switch has nothing to stamp and must not invent a trip to stamp.
    """
    assert store.unarm("anshul") is None
    assert trip_rows(store) == []


def test_un_arming_refuses_an_anonymous_actor(store: KillSwitchStore) -> None:
    """Whitespace is not a name. `unarm("  ")` would otherwise write a row that says the
    account was declared fit to trade by nobody, which is the same silence the in-memory
    switch produced -- and the trip stays armed instead, so the refusal is safe in the
    direction that matters."""
    store.arm(ts_ms=ARMED_MS, trigger="INVARIANT")
    with pytest.raises(ValueError, match="requires an actor"):
        store.unarm("   ")
    assert store.state() is not None
    with pytest.raises(KillSwitchArmed):
        store.require_clear()


def test_un_arming_stamps_the_trip_rather_than_deleting_it(store: KillSwitchStore) -> None:
    """Trips are appended, so after an incident the log can answer whether this has happened
    before and who cleared it last time.

    Two trips are armed here with one un-arm between them, so the log holds 2 rows: the first
    stamped `cleared_by = 'anshul'`, the second still open. A design that overwrote a single
    current-state row would hold 1, and the first incident -- the one with the history -- is
    the one that would be gone.
    """
    store.arm(ts_ms=ARMED_MS, trigger="INVARIANT", detail="first incident")
    store.unarm("anshul")
    store.arm(ts_ms=ARMED_MS + 60_000, trigger="DISCONNECT", detail="second incident")

    rows = trip_rows(store)
    assert len(rows) == 2
    assert rows[0]["trigger_source"] == "INVARIANT"
    assert rows[0]["cleared_by"] == "anshul"
    assert rows[1]["trigger_source"] == "DISCONNECT"
    assert rows[1]["cleared_ms"] is None
    open_trip = store.state()
    assert open_trip is not None
    assert open_trip.id == rows[1]["id"]


# ------------------------------------------------------------------- first trigger wins


def test_arming_twice_keeps_the_first_trip_and_not_the_symptom_that_followed(
    store: KillSwitchStore,
) -> None:
    """A cascade -- an invariant failure causing a liquidation causing a rejection storm --
    has to report the thing that started it, matching `core.risk.KillSwitch.trip`.

    Two `arm` calls, 1 row: the second is a no-op that returns the trip already in force, so
    its id equals the first's and the stored trigger is still INVARIANT with run 42 rather
    than the LIQUIDATION at run 99 that arrived second. A second row would make an operator
    reading the log reconstruct two incidents out of one.
    """
    first = store.arm(ts_ms=ARMED_MS, trigger="INVARIANT", detail="fees leaked", run_id=RUN_ID)
    second = store.arm(
        ts_ms=ARMED_MS + 5_000, trigger="LIQUIDATION", detail="BTCUSDT", run_id=99
    )

    assert second.id == first.id
    assert second.trigger == "INVARIANT"
    assert second.detail == "fees leaked"
    assert second.armed_ms == ARMED_MS
    assert second.run_id == RUN_ID
    assert len(trip_rows(store)) == 1


def test_a_later_arm_upgrades_cancel_only_to_flattened_but_never_back(
    store: KillSwitchStore,
) -> None:
    """The intended order is arm first, then act: the guarantee has to be on disk before the
    stop sequence starts, because the stop sequence is what tends to be interrupted.

    Whether the positions actually closed is known seconds later, so a second
    `arm(flattened=True)` lets the record catch up -- still 1 row, not a second trip. A third
    call with `flattened=False` must not undo it: the account is flat, and a record that
    said cancel-only would send the next operator hunting for a position that is not there.
    """
    store.arm(ts_ms=ARMED_MS, trigger="MANUAL", detail="red button", flattened=False)
    assert open_trip(store).flattened is False

    upgraded = store.arm(ts_ms=ARMED_MS + 3_000, trigger="MANUAL", flattened=True)
    assert upgraded.flattened is True
    assert open_trip(store).flattened is True

    store.arm(ts_ms=ARMED_MS + 4_000, trigger="MANUAL", flattened=False)
    settled = open_trip(store)
    assert settled.flattened is True
    assert len(trip_rows(store)) == 1
    # The upgrade must not have rewritten anything else about the trip.
    assert settled.detail == "red button"
    assert settled.armed_ms == ARMED_MS


def test_the_oldest_open_trip_is_the_one_reported_and_un_arming_clears_them_all(
    store: KillSwitchStore,
) -> None:
    """`arm` is a no-op while armed, so two open rows only happen if two processes tripped on
    the same incident before either could see the other's row.

    The rows are inserted directly here because the store will not produce that state on its
    own. Of the two, the one stamped 1_700_000_000_000 started the incident and the one
    stamped 1_700_000_005_000 is a symptom of it, so the earlier is what `state()` reports --
    the same first-trigger-wins rule as above, and the two must not disagree about which
    trigger explains an incident.

    Then `unarm` has to clear both. Leaving the second open would make the very next
    `require_clear()` fail with a trip the operator was never shown, which reads as an
    un-arm that did not work.
    """
    with store._connection:
        store._connection.execute(
            "INSERT INTO kill_switch_trips (armed_ms, trigger_source, detail) "
            "VALUES (?, 'LIQUIDATION', 'symptom')",
            (ARMED_MS + 5_000,),
        )
        store._connection.execute(
            "INSERT INTO kill_switch_trips (armed_ms, trigger_source, detail) "
            "VALUES (?, 'INVARIANT', 'cause')",
            (ARMED_MS,),
        )

    trip = store.state()
    assert trip is not None
    assert trip.trigger == "INVARIANT"

    cleared = store.unarm("anshul")
    assert cleared is not None
    assert cleared.trigger == "INVARIANT"
    assert store.state() is None
    store.require_clear()  # must not raise
    assert all(row["cleared_by"] == "anshul" for row in trip_rows(store))

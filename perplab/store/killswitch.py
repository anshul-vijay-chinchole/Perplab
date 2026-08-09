"""The kill switch's armed state, on disk (spec 7.6).

Spec 7's sixth kill-switch obligation is one sentence: it *"requires an explicit un-arm
action before any live session can start again"*. `core.risk.KillSwitch` implements that
inside one process, and inside one process is not where the guarantee has to hold.

**The armed state was memory, and memory is exactly what the trip destroys.** The events
that trip the switch -- an invariant failure, a reconciliation mismatch, a socket that
stayed down over an open position -- are the same events that end with somebody restarting
the API server, and often with the process dying on its own. A restart re-created the switch
in its default state, `require_clear()` passed, and a new live session started against the
account whose state had just been declared untrustworthy. The mechanism was not merely
absent after a restart: it reported "clear" with the same call and the same silence as a
switch that had genuinely never fired, which is worse than having no mechanism, because the
operator was told the account was safe to trade.

So the arming is a row in SQLite (schema v4, `store.db`) and the un-arm is a row edit that
names who did it. Two properties follow, and both are the point:

- **A process that dies cannot clear the switch.** Nothing clears it but `unarm`.
- **Somebody's name is on the un-arm.** "Explicit" in spec 7.6 means a person decided the
  account was fit to trade again, and an audit row that cannot say who decided is not a
  record of a decision.

**Trips are appended, not overwritten.** Un-arming stamps the row rather than deleting it,
so the history survives -- and after an incident the first questions are whether this has
happened before, and who cleared it last time.

This module deliberately does *not* stop processes, cancel orders or wipe key sessions.
Those are spec 7's other five obligations, they act on a live session, and they belong to
whatever owns the session. What is here is the part that has to outlive it.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from perplab.core.risk import KillSwitchArmed
from perplab.store import db

__all__ = [
    "KillSwitchArmed",
    "KillSwitchTrip",
    "KillSwitchStore",
]


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass(frozen=True, slots=True)
class KillSwitchTrip:
    """One recorded trip of the kill switch.

    Frozen, because this is a description of something that already happened. A caller that
    wants to change the armed state calls `arm` or `unarm` and gets a fresh record back,
    rather than mutating a copy and wondering later whether the database agreed.
    """

    id: int
    armed_ms: int
    trigger: str
    """Spec 7.5's *"trigger source"*: `INVARIANT`, `RECONCILIATION`, `DISCONNECT`,
    `LIQUIDATION`, a limit name, or `MANUAL` for the red button itself."""
    detail: str
    run_id: int | None
    """The session that was live when it tripped, if there was one. `None` for a manual
    trip from the top bar with nothing running."""
    flattened: bool
    """Whether the positions were closed out, as opposed to cancel-only (spec 7.3).

    The one thing here that the *next* operator needs before deciding to un-arm: a
    cancel-only trip leaves the account holding whatever it held, and the account is
    unattended from that moment until somebody looks at it."""
    cleared_ms: int | None = None
    cleared_by: str | None = None

    @property
    def armed(self) -> bool:
        return self.cleared_ms is None

    def to_json(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "armed_ms": self.armed_ms,
            "trigger": self.trigger,
            "detail": self.detail,
            "run_id": self.run_id,
            "flattened": self.flattened,
            "cleared_ms": self.cleared_ms,
            "cleared_by": self.cleared_by,
            "armed": self.armed,
        }


def _row_to_trip(row: Any) -> KillSwitchTrip:
    return KillSwitchTrip(
        id=int(row["id"]),
        armed_ms=int(row["armed_ms"]),
        trigger=str(row["trigger_source"]),
        detail=str(row["detail"] or ""),
        run_id=row["run_id"],
        flattened=bool(row["flattened"]),
        cleared_ms=row["cleared_ms"],
        cleared_by=row["cleared_by"],
    )


_OPEN_TRIP = """
SELECT * FROM kill_switch_trips
WHERE cleared_ms IS NULL
ORDER BY armed_ms ASC, id ASC
LIMIT 1
"""
"""The oldest *open* trip, not the newest.

Normally there is at most one, because `arm` is a no-op while armed. If two processes
managed to arm concurrently, the earlier one is the trip that stopped things and the later
is a symptom of it -- the same first-trigger-wins rule `core.risk.KillSwitch.trip` applies
to a cascade, and the two must not disagree about which trigger explains an incident.
"""


class KillSwitchStore:
    """The armed state of the kill switch, persisted for the machine rather than the process.

    One instance per process. The connection is the ordinary `store.db` one, so opening this
    migrates the database like every other store does.
    """

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)
        self._connection = db.connect(self.root)

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> KillSwitchStore:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------------- reading

    def state(self) -> KillSwitchTrip | None:
        """The open trip, or `None` if the switch is clear.

        `None` rather than a record with `armed=False`, so that a caller cannot write
        `if store.state():` against an object that is always truthy -- which is how a
        Boolean-shaped question becomes a bug that only shows up on the day it matters.
        """
        row = self._connection.execute(_OPEN_TRIP).fetchone()
        return None if row is None else _row_to_trip(row)

    def require_clear(self) -> None:
        """Spec 7.6: refuse to let a live session start while the switch is armed.

        Called before a session opens a socket or a key session, and it raises the same
        `KillSwitchArmed` the in-memory switch raises. One condition, one exception: a
        caller that catches the memory one and misses the disk one would be a caller for
        whom this whole module does nothing.
        """
        trip = self.state()
        if trip is None:
            return
        raise KillSwitchArmed(
            f"the kill switch was armed at {trip.armed_ms} ({trip.trigger}: {trip.detail}) "
            f"and has not been un-armed. The account was left "
            f"{'flat' if trip.flattened else 'holding whatever it held -- cancel-only'}. "
            "Check the account against the exchange, then un-arm it explicitly "
            "(KillSwitchStore.unarm) before starting a session."
        )

    # ------------------------------------------------------------------------- writing

    def arm(
        self,
        *,
        ts_ms: int,
        trigger: str,
        detail: str = "",
        run_id: int | None = None,
        flattened: bool = False,
    ) -> KillSwitchTrip:
        """Record a trip, and return the trip that is now in force.

        **First trigger wins**, exactly as `core.risk.KillSwitch.trip` decides it: arming an
        armed switch does not overwrite the trigger, because a cascade -- an invariant
        failure causing a liquidation causing a rejection storm -- has to report the thing
        that started it rather than the last symptom to arrive.

        The one field a later call may still change is `flattened`, and only from false to
        true. The intended order is *arm first, then act*: the guarantee has to be on disk
        before the stop sequence starts, since the stop sequence is what tends to be
        interrupted. Whether the positions actually got closed is known a few seconds later,
        and a second `arm(flattened=True)` is how the record catches up without inventing a
        second trip.

        `ts_ms` is passed in rather than read from the clock here: the trip's timestamp is
        the timestamp of the event that caused it, which in a replay is an event timestamp
        and not the wall clock (spec 6.2).
        """
        if not trigger:
            raise ValueError(
                "a kill-switch trip needs a trigger source (spec 7.5). Pass INVARIANT, "
                "RECONCILIATION, DISCONNECT, LIQUIDATION, a limit name, or MANUAL."
            )
        # `BEGIN IMMEDIATE` for the same reason `db._migrate` takes it: this is a read
        # followed by a write, and the API server and a session worker both hold their own
        # connection. Without the write lock up front, two processes tripping on the same
        # incident each see no open trip and each insert one.
        self._connection.execute("BEGIN IMMEDIATE")
        try:
            row = self._connection.execute(_OPEN_TRIP).fetchone()
            if row is not None:
                if flattened and not bool(row["flattened"]):
                    self._connection.execute(
                        "UPDATE kill_switch_trips SET flattened = 1 WHERE id = ?",
                        (int(row["id"]),),
                    )
                    row = self._connection.execute(
                        "SELECT * FROM kill_switch_trips WHERE id = ?", (int(row["id"]),)
                    ).fetchone()
                trip = _row_to_trip(row)
            else:
                cursor = self._connection.execute(
                    """
                    INSERT INTO kill_switch_trips
                        (armed_ms, trigger_source, detail, run_id, flattened)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (int(ts_ms), trigger, detail, run_id, 1 if flattened else 0),
                )
                trip = KillSwitchTrip(
                    id=int(cursor.lastrowid or 0),
                    armed_ms=int(ts_ms),
                    trigger=trigger,
                    detail=detail,
                    run_id=run_id,
                    flattened=flattened,
                )
        except BaseException:
            self._connection.rollback()
            raise
        self._connection.commit()
        return trip

    def unarm(self, actor: str) -> KillSwitchTrip | None:
        """Clear the switch on somebody's authority. Returns the trip that was cleared.

        `None` means it was already clear, which is not an error: a second click on the
        un-arm button is not a condition worth raising over, and an exception there would
        push callers into catching one they should not.

        An empty `actor` **is** refused. Spec 7.6 asks for an explicit action, and a record
        that cannot say who took it is not evidence that anybody did -- it is the same
        silence the in-memory switch already produced.

        Every open trip is cleared, not only the one `state()` reports. "Un-armed" has to
        mean the switch is clear; leaving a second open row behind would make the next
        `require_clear()` fail with a trip the operator was never shown.

        The clearing timestamp is the wall clock rather than a parameter, because unlike a
        trip -- which is caused by an event and carries that event's time -- this is a human
        pressing a button now.
        """
        who = actor.strip()
        if not who:
            raise ValueError(
                "un-arming the kill switch requires an actor: the operator or service "
                "taking responsibility for the account being fit to trade (spec 7.6). "
                "Pass a username."
            )
        trip = self.state()
        if trip is None:
            return None
        cleared_ms = _now_ms()
        with self._connection:
            self._connection.execute(
                "UPDATE kill_switch_trips SET cleared_ms = ?, cleared_by = ? "
                "WHERE cleared_ms IS NULL",
                (cleared_ms, who),
            )
        row = self._connection.execute(
            "SELECT * FROM kill_switch_trips WHERE id = ?", (trip.id,)
        ).fetchone()
        return _row_to_trip(row)

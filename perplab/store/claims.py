"""Which running session owns which symbol on which account.

## One strategy per symbol, per account

**A symbol on an account belongs to one session at a time.** A second session asking for a
symbol another running session already holds is refused, whatever it asked for -- the same
configuration included.

The reason is the position itself, not the settings around it. Binance holds **one position
per `(symbol, positionSide)` for the whole account**: there is no strategy field on an order,
no per-strategy position, and nothing in the API that could keep two strategies' BTCUSDT
longs apart. Two sessions trading one symbol have their fills merged into that single position
-- one quantity, one blended entry price, one margin allocation, one liquidation price --
while each session's own ledger goes on tracking the fills it sent as though they were a
position of their own. Both then report an entry price, a PnL and a liquidation price the
account does not have, and the divergence grows with every fill the *other* session sends.

That is not something the platform can model its way out of, which is why it is a refusal
rather than a feature. A per-strategy liquidation price on a merged position is a number with
no referent at the venue: the exchange will liquidate the merged position at the merged price,
and it will do so to both strategies at once.

**Side is not the escape hatch it looks like.** In hedge mode the venue does keep `LONG` and
`SHORT` apart, so two sessions confined to opposite legs genuinely would not merge. But
nothing in a session declares a side -- `StartSessionRequest` has symbols, leverage, margin
mode and position mode, and a strategy is free to buy or sell at any tick -- so "these two
sessions will not overlap" is not a fact the platform can check at start time, and it is not
one worth believing on a promise. Symbol-level ownership is what is actually enforceable, and
it is the stricter of the two.

## The settings, which were the original reason for this table

Leverage on Binance USDⓈ-M is **account state scoped to a symbol**. `POST /fapi/v1/leverage`
takes `symbol` and `leverage` and nothing else: no strategy, no sub-account, no
`positionSide`. So two strategies trading BTCUSDT at the same time share one leverage, and so
do the two legs of a hedge.

Discovering that looks like this. Session A starts on BTCUSDT at 5x; its preflight applies 5x
and its `ExchangeTransport` verifies the echo once, at construction. Session B starts on
BTCUSDT at 20x; its preflight applies 20x. Nothing tells A. A keeps sizing positions from
`Account.leverage("BTCUSDT") == 5`, keeps solving `P_liq` against a 500 USDT initial margin
the exchange has replaced with 125, and keeps drawing a liquidation price on the Live Monitor
that the real one arrives *before*. Both sessions report plausible numbers; one of them is
describing an account that does not exist.

Ownership subsumes that case -- a symbol with one owner cannot be reconfigured underneath
anybody -- but the settings are still recorded and still named in the refusal, because "run
#41 has BTCUSDT at 5x isolated" tells an operator what to do next and "BTCUSDT is taken" does
not.

## What is *not* claimed

Only what the exchange holds per symbol: leverage, margin mode and position mode. Risk limits
are deliberately absent. Spec 7's limits are per run by design and the operator has said they
stay that way -- two strategies each get their own `max_position_notional`, their own daily
loss budget, their own drawdown ceiling. That is a real decision with a real consequence
worth stating: **the account-level total is then unbounded by any single limit**, because
nothing sums two sessions' exposure. `claimed_notional_note` is where that is surfaced rather
than left implicit.

## Staleness

A claim is released when its session stops. A session that is killed with `TerminateProcess`
does not get to release anything, so a claim is also *pruned* when the run it belongs to is no
longer running -- checked against `RunStore` on every read rather than trusted from the row.
A stale claim that blocked every future session would be the worst kind of safety mechanism:
one an operator learns to delete.
"""

from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

from perplab.store.db import connect

__all__ = [
    "SymbolClaim",
    "SymbolConflict",
    "SymbolClaims",
]


class SymbolConflict(RuntimeError):
    """A session asked for a symbol another running session already holds.

    Named for the symbol rather than for the leverage because that is now the whole of the
    test: an identical configuration collides just as hard as a different one, since what
    merges at the exchange is the *position*, and the position merges regardless of what
    either session set the leverage to.

    Carries the conflicting claims so the caller can render what is in force rather than only
    the fact of the collision -- an operator told "BTCUSDT is busy" has to go looking, and an
    operator told "run #41 has BTCUSDT at 5x isolated" does not.
    """

    def __init__(self, message: str, conflicts: Sequence[SymbolClaim]) -> None:
        super().__init__(message)
        self.conflicts = tuple(conflicts)


@dataclass(frozen=True, slots=True)
class SymbolClaim:
    """One running session's hold on one symbol's exchange configuration."""

    run_id: int
    symbol: str
    leverage: int
    margin_mode: str
    hedge_mode: bool
    endpoint: str
    claimed_ms: int
    released_ms: int | None = None

    @property
    def open(self) -> bool:
        return self.released_ms is None

    def to_json(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "symbol": self.symbol,
            "leverage": self.leverage,
            "margin_mode": self.margin_mode,
            "hedge_mode": self.hedge_mode,
            "endpoint": self.endpoint,
            "claimed_ms": self.claimed_ms,
            "released_ms": self.released_ms,
        }


def _row(row: sqlite3.Row) -> SymbolClaim:
    return SymbolClaim(
        run_id=int(row["run_id"]),
        symbol=str(row["symbol"]),
        leverage=int(row["leverage"]),
        margin_mode=str(row["margin_mode"]),
        hedge_mode=bool(row["hedge_mode"]),
        endpoint=str(row["endpoint"]),
        claimed_ms=int(row["claimed_ms"]),
        released_ms=None if row["released_ms"] is None else int(row["released_ms"]),
    )


class SymbolClaims:
    """The claims table, as a context manager over its own connection.

    Opened per use rather than held as a singleton, unlike `RunStore`: this holds no process
    handles and no cache, so there is nothing a long-lived instance would preserve, and a
    short-lived connection is one fewer thing to reason about when the API server restarts
    under running sessions. `KillSwitchStore` is used the same way for the same reason.

    Guarded by a lock even so. `connect` passes `check_same_thread=False`, FastAPI runs sync
    routes in a thread pool, and `claim` is a read-then-write across two statements -- so
    without one, two simultaneous session starts on the same symbol could both read "nothing
    conflicts" before either wrote. `BEGIN IMMEDIATE` covers the cross-*process* race; the
    lock covers the in-process one, which is the one two browser tabs can cause.
    """

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)
        self._connection = connect(self.root)
        self._lock = threading.RLock()

    def __enter__(self) -> SymbolClaims:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        self._connection.close()

    # ------------------------------------------------------------------------- reading

    def open_claims(
        self, endpoint: str, *, active_run_ids: Iterable[int] | None = None
    ) -> tuple[SymbolClaim, ...]:
        """Every unreleased claim on this endpoint, stale ones pruned.

        `active_run_ids` is the set of runs that are actually still running, supplied by the
        caller because this module has no business opening `RunStore` -- and because the
        caller is the one that already knows. A claim whose run is not in that set is
        released on the spot: the session was terminated without getting to clean up, and a
        claim that outlives its session blocks every future session on that symbol for good.

        Passing `None` skips the pruning and returns the rows as stored. That is for a caller
        that genuinely wants the raw table -- a diagnostic -- and not for the start path,
        where "I could not check" must not read as "nothing is stale".
        """
        with self._lock, self._connection:
            return self._open_locked(endpoint, active_run_ids)

    def _open_locked(
        self, endpoint: str, active_run_ids: Iterable[int] | None
    ) -> tuple[SymbolClaim, ...]:
        """`open_claims`, assuming the caller already holds the transaction.

        Split out so `claim` can do its read *and* its write inside one `BEGIN IMMEDIATE`.
        Calling the public method there would open a nested transaction whose exit committed
        halfway through the check, which is the race the write lock exists to close.
        """
        rows = self._connection.execute(
            "SELECT * FROM symbol_claims WHERE endpoint = ? AND released_ms IS NULL "
            "ORDER BY symbol, run_id",
            (endpoint,),
        ).fetchall()
        claims = [_row(row) for row in rows]
        if active_run_ids is None:
            return tuple(claims)
        live = set(active_run_ids)
        stale = [c for c in claims if c.run_id not in live]
        if stale:
            self._connection.executemany(
                "UPDATE symbol_claims SET released_ms = claimed_ms "
                "WHERE run_id = ? AND symbol = ? AND released_ms IS NULL",
                [(c.run_id, c.symbol) for c in stale],
            )
        return tuple(c for c in claims if c.run_id in live)

    def for_symbol(
        self, endpoint: str, symbol: str, *, active_run_ids: Iterable[int] | None = None
    ) -> tuple[SymbolClaim, ...]:
        """Open claims on one symbol. What the Start Session form shows before you submit."""
        return tuple(
            c
            for c in self.open_claims(endpoint, active_run_ids=active_run_ids)
            if c.symbol == symbol
        )

    # ------------------------------------------------------------------------- writing

    def claim(
        self,
        *,
        run_id: int,
        symbols: Sequence[str],
        leverage: int,
        margin_mode: str,
        hedge_mode: bool,
        endpoint: str,
        now_ms: int,
        active_run_ids: Iterable[int] | None = None,
    ) -> tuple[SymbolClaim, ...]:
        """Take this run's exclusive hold on every symbol, or refuse the whole set.

        **A symbol has one owner.** Any overlap with another running session on this endpoint
        is refused -- an identical leverage, margin mode and position mode included. See the
        module docstring: what collides is the exchange position, which is keyed by symbol and
        side for the whole account and cannot be split per strategy, so two sessions agreeing
        about the leverage still end up sharing one entry price and one liquidation price
        while each reports its own.

        **All or nothing.** A partial claim would leave a session holding two of its three
        symbols with no way to trade the third, and the cleanup for that is exactly the
        cleanup a refusal already does. The conflicting symbols are all reported, not just
        the first, so an operator changing the configuration only has to be told once.

        A run re-claiming its own symbols is idempotent, which matters because the API takes
        the claim at session start and the worker re-takes it after it has read its own spec:
        a check only in the caller is a check another caller can skip.

        **`BEGIN IMMEDIATE`, for the reason `KillSwitchStore.arm` takes it.** This is a read
        followed by a write, and the readers are in different *connections* -- the API opens
        a `SymbolClaims` per request and each session worker opens its own -- so the
        instance lock protects nothing between them. Without the database's write lock held
        across both halves, two session starts landing together each see "nothing conflicts"
        and each insert, and the platform ends up holding two claims that disagree about the
        one setting the exchange has. That is precisely the state this table exists to make
        impossible, arrived at through the mechanism meant to prevent it.

        The instance lock is still taken, and still earns its place: `sqlite3.Connection` is
        not safe to use from two threads at once, and FastAPI serves sync routes from a
        thread pool.
        """
        wanted = [s for s in dict.fromkeys(symbols)]
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                held = self._open_locked(endpoint, active_run_ids)
                # Overlap alone, with no test on the configuration. A matching leverage used
                # to be grounds to let both run, and that was the defect: the exchange merges
                # the two sessions' fills into one position whether or not they agree about
                # the settings around it.
                conflicts = [
                    c for c in held if c.run_id != run_id and c.symbol in wanted
                ]
                if conflicts:
                    raise SymbolConflict(
                        _conflict_message(
                            conflicts, leverage, margin_mode, hedge_mode, endpoint
                        ),
                        conflicts,
                    )
                self._connection.executemany(
                    "INSERT INTO symbol_claims "
                    "(run_id, symbol, leverage, margin_mode, hedge_mode, endpoint, "
                    " claimed_ms, released_ms) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, NULL) "
                    "ON CONFLICT(run_id, symbol) DO UPDATE SET "
                    " leverage = excluded.leverage, margin_mode = excluded.margin_mode, "
                    " hedge_mode = excluded.hedge_mode, claimed_ms = excluded.claimed_ms, "
                    " released_ms = NULL",
                    [
                        (
                            run_id,
                            symbol,
                            int(leverage),
                            margin_mode,
                            1 if hedge_mode else 0,
                            endpoint,
                            now_ms,
                        )
                        for symbol in wanted
                    ],
                )
            except BaseException:
                self._connection.rollback()
                raise
            self._connection.commit()
        return tuple(
            SymbolClaim(
                run_id=run_id,
                symbol=symbol,
                leverage=int(leverage),
                margin_mode=margin_mode,
                hedge_mode=bool(hedge_mode),
                endpoint=endpoint,
                claimed_ms=now_ms,
            )
            for symbol in wanted
        )

    def release(self, run_id: int, now_ms: int) -> int:
        """Give up every symbol this run holds. Returns how many were released.

        Idempotent, and called from the session's own shutdown path *and* from the API's
        stop path. A session that dies without releasing is covered by `open_claims`'
        pruning instead; this is the orderly route, and having both is the difference
        between a symbol freed in milliseconds and one freed when somebody next looks.
        """
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "UPDATE symbol_claims SET released_ms = ? "
                "WHERE run_id = ? AND released_ms IS NULL",
                (now_ms, run_id),
            )
            return int(cursor.rowcount or 0)


def _conflict_message(
    conflicts: Sequence[SymbolClaim],
    leverage: int,
    margin_mode: str,
    hedge_mode: bool,
    endpoint: str,
) -> str:
    """Say who holds the symbol, at what configuration, and why it cannot be shared.

    The message is the entire interface of this refusal -- it is what the API returns as a
    409 and what the Start Session dialog puts in front of the operator -- so it has to carry
    the reason and not only the verdict. An operator who is only told "no" concludes the
    platform is being difficult and looks for the flag that turns it off.
    """
    lines = []
    for claim in conflicts:
        differences = []
        if claim.leverage != int(leverage):
            differences.append(f"leverage {claim.leverage}x (you asked for {leverage}x)")
        if claim.margin_mode != margin_mode:
            differences.append(
                f"margin mode {claim.margin_mode} (you asked for {margin_mode})"
            )
        if claim.hedge_mode != bool(hedge_mode):
            differences.append(
                f"{'hedge' if claim.hedge_mode else 'one-way'} position mode "
                f"(you asked for {'hedge' if hedge_mode else 'one-way'})"
            )
        if differences:
            lines.append(
                f"{claim.symbol}: run #{claim.run_id} is running it at "
                + ", ".join(differences)
            )
        else:
            # Same configuration, and still refused. Saying so explicitly matters: an
            # operator who matched the running session's settings on purpose needs to be
            # told that was not the problem, or the next thing they try is matching them
            # harder.
            lines.append(
                f"{claim.symbol}: run #{claim.run_id} is already trading it at "
                f"leverage {claim.leverage}x, {claim.margin_mode}, "
                f"{'hedge' if claim.hedge_mode else 'one-way'} mode -- the same "
                "configuration you asked for, which does not make it shareable"
            )
    return (
        "cannot start this session -- another running session at "
        f"{endpoint} already holds the same symbol(s):\n  "
        + "\n  ".join(lines)
        + "\n\nBinance holds **one position per symbol and side for the whole account**. "
        "There is no strategy field on an order and no per-strategy position, so two "
        "sessions trading one symbol have their fills merged into that single position -- "
        "one quantity, one blended entry price, one margin allocation, one liquidation "
        "price -- while each session's ledger goes on tracking only the fills it sent. Both "
        "would then report an entry price, a PnL and a liquidation price the account does "
        "not have, and the gap would widen with every fill the other session made. The "
        "exchange would liquidate the merged position at the merged price, to both "
        "strategies at once.\n\n"
        "Matching the configuration does not help, because it is the position that merges "
        "and not the settings. Leverage, margin mode and position mode are **account "
        "settings scoped to a symbol** in the same way -- POST /fapi/v1/leverage takes a "
        "symbol and applies account-wide, with no per-strategy scope -- so they cannot be "
        "held separately either.\n\n"
        "Run one strategy per symbol on an account. Choose a different symbol, stop the "
        "running session first, or use a separate Binance account for the second strategy."
    )

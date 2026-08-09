"""Symbol ownership, and running more than one session at a time.

## What this file is about

**A symbol on an account belongs to one running session.** Binance holds one position per
`(symbol, positionSide)` for the whole account -- there is no strategy field on an order and
no per-strategy position -- so two sessions trading one symbol have their fills merged into a
single position with one entry price, one margin allocation and one liquidation price, while
each session's ledger goes on tracking only the fills it sent. Both then report numbers the
account does not have, and the divergence grows with every fill the other one makes.

That is an exchange limitation and not a modelling gap: a per-strategy liquidation price on a
merged position has no referent, because the venue will liquidate the merged position at the
merged price and take both strategies down together. So the platform refuses the second
session rather than inventing an attribution the account cannot honour.

**An identical configuration is refused too**, which is the part worth stating twice: what
merges is the position, not the settings around it, so agreeing about the leverage changes
nothing. `test_a_second_run_is_refused_even_at_the_same_configuration` is that case.

Side is not an escape hatch. In hedge mode the venue does keep `LONG` and `SHORT` apart, so
two sessions confined to opposite legs genuinely would not merge -- but nothing in a session
declares a side, and a strategy may buy or sell at any tick, so disjointness is not checkable
at start time. Symbol-level ownership is what is enforceable, and it is the stricter rule.

## The settings, which were the original reason for this table

Leverage on Binance USDⓈ-M is also **account state scoped to a symbol**. `POST
/fapi/v1/leverage` takes `symbol` and `leverage` and nothing else -- no strategy, no
sub-account, no `positionSide`.

Discovering that is silent and one-directional. Session B's preflight reconfigures the symbol
under session A; A's `ExchangeTransport` preflight check ran once at construction and never
runs again; A keeps sizing positions and solving `P_liq` at a leverage the venue stopped
using, and the liquidation price on its Live Monitor sits *further* from the mark than the
real one. Both sessions report plausible numbers.

Ownership subsumes that -- a symbol with one owner cannot be reconfigured underneath anybody
-- but the settings are still recorded and still named in the refusal, because an operator
told what is in force knows what to do next.

`store.claims.SymbolClaims` is the enforcement, and these tests are what say it enforces
rather than merely exists.

## What is deliberately *not* enforced

Risk limits are per session and stay that way -- that is an explicit product decision, not an
oversight. `test_risk_limits_are_not_pooled_across_sessions` pins it, including the
consequence: two sessions each under a 5x cap are two accounts-worth of exposure on one real
account, and nothing sums them.
"""

from __future__ import annotations

import json
import sys
import threading
import warnings
from pathlib import Path
from typing import Any

import pytest

warnings.filterwarnings(
    "ignore", message=".*httpx.*starlette.testclient.*", category=DeprecationWarning
)

from fastapi.testclient import TestClient  # noqa: E402

from perplab.api.app import create_app  # noqa: E402
from perplab.store import db  # noqa: E402
from perplab.store.claims import SymbolClaims, SymbolConflict  # noqa: E402
from perplab.store.runs import RunStore  # noqa: E402

T = 1_700_000_000_000


# --------------------------------------------------------------------------- the store


@pytest.fixture()
def claims(tmp_path: Path) -> SymbolClaims:
    with SymbolClaims(tmp_path) as handle:
        yield handle


def test_a_second_run_is_refused_even_at_the_same_configuration(
    claims: SymbolClaims,
) -> None:
    """**The guard.** Matching the running session's settings does not make a symbol shareable.

    Two strategies at 5x isolated one-way on BTCUSDT are asking the account for the same
    configuration, and this used to be allowed on exactly that reasoning. It was wrong,
    because the configuration was never what collided: Binance holds one position per symbol
    and side for the whole account, so both strategies' fills land in the same position with
    one entry price and one liquidation price, while each ledger reports only its own half.
    Neither session's numbers describe the account, and nothing local can pull them apart
    afterwards -- the fills are indistinguishable at the venue by then.

    The refusal is the whole fix. There is no accounting change that recovers per-strategy
    attribution from a merged position, because the attribution does not exist to recover.
    """
    claims.claim(
        run_id=1, symbols=["BTCUSDT"], leverage=5, margin_mode="ISOLATED",
        hedge_mode=False, endpoint="testnet", now_ms=T, active_run_ids=[1, 2],
    )
    with pytest.raises(SymbolConflict) as excinfo:
        claims.claim(
            run_id=2, symbols=["BTCUSDT"], leverage=5, margin_mode="ISOLATED",
            hedge_mode=False, endpoint="testnet", now_ms=T, active_run_ids=[1, 2],
        )
    assert excinfo.value.conflicts[0].run_id == 1
    # Run 1 still holds it alone -- a refused claim writes nothing.
    held = claims.for_symbol("testnet", "BTCUSDT", active_run_ids=[1, 2])
    assert {c.run_id for c in held} == {1}


def test_the_same_configuration_refusal_says_so_rather_than_listing_no_differences(
    claims: SymbolClaims,
) -> None:
    """An operator who matched the settings deliberately must be told that was not the issue.

    The message is the entire interface of the refusal. Rendered from the old
    difference-listing code path this case produced `"BTCUSDT: run #1 is running it at "` --
    a sentence that stops mid-clause, because there were no differences to list. Somebody
    reading that concludes the check is broken and looks for the way around it.
    """
    claims.claim(
        run_id=1, symbols=["BTCUSDT"], leverage=5, margin_mode="ISOLATED",
        hedge_mode=False, endpoint="testnet", now_ms=T, active_run_ids=[1, 2],
    )
    with pytest.raises(SymbolConflict) as excinfo:
        claims.claim(
            run_id=2, symbols=["BTCUSDT"], leverage=5, margin_mode="ISOLATED",
            hedge_mode=False, endpoint="testnet", now_ms=T, active_run_ids=[1, 2],
        )
    message = str(excinfo.value)
    assert "run #1 is already trading it" in message
    assert "the same configuration you asked for" in message
    # And the reason, which is the position rather than the settings.
    assert "one position per symbol and side" in message
    assert "merged" in message
    # No dangling clause from the difference-listing branch.
    assert "is running it at \n" not in message
    assert not message.rstrip().endswith("running it at")


def test_the_refusal_tells_the_operator_what_to_do_instead(claims: SymbolClaims) -> None:
    """A refusal with no exit leaves somebody stuck, and stuck operators disable checks."""
    claims.claim(
        run_id=1, symbols=["BTCUSDT"], leverage=5, margin_mode="ISOLATED",
        hedge_mode=False, endpoint="testnet", now_ms=T, active_run_ids=[1, 2],
    )
    with pytest.raises(SymbolConflict) as excinfo:
        claims.claim(
            run_id=2, symbols=["BTCUSDT"], leverage=5, margin_mode="ISOLATED",
            hedge_mode=False, endpoint="testnet", now_ms=T, active_run_ids=[1, 2],
        )
    message = str(excinfo.value)
    assert "different symbol" in message
    assert "stop the running session" in message
    assert "separate Binance account" in message


def test_hedge_mode_does_not_make_a_symbol_shareable(claims: SymbolClaims) -> None:
    """Two hedge sessions on one symbol are refused, even though the venue splits the sides.

    Binance really does keep `LONG` and `SHORT` as separate positions, so two sessions
    confined to opposite legs would not merge -- the tempting exception. It is not taken,
    because **nothing in a session declares a side**: `StartSessionRequest` carries symbols,
    leverage, margin mode and position mode, and a strategy may buy or sell at any tick. The
    disjointness would be a promise rather than a checkable fact, and the failure when it
    broke would be a silently merged position, which is the thing being prevented.
    """
    claims.claim(
        run_id=1, symbols=["BTCUSDT"], leverage=5, margin_mode="ISOLATED",
        hedge_mode=True, endpoint="testnet", now_ms=T, active_run_ids=[1, 2],
    )
    with pytest.raises(SymbolConflict):
        claims.claim(
            run_id=2, symbols=["BTCUSDT"], leverage=5, margin_mode="ISOLATED",
            hedge_mode=True, endpoint="testnet", now_ms=T, active_run_ids=[1, 2],
        )


@pytest.mark.parametrize(
    "field, value",
    [
        ("leverage", 20),
        ("margin_mode", "CROSSED"),
        ("hedge_mode", True),
    ],
)
def test_a_different_configuration_on_a_running_symbol_is_refused(
    claims: SymbolClaims, field: str, value: Any
) -> None:
    """All three settings are account state per symbol, so all three collide."""
    claims.claim(
        run_id=1, symbols=["BTCUSDT"], leverage=5, margin_mode="ISOLATED",
        hedge_mode=False, endpoint="testnet", now_ms=T, active_run_ids=[1, 2],
    )
    kwargs: dict[str, Any] = {
        "leverage": 5, "margin_mode": "ISOLATED", "hedge_mode": False, **{field: value}
    }
    with pytest.raises(SymbolConflict) as excinfo:
        claims.claim(
            run_id=2, symbols=["BTCUSDT"], endpoint="testnet", now_ms=T,
            active_run_ids=[1, 2], **kwargs,
        )
    assert excinfo.value.conflicts[0].run_id == 1


def test_the_refusal_names_the_symbol_the_run_and_the_leverage_in_force(
    claims: SymbolClaims,
) -> None:
    """An operator told "BTCUSDT is busy" has to go looking. This one does not.

    The message is the whole interface of the refusal -- it is what appears in the API's 409
    and in the Start Session dialog's toast -- so its contents are the contract.
    """
    claims.claim(
        run_id=41, symbols=["BTCUSDT"], leverage=5, margin_mode="ISOLATED",
        hedge_mode=False, endpoint="testnet", now_ms=T, active_run_ids=[41, 42],
    )
    with pytest.raises(SymbolConflict) as excinfo:
        claims.claim(
            run_id=42, symbols=["BTCUSDT"], leverage=20, margin_mode="ISOLATED",
            hedge_mode=False, endpoint="testnet", now_ms=T, active_run_ids=[41, 42],
        )
    message = str(excinfo.value)
    assert "BTCUSDT" in message
    assert "run #41" in message
    assert "5x" in message and "20x" in message
    # And the *reason*, so the refusal reads as a constraint rather than as a policy.
    assert "account settings scoped to a symbol" in message


def test_only_the_overlapping_symbols_collide(claims: SymbolClaims) -> None:
    """Two strategies on different symbols are the ordinary concurrent case."""
    claims.claim(
        run_id=1, symbols=["BTCUSDT"], leverage=5, margin_mode="ISOLATED",
        hedge_mode=False, endpoint="testnet", now_ms=T, active_run_ids=[1, 2],
    )
    claims.claim(
        run_id=2, symbols=["ETHUSDT"], leverage=20, margin_mode="ISOLATED",
        hedge_mode=False, endpoint="testnet", now_ms=T, active_run_ids=[1, 2],
    )
    assert len(claims.open_claims("testnet", active_run_ids=[1, 2])) == 2


def test_a_partial_overlap_refuses_the_whole_set(claims: SymbolClaims) -> None:
    """All or nothing: a session holding two of its three symbols cannot trade the third.

    Cleaning that up is exactly what a refusal already does, and every conflicting symbol is
    reported so a reconfiguration only has to happen once.
    """
    claims.claim(
        run_id=1, symbols=["BTCUSDT", "SOLUSDT"], leverage=5, margin_mode="ISOLATED",
        hedge_mode=False, endpoint="testnet", now_ms=T, active_run_ids=[1, 2],
    )
    with pytest.raises(SymbolConflict) as excinfo:
        claims.claim(
            run_id=2, symbols=["ETHUSDT", "BTCUSDT", "SOLUSDT"], leverage=10,
            margin_mode="ISOLATED", hedge_mode=False, endpoint="testnet", now_ms=T,
            active_run_ids=[1, 2],
        )
    assert {c.symbol for c in excinfo.value.conflicts} == {"BTCUSDT", "SOLUSDT"}
    # Nothing was taken. ETHUSDT is still free.
    assert not claims.for_symbol("testnet", "ETHUSDT", active_run_ids=[1, 2])


def test_testnet_and_production_are_different_accounts(claims: SymbolClaims) -> None:
    """A testnet session must not block a production one. They are separate accounts."""
    claims.claim(
        run_id=1, symbols=["BTCUSDT"], leverage=5, margin_mode="ISOLATED",
        hedge_mode=False, endpoint="testnet", now_ms=T, active_run_ids=[1, 2],
    )
    claims.claim(
        run_id=2, symbols=["BTCUSDT"], leverage=20, margin_mode="ISOLATED",
        hedge_mode=False, endpoint="production", now_ms=T, active_run_ids=[1, 2],
    )
    assert len(claims.for_symbol("testnet", "BTCUSDT", active_run_ids=[1, 2])) == 1
    assert len(claims.for_symbol("production", "BTCUSDT", active_run_ids=[1, 2])) == 1


def test_releasing_frees_the_symbol(claims: SymbolClaims) -> None:
    claims.claim(
        run_id=1, symbols=["BTCUSDT"], leverage=5, margin_mode="ISOLATED",
        hedge_mode=False, endpoint="testnet", now_ms=T, active_run_ids=[1],
    )
    assert claims.release(1, T + 1) == 1
    assert not claims.for_symbol("testnet", "BTCUSDT", active_run_ids=[1])

    # And the symbol is now available at a different leverage.
    claims.claim(
        run_id=2, symbols=["BTCUSDT"], leverage=20, margin_mode="ISOLATED",
        hedge_mode=False, endpoint="testnet", now_ms=T + 2, active_run_ids=[1, 2],
    )


def test_a_claim_whose_run_has_died_is_pruned_rather_than_honoured(
    claims: SymbolClaims,
) -> None:
    """A session killed with `TerminateProcess` never reaches its own release.

    A stale claim that blocked every future session on the symbol would be the worst kind of
    safety mechanism: one an operator learns to delete. So the live run set is the authority,
    and a claim whose run is not in it is released on the spot.
    """
    claims.claim(
        run_id=1, symbols=["BTCUSDT"], leverage=5, margin_mode="ISOLATED",
        hedge_mode=False, endpoint="testnet", now_ms=T, active_run_ids=[1],
    )
    # Run 1 is gone. Nothing released its claim.
    assert claims.open_claims("testnet", active_run_ids=[]) == ()
    claims.claim(
        run_id=2, symbols=["BTCUSDT"], leverage=20, margin_mode="ISOLATED",
        hedge_mode=False, endpoint="testnet", now_ms=T + 1, active_run_ids=[2],
    )


def test_a_run_reclaiming_its_own_symbols_is_idempotent(claims: SymbolClaims) -> None:
    """The API claims at session start and the worker re-claims after reading its spec.

    A check only in the caller is a check another caller can skip, so both take it -- which
    means the second one has to be a no-op rather than a self-collision.
    """
    for _ in range(3):
        claims.claim(
            run_id=1, symbols=["BTCUSDT"], leverage=5, margin_mode="ISOLATED",
            hedge_mode=False, endpoint="testnet", now_ms=T, active_run_ids=[1],
        )
    assert len(claims.for_symbol("testnet", "BTCUSDT", active_run_ids=[1])) == 1


def test_concurrent_claims_on_one_symbol_cannot_both_win(tmp_path: Path) -> None:
    """Two connections, one symbol, two threads. They cannot end up disagreeing.

    **`claim` is a read-then-write, and the two readers are in different connections** -- the
    API opens a `SymbolClaims` per request, and each session worker opens its own. So the
    instance lock protects nothing between them, and without the database's `BEGIN IMMEDIATE`
    write lock held across both halves, both threads see "nothing conflicts" and both insert.
    The table then holds two claims that disagree about the one setting the exchange has,
    which is the exact state it exists to prevent -- reached through the mechanism meant to
    prevent it.

    Two stores are opened *before* the threads start, and the barrier has a timeout. Both are
    deliberate: constructing a store runs migrations, and a thread that failed there would
    never reach the barrier, leaving the other blocked forever -- a test that hangs instead
    of failing, which is worse than no test.
    """
    outcomes: list[str] = []
    outcomes_lock = threading.Lock()
    barrier = threading.Barrier(2, timeout=10)
    stores = [SymbolClaims(tmp_path), SymbolClaims(tmp_path)]

    def attempt(handle: SymbolClaims, run_id: int, leverage: int) -> None:
        barrier.wait()
        try:
            handle.claim(
                run_id=run_id, symbols=["BTCUSDT"], leverage=leverage,
                margin_mode="ISOLATED", hedge_mode=False, endpoint="testnet",
                now_ms=T, active_run_ids=[1, 2],
            )
            result = f"claimed:{run_id}"
        except SymbolConflict:
            result = f"refused:{run_id}"
        with outcomes_lock:
            outcomes.append(result)

    try:
        threads = [
            threading.Thread(target=attempt, args=(stores[0], 1, 5)),
            threading.Thread(target=attempt, args=(stores[1], 2, 20)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
            assert not thread.is_alive(), "a claim deadlocked rather than resolving"

        assert len(outcomes) == 2
        assert sum(1 for o in outcomes if o.startswith("claimed")) == 1
        assert sum(1 for o in outcomes if o.startswith("refused")) == 1

        # The surviving rows agree with each other. Which thread won is a genuine race and is
        # not asserted; that the table cannot hold two contradictory configurations is.
        held = stores[0].for_symbol("testnet", "BTCUSDT", active_run_ids=[1, 2])
        assert len({(c.leverage, c.margin_mode, c.hedge_mode) for c in held}) == 1
    finally:
        for store in stores:
            store.close()


# ----------------------------------------------------------------------------- the API


STRATEGY_SOURCE = """
from perplab.strategy import Strategy

class Idea(Strategy):
    symbols = ["BTCUSDT"]
    timeframe = "1m"
    def on_bar(self, ctx):
        pass
"""


def _add_strategy(root: Path) -> int:
    connection = db.connect(root)
    with connection:
        strategy_id = connection.execute(
            "INSERT INTO strategies (name, created_ms, updated_ms) VALUES ('Wobble', 0, 0)"
        ).lastrowid
        version_id = connection.execute(
            """
            INSERT INTO strategy_versions
                (strategy_id, version_no, code, code_sha256, created_ms, valid,
                 params_json, requires_json, class_name)
            VALUES (?, 1, ?, 'sha', 0, 1, '[]', ?, 'Idea')
            """,
            (
                strategy_id,
                STRATEGY_SOURCE,
                json.dumps({"symbols": ["BTCUSDT"], "timeframe": "1m", "history": 1}),
            ),
        ).lastrowid
        connection.execute(
            "UPDATE strategies SET head_version_id = ? WHERE id = ?",
            (version_id, strategy_id),
        )
    connection.close()
    return int(strategy_id)


@pytest.fixture()
def api(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """A live API over a temp root, with the session worker replaced by a recorder.

    Nothing spawns `perplab.live.worker` here: the claim is taken by the route *before* the
    launch, so the route's behaviour is fully observable without a process.
    """
    launched: list[int] = []

    def record(self: RunStore, run_id: int, *, secrets: Any = None) -> None:
        launched.append(run_id)

    monkeypatch.setattr(RunStore, "launch_session", record)
    with TestClient(create_app(tmp_path)) as client:
        yield client, tmp_path, launched


def _start(client: TestClient, strategy_id: int, **overrides: Any):
    body = {
        "strategy_id": strategy_id,
        "symbols": ["BTCUSDT"],
        "timeframe": "1m",
        "leverage": 5,
        "endpoint": "testnet",
        **overrides,
    }
    return client.post("/api/sessions", json=body)


def test_two_sessions_on_different_symbols_both_start(api: Any) -> None:
    """Concurrency works. This is the case the constraint must not break."""
    client, root, launched = api
    strategy_id = _add_strategy(root)

    first = _start(client, strategy_id, symbols=["BTCUSDT"])
    second = _start(client, strategy_id, symbols=["ETHUSDT"])

    assert first.status_code == 201, first.text
    assert second.status_code == 201, second.text
    assert len(launched) == 2
    assert first.json()["run"]["id"] != second.json()["run"]["id"]


def test_a_second_session_at_a_different_leverage_is_refused_with_the_number(
    api: Any,
) -> None:
    """The headline behaviour: 409, naming the leverage in force and the run holding it."""
    client, root, launched = api
    strategy_id = _add_strategy(root)

    first = _start(client, strategy_id, leverage=5)
    assert first.status_code == 201, first.text
    first_id = first.json()["run"]["id"]

    second = _start(client, strategy_id, leverage=20)
    assert second.status_code == 409, second.text
    detail = second.json()["detail"]
    assert "BTCUSDT" in detail
    assert f"run #{first_id}" in detail
    assert "5x" in detail
    # Refused *before* the worker was spawned, which is the point of claiming at the route.
    assert launched == [first_id]


def test_a_second_session_at_the_same_leverage_is_refused_too(api: Any) -> None:
    """409 at the route, and **the second worker is never spawned**.

    The end-to-end half of `test_a_second_run_is_refused_even_at_the_same_configuration`.
    What makes it worth asserting separately is `launched`: the refusal has to land before
    the process starts, because a worker that reaches its first order has already sent
    fills into the other session's position and there is no undoing that from here.
    """
    client, root, launched = api
    strategy_id = _add_strategy(root)

    first = _start(client, strategy_id, leverage=5)
    assert first.status_code == 201, first.text
    first_id = first.json()["run"]["id"]

    second = _start(client, strategy_id, leverage=5)
    assert second.status_code == 409, second.text
    detail = second.json()["detail"]
    assert f"run #{first_id}" in detail
    assert "one position per symbol and side" in detail
    assert launched == [first_id]


def test_hedge_and_one_way_on_the_same_symbol_collide(api: Any) -> None:
    """Position mode is one account-wide flag, so the two cannot run side by side."""
    client, root, _launched = api
    strategy_id = _add_strategy(root)

    assert _start(client, strategy_id, hedge_mode=False).status_code == 201
    clash = _start(client, strategy_id, hedge_mode=True)
    assert clash.status_code == 409
    assert "position mode" in clash.json()["detail"]


def test_the_form_can_see_what_is_in_force_before_it_submits(api: Any) -> None:
    """`GET /api/symbol-claims` is what the Start Session dialog renders inline."""
    client, root, _launched = api
    strategy_id = _add_strategy(root)
    started = _start(client, strategy_id, leverage=5)
    run_id = started.json()["run"]["id"]

    payload = client.get("/api/symbol-claims", params={"endpoint": "testnet"}).json()
    assert payload["endpoint"] == "testnet"
    assert payload["claims"] == [
        {
            "run_id": run_id,
            "symbol": "BTCUSDT",
            "leverage": 5,
            "margin_mode": "ISOLATED",
            "hedge_mode": False,
            "endpoint": "testnet",
            "claimed_ms": payload["claims"][0]["claimed_ms"],
            "released_ms": None,
        }
    ]

    # A different endpoint sees nothing, because it is a different account.
    other = client.get("/api/symbol-claims", params={"endpoint": "production"}).json()
    assert other["claims"] == []


def test_a_refused_session_does_not_hold_the_symbol_it_was_refused_for(api: Any) -> None:
    """The refused run must not leave a claim behind, or it blocks the symbol forever."""
    client, root, _launched = api
    strategy_id = _add_strategy(root)

    _start(client, strategy_id, leverage=5)
    assert _start(client, strategy_id, leverage=20).status_code == 409

    claims = client.get("/api/symbol-claims", params={"endpoint": "testnet"}).json()
    assert len(claims["claims"]) == 1
    assert claims["claims"][0]["leverage"] == 5


def test_risk_limits_are_not_pooled_across_sessions(api: Any) -> None:
    """**A deliberate decision, pinned with its consequence.**

    Each session gets its own `RiskLimits`, evaluated against its own simulated account.
    Nothing sums two sessions' exposure, so two sessions each under a 5x cap are two
    accounts-worth of exposure on one real account. That is what was asked for -- limits per
    strategy, not pooled -- and this test exists so the absence of pooling is a recorded
    choice rather than something to be discovered later.

    If pooling is ever wanted, this test is the one that has to change, and changing it is
    the moment to decide what "the account's leverage" means when two strategies disagree
    about who owns the balance.
    """
    client, root, _launched = api
    strategy_id = _add_strategy(root)

    first = _start(client, strategy_id, symbols=["BTCUSDT"], max_leverage="5")
    second = _start(client, strategy_id, symbols=["ETHUSDT"], max_leverage="5")
    assert first.status_code == 201 and second.status_code == 201

    store = RunStore(root)
    try:
        for response in (first, second):
            spec = store.read_json(response.json()["run"]["id"], "spec.json")
            assert spec["risk_limits"]["max_leverage"] == "5"
    finally:
        store.close()


def test_a_run_may_change_its_own_claim(claims: SymbolClaims) -> None:
    """A run never collides with itself, even at a different configuration.

    Written because a mutation dropping the `c.run_id != run_id` guard survived: the API and
    the worker re-claim with *identical* settings, so a self-collision is invisible in the
    ordinary path. It stops being invisible the moment the two disagree -- and then a run
    would be refused permission to trade by its own claim, with a message naming itself.
    """
    claims.claim(
        run_id=7, symbols=["BTCUSDT"], leverage=5, margin_mode="ISOLATED",
        hedge_mode=False, endpoint="testnet", now_ms=T, active_run_ids=[7],
    )
    claims.claim(
        run_id=7, symbols=["BTCUSDT"], leverage=20, margin_mode="ISOLATED",
        hedge_mode=True, endpoint="testnet", now_ms=T + 1, active_run_ids=[7],
    )
    held = claims.for_symbol("testnet", "BTCUSDT", active_run_ids=[7])
    assert len(held) == 1
    assert held[0].leverage == 20
    assert held[0].hedge_mode is True


def test_concurrent_run_creation_hands_out_distinct_ids(tmp_path: Path) -> None:
    """Eight sessions started at once get eight run ids and eight directories.

    The concurrency requirement itself: starting a second session while the first is running
    must work, and `RunStore.create` is on that path with a connection shared across
    FastAPI's thread pool.

    **What this test does *not* claim**, having been written to claim it and then checked:
    the placement of the `cursor.lastrowid` read relative to the transaction is *not* what
    keeps these ids distinct. `sqlite3.Cursor.lastrowid` is stamped on the cursor at
    `execute` time rather than read back from the connection, so interleaved INSERTs on one
    connection cannot hand two callers the same id however the read is ordered. A mutation
    moving that line outside the lock survives this test, and survives it *correctly* --
    it is an equivalent mutant, and `store/runs.py` says so at the line itself.

    What the lock does earn is `_reap`, which mutates two dicts on every `get` and every
    `list` while the Live Monitor polls once a second per session.
    """
    store = RunStore(tmp_path)
    connection = db.connect(tmp_path)
    with connection:
        strategy_id = connection.execute(
            "INSERT INTO strategies (name, created_ms, updated_ms) VALUES ('S', 0, 0)"
        ).lastrowid
        version_id = connection.execute(
            """
            INSERT INTO strategy_versions
                (strategy_id, version_no, code, code_sha256, created_ms, valid,
                 params_json, requires_json, class_name)
            VALUES (?, 1, 'x', 'sha', 0, 1, '[]', '{}', 'S')
            """,
            (strategy_id,),
        ).lastrowid
    connection.close()

    spec = {
        "symbols": ["BTCUSDT"], "timeframe": "1m", "start_ms": 0, "end_ms": 1,
        "seed": 0, "engine_version": 1, "params": {}, "fill_tier": "BAR_CLOSE",
    }
    threads_n = 8
    ids: list[int] = []
    ids_lock = threading.Lock()
    barrier = threading.Barrier(threads_n, timeout=15)

    def make() -> None:
        barrier.wait()
        run_id = store.create(
            strategy_id=int(strategy_id), version_id=int(version_id),
            spec=dict(spec), label="", mode="paper",
        )
        with ids_lock:
            ids.append(run_id)

    # **The interpreter is told to switch threads as often as it can.** The window between
    # the INSERT and the `lastrowid` read is a few bytecodes wide, so at the default 5 ms
    # switch interval a thread almost always finishes `create` before it is preempted -- and
    # the test would pass against the racy version by luck rather than by construction. This
    # is the standard way to make a narrow data race reproducible, and it is restored in the
    # `finally` because it is a process-wide setting.
    previous_interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    try:
        threads = [threading.Thread(target=make) for _ in range(threads_n)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
            assert not thread.is_alive()
        assert len(ids) == threads_n
        assert len(set(ids)) == threads_n, f"duplicate run ids handed out: {sorted(ids)}"
        # And each got its own directory, which is the consequence that actually bites: two
        # sessions handed the same id write their `spec.json` over each other.
        assert len({store.directory(i) for i in ids}) == threads_n
    finally:
        sys.setswitchinterval(previous_interval)
        store.close()

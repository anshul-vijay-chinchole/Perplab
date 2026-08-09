"""The paper-session, exchange-connection and kill-switch endpoints (spec 7, 10.3, 11).

Three of the properties pinned here failed silently before they were pinned, and silence is
what makes them worth a test rather than a review comment.

**A credential that comes back out is a credential on disk.** Spec 11 allows the alias, the
balance and a countdown, and nothing else: an API key echoed into a JSON body is a key in the
browser's memory, in its devtools history, and in whatever proxy log sits between. So the
tests below check the **raw response text** of every route in this router rather than the
parsed fields -- a leak that arrives under an unexpected key, or inside a nested error
message, passes a field-by-field check and fails a substring one. That includes the
*failure* responses: pydantic puts the offending value in a validation error and FastAPI
renders it, so the 400s are checked as carefully as the 200s. The same check is run over
every file under the data root, because spec 11 names "disk" and "database" by hand, and the
only channel a session's credentials may ever take is the stdin pipe
`RunStore.launch_session` writes -- which nothing uses today, because no session worker
signs anything.

**An armed kill switch that a restart clears is worse than no kill switch.** Spec 7.6 wants
an explicit un-arm before anything starts again, and the events that arm the switch are the
same events that end with somebody restarting the API. When the armed state lived in memory,
the restarted server answered `require_clear()` in exactly the words a switch that had never
fired would have used. `test_the_armed_state_survives_a_new_app_over_the_same_root` is that
regression, and it builds a second app over the first one's root to get it.

**A route registered after the SPA catch-all is a route that returns the app shell.**
`create_app` claims `/{path:path}` for the frontend, Starlette matches in registration order,
and the failure mode is a `200 text/html` where the client expected JSON.

Nothing here reaches the network. `POST /api/exchange/connect` makes a signed balance request,
so most tests replace `_validate` with a scripted answer; the ones that are about `_validate`
itself replace `SignedRestClient` instead, with `ScriptedSignedClient` -- a stand-in that
answers exactly what the real class answers, because the defect it pins was a call to a
method that does not exist and a mock would have answered that too. `POST /api/sessions`
spawns `perplab.live.worker`, so `RunStore.launch_session` is replaced with a recorder and
asserted on. No strategy code is executed either -- the worker that would compile it is never
started.
"""

from __future__ import annotations

import json
import re
import time
import warnings
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

# Starlette's TestClient warns that httpx support is deprecated, and `pyproject.toml` turns
# DeprecationWarning into an error to keep the accounting-seam test loud. Filtered to this
# one message rather than globally, so a real deprecation in our own code still fails.
warnings.filterwarnings(
    "ignore", message=".*httpx.*starlette.testclient.*", category=DeprecationWarning
)

from fastapi.testclient import TestClient  # noqa: E402

import httpx  # noqa: E402

from perplab.api import app as app_module  # noqa: E402
from perplab.api.app import create_app  # noqa: E402
from perplab.api.routers import sessions as sessions_router  # noqa: E402
from perplab.core.money import Money  # noqa: E402
from perplab.core.risk import RiskLimits  # noqa: E402
from perplab.exchange.keys import (  # noqa: E402
    KEY_SESSION_TTL_S,
    KeySession,
    KeySessionExpired,
)
from perplab.exchange.rest import BinanceRestError, TESTNET_BASE  # noqa: E402
from perplab.exchange.signed import SignedRestClient  # noqa: E402
from perplab.store import db  # noqa: E402
from perplab.store.runs import RunStore  # noqa: E402

API_KEY = "PerpLabTestKey-Qx7fZ2mW9v"
API_SECRET = "PerpLabTestSecret-Nb4kJ8rT6h"
"""Deliberately long, unique and unlike anything else this repo writes, so that searching a
response body or a database file for them cannot match by accident. Both clear the
`min_length=8` the connect schema imposes."""

ALIAS = "test-account"
BALANCE = Money(Decimal("1234.50"))
DRIFT_MS = 7

STATE_FIELDS = {"connected", "alias", "balance", "expires_in_s", "endpoint", "drift_ms"}
"""What `/api/exchange/status` is allowed to say. Asserted as an exact set: spec 11's rule is
about what is *absent*, so a test that only checks the fields it names would not notice a
seventh one appearing."""

SPEC_SEVEN_DEFAULTS = {
    "max_position_notional": None,
    "max_leverage": "5",
    "max_daily_loss_pct": "0.02",
    "max_drawdown_pct": "0.15",
    "max_open_orders": 10,
    "max_orders_per_minute": 30,
    "max_consecutive_losses": None,
    "halt_on_liquidation": True,
    "min_equity_pct": "0.50",
    "max_consecutive_rejections": 5,
    "max_disconnect_seconds": 30,
}
"""Spec 7's table of defaults, plus the disconnect trigger a session is the only mode that
can have.

Written out here rather than imported from `RiskLimits`, so that a change to the platform's
defaults fails this test rather than being ratified by it. `max_disconnect_seconds` is 30
because spec 7 says 30; `RiskLimits` defaults it to `None` because a backtest replays a file
and has no socket to lose, and `core/risk.py` says the builder that has one sets it here.
"""

STRATEGY_SOURCE = (
    "from perplab import Strategy\n\n\n"
    "class Idea(Strategy):\n"
    '    requires = {"symbols": ["BTCUSDT"], "timeframe": "1m", "history": 1}\n\n'
    "    def on_bar(self, ctx, bar):\n"
    "        pass\n"
)
"""Stored, never executed. The API process compiles no strategy code (spec 2.3, 11) and the
worker that would is never spawned here."""


# --------------------------------------------------------------------------- fixtures


@pytest.fixture()
def launched(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Record `RunStore.launch_session` calls instead of spawning `perplab.live.worker`.

    A copy of `secrets` is kept rather than the mapping itself, because the route clears the
    dict it passed as soon as the call returns -- the credential is meant to live for exactly
    the length of that call.
    """
    calls: list[dict[str, Any]] = []

    def record(self: RunStore, run_id: int, *, secrets: Any = None) -> None:
        calls.append({"run_id": run_id, "secrets": dict(secrets or {})})

    monkeypatch.setattr(RunStore, "launch_session", record)
    return calls


@pytest.fixture()
def client(tmp_path: Path) -> TestClient:
    with TestClient(create_app(tmp_path)) as handle:
        yield handle


def add_strategy(root: Path, *, valid: bool = True) -> int:
    """Insert a strategy and one saved version straight into the store.

    Written to SQLite rather than posted to `/api/strategies`, which would spawn the
    validator subprocess for every test in this file to obtain a row this file only ever
    reads.
    """
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
            VALUES (?, 1, ?, 'sha', 0, ?, '[]', ?, 'Idea')
            """,
            (
                strategy_id,
                STRATEGY_SOURCE,
                1 if valid else 0,
                json.dumps({"symbols": ["BTCUSDT"], "timeframe": "1m", "history": 1}),
            ),
        ).lastrowid
        connection.execute(
            "UPDATE strategies SET head_version_id = ? WHERE id = ?",
            (version_id, strategy_id),
        )
    connection.close()
    return int(strategy_id)


def patch_validation(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Answer the signed balance request from a script. Returns the bases it was called with.

    `_validate` is the one function in this router that opens a socket, so replacing it is
    what makes the whole file runnable offline.
    """
    bases: list[str] = []

    async def fake_validate(base: str, session: KeySession) -> tuple[str, Money, int]:
        bases.append(base)
        return ALIAS, BALANCE, DRIFT_MS

    monkeypatch.setattr(sessions_router, "_validate", fake_validate)
    return bases


SCRIPTED_DRIFT_MS = -3
"""The drift the stand-in client below reports, so the number asserted downstream is one
this file wrote."""


class ScriptedSignedClient:
    """A stand-in for `SignedRestClient` carrying exactly the surface the real class has.

    Deliberately not a mock. The defect this exists to pin is a call to a method that does
    not exist, and a mock answers every attribute name it is asked for -- so a mock would
    have kept the endpoint green while it was dead. This object answers `server_time_ms`,
    `drift_ms` and `account`, and raises `AttributeError` for anything else, exactly as
    `SignedRestClient` does.
    """

    def __init__(self, base: str, keys: KeySession, **_: Any) -> None:
        self.base = base
        self.keys = keys
        self.drift_ms: int | None = None
        self.calls: list[str] = []

    async def __aenter__(self) -> ScriptedSignedClient:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    async def server_time_ms(self) -> int:
        """As on the real client: the measurement lands on `drift_ms`, not in the return."""
        self.calls.append("server_time_ms")
        self.drift_ms = SCRIPTED_DRIFT_MS
        return 1_700_000_000_000

    async def account(self) -> dict[str, Any]:
        self.calls.append("account")
        return {"accountAlias": ALIAS, "totalWalletBalance": "1234.50"}


def patch_signed_client(monkeypatch: pytest.MonkeyPatch) -> list[ScriptedSignedClient]:
    """Build `ScriptedSignedClient`s instead of real ones, and return the ones built."""
    made: list[ScriptedSignedClient] = []

    def factory(base: str, keys: KeySession, **kwargs: Any) -> ScriptedSignedClient:
        made.append(ScriptedSignedClient(base, keys, **kwargs))
        return made[-1]

    monkeypatch.setattr(sessions_router, "SignedRestClient", factory)
    return made


def patch_validation_failure(monkeypatch: pytest.MonkeyPatch, exc: Exception) -> None:
    """Make the one signed call this router owns raise `exc`."""

    async def failing(base: str, session: KeySession) -> tuple[str, Money, int]:
        raise exc

    monkeypatch.setattr(sessions_router, "_validate", failing)


def read_spec(client: TestClient, run_id: int) -> dict[str, Any]:
    """The `spec.json` the API wrote for a run -- the file its worker reads."""
    path = client.app.state.runs.artefact(run_id, "spec.json")
    return json.loads(path.read_text(encoding="utf-8"))


def connect(client: TestClient, **overrides: Any) -> Any:
    body: dict[str, Any] = {
        "api_key": API_KEY,
        "api_secret": API_SECRET,
        "endpoint": "testnet",
    }
    body.update(overrides)
    return client.post("/api/exchange/connect", json=body)


def start_session(client: TestClient, strategy_id: int, **overrides: Any) -> Any:
    body: dict[str, Any] = {"strategy_id": strategy_id}
    body.update(overrides)
    return client.post("/api/sessions", json=body)


def files_holding(root: Path, needle: str) -> list[Path]:
    """Every file under `root` whose bytes contain `needle`.

    Bytes rather than decoded text, so the SQLite database and its write-ahead log are
    searched too: spec 11 names "database" alongside "disk", and a credential that reached a
    row would be invisible to a check that only opened the JSON artefacts.
    """
    probe = needle.encode("utf-8")
    return [
        path
        for path in sorted(root.rglob("*"))
        if path.is_file() and probe in path.read_bytes()
    ]


def write_events(client: TestClient, run_id: int, events: list[dict[str, Any]]) -> None:
    """Write an engine event log for `run_id`, one JSON object per line."""
    path = client.app.state.runs.artefact(run_id, "events.jsonl")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(event) + "\n" for event in events), encoding="utf-8"
    )


# ------------------------------------------------------------------ exchange session


def test_with_nothing_connected_the_status_is_disconnected_and_every_field_is_null(
    client: TestClient,
) -> None:
    """The Data & Feed panel renders from this payload before a key has ever been entered.

    Every field is present and null rather than absent: the frontend reads `alias` and
    `expires_in_s` unconditionally, and `undefined` and `null` render differently there.
    """
    response = client.get("/api/exchange/status")
    assert response.status_code == 200
    payload = response.json()
    assert set(payload) == STATE_FIELDS
    assert payload["connected"] is False
    assert [payload[field] for field in sorted(STATE_FIELDS - {"connected"})] == [
        None,
        None,
        None,
        None,
        None,
    ]


def test_connecting_to_an_unknown_endpoint_is_a_400_naming_the_valid_ones(
    client: TestClient,
) -> None:
    """Refused before a `KeySession` is built, so a typo in the endpoint never becomes a
    signed request to a host nobody chose. The message lists the alternatives because the
    operator typing the field is the person who has to pick one."""
    response = connect(client, endpoint="mainnet")
    assert response.status_code == 400
    detail = response.json()["detail"]
    assert "mainnet" in detail
    assert "testnet" in detail and "production" in detail
    assert client.app.state.keys is None


def test_a_successful_connect_shows_the_alias_the_balance_and_a_countdown(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Spec 11's permitted view, and nothing wider.

    The balance is the one the validating request observed, rendered by `money_to_str`: this
    test writes `Decimal("1234.50")` into the scripted answer, so `"1234.50"` is what the
    payload has to say. The countdown starts at the full idle TTL and is already ticking, so
    it must sit in `(0, KEY_SESSION_TTL_S]`.
    """
    bases = patch_validation(monkeypatch)
    payload = connect(client).json()

    assert bases == [TESTNET_BASE]
    assert set(payload) == STATE_FIELDS | {"connected_ms"}
    assert payload["connected"] is True
    assert payload["alias"] == ALIAS
    assert payload["balance"] == "1234.50"
    assert payload["endpoint"] == "testnet"
    assert payload["drift_ms"] == DRIFT_MS
    assert 0.0 < payload["expires_in_s"] <= KEY_SESSION_TTL_S


def test_a_connected_credential_is_never_echoed_by_any_response_in_this_router(
    client: TestClient,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    launched: list[dict[str, Any]],
) -> None:
    """Spec 11: keys are "never echoed back to the UI -- only the account alias and balance".

    Checked against the **raw response text**, not the parsed fields. A leak that arrives
    under an unexpected key, or inside the text of an error message, satisfies every
    field-by-field assertion anybody would think to write and fails this one.

    The alias and balance are asserted to be present in the same bodies, so that the test
    cannot pass because the responses turned out to be empty.

    The session is started *before* the key is connected, so that this test pins the echo
    rule alone and does not also depend on the credential hand-off below.
    """
    patch_validation(monkeypatch)
    strategy_id = add_strategy(tmp_path)

    run_id = start_session(client, strategy_id).json()["run"]["id"]
    connected = connect(client)
    bodies = {
        "connect": connected.text,
        "status": client.get("/api/exchange/status").text,
        "sessions": client.get("/api/sessions").text,
        "monitor": client.get(f"/api/sessions/{run_id}/monitor").text,
        "feed": client.get(f"/api/runs/{run_id}/feed").text,
        "kill": client.get("/api/kill").text,
    }
    for name, text in bodies.items():
        assert API_KEY not in text, f"the api key appeared in the {name} response"
        assert API_SECRET not in text, f"the api secret appeared in the {name} response"

    assert ALIAS in bodies["connect"] and ALIAS in bodies["status"]
    assert "1234.50" in bodies["connect"] and "1234.50" in bodies["status"]
    assert str(run_id) in bodies["sessions"]

    # Spec 11 names disk and database beside the UI. A connected key must exist in this
    # process's memory and in no file the server owns -- `store.db` and its write-ahead log
    # included, which is why this searches bytes rather than parsed artefacts.
    assert files_holding(tmp_path, API_KEY) == []
    assert files_holding(tmp_path, API_SECRET) == []


def test_connecting_measures_drift_with_the_method_the_signed_client_actually_has(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The whole of spec 11's key entry, over a client that answers only what the real one
    answers.

    `_validate` called `client.measure_drift_ms()`, a name that exists on no object in this
    package. It raised `AttributeError` before the first socket was opened, the caller
    reported it as the exchange refusing the key, and there was no configuration in which a
    credential could be entered at all. Every test in this file patched `_validate` whole,
    so the endpoint was dead under a green suite.

    The two calls asserted here are the ones the route makes in the order it makes them, and
    `drift_ms` is `SCRIPTED_DRIFT_MS` because the stand-in wrote that value onto itself --
    reading the attribute rather than a return value is what the real client offers.
    """
    made = patch_signed_client(monkeypatch)

    response = connect(client)
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["connected"] is True
    assert payload["alias"] == ALIAS
    assert payload["balance"] == "1234.50"
    assert payload["drift_ms"] == SCRIPTED_DRIFT_MS

    assert len(made) == 1
    assert made[0].base == TESTNET_BASE
    assert made[0].calls == ["server_time_ms", "account"]

    # And the stand-in is shaped after the real class rather than after the call site.
    assert not hasattr(SignedRestClient, "measure_drift_ms")
    assert callable(SignedRestClient.server_time_ms)


def test_a_fault_in_our_own_code_is_a_500_and_does_not_blame_the_operators_key(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A bug here is not a refused credential, and saying so cost the operator their key.

    One `except Exception` reported every failure as "the exchange refused these
    credentials", with instructions to check Futures permissions, Withdrawals and the IP
    whitelist -- so an `AttributeError` raised in this process, before any request left the
    machine, sent people to regenerate a Binance key that was never the problem. The status
    is 500 because the request was well formed and the server is the broken party.
    """
    patch_validation_failure(monkeypatch, AttributeError("no attribute 'measure_drift_ms'"))

    response = connect(client)
    assert response.status_code == 500
    detail = response.json()["detail"]
    assert "fault in the platform" in detail
    assert "AttributeError" in detail
    assert "refused these credentials" not in detail
    assert "whitelist" not in detail

    assert API_KEY not in response.text and API_SECRET not in response.text
    assert client.app.state.keys is None


def test_a_refusal_by_the_exchange_is_a_400_carrying_binances_own_reason(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The one case the "check your permissions" advice is for.

    `BinanceRestError` is the exchange answering, so its code and message go in front of the
    operator together with the three things they can act on. `-2015` is the code this test
    writes into the error, and it has to survive into the body or the advice is untethered.
    """
    patch_validation_failure(
        monkeypatch,
        BinanceRestError(401, -2015, "Invalid API-key, IP, or permissions for action."),
    )

    response = connect(client)
    assert response.status_code == 400
    detail = response.json()["detail"]
    assert "refused these credentials" in detail
    assert "-2015" in detail
    assert "whitelist" in detail
    assert API_KEY not in response.text and API_SECRET not in response.text
    assert client.app.state.keys is None


def test_an_unreachable_exchange_is_a_502_and_says_the_key_was_never_checked(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A network that is down says nothing about the credential.

    Reported as 502 rather than 400 so the operator is not sent to their key at all: nothing
    was validated, nothing was rejected, and the honest statement is that the exchange could
    not be reached from this machine.
    """
    patch_validation_failure(monkeypatch, httpx.ConnectError("getaddrinfo failed"))

    response = connect(client)
    assert response.status_code == 502
    detail = response.json()["detail"]
    assert "could not reach the exchange" in detail
    assert "not the credential" in detail
    assert "refused these credentials" not in detail
    assert client.app.state.keys is None


def test_no_validation_failure_on_connect_can_echo_the_credential_back(
    client: TestClient,
) -> None:
    """Spec 11: a key is "never echoed back to the UI" -- on the failure paths too.

    `ConnectRequest` used `Field(min_length=8, max_length=256)`, so a badly shaped body was
    a pydantic `ValidationError`, and FastAPI's default 422 renders `input`: the offending
    value. A secret that was too short came back whole; a 300-character paste came back
    whole; and a *missing* `api_key` came back with the entire request body, secret
    included, because that is the input to the field that was missing.

    Each case below is checked against the raw response text, which is the only check that
    catches a value arriving inside a nested error message.
    """
    long_secret = "S" * 300
    cases = {
        "secret too short": ({"api_key": API_KEY, "api_secret": "oops"}, "api_secret"),
        "secret too long": ({"api_key": API_KEY, "api_secret": long_secret}, "api_secret"),
        "key missing": ({"api_secret": API_SECRET}, "api_key"),
        "secret not a string": ({"api_key": API_KEY, "api_secret": 12345678}, "api_secret"),
    }
    for name, (body, field) in cases.items():
        response = client.post("/api/exchange/connect", json={**body, "endpoint": "testnet"})
        for secret in (API_SECRET, long_secret, "oops", "12345678"):
            assert secret not in response.text, f"{name} echoed the credential"
        assert response.status_code == 400, f"{name}: {response.text}"
        assert field in response.json()["detail"], name

    # A body that is not an object at all: pydantic's `model_attributes_type` error puts the
    # whole body in `input`, so a client that posted a bare JSON string got it straight back.
    bare = client.post("/api/exchange/connect", json=API_SECRET)
    assert API_SECRET not in bare.text
    assert bare.status_code == 400

    assert client.app.state.keys is None


def test_disconnecting_wipes_the_session_so_the_status_reads_disconnected(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Spec 11: `Disconnect` wipes the keys immediately -- the object is scrubbed, not just
    dropped from app state, because dropping a reference leaves the bytes in the heap."""
    patch_validation(monkeypatch)
    assert connect(client).json()["connected"] is True
    session = client.app.state.keys
    assert session is not None and not session.wiped

    payload = client.post("/api/exchange/disconnect", json={}).json()
    assert payload["connected"] is False
    # The state fields exactly, plus the report of what the action did: which keys were
    # wiped (a bool) and which live sessions were told to stop (run ids). Neither carries
    # a credential, and an action the endpoint is documented to take is one the operator
    # should be told it took -- the same rule `POST /kill` follows.
    assert set(payload) == STATE_FIELDS | {"keys_wiped", "stopped"}
    assert all(payload[field] is None for field in STATE_FIELDS - {"connected"})
    assert payload["keys_wiped"] is True
    assert payload["stopped"] == []

    assert session.wiped is True
    assert client.app.state.keys is None
    assert client.get("/api/exchange/status").json()["connected"] is False


def test_an_expired_key_session_reads_as_disconnected_and_is_wiped_on_that_read(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Spec 11's idle expiry, enforced on the read rather than by a timer thread.

    The session is back-dated by one second past its own TTL -- a number this test writes,
    not one it observed -- so it is expired and not yet wiped when the status is asked for.
    Reading the status has to do the wiping: without it the credential outlives its deadline
    in the memory of a process that has just told the operator there is no key here.
    """
    patch_validation(monkeypatch)
    connect(client)
    session = client.app.state.keys
    session.touch(now_s=time.monotonic() - KEY_SESSION_TTL_S - 1.0)
    assert session.expired() is True
    assert session.wiped is False

    payload = client.get("/api/exchange/status").json()
    assert payload["connected"] is False
    assert all(payload[field] is None for field in STATE_FIELDS - {"connected"})
    assert session.wiped is True


def test_an_expired_key_session_halts_running_sessions_and_leaves_positions_open(
    client: TestClient,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    launched: list[dict[str, Any]],
) -> None:
    """Spec 11's expiry is three actions and only one of them was happening.

    "Session expiry: default 12 h of inactivity -> keys wiped, live sessions halted with
    positions left open and an alert fired." The wipe ran; nothing stopped a session. That
    is the worse half to do alone -- it removes this process's ability to reconcile or
    cancel while the session carries on trading.

    `flatten` must be `False`: the spec leaves positions open on purpose, because "a forced
    market close on session expiry would be worse than the exposure". The reason string is
    the alert, since it is what the session writes into its Feed on the way down, so it has
    to say which deadline passed and that the position is still there.
    """
    patch_validation(monkeypatch)
    strategy_id = add_strategy(tmp_path)
    connect(client)
    run_id = start_session(client, strategy_id).json()["run"]["id"]
    client.app.state.runs.mark_running(run_id)
    control = client.app.state.runs.artefact(run_id, "control.json")
    assert not control.exists(), "nothing has asked this session to stop yet"

    session = client.app.state.keys
    session.touch(now_s=time.monotonic() - KEY_SESSION_TTL_S - 1.0)
    assert client.get("/api/exchange/status").json()["connected"] is False

    assert control.exists(), "the expired key session did not halt the run"
    payload = json.loads(control.read_text(encoding="utf-8"))
    assert payload["stop"] is True
    assert payload["flatten"] is False
    assert "12 h" in payload["reason"]
    assert "spec 11" in payload["reason"]
    assert "left open" in payload["reason"]
    assert API_KEY not in payload["reason"] and API_SECRET not in payload["reason"]


# --------------------------------------------------------------------------- sessions


def test_a_session_is_spawned_with_its_run_spec_on_disk_and_no_credential_in_it(
    client: TestClient, tmp_path: Path, launched: list[dict[str, Any]]
) -> None:
    """What reaches the worker, on the path that needs no account at all.

    Local fill simulation against a live market is the only mode that can run before a key
    has ever been entered, so `secrets` here holds the runtime cap and nothing else -- which
    is what makes the credential assertion in the test below meaningful rather than
    vacuous. `spec.json` is the file the worker reads, and it carries the session's identity
    (`session_kind`, `endpoint`) but never a credential, because it is on disk.
    """
    strategy_id = add_strategy(tmp_path)

    created = start_session(client, strategy_id, label="paper one", max_runtime_s=60.0)
    assert created.status_code == 201
    run = created.json()["run"]
    assert run["mode"] == "paper"
    assert run["label"] == "paper one"

    assert len(launched) == 1
    assert launched[0]["run_id"] == run["id"]
    assert launched[0]["secrets"] == {"max_runtime_s": 60.0}

    spec = json.loads(
        client.app.state.runs.artefact(run["id"], "spec.json").read_text(encoding="utf-8")
    )
    assert spec["session_kind"] == "paper"
    assert spec["endpoint"] == "testnet"
    assert spec["symbols"] == ["BTCUSDT"]
    assert "api_key" not in spec and "api_secret" not in spec


def test_a_session_worker_is_not_handed_a_credential_it_has_no_way_to_use(
    client: TestClient,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    launched: list[dict[str, Any]],
) -> None:
    """Spec 11: a credential exists in as few places, for as long, as the work needs.

    A connected key used to be exported to every session worker unconditionally. Nothing in
    `perplab.live.worker` reads it: a paper session runs the engine's simulated transport,
    and `ExchangeTransport`, `UserDataStream` and `Reconciler` are constructed nowhere in
    `perplab/`. The copy was a plain `str` in a second address space, outside any
    `KeySession`, with no idle timer and nothing `wipe()` could reach, for the unbounded
    life of the session -- in the process that runs strategy code, and so the one likeliest
    to produce the crash dump spec 11 names.

    This test previously asserted the opposite, and it was pinning the exposure.
    """
    patch_validation(monkeypatch)
    strategy_id = add_strategy(tmp_path)
    connect(client)
    assert client.app.state.keys is not None, "there is a credential available to leak"

    created = start_session(client, strategy_id, max_runtime_s=60.0)
    assert created.status_code == 201

    assert len(launched) == 1
    assert launched[0]["secrets"] == {"max_runtime_s": 60.0}
    assert files_holding(tmp_path, API_KEY) == []
    assert files_holding(tmp_path, API_SECRET) == []


def test_a_live_worker_gets_the_credential_on_the_pipe_with_a_deadline(
    client: TestClient,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    launched: list[dict[str, Any]],
) -> None:
    """Spec 11's one permitted channel, now that a live worker has something to sign with.

    `spec.json` is the existing API-to-worker channel and it is a file, so the assertion is
    not merely that the key is in `secrets` but that it is in *no file under the data root*
    -- including `store.db` and its write-ahead log, since spec 11 names the database too.

    The deadline travels with the copy as a staleness bound on the handoff; the worker
    wraps its copy in a `KeySession` of its own the moment it arrives, which is where the
    running idle timer lives. The clock is read either side of the call, so the deadline
    must fall in `(before, after + KEY_SESSION_TTL_S]` -- both ends derived from constants
    this test names rather than from what the route produced.
    """
    patch_validation(monkeypatch)
    strategy_id = add_strategy(tmp_path)
    connect(client)

    before_ms = int(time.time() * 1000)
    created = start_session(client, strategy_id, max_runtime_s=60.0, mode="live")
    after_ms = int(time.time() * 1000)
    assert created.status_code == 201
    run_id = created.json()["run"]["id"]

    assert len(launched) == 1
    assert launched[0]["run_id"] == run_id
    secrets = launched[0]["secrets"]
    assert set(secrets) == {"max_runtime_s", "api_key", "api_secret", "keys_expire_ms"}
    assert secrets["api_key"] == API_KEY
    assert secrets["api_secret"] == API_SECRET
    assert before_ms < secrets["keys_expire_ms"] <= after_ms + KEY_SESSION_TTL_S * 1000

    assert files_holding(tmp_path, API_KEY) == []
    assert files_holding(tmp_path, API_SECRET) == []

    spec = json.loads(
        client.app.state.runs.artefact(run_id, "spec.json").read_text(encoding="utf-8")
    )
    assert spec["session_kind"] == "live"


def test_a_live_start_is_refused_without_a_connected_key(
    client: TestClient, tmp_path: Path, launched: list[dict[str, Any]]
) -> None:
    """A live session that cannot sign is refused before a run row exists.

    The refusal must happen at the API, where the operator reads it -- a worker started
    without a credential would fail on its own first line and the run row would read as a
    platform fault rather than as a missing key.
    """
    strategy_id = add_strategy(tmp_path)
    refused = start_session(client, strategy_id, mode="live")
    assert refused.status_code == 409
    assert "Data & Feed" in refused.json()["detail"]
    assert launched == []
    assert client.get("/api/sessions").json()["sessions"] == []


def test_a_live_start_is_refused_when_the_key_was_validated_elsewhere(
    client: TestClient,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    launched: list[dict[str, Any]],
) -> None:
    """A key is an identity at one venue; exporting it to another is refused.

    The connected endpoint and the session's endpoint are separate fields that default the
    same way, so the mismatch only appears when an operator changes one of them -- which is
    exactly when the refusal has to be readable rather than a signature error at Binance.
    """
    patch_validation(monkeypatch)
    strategy_id = add_strategy(tmp_path)
    connect(client, endpoint="production")

    refused = start_session(client, strategy_id, mode="live", endpoint="testnet")
    assert refused.status_code == 409
    assert "testnet" in refused.json()["detail"]
    assert launched == []


def test_production_live_is_refused_until_the_phase8_criterion_is_met(
    client: TestClient,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    launched: list[dict[str, Any]],
) -> None:
    """`PRODUCTION_LIVE_ENABLED` is False until a real testnet round trip has reconciled.

    Refused with 403 -- the request is well-formed and understood, and the answer is no --
    and refused *before* the key-session checks, so the message names the real reason
    rather than whatever the connection state happens to be.
    """
    patch_validation(monkeypatch)
    strategy_id = add_strategy(tmp_path)
    connect(client, endpoint="production")

    refused = start_session(client, strategy_id, mode="live", endpoint="production")
    assert refused.status_code == 403
    assert "testnet" in refused.json()["detail"]
    assert launched == []


def test_a_paper_session_runs_under_spec_sevens_default_limits(
    client: TestClient, tmp_path: Path, launched: list[dict[str, Any]]
) -> None:
    """Spec 7: "a limit that stops a backtest stops live trading identically".

    A request that chooses nothing used to store `risk_limits = {}`, and
    `RiskLimits.from_json({})` is `unlimited()`: no leverage cap, no daily-loss cap, no
    drawdown cap, no equity floor, `halt_on_liquidation` off, and not one of spec 7's
    auto-triggers armed -- while the same empty choice on the backtest route got the whole
    table. The mode watching a live market was the unbounded one.

    `SPEC_SEVEN_DEFAULTS` is written in this file, so the values asserted are derived from
    the spec rather than from whatever the code happens to do.
    """
    strategy_id = add_strategy(tmp_path)
    run_id = start_session(client, strategy_id).json()["run"]["id"]

    stored = read_spec(client, run_id)["risk_limits"]
    assert stored == SPEC_SEVEN_DEFAULTS

    limits = RiskLimits.from_json(stored)
    assert limits.any_limit is True
    assert limits.unbounded_exposure is False
    assert limits.halt_on_liquidation is True
    assert limits.max_disconnect_seconds == 30


def test_the_flat_risk_fields_the_frontend_sends_reach_the_session_spec(
    client: TestClient, tmp_path: Path, launched: list[dict[str, Any]]
) -> None:
    """The frontend's `RiskLimitFields` is one shared declaration for both request bodies.

    Against the old nested `risk_limits` dict these flat fields were not rejected -- pydantic
    ignored them -- so a caller who asked for a 3x cap got a session with no cap at all and
    a 201 saying it had started. The fields left alone must still carry spec 7's defaults,
    or "override one limit" would silently mean "drop the rest".
    """
    strategy_id = add_strategy(tmp_path)
    created = start_session(
        client,
        strategy_id,
        max_leverage="3",
        max_open_orders=2,
        halt_on_liquidation=False,
    )
    assert created.status_code == 201

    stored = read_spec(client, created.json()["run"]["id"])["risk_limits"]
    assert stored["max_leverage"] == "3"
    assert stored["max_open_orders"] == 2
    assert stored["halt_on_liquidation"] is False
    assert stored["max_drawdown_pct"] == SPEC_SEVEN_DEFAULTS["max_drawdown_pct"]
    assert stored["max_consecutive_rejections"] == 5

    limits = RiskLimits.from_json(stored)
    assert limits.max_leverage == Decimal("3")
    assert limits.max_open_orders == 2
    assert limits.halt_on_liquidation is False


def test_running_a_session_with_no_risk_layer_takes_saying_so(
    client: TestClient, tmp_path: Path, launched: list[dict[str, Any]]
) -> None:
    """`risk_enabled: false` is the explicit opt-out, and it is the only way to get one.

    Spec 7's defaults being defaults does not make them compulsory -- a session whose whole
    purpose is to watch an unconstrained strategy is legitimate. What it may not be is the
    thing that happens when nobody said anything.
    """
    strategy_id = add_strategy(tmp_path)
    created = start_session(client, strategy_id, risk_enabled=False)
    assert created.status_code == 201

    stored = read_spec(client, created.json()["run"]["id"])["risk_limits"]
    assert stored == RiskLimits.unlimited().to_json()
    assert RiskLimits.from_json(stored).any_limit is False


def test_a_limit_written_as_a_percent_is_refused_before_a_session_exists(
    client: TestClient, tmp_path: Path, launched: list[dict[str, Any]]
) -> None:
    """The refusal the backtest route already makes, for the reason it already gives.

    `0.15` is fifteen percent and `15` is fifteen hundred percent, which is a limit that can
    never fire. Unchecked, the value reached the worker and killed it on its first line: the
    caller got a 201, and then a run row that said `failed` for a session that never ran.
    Nothing may be spawned and no row may be left behind, so both the launch recorder and
    the session list are asserted to be untouched.
    """
    strategy_id = add_strategy(tmp_path)
    before = client.get("/api/sessions").json()["sessions"]

    refused = start_session(client, strategy_id, max_drawdown_pct="15")
    assert refused.status_code == 400
    detail = refused.json()["detail"]
    assert "max_drawdown_pct" in detail
    assert "fraction" in detail

    assert launched == []
    assert client.get("/api/sessions").json()["sessions"] == before


def test_a_version_the_validator_rejected_cannot_start_a_session(
    client: TestClient, tmp_path: Path, launched: list[dict[str, Any]]
) -> None:
    """The rule the backtest route is held to, on the mode that watches a live market.

    Spec 5.6 stores a version that failed validation so the author's work is never lost, which
    is the whole reason a `valid = 0` row can reach this endpoint at all. Starting one is a
    separate question, and a harder one here than for a backtest: the findings the validator
    refuses on are look-ahead, a banned import and code that does not compile, so this would
    be a strategy reading bars it could not have had, in front of a live book. A backtest
    built on that code produces numbers that mean nothing; a session built on it places
    orders.

    The refusal is pinned to the `valid` column and to nothing else -- the same strategy, the
    same body and the same client start a session for real once the column is flipped to 1 --
    so the 400 cannot be a fixture that was broken some other way. Nothing may be spawned and
    no row may be left behind, so the launch recorder and the run store are both asserted
    empty: a run created and then refused is a queued paper session nobody started.
    """
    strategy_id = add_strategy(tmp_path, valid=False)

    refused = start_session(client, strategy_id)
    assert refused.status_code == 400, refused.text
    detail = refused.json()["detail"]
    assert "did not pass validation" in detail
    assert "cannot trade" in detail

    assert launched == []
    assert client.get("/api/sessions").json()["sessions"] == []
    assert client.app.state.runs.list(limit=50) == []

    connection = db.connect(tmp_path)
    with connection:
        connection.execute(
            "UPDATE strategy_versions SET valid = 1 WHERE strategy_id = ?", (strategy_id,)
        )
    connection.close()

    created = start_session(client, strategy_id)
    assert created.status_code == 201, created.text
    assert [call["run_id"] for call in launched] == [created.json()["run"]["id"]]


# ------------------------------------------------------------------------ kill switch


def test_firing_the_kill_switch_arms_it_and_records_trigger_detail_and_flatten(
    client: TestClient, tmp_path: Path, launched: list[dict[str, Any]]
) -> None:
    """Spec 7.5: the trip record is what an incident is reconstructed from afterwards.

    The one running session is asked to stop first and then the switch arms, so `stopped`
    names it and the trip's `run_id` is that session. The detail carries the one this test
    sent plus the requested mode -- and `flattened` is **false even though `flatten: true`
    was fired**, because at arm time no session has acted on the request: the stop is a
    control file each worker reads on its next pump, and the flatten it triggers can fail
    against the venue. `flattened` is reserved for the worker that verifiably closed the
    book flat (`KillSwitchStore.arm`'s false-to-true catch-up); recording the *intent* as
    fact here was how `require_clear` could promise the next operator a flat account while
    a position was still open.
    """
    strategy_id = add_strategy(tmp_path)
    run_id = start_session(client, strategy_id).json()["run"]["id"]

    fired = client.post(
        "/api/kill", json={"flatten": True, "detail": "reconciliation mismatch"}
    )
    assert fired.status_code == 200
    payload = fired.json()
    assert payload["stopped"] == [run_id]
    assert payload["stop_failures"] == []
    trip = payload["kill"]
    assert trip["trigger"] == "MANUAL"
    assert trip["detail"] == "reconciliation mismatch (close-all requested)"
    assert trip["flattened"] is False
    assert trip["run_id"] == run_id
    assert trip["armed"] is True
    assert trip["cleared_ms"] is None and trip["cleared_by"] is None

    assert client.get("/api/kill").json()["kill"] == trip

    control = json.loads(
        client.app.state.runs.artefact(run_id, "control.json").read_text(encoding="utf-8")
    )
    assert control["stop"] is True
    assert control["flatten"] is True
    assert "close-all" in control["reason"]


def test_the_kill_switch_defaults_to_cancel_only(
    client: TestClient, tmp_path: Path, launched: list[dict[str, Any]]
) -> None:
    """Spec 7.3 item 3: cancel-only by default, "because force-closing everything at market
    during a flash crash can be worse than the exposure".

    The request below sends no `flatten` key at all, so every `false` asserted here is the
    schema's default rather than an echo of the input.
    """
    strategy_id = add_strategy(tmp_path)
    run_id = start_session(client, strategy_id).json()["run"]["id"]

    trip = client.post("/api/kill", json={}).json()["kill"]
    assert trip["flattened"] is False

    control = json.loads(
        client.app.state.runs.artefact(run_id, "control.json").read_text(encoding="utf-8")
    )
    assert control["flatten"] is False
    assert "cancel-only" in control["reason"]


def test_the_kill_switch_wipes_the_keys_as_spec_11_says_both_controls_do(
    client: TestClient, monkeypatch: pytest.MonkeyPatch, launched: list[dict[str, Any]]
) -> None:
    """Spec 11: "`Disconnect` and the kill switch both wipe keys immediately."

    Only `Disconnect` did. `kill()` did not even take the request, so it had no way to reach
    the key session at all -- and the omission was invisible, because `/api/exchange/status`
    went on reporting the alias and the balance after the emergency stop. The credential was
    really still there and could still sign, which is the property `SignedRestClient`'s own
    class docstring argues its design from.

    Signing is attempted afterwards because "wiped" has to mean the buffers are scrubbed,
    not that a reference was dropped: an object dropped from app state is still a signing
    oracle for anything holding it.
    """
    patch_validation(monkeypatch)
    assert connect(client).json()["connected"] is True
    session = client.app.state.keys

    fired = client.post("/api/kill", json={})
    assert fired.status_code == 200
    assert fired.json()["keys_wiped"] is True

    assert session.wiped is True
    assert client.app.state.keys is None
    assert client.get("/api/exchange/status").json()["connected"] is False
    with pytest.raises(KeySessionExpired):
        session.sign("symbol=BTCUSDT&timestamp=1")


def test_firing_the_kill_switch_with_nothing_connected_says_there_was_nothing_to_wipe(
    client: TestClient, launched: list[dict[str, Any]]
) -> None:
    """`keys_wiped` reports what happened rather than what was attempted.

    A flag that is always `true` tells the operator nothing, and the kill switch is fired
    exactly when what actually happened is the question being asked.
    """
    fired = client.post("/api/kill", json={})
    assert fired.status_code == 200
    assert fired.json()["keys_wiped"] is False


def test_a_session_cannot_start_while_the_kill_switch_is_armed(
    client: TestClient, tmp_path: Path, launched: list[dict[str, Any]]
) -> None:
    """Spec 7.6: an explicit un-arm is required before anything starts again.

    409 rather than 400: the request is well formed and the account is the thing that is not
    ready. The refusal has to land before anything is spawned, so both the launch recorder
    and the session list must be exactly as they were -- a run row created and then refused
    would be a queued paper session nobody started.
    """
    strategy_id = add_strategy(tmp_path)
    client.post("/api/kill", json={})
    before = client.get("/api/sessions").json()["sessions"]

    refused = start_session(client, strategy_id)
    assert refused.status_code == 409
    detail = refused.json()["detail"]
    assert "kill switch was armed" in detail
    assert "un-arm" in detail
    assert launched == []
    assert client.get("/api/sessions").json()["sessions"] == before


def test_un_arming_records_the_actor_and_lets_a_session_start_again(
    client: TestClient, tmp_path: Path, launched: list[dict[str, Any]]
) -> None:
    """Spec 7.6 wants an explicit action by a named actor, so the cleared trip carries the
    name, and the trip itself survives as history rather than being deleted. Only after that
    does a session start -- and it starts for real, reaching `launch_session`."""
    strategy_id = add_strategy(tmp_path)
    client.post("/api/kill", json={"detail": "invariant failure"})

    cleared = client.post("/api/kill/unarm", json={"actor": "anshul"}).json()["cleared"]
    assert cleared["cleared_by"] == "anshul"
    assert cleared["cleared_ms"] is not None
    assert cleared["armed"] is False
    assert cleared["detail"] == "invariant failure (cancel-only)"
    assert client.get("/api/kill").json()["kill"] is None

    created = start_session(client, strategy_id)
    assert created.status_code == 201
    assert [call["run_id"] for call in launched] == [created.json()["run"]["id"]]


def test_the_armed_state_survives_a_new_app_over_the_same_root(
    tmp_path: Path, launched: list[dict[str, Any]]
) -> None:
    """The regression this state was moved out of memory for (spec 7.6).

    The events that arm the kill switch -- an invariant failure, a reconciliation mismatch, a
    socket down over an open position -- are the same events that end with the API being
    restarted. While the armed flag lived in the server process, the restarted server
    reported "clear" in exactly the words a switch that had never fired would have used, and
    a new session opened against the account whose state had just been declared untrustworthy.

    So: arm on one app, shut it down, build a second app over the same root, and require that
    the second one still refuses. The open row is also read straight out of `store.db` on a
    connection neither app owns, because a second app built inside this same interpreter
    would still see a module-level global -- and a restart is a new process, not a new
    object.
    """
    strategy_id = add_strategy(tmp_path)
    with TestClient(create_app(tmp_path)) as first:
        armed = first.post("/api/kill", json={"detail": "socket down over a position"})
        trip_id = armed.json()["kill"]["id"]

    connection = db.connect(tmp_path)
    open_rows = connection.execute(
        "SELECT id, detail FROM kill_switch_trips WHERE cleared_ms IS NULL"
    ).fetchall()
    connection.close()
    # The detail carries the requested mode ("cancel-only" here, the schema default)
    # because the trip's `flattened` field is reserved for a verified outcome.
    assert [(row["id"], row["detail"]) for row in open_rows] == [
        (trip_id, "socket down over a position (cancel-only)")
    ]

    with TestClient(create_app(tmp_path)) as second:
        after_restart = second.get("/api/kill").json()["kill"]
        assert after_restart is not None, "a restart cleared the kill switch"
        assert after_restart["id"] == trip_id
        assert after_restart["armed"] is True
        assert after_restart["detail"] == "socket down over a position (cancel-only)"

        refused = start_session(second, strategy_id)
        assert refused.status_code == 409
        assert launched == []

        second.post("/api/kill/unarm", json={"actor": "anshul"})
        assert start_session(second, strategy_id).status_code == 201


# --------------------------------------------------------------------------- the feed


def test_the_feed_pages_by_cursor_and_never_re_serves_an_entry_already_seen(
    client: TestClient, tmp_path: Path, launched: list[dict[str, Any]]
) -> None:
    """Spec 10.3's Feed, paged by sequence rather than offset.

    The log this test writes holds five entries with sequences 1 to 5. Read two at a time,
    following `next_seq`, that is pages of 2, 2 and 1 -- and the sixth request, made with the
    cursor the fifth entry set, has to come back empty rather than replaying the tail. An
    off-by-one in the cursor comparison shows up as the second page starting at 2.
    """
    strategy_id = add_strategy(tmp_path)
    run_id = start_session(client, strategy_id).json()["run"]["id"]
    write_events(
        client,
        run_id,
        [{"seq": n, "kind": "BAR", "ts_ms": 1_000 * n} for n in range(1, 6)],
    )

    pages: list[list[int]] = []
    cursor = 0
    for _ in range(3):
        payload = client.get(
            f"/api/runs/{run_id}/feed", params={"since_seq": cursor, "limit": 2}
        ).json()
        pages.append([entry["seq"] for entry in payload["entries"]])
        cursor = payload["next_seq"]

    assert pages == [[1, 2], [3, 4], [5]]
    assert cursor == 5

    seen = [seq for page in pages for seq in page]
    assert sorted(seen) == list(range(1, 6))
    assert len(set(seen)) == len(seen)

    tail = client.get(
        f"/api/runs/{run_id}/feed", params={"since_seq": cursor, "limit": 2}
    ).json()
    assert tail["entries"] == []
    assert tail["next_seq"] == 5


def test_a_risk_reject_is_classified_as_an_error_and_can_be_filtered_for(
    client: TestClient, tmp_path: Path, launched: list[dict[str, Any]]
) -> None:
    """The Feed's severity column is derived from the event kind, not stored with it.

    A `RISK_REJECT` is an order the risk layer refused (spec 7): the strategy asked for
    something and did not get it, which is the class of thing an operator has to see. An
    `EXPIRED` order is a warning -- expected, occasionally interesting -- and a bar close is
    ordinary traffic. The three entries below are the test's own input, so the three
    severities asserted are derivable without running anything.
    """
    strategy_id = add_strategy(tmp_path)
    run_id = start_session(client, strategy_id).json()["run"]["id"]
    write_events(
        client,
        run_id,
        [
            {"seq": 1, "kind": "BAR"},
            {"seq": 2, "kind": "RISK_REJECT", "reason": "max position"},
            {"seq": 3, "kind": "EXPIRED"},
        ],
    )

    entries = client.get(f"/api/runs/{run_id}/feed").json()["entries"]
    # "warning", never "warn": the client types, filters and styles on the same
    # three-word vocabulary the validator's diagnostics use, and this test once pinned
    # the server to a fourth spelling the UI could not see -- a real
    # AUTO_FLATTEN_FAILED rendered with the info glyph while the Warning filter
    # matched nothing forever.
    assert [entry["severity"] for entry in entries] == ["info", "error", "warning"]
    assert all(entry["source"] == "engine" for entry in entries)

    errors = client.get(
        f"/api/runs/{run_id}/feed", params={"severity": "error"}
    ).json()["entries"]
    assert [entry["kind"] for entry in errors] == ["RISK_REJECT"]
    assert errors[0]["reason"] == "max position"

    warnings_only = client.get(
        f"/api/runs/{run_id}/feed", params={"severity": "warning"}
    ).json()["entries"]
    assert [entry["kind"] for entry in warnings_only] == ["EXPIRED"]


def test_a_filtered_feed_advances_its_cursor_over_non_matching_entries(
    client: TestClient, tmp_path: Path, launched: list[dict[str, Any]]
) -> None:
    """The cursor covers every *examined* entry, not every matching one.

    With `?severity=error` open on a healthy session, a cursor that advanced only on
    matches never moved at all, so each 1 Hz poll re-opened the event log and re-parsed
    every line from seq zero -- quadratic work on the API's own thread pool, growing for
    as long as the session ran. Skipping a non-match permanently is sound because
    severity is a pure function of kind: an entry rejected by this filter today cannot
    match the same filter tomorrow.
    """
    strategy_id = add_strategy(tmp_path)
    run_id = start_session(client, strategy_id).json()["run"]["id"]
    write_events(
        client,
        run_id,
        [
            {"seq": 1, "kind": "BAR"},
            {"seq": 2, "kind": "BAR"},
            {"seq": 3, "kind": "BAR"},
        ],
    )

    page = client.get(
        f"/api/runs/{run_id}/feed", params={"severity": "error"}
    ).json()
    assert page["entries"] == []
    assert page["next_seq"] == 3, (
        "an all-filtered page must still hand back a cursor past what it examined, or "
        "the next poll re-reads the log from the top"
    )

    # And the page limit still bounds the cursor: nothing beyond the returned page is
    # claimed, so no entry can be skipped un-examined.
    limited = client.get(
        f"/api/runs/{run_id}/feed", params={"limit": 2}
    ).json()
    assert [entry["seq"] for entry in limited["entries"]] == [1, 2]
    assert limited["next_seq"] == 2


def test_parity_is_a_404_with_an_explanation_until_a_shadow_has_run(
    client: TestClient, tmp_path: Path, launched: list[dict[str, Any]]
) -> None:
    """Spec 6.7.1's report is written when the shadow backtest finishes, so its absence is
    the ordinary state of a session that is still running -- and a bare 404 would read as
    "no such run" to the panel asking for it. The report is served once it exists, which is
    what shows the 404 was about the artefact rather than about the route."""
    strategy_id = add_strategy(tmp_path)
    run_id = start_session(client, strategy_id).json()["run"]["id"]

    missing = client.get(f"/api/runs/{run_id}/parity")
    assert missing.status_code == 404
    detail = missing.json()["detail"]
    assert f"run {run_id}" in detail
    assert "shadow" in detail

    report = {"fills_matched": 3, "fills_total": 3}
    client.app.state.runs.artefact(run_id, "parity.json").write_text(
        json.dumps(report), encoding="utf-8"
    )
    served = client.get(f"/api/runs/{run_id}/parity")
    assert served.status_code == 200
    assert served.json()["parity"] == report


def test_the_parity_client_unwraps_the_envelope() -> None:
    """The route answers `{parity: report}`, and `api.parity` must hand the caller the
    *report*.

    This is the wire-shape defect class in its purest form and it cost a whole tab. The
    client typed the response as the bare report, `request` casts its body with `as T`, and
    so TypeScript agreed the envelope was the report. Nothing failed until a run existed
    that actually had a shadow backtest -- run 21, the first paper session -- and then
    `report.fills` was `undefined` and `fills.avg_delta_bps` took the Runs tab down. Neither
    `tsc` nor the build nor any test on this side could see it, because both halves were
    internally consistent and disagreed only with each other.

    Asserted against the source text rather than through a browser for the reason the whole
    file gives: `frontend/src/api.ts` mirrors these shapes by hand, so the mismatch has to
    break a Python test instead of a React runtime.
    """
    source = (
        Path(__file__).resolve().parents[2] / "frontend" / "src" / "api.ts"
    ).read_text(encoding="utf-8")
    helper = re.search(r"^  parity: .*?(?=\n\n)", source, re.S | re.M)
    assert helper is not None, "api.ts no longer defines a `parity` client helper"
    body = helper.group(0)
    assert "request<{ parity:" in body, (
        "the parity helper must type the envelope, not the report -- `request` casts with "
        "`as T` and cannot tell them apart"
    )
    assert ".parity" in body.split("request<", 1)[1], (
        "the parity helper types the envelope but never unwraps it, so callers get "
        "`{parity: ...}` where they expect the report"
    )


# ------------------------------------------------------------------------ route order


def test_the_sessions_routes_are_not_shadowed_by_the_spa_catch_all(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`create_app` claims `/{path:path}` for the built frontend, and Starlette matches in
    registration order: a router included after that mount answers every one of its endpoints
    with the app shell.

    A built `frontend/dist` is not assumed -- one is written here and `frontend_dist` pointed
    at it -- so this test exercises the shadowing case on a checkout where the UI has never
    been built. The catch-all is asserted to be live first, otherwise the rest proves nothing.
    """
    dist = tmp_path / "dist"
    (dist / "assets").mkdir(parents=True)
    (dist / "index.html").write_text("<!-- SPA-SHELL-MARKER -->", encoding="utf-8")
    monkeypatch.setattr(app_module, "frontend_dist", lambda: dist)

    root = tmp_path / "userdata"
    root.mkdir()
    with TestClient(create_app(root)) as client:
        shell = client.get("/runs/1")
        assert "SPA-SHELL-MARKER" in shell.text, "the catch-all is not installed"

        for path in ("/api/sessions", "/api/kill", "/api/exchange/status"):
            response = client.get(path)
            assert response.status_code == 200, f"{path} was shadowed by the SPA fallback"
            assert response.headers["content-type"].startswith("application/json")
            assert "SPA-SHELL-MARKER" not in response.text

        assert client.get("/api/sessions").json()["sessions"] == []

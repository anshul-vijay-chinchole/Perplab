"""Paper-session, exchange-connection and kill-switch endpoints (spec 7, 10.2, 10.3, 11).

Three rules shape this module, and each is a spec requirement rather than a preference.

**Credentials never touch this process's disk and never come back out.** Spec 11: keys are
"held only in backend process memory, in a session object. Never written to disk, database,
config file, log line, browser storage, or crash dump", and "never echoed back to the UI --
only the account alias and balance are shown". So `connect` validates with a signed balance
request, keeps a `KeySession` on app state, and every response here is the alias, the balance
and a countdown -- including the failure responses, which is why the key-entry body is parsed
by hand rather than declared (see `_connect_body`). If a session worker ever does need a
credential, its stdin pipe is the one channel that is neither a file nor an environment
variable; none does today, and `SESSION_WORKER_SIGNS_AT_THE_EXCHANGE` is where that is
stated and where it changes.

**No strategy code runs here.** Spec 11 again, and spec 2.3's isolation argument: starting a
session spawns `perplab.live.worker` exactly as starting a backtest spawns
`perplab.engine.worker`. This process reads SQLite and the run directory and nothing else.

**Stopping asks; killing is a separate, human act.** `RunStore.cancel` terminates, and on
Windows that is `TerminateProcess` -- no `finally`, no `atexit`. A session killed that way
never cancels its resting orders at the exchange, which is items 1 and 2 of spec 7.3 and
the whole point of the kill switch. So a stop writes `control.json` and the session sees it
within 250 ms and shuts down in order. There is deliberately **no automatic escalation to
terminate**: the one state where the ask goes unheard is an event loop wedged inside a
strategy hook, and a wedged loop cannot sign orders either -- while an automatic kill would
routinely fire on the good case, a session legitimately spending half a minute closing its
book at the venue. The escalation is the Cancel button on the run page, pressed by someone
who has read the monitor; the store's stale sweep (`lost`) is the backstop for a corpse.

**There is no WebSocket here, deliberately.** `_PasswordMiddleware` is a Starlette
`BaseHTTPMiddleware`, whose dispatch only runs for `scope["type"] == "http"` -- a WS endpoint
would bypass the bearer gate entirely, which on a `--host 0.0.0.0` instance is exactly the
case spec 11 makes the password mandatory for. The Feed is polled with a cursor instead.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field, ValidationError

from perplab.api.deps import get_library, get_root, get_runs
from perplab.core.money import Money, parse_money
from perplab.core.risk import RiskLimits
from perplab.core.types import MarginMode
from perplab.data.query import TIMEFRAMES
from perplab.data.schemas import normalise_symbol
from perplab.engine import ENGINE_VERSION
from perplab.engine.tiers import tier_from_name
from perplab.engine.backtest import AutoFlatten
from perplab.engine.runspec import RunSpec
from perplab.exchange.keys import KeySession
from perplab.exchange.rest import BinanceRestError, PRODUCTION_BASE, TESTNET_BASE
from perplab.exchange.signed import SignedRestClient
from perplab.live import PRODUCTION_LIVE_ENABLED
from perplab.live.session import VENUES
from perplab.store.claims import SymbolClaims, SymbolConflict
from perplab.store.killswitch import KillSwitchStore
from perplab.store.runs import RunNotFound, RunStatus, RunStore
from perplab.strategy.library import StrategyLibrary

router = APIRouter(tags=["sessions"])

__all__ = ["router"]

log = logging.getLogger(__name__)

FEED_PAGE_LIMIT = 500

_BASES = {"testnet": TESTNET_BASE, "production": PRODUCTION_BASE}

SESSION_WORKER_SIGNS_AT_THE_EXCHANGE = True
"""Whether a session worker has anything to sign with -- and so whether the credential
should cross the process boundary to reach it at all.

`True` since the change this constant's own docstring demanded: `perplab.live.worker`
now constructs `ExchangeTransport` for a live run, `await`s
`live.preflight.configure_account(...)` first with the run's own leverage and margin mode
(the transport refuses construction without the resulting `PreflightReport`), and wraps
the credential in its own `KeySession` the moment it arrives -- an idle timer and a
`wipe()` of its own, which the plain strings on the pipe have neither of.

The narrowing spec 11 argued for while this was `False` still holds and is enforced
below: the credential crosses only for a run whose `mode` is `live`. A paper session runs
the engine's simulated transport, its worker has nothing to sign, and it receives nothing
-- a key in a process that cannot use it is a key in one more crash dump for no work at
all. `keys_expire_ms` travels with the export as a staleness bound on the handoff; the
worker's own idle timer governs from there (see `live.stack.LiveStack.keys_expire_ms`).
"""


# --------------------------------------------------------------------------- schemas


CONNECT_MIN_CHARS = 8
CONNECT_MAX_CHARS = 256


class ConnectRequest(BaseModel):
    """The key-entry body. **Every field is optional and unconstrained here, deliberately.**

    A pydantic `ValidationError` carries the offending value in `input`, and FastAPI renders
    that straight into its default 422 body -- so a secret that was too short, too long or
    not a string came back out in the response, and a *missing* `api_key` came back with the
    whole request body, secret included. Spec 11: keys are "never echoed back to the UI".

    The constraints therefore live in `_connect_body`, which reports the field and the rule
    and never the value, and this model is here for its types alone. Defaults rather than
    required fields for the same reason: a required field is a `missing` error, and a
    `missing` error's `input` is everything the caller sent.
    """

    api_key: str = ""
    api_secret: str = ""
    endpoint: str = "testnet"


_CONNECT_REQUEST_BODY = {
    "required": True,
    "content": {
        "application/json": {
            "schema": {
                "type": "object",
                "required": ["api_key", "api_secret"],
                "properties": {
                    "api_key": {
                        "type": "string",
                        "minLength": CONNECT_MIN_CHARS,
                        "maxLength": CONNECT_MAX_CHARS,
                    },
                    "api_secret": {
                        "type": "string",
                        "minLength": CONNECT_MIN_CHARS,
                        "maxLength": CONNECT_MAX_CHARS,
                    },
                    "endpoint": {
                        "type": "string",
                        "enum": sorted(_BASES),
                        "default": "testnet",
                    },
                },
            }
        }
    },
}
"""The body schema for `/api/docs`, written by hand.

`ConnectRequest` is not declared as a route parameter -- see its docstring -- and an
undeclared body is an endpoint the documentation says takes nothing. The constraints here
are the ones `_connect_body` enforces, and they are the reason both are stated once as
`CONNECT_MIN_CHARS` / `CONNECT_MAX_CHARS`.
"""


class StartSessionRequest(BaseModel):
    """Everything a paper session needs. Deliberately a subset of a backtest's request:
    a session has no range to choose -- it starts now and runs until it is stopped."""

    strategy_id: int
    version_id: int | None = None
    symbols: list[str] = Field(default_factory=lambda: ["BTCUSDT"])
    timeframe: str = "1m"
    label: str = ""
    endpoint: str = "testnet"
    mode: str = "paper"
    """`paper` or `live` -- whether accepted orders reach the exchange (spec 13, Phase 8).

    Paper is the default and needs no credential: the engine's simulated transport prices
    every fill locally against the live feed. Live sends real signed orders to the
    endpoint above and is refused unless the exchange is connected in the Data & Feed tab,
    the connected endpoint matches this one, and -- until the Phase 8 exit criterion is
    met on testnet -- the endpoint is not production. See `start_session`.
    """
    seed: int = 0
    opening_balance: str = "10000"
    leverage: int = Field(default=5, ge=1, le=125)
    hedge_mode: bool = False
    """Spec 3.3 extended: hold a long **and** a short position per symbol.

    Off by default. In hedge mode every order must name a `position_side`, and the exchange
    account has to be in the same mode -- `live.preflight.configure_account` refuses a
    session whose ledger and account disagree, in either direction."""
    margin_mode: str = "ISOLATED"
    """Spec 3.7, and the setting the exchange preflight applies per symbol before the first
    order. `CROSSED` is refused -- see `core.types.MarginMode`."""
    maker_rate: str = "0.0002"
    taker_rate: str = "0.0005"
    fill_tier: str = "BOOK_WALK"
    reorder_buffer_ms: int = Field(default=250, ge=0, le=10_000)
    max_runtime_s: float = Field(default=0.0, ge=0.0)
    kill_switch_flatten: bool = False
    auto_flatten: dict[str, Any] = Field(default_factory=dict)

    # ------------------------------------------------------------------- risk (spec 7)

    risk_enabled: bool = True
    """**Spec 7's defaults apply unless the caller turns them off**, exactly as they do for
    a backtest -- `api/routers/runs.py` says the same thing in the same words, and this is
    now the second place it is true.

    These fields were one nested `risk_limits` dict defaulting to `{}`, and
    `RiskLimits.from_json({})` is `unlimited()`. So a paper session started through this
    route ran with no leverage cap, no daily-loss cap, no drawdown cap, no equity floor,
    `halt_on_liquidation` off and not one of spec 7's four auto-triggers able to fire --
    while the identical "the caller chose nothing" request to the backtest route got the
    whole table. Spec 7's first sentence is that "a limit that stops a backtest stops live
    trading identically", and the mode that watches a live market is the wrong one of the
    two to leave unbounded.

    Flat, and spelled exactly as the backtest request spells them, because that is what the
    frontend's one shared `RiskLimitFields` declaration sends. Against a nested dict those
    fields were not rejected -- they were dropped, and the session ran without the limits
    the caller had asked for.
    """

    max_position_notional: str | None = None
    max_leverage: str | None = "5"
    max_daily_loss_pct: str | None = "0.02"
    max_drawdown_pct: str | None = "0.15"
    max_open_orders: int | None = 10
    max_orders_per_minute: int | None = 30
    max_consecutive_losses: int | None = None
    halt_on_liquidation: bool = True
    min_equity_pct: str | None = "0.50"
    max_consecutive_rejections: int | None = 5

    max_disconnect_seconds: int | None = 30
    """Spec 7's fourth auto-trigger: the socket down this long over an open position.

    Set here rather than in `RiskLimits`, whose default is `None` because a backtest replays
    a file and has nothing that can disconnect. `core/risk.py` says the session builder that
    has a socket sets it explicitly, "where somebody can see the number" -- this is that
    builder, and 30 is spec 7's number.
    """

    def risk_json(self) -> dict[str, Any]:
        """Spec 7's table for this session, or every limit off if the caller opted out."""
        if not self.risk_enabled:
            return RiskLimits.unlimited().to_json()
        return {
            "max_position_notional": self.max_position_notional,
            "max_leverage": self.max_leverage,
            "max_daily_loss_pct": self.max_daily_loss_pct,
            "max_drawdown_pct": self.max_drawdown_pct,
            "max_open_orders": self.max_open_orders,
            "max_orders_per_minute": self.max_orders_per_minute,
            "max_consecutive_losses": self.max_consecutive_losses,
            "halt_on_liquidation": self.halt_on_liquidation,
            "min_equity_pct": self.min_equity_pct,
            "max_consecutive_rejections": self.max_consecutive_rejections,
            "max_disconnect_seconds": self.max_disconnect_seconds,
        }


class StopSessionRequest(BaseModel):
    flatten: bool = False
    reason: str = "stopped from the API"


class KillRequest(BaseModel):
    flatten: bool = False
    """Spec 7.3 item 3: **cancel-only by default**, "because force-closing everything at
    market during a flash crash can be worse than the exposure"."""

    detail: str = ""


class UnarmRequest(BaseModel):
    actor: str = "operator"


# ------------------------------------------------------------------ exchange session


def _live_keys(request: Request) -> KeySession | None:
    """The usable key session, or `None` -- performing spec 11's idle expiry on the way.

    The single reader of `expired()` in this module, so that everything expiry is supposed
    to do happens wherever the question is asked. It used to happen only in the status
    payload, and only the wiping half of it.

    **A running live session counts as use of this credential.** Its worker signs with a
    copy exported from this very object, every sixty seconds, for as long as it runs --
    `exchange.keys`' own docstring calls that the intended reading of "idle". This process
    cannot see those signatures, so without the check below the parent copy idle-expired
    twelve hours into every healthy 48-hour session and `_halt_sessions_on_expiry` then
    stopped it, positions open, unattended -- the exact outcome the intended reading
    exists to rule out. So while a live session is running, an expiry check refreshes the
    timer instead of firing; the idle clock starts counting only once nothing is trading
    on the credential's behalf.
    """
    session: KeySession | None = getattr(request.app.state, "keys", None)
    if session is None or session.wiped:
        return None
    if not session.expired():
        return session
    if _live_session_running(request):
        session.touch()
        return session

    # Spec 11: expiry wipes the keys **and halts live sessions, with positions left open**,
    # and fires an alert. All three, not the first one: wiping alone is the half that
    # removes this process's ability to reconcile or cancel while leaving the session
    # trading, which is the wrong half to do on its own. The halt loop is a race net --
    # with the check above, a live session that is still running means this branch was not
    # taken -- and the worker's own `KeySession` idle timer is the backstop underneath
    # both (`PaperSession._pump` stops the session itself if its copy ever idles out).
    session.wipe()
    _halt_sessions_on_expiry(request, session)
    return None


def _live_session_running(request: Request) -> bool:
    """Whether any live run is still going -- the sessions signing with exported copies."""
    runs: RunStore | None = getattr(request.app.state, "runs", None)
    if runs is None:  # pragma: no cover - always installed by `create_app`
        return False
    # `active()` and not `list(limit=500)`: a listing window is a recency filter, and
    # "still signing with an exported credential" is a property of status. See
    # `RunStore.active` for the incident shape this closes.
    return any(summary.mode == "live" for summary in runs.active())


def _halt_sessions_on_expiry(request: Request, session: KeySession) -> list[int]:
    """Stop every live session because the credential behind it has expired (spec 11).

    `flatten=False` is the requirement rather than a default: spec 11 halts "with positions
    left open" because "a forced market close on session expiry would be worse than the
    exposure". The reason string carries the alert, because it is what the session adopts as
    its `stopped_reason` and what the worker turns into the run's warning and the monitor
    payload -- that is the whole of this platform's alerting (there is no webhook, no
    notifier and no alert store anywhere in `perplab/`), so the difference the wording makes
    is between an operator who learns which deadline passed and one who finds a session
    stopped for no stated reason with a position still open.

    Triggered by a read rather than by a timer, like the wipe it accompanies: this process
    holds no timer thread and inventing one to nurse a credential is the wrong direction of
    travel. The consequence is honest and worth naming -- with nothing reading the status,
    the halt waits for the next reader.
    """
    runs: RunStore | None = getattr(request.app.state, "runs", None)
    if runs is None:  # pragma: no cover - always installed by `create_app`
        return []
    reason = (
        f"the API key session idled past its {session.ttl_s / 3600:.0f} h timeout and was "
        "wiped (spec 11), so this session is halted. Positions are left open deliberately "
        "-- a forced market close on expiry would be worse than the exposure -- and still "
        "need attention."
    )
    # Both modes on purpose, though only live sessions hold a credential: twelve idle
    # hours means nobody has touched the platform, and the conservative reading of spec
    # 11's expiry stops everything that is trading a market feed unattended. A paper
    # session halted spuriously costs simulation time; a live one not halted costs
    # exposure, and the asymmetry decides. Each stop is guarded so one control file that
    # cannot be written does not leave the sessions after it never asked (see `kill`).
    halted: list[int] = []
    for summary in runs.active():
        if summary.mode in ("paper", "live"):
            try:
                runs.request_stop(summary.id, flatten=False, reason=reason)
                halted.append(summary.id)
            except OSError as exc:  # noqa: PERF203 - per-item isolation is the point
                log.error("could not ask session %s to stop on key expiry: %s", summary.id, exc)
    return halted


def _wipe_keys(request: Request) -> bool:
    """Destroy the in-memory credential. Returns whether there was one to destroy.

    Shared by `Disconnect` and the kill switch because spec 11 names them in one sentence:
    "`Disconnect` and the kill switch both wipe keys immediately". Only the first of the two
    did it, and the omission was invisible -- `/api/exchange/status` went on reporting the
    alias and the balance after the emergency stop, and the session could still sign.
    `SignedRestClient`'s class docstring argues its whole design from the premise that this
    happens.
    """
    session: KeySession | None = getattr(request.app.state, "keys", None)
    request.app.state.keys = None
    request.app.state.endpoint = None
    if session is None or session.wiped:
        return False
    session.wipe()
    return True


def _exchange_state(request: Request) -> dict[str, Any]:
    session = _live_keys(request)
    if session is None:
        return {
            "connected": False,
            "alias": None,
            "balance": None,
            "expires_in_s": None,
            "endpoint": None,
            "drift_ms": None,
        }
    return {
        "connected": True,
        **session.to_json(),
        "endpoint": getattr(request.app.state, "endpoint", None),
        "drift_ms": getattr(request.app.state, "drift_ms", None),
    }


@router.get("/exchange/status")
def exchange_status(request: Request) -> dict[str, Any]:
    return _exchange_state(request)


async def _connect_body(request: Request) -> ConnectRequest:
    """Parse and check the key-entry body without ever quoting it back.

    Read off the raw request rather than declared as a route parameter, because FastAPI
    turns a validation failure on a declared body into a 422 whose `input` field *is* the
    offending value -- and, for a missing field, is the entire request body. That put the
    API secret in the response, in the browser's network log, and in any error toast that
    renders `detail`; spec 11 says a key is never echoed back. Every exit from here names
    the field and the rule and nothing else, which is also why the length is reported and
    the value is not.
    """
    try:
        payload = await request.json()
    except ValueError:
        raise HTTPException(
            status_code=400, detail="the request body is not valid JSON"
        ) from None
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=400,
            detail=(
                "the request body must be a JSON object carrying api_key, api_secret and "
                "endpoint"
            ),
        )
    try:
        body = ConnectRequest.model_validate(payload)
    except ValidationError as exc:
        # `loc` and `msg` are pydantic's words about the *shape* of the field. `input` and
        # `ctx` are the value, and are exactly what is not allowed out of here.
        detail = "; ".join(
            f"{'.'.join(str(part) for part in error['loc']) or 'body'}: {error['msg']}"
            for error in exc.errors()
        )
        raise HTTPException(status_code=400, detail=detail) from None
    for name in ("api_key", "api_secret"):
        length = len(str(getattr(body, name)))
        if not CONNECT_MIN_CHARS <= length <= CONNECT_MAX_CHARS:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"{name} must be {CONNECT_MIN_CHARS} to {CONNECT_MAX_CHARS} characters; "
                    f"this one is {length}. The value is deliberately not repeated back "
                    "(spec 11)."
                ),
            )
    return body


@router.post("/exchange/connect", openapi_extra={"requestBody": _CONNECT_REQUEST_BODY})
async def exchange_connect(request: Request) -> dict[str, Any]:
    """Validate a key pair and hold it in memory for this process only (spec 11).

    Validated with a real signed call rather than accepted on faith: a key that is wrong, or
    that lacks Futures permission, should fail here -- while someone is looking at the form
    -- rather than at the first order of a session.

    **The three ways this fails are three different answers, and they used to be one.** A
    single `except Exception` reported every one of them as "the exchange refused these
    credentials", so a fault in this process -- a call to a `measure_drift_ms` that
    `SignedRestClient` has never had -- sent the operator to regenerate their Binance key,
    toggle its permissions and edit their IP whitelist, none of which could ever work
    because no request had left the machine. A refusal is a 400 and is about the key; an
    unreachable exchange is a 502 and is about the network; anything else is a 500 and is
    about us.
    """
    body = await _connect_body(request)
    base = _BASES.get(body.endpoint)
    if base is None:
        raise HTTPException(
            status_code=400,
            detail=f"unknown endpoint {body.endpoint!r}; expected one of {sorted(_BASES)}",
        )

    session = KeySession(api_key=body.api_key, api_secret=body.api_secret)
    try:
        alias, balance, drift = await _validate(base, session)
    except BinanceRestError as exc:
        session.wipe()
        raise HTTPException(
            status_code=400,
            detail=(
                f"the exchange refused these credentials: {exc}. "
                "Check that the key has Reading and Futures Trading enabled, that "
                "Withdrawals is OFF, and that this machine's IP is whitelisted if you set "
                "a whitelist."
            ),
        ) from None
    except (httpx.HTTPError, OSError) as exc:
        session.wipe()
        raise HTTPException(
            status_code=502,
            detail=(
                f"could not reach the exchange at {base}: {type(exc).__name__}: {exc}. "
                "The key was not checked and has not been kept -- this is the network "
                "between here and Binance, not the credential."
            ),
        ) from None
    except Exception as exc:  # noqa: BLE001 - the reason belongs in front of the operator
        session.wipe()
        raise HTTPException(
            status_code=500,
            detail=(
                f"PerpLab failed while validating this key: {type(exc).__name__}: {exc}. "
                "This is a fault in the platform, not in the credential -- the exchange "
                "did not refuse anything and the key has been wiped. Report this message."
            ),
        ) from None

    session.record_validation(alias=alias, balance=balance)
    previous: KeySession | None = getattr(request.app.state, "keys", None)
    if previous is not None:
        previous.wipe()
    request.app.state.keys = session
    request.app.state.endpoint = body.endpoint
    request.app.state.drift_ms = drift
    return _exchange_state(request)


async def _validate(base: str, session: KeySession) -> tuple[str, Money | None, int]:
    """One signed call, for the alias and the balance spec 11 permits showing.

    The balance is parsed into `Money` here rather than passed through as Binance's JSON
    string. `KeySession.balance` is typed `Money | None` and `to_json` renders it with
    `money_to_str`, which raises on a `str` -- so leaving it unparsed turned a *successful*
    connection into a 500, with the key session already installed on app state. The operator
    saw a failure and had a live credential.

    Drift is spec 11's clock measurement and `server_time_ms` is what takes it: the figure
    lands on the client's `drift_ms` rather than coming back as a return value. There is no
    `measure_drift_ms` and there never was -- the name appeared at this one call site, and
    while it stood there the whole key-entry path raised `AttributeError` before opening a
    socket, in every configuration.
    """
    async with SignedRestClient(base, session) as client:
        # Measured before the signed call, so that a drift large enough to break signing is
        # already on the record beside the -1021 it causes.
        await client.server_time_ms()
        drift = int(client.drift_ms or 0)
        account = await client.account()
    alias = str(account.get("accountAlias") or account.get("feeTier") or "account")
    raw = account.get("totalWalletBalance")
    balance = None if raw in (None, "") else parse_money(str(raw))
    return alias, balance, drift


@router.post("/exchange/disconnect")
def exchange_disconnect(
    request: Request, runs: RunStore = Depends(get_runs)
) -> dict[str, Any]:
    """Spec 11: `Disconnect` wipes the keys immediately -- *all* of them.

    A live session's worker signs with its own copy of the credential, which this
    process's wipe cannot reach -- so a disconnect that wiped only the local `KeySession`
    would leave the UI reporting "not connected" while another process kept placing signed
    orders under the very credential the operator just revoked. Every running live session
    is therefore asked to stop, cancel-only (`flatten=False`, spec 11's expiry rule applied
    to the same situation: a forced market close is worse than the exposure), and its own
    shutdown wipes its copy. Paper sessions hold no credential and are left alone.
    """
    wiped = _wipe_keys(request)
    stopped: list[int] = []
    reason = (
        "the exchange was disconnected and the API keys wiped (spec 11), so this live "
        "session's credential is revoked. Positions are left open deliberately and still "
        "need attention."
    )
    for summary in runs.active():
        if summary.mode == "live":
            try:
                runs.request_stop(summary.id, flatten=False, reason=reason)
                stopped.append(summary.id)
            except OSError as exc:  # noqa: PERF203 - one bad write must not skip the rest
                log.error("disconnect could not stop session %s: %s", summary.id, exc)
    return {**_exchange_state(request), "keys_wiped": wiped, "stopped": stopped}


# ------------------------------------------------------------------------- sessions


def _active_session_ids(runs: RunStore) -> list[int]:
    """Runs that are still going, for pruning claims left by a terminated worker.

    A session killed with `TerminateProcess` never reaches its own release, so a claim can
    outlive its session. Reconciled against the run table on every read rather than trusted
    from the row: a stale claim that blocked every future session on a symbol would be a
    safety mechanism an operator learns to work around, which is worse than not having one.

    **Enumerated by status, never through a listing window.** This feeds
    `SymbolClaims`' pruner, which *destructively releases* any claim whose run is absent
    from this list -- so a live session that merely fell out of a recency window would
    have its symbol claim freed while it traded, and a second session could fill into
    the same venue position. See `RunStore.active`.
    """
    return [summary.id for summary in runs.active()]


@router.get("/symbol-claims")
def symbol_claims(
    endpoint: str = Query("testnet"),
    runs: RunStore = Depends(get_runs),
    root: Path = Depends(get_root),
) -> dict[str, Any]:
    """What every running session has configured at the exchange, per symbol.

    Read by the Start Session form so the operator sees the leverage already in force on a
    symbol *before* submitting, rather than being refused after filling the whole form. The
    refusal in `start_session` is still the authority -- this is advisory, and a claim taken
    between the two reads is exactly why the check cannot live only here.
    """
    with SymbolClaims(root) as claims:
        held = claims.open_claims(endpoint, active_run_ids=_active_session_ids(runs))
    return {"endpoint": endpoint, "claims": [c.to_json() for c in held]}


@router.get("/sessions")
def list_sessions(
    limit: int = Query(50, ge=1, le=500),
    runs: RunStore = Depends(get_runs),
) -> dict[str, Any]:
    items = [r for r in runs.list(limit=limit * 4) if r.mode in ("paper", "live", "shadow")]
    return {"sessions": [item.to_json() for item in items[:limit]]}


@router.post("/sessions", status_code=status.HTTP_201_CREATED)
def start_session(
    body: StartSessionRequest,
    request: Request,
    runs: RunStore = Depends(get_runs),
    library: StrategyLibrary = Depends(get_library),
    root: Path = Depends(get_root),
) -> dict[str, Any]:
    """Queue a session -- paper or live -- and spawn its worker."""
    if body.endpoint not in VENUES:
        raise HTTPException(
            status_code=400,
            detail=f"unknown endpoint {body.endpoint!r}; expected one of {sorted(VENUES)}",
        )
    if body.mode not in ("paper", "live"):
        raise HTTPException(
            status_code=400,
            detail=f"unknown mode {body.mode!r}; expected 'paper' or 'live'",
        )
    if body.mode == "live":
        # Refused here, where the person who asked can read the refusal; the worker checks
        # every one of these again, because a check only in the caller is a check another
        # caller can skip.
        if body.endpoint == "production" and not PRODUCTION_LIVE_ENABLED:
            raise HTTPException(
                status_code=403,
                detail=(
                    "live trading against production is disabled until the Phase 8 exit "
                    "criterion has been met on testnet (spec 13): one real order placed, "
                    "filled and reconciled to the cent. Start this session on testnet."
                ),
            )
        key_session = _live_keys(request)
        if key_session is None:
            raise HTTPException(
                status_code=409,
                detail=(
                    "a live session needs a connected exchange key and none is held (it "
                    "was never entered, expired, or was wiped). Connect the exchange in "
                    "the Data & Feed tab first."
                ),
            )
        connected_endpoint = getattr(request.app.state, "endpoint", None)
        if connected_endpoint != body.endpoint:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"the connected exchange key was validated against "
                    f"{connected_endpoint!r} and this session asks for {body.endpoint!r}. "
                    f"A key is an identity at one venue; reconnect against "
                    f"{body.endpoint!r} or start the session there."
                ),
            )
    try:
        RiskLimits.from_json(body.risk_json())
        AutoFlatten.from_json(dict(body.auto_flatten))
    except ValueError as exc:
        # A limit written as a percent, or a misspelled field -- the same refusal the
        # backtest route makes, for the same reason. Unchecked, these reached the worker and
        # killed it on its first line, which reads as a platform fault rather than as a
        # rejected request, and the run row said `failed` with no session ever having run.
        raise HTTPException(status_code=400, detail=str(exc)) from None
    # The three bare money strings and the two enums, held to the same standard (H20).
    # The comment above applies verbatim: an unparseable balance or rate reached the
    # worker and killed it on its first line, which read as a platform fault. The risk
    # block was validated here from day one while the fields beside it were not -- the
    # argument never distinguished them.
    for field_name, raw in (
        ("opening_balance", body.opening_balance),
        ("maker_rate", body.maker_rate),
        ("taker_rate", body.taker_rate),
    ):
        try:
            value = parse_money(raw)
        except (ValueError, ArithmeticError) as exc:
            raise HTTPException(
                status_code=400, detail=f"{field_name}: {exc}"
            ) from None
        if field_name == "opening_balance" and value <= 0:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"opening_balance must be positive, got {raw!r}. Every "
                    f"equity-relative risk limit is a fraction of it."
                ),
            )
        if field_name != "opening_balance" and not (0 <= value < 1):
            raise HTTPException(
                status_code=400,
                detail=(
                    f"{field_name} is a fraction of notional and must be in [0, 1), got "
                    f"{raw!r}. A rate written in percent or basis points would charge a "
                    f"hundred or ten thousand times the intended fee."
                ),
            )
    if body.timeframe not in TIMEFRAMES:
        raise HTTPException(
            status_code=400,
            detail=f"unknown timeframe {body.timeframe!r}; known: {', '.join(TIMEFRAMES)}",
        )
    try:
        tier_from_name(body.fill_tier)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None

    # Spec 7.6: an armed kill switch blocks a new session until it is explicitly un-armed.
    # Checked here so the refusal reaches the person pressing the button; the worker checks
    # again, because a check only in the caller is a check another caller can skip.
    with KillSwitchStore(root) as switch:
        try:
            switch.require_clear()
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=409, detail=str(exc)) from None

    strategy = library.get(body.strategy_id)
    versions = library.versions(body.strategy_id)
    if not versions:
        raise HTTPException(
            status_code=400, detail=f"strategy {body.strategy_id} has no saved version"
        )
    if body.version_id is None:
        version = versions[0]  # `versions` is newest first
    else:
        version = next((v for v in versions if v.id == body.version_id), None)
        if version is None:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"version {body.version_id} does not belong to strategy "
                    f"{body.strategy_id}. Running a session against another strategy's code "
                    "would record results under the wrong name."
                ),
            )
    if not version.valid:
        # The same rule a backtest is held to: a run whose numbers come from code the
        # validator rejected is a run whose numbers mean nothing, and a *live* one would
        # place real orders from it.
        raise HTTPException(
            status_code=400,
            detail=(
                f"version {version.version_no} of {strategy.name} did not pass validation, "
                "so it cannot trade. Open it in the editor and fix the reported findings."
            ),
        )

    normalised = tuple(normalise_symbol(s) for s in body.symbols)
    try:
        margin_mode = MarginMode.parse(body.margin_mode)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    now_ms = _now_ms()
    spec = RunSpec(
        strategy_id=strategy.id,
        version_id=version.id,
        version_no=version.version_no,
        strategy_name=strategy.name,
        code=version.code or "",
        class_name=version.class_name,
        params={},
        symbols=normalised,
        timeframe=body.timeframe,
        start_ms=now_ms,
        # A session has no end until it is stopped. The nominal end is recorded so the row
        # has a range like every other run; `effective_end_ms` is what the metrics use.
        end_ms=now_ms + int((body.max_runtime_s or 48 * 3600) * 1000),
        seed=body.seed,
        opening_balance=body.opening_balance,
        leverage=body.leverage,
        margin_mode=margin_mode.value,
        hedge_mode=body.hedge_mode,
        maker_rate=body.maker_rate,
        taker_rate=body.taker_rate,
        fee_source="session-default",
        latency={"model": "fixed", "submit_ms": 120, "cancel_ms": 120},
        fill_tier=body.fill_tier,
        fill_model={"tier": body.fill_tier},
        liquidation_recovery_pct="0",
        timeout_s=0.0,
        engine_version=ENGINE_VERSION,
        risk_limits=body.risk_json(),
        auto_flatten=dict(body.auto_flatten),
        kill_switch_flatten=body.kill_switch_flatten,
        endpoint=body.endpoint,
        reorder_buffer_ms=body.reorder_buffer_ms,
        session_kind=body.mode,
    )
    run_id = runs.create(
        strategy_id=strategy.id,
        version_id=version.id,
        spec=spec.to_storage(),
        label=body.label,
        mode=body.mode,
    )

    # **The symbol claim, taken before the worker is spawned.**
    #
    # A symbol on an account belongs to one running session. Binance holds one position per
    # symbol and side for the whole account, with no per-strategy scope, so two sessions on
    # one symbol would have their fills merged into a single exchange position while each
    # ledger tracked its own -- and no amount of local bookkeeping can recover the split.
    # Refusing here is the only point at which nothing has been configured and no order sent.
    #
    # Taken *after* `create` because a claim needs a run id, and released again if the
    # launch fails: a claim held by a run that never started would block the symbol until
    # somebody noticed. The run row is left behind in `queued` either way, which is what
    # every other refusal on this path does.
    try:
        with SymbolClaims(root) as claims:
            claims.claim(
                run_id=run_id,
                symbols=normalised,
                leverage=body.leverage,
                margin_mode=margin_mode.value,
                hedge_mode=body.hedge_mode,
                endpoint=body.endpoint,
                now_ms=now_ms,
                active_run_ids=_active_session_ids(runs),
            )
    except SymbolConflict as exc:
        runs.fail(run_id, str(exc), status=RunStatus.CANCELLED)
        raise HTTPException(status_code=409, detail=str(exc)) from None

    secrets: dict[str, Any] = {"max_runtime_s": body.max_runtime_s}
    if body.mode == "live" and SESSION_WORKER_SIGNS_AT_THE_EXCHANGE:
        session = _live_keys(request)
        if session is None:
            # Checked above, before the run row and the claim existed -- but the key can
            # expire between the two reads, and a live worker started with no credential
            # would fail on its first line while the row read as a platform fault. Refuse
            # cleanly and release what this request created.
            with SymbolClaims(root) as claims:
                claims.release(run_id, _now_ms())
            runs.fail(
                run_id,
                "the exchange key expired between validation and launch",
                status=RunStatus.CANCELLED,
            )
            raise HTTPException(
                status_code=409,
                detail=(
                    "the exchange key session expired while this request was being "
                    "prepared. Reconnect in the Data & Feed tab and start again."
                ),
            )
        # Handed to the child on its stdin pipe and nowhere else, and **only for a live
        # run** -- a paper worker has nothing to sign, and a key in a process that cannot
        # use it is one more crash dump holding a credential for no work at all. Never
        # written into `spec.json`, which is on disk and which spec 11 forbids for a
        # credential. `export_for_child` is the only method that materialises the
        # credential as a `str`, and it is named so that this line reads as what it is.
        secrets.update(session.export_for_child())
        # The child wraps its copy in a `KeySession` of its own the moment it arrives, so
        # an idle timer does travel -- what this deadline adds is a staleness bound on the
        # handoff itself, from the parent copy's remaining life at the moment of export.
        secrets["keys_expire_ms"] = _now_ms() + int(session.expires_in_s * 1000)
    try:
        runs.launch_session(run_id, secrets=secrets)
    except Exception as exc:  # noqa: BLE001 - a failed spawn must free what it claimed
        # Without this, a `Popen` that raises (interpreter path gone, resource limits)
        # left the run `queued` and the symbol claimed until the stale sweep called it
        # `lost` three minutes later -- a dead run blocking every new session on the
        # symbol, over a failure the operator was shown only as a bare 500.
        with SymbolClaims(root) as claims:
            claims.release(run_id, _now_ms())
        detail = f"the session worker could not be spawned: {type(exc).__name__}: {exc}"
        runs.fail(run_id, detail, status=RunStatus.FAILED)
        raise HTTPException(status_code=500, detail=detail) from None
    finally:
        secrets.clear()
    return {"run": runs.get(run_id).to_json()}


@router.get("/sessions/{run_id}/monitor")
def session_monitor(run_id: int, runs: RunStore = Depends(get_runs)) -> dict[str, Any]:
    """The live monitor's payload (spec 10.3), republished by the session every second.

    Served from the file the session writes rather than computed here, because this process
    does not hold the session's state -- it is in another process by spec 11's design, and
    the file is the seam. A missing file means the session has not published yet, which is a
    different thing from a session that is not running, so the run row travels with it.
    """
    summary = runs.get(run_id)
    payload: dict[str, Any] = {"run": summary.to_json(), "monitor": None}
    try:
        payload["monitor"] = runs.read_json(run_id, "monitor.json")
    except RunNotFound:
        pass
    return payload


@router.post("/sessions/{run_id}/stop")
def stop_session(
    run_id: int,
    body: StopSessionRequest,
    runs: RunStore = Depends(get_runs),
) -> dict[str, Any]:
    """Ask a session to stop in an orderly way. See the module docstring for why not kill.

    The symbol claim is **not** released here, deliberately. A stop is a request, not an
    event: the session reads `control.json` within 250 ms and then spends as long as it needs
    cancelling resting orders and, if armed, flattening -- during which it is still trading
    on the leverage it claimed. Freeing the symbol at the moment the button is pressed would
    let a second session reconfigure the account under a position that is still being closed.
    The worker releases it in its own `finally`, on the clean path and the crash path alike.
    """
    summary = runs.get(run_id)
    if summary.status in RunStatus.TERMINAL:
        return {"run": summary.to_json(), "already_finished": True}
    runs.request_stop(run_id, flatten=body.flatten, reason=body.reason)
    return {"run": runs.get(run_id).to_json(), "already_finished": False}


@router.get("/runs/{run_id}/feed")
def run_feed(
    run_id: int,
    since_seq: int = Query(0, ge=0),
    limit: int = Query(FEED_PAGE_LIMIT, ge=1, le=2000),
    severity: str | None = None,
    kind: str | None = None,
    runs: RunStore = Depends(get_runs),
) -> dict[str, Any]:
    """The Feed panel (spec 10.3), paged by cursor rather than by offset.

    Offset paging over a log that is still growing shifts the window under the reader: page
    two of a hundred-entry log is a different hundred entries by the time it is fetched. A
    sequence cursor names entries rather than positions, so a reader that has seen up to
    `n` asks for what came after `n` and gets exactly that.
    """
    entries: list[dict[str, Any]] = []
    next_seq = since_seq
    try:
        for event in runs.iter_events(run_id):
            seq = int(event.get("seq", 0))
            if seq <= since_seq:
                continue
            if len(entries) >= limit:
                # Checked before examining the next event so the cursor never claims
                # ground past what this page actually covered.
                break
            # **The cursor advances over every examined entry, filtered or kept.** It used
            # to advance only on matches, which read as harmless and was quadratic in
            # disguise: with `?severity=error` open on a healthy session nothing ever
            # matched, `next_seq` never moved, and each 1 Hz poll re-opened the log and
            # re-parsed every line from the top -- hundreds of MB of JSON per second by
            # hour twelve, on the same thread pool every other request needs. Skipping a
            # non-match permanently is sound because severity is a pure function of kind:
            # an entry this filter rejected today cannot match the same filter tomorrow.
            next_seq = max(next_seq, seq)
            if kind and event.get("kind") != kind:
                continue
            level = _severity(str(event.get("kind", "")))
            if severity and level != severity:
                continue
            entries.append({**event, "severity": level, "source": "engine"})
    except RunNotFound:
        entries = []
    return {"entries": entries, "next_seq": next_seq}


_ERROR_KINDS = frozenset({"REJECT", "RISK_REJECT", "KILL_SWITCH", "RISK_HALT", "LIQUIDATION"})
_WARN_KINDS = frozenset(
    {"CANCEL_TOO_LATE", "AUTO_FLATTEN_FAILED", "NO_QUOTE_FILL", "DEPTH_EXHAUSTED", "EXPIRED"}
)


def _severity(kind: str) -> str:
    """`error` | `warning` | `info` -- the client's vocabulary, spelled identically.

    It said `"warn"` for one release while the client typed, filtered and styled on
    `"warning"`: selecting Warning in the Feed matched nothing forever, and a real
    AUTO_FLATTEN_FAILED rendered with the info glyph. Two vocabularies for one enum is
    the whole wire-shape defect class in miniature; the client's spelling wins because
    it is also the validator's (`Diagnostic.severity`), making one platform-wide set.
    """
    if kind in _ERROR_KINDS:
        return "error"
    if kind in _WARN_KINDS:
        return "warning"
    return "info"


@router.get("/runs/{run_id}/parity")
def run_parity(run_id: int, runs: RunStore = Depends(get_runs)) -> dict[str, Any]:
    """Spec 6.7.1's report, or 404 when no shadow backtest has been run for this session."""
    try:
        return {"parity": runs.read_json(run_id, "parity.json")}
    except RunNotFound:
        raise HTTPException(
            status_code=404,
            detail=(
                f"run {run_id} has no parity report. One is written when its shadow "
                "backtest finishes; a session that crashed without sealing its tape cannot "
                "have one."
            ),
        ) from None


# ---------------------------------------------------------------------- kill switch


@router.get("/kill")
def kill_state(root: Path = Depends(get_root)) -> dict[str, Any]:
    with KillSwitchStore(root) as switch:
        trip = switch.state()
    return {"kill": None if trip is None else trip.to_json()}


@router.post("/kill")
def kill(
    body: KillRequest,
    request: Request,
    root: Path = Depends(get_root),
    runs: RunStore = Depends(get_runs),
) -> dict[str, Any]:
    """Fire the kill switch: stop every session, wipe the keys, then arm (spec 7.1-7.6, 11).

    The order matters. Sessions are asked to stop *first*, because each one cancels its own
    resting orders at the exchange as it shuts down, and arming before asking would leave a
    session refusing to act on a control file it had already read. Arming last is also what
    makes the state on disk mean "everything has been told".

    The wipe sits between them, and it is safe there because a session signs with its own
    copy of the credential and never with this process's `KeySession` -- wiping ours cancels
    nothing and stops nothing. It was absent entirely: spec 11 says "`Disconnect` and the
    kill switch both wipe keys immediately", the confirmation dialog the operator reads
    before clicking promises it in those words, and `/api/exchange/status` went on serving
    the alias and the balance afterwards while the credential stayed signable.
    """
    # Each stop individually guarded, and the failures carried into the response rather
    # than raised: this is the emergency endpoint, and one control file that cannot be
    # written (disk full, a transient Windows lock on `os.replace`) must not 500 out of
    # here with the credential still signable, the switch un-armed, and every session
    # after the failing one never asked to stop.
    stopped: list[int] = []
    stop_failures: list[str] = []
    # `active()` and not a windowed listing: the kill switch must reach *everything* that
    # could still be trading, and "the newest 500 runs" stopped being everything the day a
    # parameter sweep could mint 500 rows while a 48-hour session kept trading past the
    # window's edge. Alive is a status, not a recency (see `RunStore.active`).
    for summary in runs.active():
        if summary.mode in ("paper", "live"):
            try:
                runs.request_stop(
                    summary.id,
                    flatten=body.flatten,
                    reason="kill switch"
                    + (" (close-all)" if body.flatten else " (cancel-only)"),
                )
                stopped.append(summary.id)
            # `Exception`, not `OSError`: any error that escapes this loop 500s out of the
            # emergency endpoint with the credential still signable, the switch un-armed,
            # and every session after the failing one never asked to stop. A `RunNotFound`
            # from a row deleted mid-sweep is precisely as survivable as a locked control
            # file, and the response's `stop_failures` is where both belong.
            except Exception as exc:  # noqa: PERF203, BLE001 - per-item isolation is the point
                log.error("kill switch could not stop session %s: %s", summary.id, exc)
                stop_failures.append(
                    f"session {summary.id}: {type(exc).__name__}: {exc}. Stop it from "
                    f"its own page, or cancel the run."
                )

    # The wipe is individually guarded for the same reason each stop is: if it throws,
    # the switch below must still arm -- an armed switch blocks the next session, which
    # is the one protection left when the credential could not be destroyed.
    try:
        wiped = _wipe_keys(request)
    except Exception as exc:  # noqa: BLE001 - arming must not be skippable
        log.error("kill switch could not wipe the keys: %s", exc)
        wiped = False
        stop_failures.append(
            f"key wipe failed ({type(exc).__name__}: {exc}); restart the platform to "
            f"destroy the credential."
        )

    with KillSwitchStore(root) as switch:
        trip = switch.arm(
            ts_ms=_now_ms(),
            trigger="MANUAL",
            # The *intent* lives in the detail so the record still says what was asked
            # for; the `flattened` field below is reserved for what verifiably happened.
            detail=(body.detail or "fired from the kill switch")
            + (" (close-all requested)" if body.flatten else " (cancel-only)"),
            run_id=stopped[0] if len(stopped) == 1 else None,
            # **False even when a close-all was requested.** `flattened` is the field
            # `require_clear` turns into "The account was left flat" for the next
            # operator, and at this instant no session has acted on the request -- the
            # stop is a control file each worker reads on its next pump, and the flatten
            # it triggers can itself fail against the venue. The store's own contract
            # (see `KillSwitchStore.arm`) is that `flattened` may only catch up from
            # false to true, written by the worker that actually closed the book flat.
            # Recording the *intent* here as fact was how a trip could promise a flat
            # account while a position was still open.
            flattened=False,
        )
    # Reported rather than left to be inferred from `/api/exchange/status`: an action the
    # switch is documented to take is one the operator should be told it took -- and so is
    # one it tried to take and could not.
    return {
        "kill": trip.to_json(),
        "stopped": stopped,
        "keys_wiped": wiped,
        "stop_failures": stop_failures,
    }


@router.post("/kill/unarm")
def kill_unarm(body: UnarmRequest, root: Path = Depends(get_root)) -> dict[str, Any]:
    """Spec 7.6: an explicit action, by a named actor, before anything may start again."""
    with KillSwitchStore(root) as switch:
        trip = switch.unarm(body.actor)
    return {"cleared": None if trip is None else trip.to_json()}


def _now_ms() -> int:
    return int(time.time() * 1000)

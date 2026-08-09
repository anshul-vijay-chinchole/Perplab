"""FastAPI application factory (spec 2.2, 11).

Binds to `127.0.0.1` by default. Spec 11 says exposing the platform to a network is opt-in,
gated behind an explicit flag, and that a password then becomes mandatory **enforced in
code, not documentation** -- so `create_app` refuses to build a network-exposed app without
one rather than logging a warning nobody reads. A trading platform reachable from a hotel
Wi-Fi with no auth is not a configuration mistake, it is an incident.

Two structural rules:

**No strategy code executes in this process.** Validation shells out to
`perplab.strategy.sandbox`. An endpoint that `exec`'d user code to save a keystroke would
put an unbounded loop inside the server the author needs in order to fix it (spec 2.3).

**A localhost bind is not access control, and `_LocalOriginMiddleware` is what makes it one
for browsers.** Any page the operator has open can send a cross-origin `POST` to
`http://127.0.0.1:8756` -- CORS withholds the *response* from the attacker's script, it does
not stop the request from arriving and being executed. `POST /api/strategies/import` is a
`multipart/form-data` submission, a CORS "simple request" that is not even preflighted, and
it ends in `validate_code()` running the uploaded bundle: drive-by code execution from any
tab, with no password because there is none on a loopback bind. The middleware refuses any
state-changing request carrying a foreign `Origin`, and any request claiming a `Host` this
process is not serving. See its docstring for why a missing `Origin` is allowed through.

**Every route is synchronous (`def`, not `async def`).** SQLite calls and the validator's
subprocess both block; declared this way FastAPI runs them in a worker thread and the event
loop stays free. An `async def` route doing blocking work stalls every other request,
which on a single-user platform looks exactly like the app hanging.
"""

from __future__ import annotations

import logging
import os
import secrets
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator, Callable

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware

from perplab import __version__
from perplab.api.routers import data as data_router
from perplab.api.routers import lab as lab_router
from perplab.api.routers import runs as runs_router
from perplab.api.routers import sessions as sessions_router
from perplab.api.routers import settings as settings_router
from perplab.api.routers import strategies as strategies_router
from perplab.store.ingests import IngestStore
from perplab.store.lab import LabNotFound, LabStore
from perplab.store.runs import RunNotFound, RunStatus, RunStore
from perplab.strategy.library import (
    DeleteBlocked,
    LibraryError,
    NameInUse,
    NotFound,
    StrategyLibrary,
)

__all__ = [
    "create_app",
    "DEFAULT_HOST",
    "DEFAULT_PORT",
    "API_HOST_DEFAULTS",
    "DEV_ORIGINS",
    "LOOPBACK_HOSTS",
    "STATE_CHANGING_METHODS",
    "is_local_host",
    "is_local_origin",
]

log = logging.getLogger(__name__)

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8756

API_HOST_DEFAULTS = (DEFAULT_HOST, DEFAULT_PORT)
"""The pair `perplab.cli` duplicates.

`cmd_serve` imports FastAPI lazily so that every other command -- including the collector,
which has to start on a machine where the UI has never been opened -- does not pay for the
dependency tree at import time. That means the CLI cannot import these constants, so it
repeats them, and `tests/unit/test_api.py` asserts the two agree.
"""
DEV_ORIGINS = (
    "http://localhost:5173",
    "http://127.0.0.1:5173",
)
"""Vite's dev server. Listed explicitly rather than allowing `*`: with credentials enabled
a wildcard origin lets any page the browser happens to load talk to the trading platform on
localhost, which is a real and well-known attack on local-only services."""


LOOPBACK_HOSTS = frozenset({"localhost", "::1", "[::1]"})
"""Host names that always mean this machine. `127.0.0.0/8` is handled numerically."""

STATE_CHANGING_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})
"""The methods a cross-origin page can use to *do* something.

`GET` and `HEAD` are left alone deliberately. A cross-origin `GET` cannot be read by the
attacker's script -- that is the part CORS does enforce -- and refusing them would break
`<img>`, the OpenAPI page and every other benign embed for no gain.
"""


def _hostname(value: str) -> str:
    """The host part of a `Host` or `Origin` value: no scheme, no port, lower-cased."""
    host = value.strip().lower()
    if "//" in host:
        host = host.partition("//")[2]
    host = host.partition("/")[0]
    if host.startswith("["):  # [::1]:8756
        return host.partition("]")[0] + "]"
    return host.rpartition(":")[0] if host.count(":") == 1 else host


def is_local_host(value: str) -> bool:
    """Whether `value` names this machine, for DNS-rebinding purposes.

    **A dotted name that is not a loopback literal is refused, and that is the whole rule.**
    DNS rebinding works by pointing a name the attacker controls at `127.0.0.1`: the browser
    then treats `http://evil.example/` as same-origin with the trading platform and reads
    every response. The one thing such a name always is, is *registered* -- and every
    registered name has a dot in it.

    So a single-label host passes: `testserver` (what Starlette's `TestClient` sends), the
    machine's own NetBIOS name, anything an operator typed into their hosts file. None of
    those can be resolved by an attacker's DNS server, because there is no public registry
    to put them in. This is a weaker rule than an exact allow-list and it is the rule that
    can be stated honestly: an exact list would have to carry `testserver` as a production
    exemption, which is the same hole with a name on it.
    """
    host = _hostname(value)
    if not host:
        return False
    if host in LOOPBACK_HOSTS:
        return True
    if host.startswith("127.") and all(part.isdigit() for part in host.split(".")[1:]):
        return True
    return "." not in host


def is_local_origin(origin: str, allowed: frozenset[str]) -> bool:
    """Whether a browser at `origin` may make state-changing calls to this API.

    `allowed` holds the origins configured explicitly -- Vite's dev server, plus anything
    passed as `extra_origins`. Beyond those, any `http`/`https` origin on a loopback host is
    accepted whatever its port: the operator's own tooling moves between ports constantly,
    and a page served *from* loopback is already code running on this machine, so refusing
    it buys nothing while breaking every local proxy the operator might put in front.

    `file://` and extension origins arrive as the literal string `null`, which matches
    nothing here and is refused. That is the intended answer: a `null` origin is precisely
    the case where the request's provenance cannot be established.
    """
    candidate = origin.strip()
    if candidate in allowed:
        return True
    scheme, separator, _ = candidate.partition("://")
    if not separator or scheme.lower() not in ("http", "https"):
        return False
    return is_local_host(candidate)


class _LocalOriginMiddleware(BaseHTTPMiddleware):
    """Refuses cross-origin writes and foreign `Host` headers (finding C3).

    Two checks, against two different attacks:

    **`Origin`, on state-changing methods.** A browser attaches `Origin` to every `POST`,
    `PUT`, `PATCH` and `DELETE` it makes, cross-origin or not -- that is required of it, not
    optional, and unlike `Referer` a page cannot suppress it. So a request arriving with a
    foreign `Origin` is a cross-site request, and it is refused before it reaches a route.
    This is what closes drive-by strategy import, which needed no password, no preflight and
    no cooperation from the operator beyond having a tab open.

    **A request with no `Origin` at all is allowed through.** That is not an oversight and it
    is not a hole a browser can walk into: `curl`, the CLI, the test suite and any other
    non-browser client omit the header, while the browser attack this defends against cannot
    -- it has an origin, and it is required to send it. Rejecting header-less requests would
    break every scripted client to defend against nothing, so the rule is "a *foreign* origin
    is refused", not "an origin is required".

    **`Host`, on every method, when bound to loopback.** DNS rebinding defeats the `Origin`
    check by making the attacker's page *same-origin*: `evil.example` resolves to `127.0.0.1`
    on its second lookup, and the browser then sends `Origin: http://evil.example` and
    `Host: evil.example` -- consistent, same-origin, and pointed at this server. The `Host`
    header is the one thing that still names where the browser thought it was going, so a
    `Host` this process is not serving is refused. Skipped entirely when the app is exposed
    beyond loopback, where the operator legitimately reaches it by a LAN address or hostname
    and spec 11's mandatory password is the control instead.
    """

    def __init__(
        self, app: Any, *, allowed_origins: frozenset[str], check_host: bool
    ) -> None:
        super().__init__(app)
        self._allowed_origins = allowed_origins
        self._check_host = check_host

    async def dispatch(self, request: Request, call_next: Callable[..., Any]) -> Any:
        if self._check_host:
            host = request.headers.get("host", "")
            if host and not is_local_host(host):
                return _refused(
                    f"this server answers on localhost only; the request claimed "
                    f"Host: {host}. A name that resolves here from the outside is DNS "
                    "rebinding, not configuration."
                )
        origin = request.headers.get("origin")
        if (
            request.method in STATE_CHANGING_METHODS
            and origin
            and not is_local_origin(origin, self._allowed_origins)
        ):
            log.warning(
                "refused cross-origin %s %s from %s",
                request.method,
                request.url.path,
                origin,
            )
            return _refused(
                f"cross-origin {request.method} refused: {origin} is not a local origin. "
                "PerpLab executes uploaded strategy code, so a request from another site "
                "is code execution by any page the browser happens to have open."
            )
        return await call_next(request)


def _refused(detail: str) -> JSONResponse:
    return JSONResponse({"detail": detail}, status_code=status.HTTP_403_FORBIDDEN)


class _PasswordMiddleware(BaseHTTPMiddleware):
    """Bearer-token gate, active only when the app is exposed beyond localhost.

    `secrets.compare_digest` rather than `==`, so the comparison does not leak the prefix
    length through timing. Overkill for a single-user tool and free.
    """

    def __init__(self, app: Any, password: str) -> None:
        super().__init__(app)
        # Compared as **bytes**. `secrets.compare_digest` raises `TypeError` on `str`
        # arguments containing non-ASCII, so a perfectly reasonable password with an umlaut
        # in it turned every authenticated request into a 500 -- the operator sees an opaque
        # crash while typing the right password. Encoding both sides removes the whole class.
        self._password = password.encode("utf-8")

    async def dispatch(self, request: Request, call_next: Callable[..., Any]) -> Any:
        if request.method == "OPTIONS" or request.url.path == "/api/health":
            return await call_next(request)
        header = request.headers.get("authorization", "")
        token = header[7:] if header.lower().startswith("bearer ") else ""
        if not secrets.compare_digest(token.encode("utf-8"), self._password):
            return JSONResponse(
                {"detail": "authentication required"},
                status_code=status.HTTP_401_UNAUTHORIZED,
            )
        return await call_next(request)


def create_app(
    root: Path | str = "userdata",
    *,
    host: str = DEFAULT_HOST,
    password: str | None = None,
    extra_origins: tuple[str, ...] = (),
) -> FastAPI:
    """Build the API. `host` is passed in only so the auth rule can be enforced here."""
    root = Path(root)
    exposed = host not in ("127.0.0.1", "localhost", "::1")
    if exposed and not password:
        raise ValueError(
            f"binding to {host} exposes PerpLab beyond this machine, so a password is "
            "mandatory (spec 11). Pass --password, or bind to 127.0.0.1."
        )

    @asynccontextmanager
    async def lifespan(instance: FastAPI) -> AsyncIterator[None]:
        """Stop orphaned live sessions on startup; close the SQLite handle on shutdown.

        **Startup: a live session outliving the API that spawned it is an orphan and is
        told to stop.** `app.state.keys` is always `None` in a fresh process, so any live
        run still going at this moment is signing with a credential whose parent copy died
        with the previous process -- the UI reads "not connected" while a worker keeps
        placing signed orders, which is exactly the state `exchange_disconnect` exists to
        prevent. Disconnect, the kill switch and idle expiry all couple "the parent copy is
        gone" to "the sessions are stopped"; a restart is the fourth way the copy goes, and
        it used to be the one with no coupling. Cancel-only, positions left open, the same
        rule as every other credential-loss stop (spec 11).

        The lifespan protocol rather than `@app.on_event("shutdown")`, which FastAPI has
        deprecated. Worth doing properly rather than silencing: an unclosed WAL connection
        leaves `-wal` and `-shm` files beside the database, and the next open has to
        recover from them.
        """
        startup_runs: RunStore | None = getattr(instance.state, "runs", None)
        if startup_runs is not None:
            reason = (
                "the API process restarted, so the exchange connection this live session "
                "was started under no longer exists (spec 11). The session is stopped "
                "cancel-only; positions are left open deliberately and still need "
                "attention. Reconnect the exchange and start a new session."
            )
            for summary in startup_runs.list(limit=500):
                if summary.mode == "live" and summary.status not in RunStatus.TERMINAL:
                    try:
                        startup_runs.request_stop(summary.id, flatten=False, reason=reason)
                        log.warning(
                            "stopped orphaned live session %s at startup", summary.id
                        )
                    except OSError:  # noqa: PERF203 - one bad write must not skip the rest
                        log.exception(
                            "could not stop orphaned live session %s at startup",
                            summary.id,
                        )
        startup_ingests: IngestStore | None = getattr(instance.state, "ingests", None)
        if startup_ingests is not None:
            # **A refresh job still marked running in a fresh process is an orphan, and it
            # is marked `lost` rather than `failed`.** This process holds no handle to that
            # worker, so it cannot poll it and cannot tell whether it died with the previous
            # server or is still downloading -- and those are different facts. `failed`
            # would claim the second one is the first, and would make the row deletable
            # while a live worker was still writing into its directory. `lost` polls as
            # finished, so the UI stops spinning, and says plainly that nobody can tell.
            #
            # Nothing here tries to stop such a worker. The lake-wide refresh lock is what
            # keeps a new refresh from starting beside it -- an orphan that is genuinely
            # alive is heartbeating that lock, and one that is dead lets it age out -- and
            # signalling the recorded pid is the mistake `RunStore.cancel` documents: pids
            # are recycled, and killing an unrelated program is worse than a stale row.
            reason = (
                "the API process restarted while this refresh was in flight, so this "
                "server has no handle to its worker and cannot tell whether it stopped or "
                "is still downloading. Nothing written is lost -- every published "
                "partition is one atomic replace and its receipt is written last -- so "
                "starting the refresh again resumes rather than repeats. If it is still "
                "running it still holds the lake's refresh lock and the next attempt will "
                "say so."
            )
            for job in startup_ingests.list(limit=500):
                if job.status not in RunStatus.TERMINAL:
                    try:
                        startup_ingests.fail(job.id, reason, status=RunStatus.LOST)
                        log.warning("marked orphaned refresh job %s lost at startup", job.id)
                    except OSError:  # noqa: PERF203 - one bad write must not skip the rest
                        log.exception(
                            "could not reconcile orphaned refresh job %s at startup", job.id
                        )
        yield
        # Keys first. A shutdown that failed while closing SQLite would otherwise leave the
        # credential in memory of a process that is on its way out, and spec 11's whole
        # argument is that the window in which a key exists should be as short as the work
        # requires and not one step longer.
        keys = getattr(instance.state, "keys", None)
        if keys is not None:
            keys.wipe()
            instance.state.keys = None
        library: StrategyLibrary | None = getattr(instance.state, "library", None)
        if library is not None:
            library.close()
        runs: RunStore | None = getattr(instance.state, "runs", None)
        if runs is not None:
            runs.close()
        lab: LabStore | None = getattr(instance.state, "lab", None)
        if lab is not None:
            lab.close()
        ingests: IngestStore | None = getattr(instance.state, "ingests", None)
        if ingests is not None:
            ingests.close()

    app = FastAPI(
        lifespan=lifespan,
        title="PerpLab",
        version=__version__,
        description=(
            "Local API for the PerpLab research platform. Single-user, localhost by "
            "default. See PERPLAB_SPEC.md."
        ),
        docs_url="/api/docs",
        openapi_url="/api/openapi.json",
    )

    app.state.root = root
    app.state.library = StrategyLibrary(root)
    app.state.runs = RunStore(root)
    app.state.lab = LabStore(root)
    # One instance per process, like `runs` and `lab` and for the same reason: it owns the
    # `Popen` handles of the refresh workers this process launched, and a store built per
    # request would have none -- every crashed refresh would then claim to be running until
    # its heartbeat expired.
    app.state.ingests = IngestStore(root)
    app.state.exposed = exposed
    # Spec 11: the key session lives **only** here, in this process's memory. Attached to app
    # state rather than to a module global so a test app and a serving app cannot share one,
    # and so the lifespan below can guarantee it is wiped on the way out.
    app.state.keys = None
    app.state.endpoint = None
    app.state.drift_ms = None

    # Order matters and is the reverse of what it reads like: `add_middleware` *prepends*,
    # so the last one added is the outermost. CORS has to be outside the password gate, or
    # the 401 goes back without CORS headers and the browser reports an opaque network
    # failure instead of "authentication required" — leaving the frontend unable to tell a
    # wrong password from a server that is down. The origin guard sits between them: outside
    # every route, so no handler runs for a cross-site write, and inside CORS for the same
    # reason the password gate is — a 403 the browser cannot read is a 403 the frontend
    # cannot report, and the dev server's origin is one of the ones this allows.
    if password:
        app.add_middleware(_PasswordMiddleware, password=password)
    app.add_middleware(
        _LocalOriginMiddleware,
        allowed_origins=frozenset({*DEV_ORIGINS, *extra_origins}),
        # Only meaningful on a loopback bind. Exposed deliberately, the operator reaches
        # this by a LAN name or address that is neither loopback nor single-label, and the
        # mandatory password above is the control that applies there (spec 11).
        check_host=not exposed,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[*DEV_ORIGINS, *extra_origins],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["Content-Disposition"],
    )

    _install_error_handlers(app)
    app.include_router(strategies_router.router, prefix="/api")
    app.include_router(runs_router.router, prefix="/api")
    # Registered before `_mount_frontend`, which claims `/{path:path}`. Starlette matches in
    # registration order, so a router added afterwards is shadowed by the SPA catch-all and
    # every one of its endpoints returns the app shell with a 200.
    app.include_router(sessions_router.router, prefix="/api")
    app.include_router(lab_router.router, prefix="/api")
    app.include_router(settings_router.router, prefix="/api")
    app.include_router(data_router.router, prefix="/api")

    @app.get("/api/health", tags=["system"])
    def health() -> dict[str, Any]:
        payload: dict[str, Any] = {
            "status": "ok",
            "version": __version__,
            "exposed": exposed,
        }
        if not exposed:
            # Health is the one route the password gate exempts, so that a client can tell
            # "wrong password" from "not running". On a loopback-only instance the data
            # directory is a useful thing to see there. On a network-exposed one it is an
            # absolute filesystem path handed to anyone who can reach the port, without
            # authenticating — which is not what an exemption for a liveness check is for.
            payload["root"] = str(root)
        return payload

    _mount_frontend(app)
    return app


def app_from_env() -> FastAPI:
    """Build the app from `PERPLAB_ROOT` / `PERPLAB_HOST` / `PERPLAB_PASSWORD`.

    The factory `perplab serve --reload` points uvicorn at. Reload re-imports the
    application in a fresh process after every source change, so a configured object cannot
    be handed across -- and a factory taking arguments cannot be either, because uvicorn
    calls it with none. The environment is what survives, and reading it here means the
    spec-11 refusal in `create_app` applies to the app that actually serves rather than to
    one built and discarded.
    """
    return create_app(
        os.environ.get("PERPLAB_ROOT", "userdata"),
        host=os.environ.get("PERPLAB_HOST", DEFAULT_HOST),
        password=os.environ.get("PERPLAB_PASSWORD") or None,
    )


def frontend_dist() -> Path:
    """`frontend/dist` in a source checkout, whether or not it has been built."""
    return Path(__file__).resolve().parents[2] / "frontend" / "dist"


def safe_asset(dist: Path, path: str) -> Path | None:
    """Resolve a request path to a file inside `dist`, or `None`.

    A named function rather than three lines inside the route, because the guard is the
    interesting part and inside a closure it cannot be tested. Starlette normalises `..`
    out of a URL before routing, so in practice this never sees one -- which is exactly
    why it has to be here and has to be tested directly. Defence that depends on a layer
    above it continuing to behave is not defence, and the failure mode is a static mount
    that serves the whole disk.

    `resolve()` before the comparison is load-bearing: `dist / "../../secrets"` compares as
    *inside* `dist` until it is resolved, because `Path` does not collapse `..` on its own.
    """
    if not path:
        return None
    root = dist.resolve()
    candidate = (root / path).resolve()
    if not candidate.is_relative_to(root):
        return None
    return candidate if candidate.is_file() else None


def _mount_frontend(app: FastAPI) -> None:
    """Serve the built UI at `/`, if it has been built.

    Mounted last, after `/api`, so an unbuilt frontend never shadows the API. When the
    bundle is missing the root route explains how to build it instead of returning a 404 --
    "not found" for the whole application is the least useful thing to say to someone who
    has just run `perplab serve` for the first time.
    """
    dist = frontend_dist()
    index = dist / "index.html"
    app.state.frontend = dist if index.exists() else None

    if not index.exists():

        @app.get("/", include_in_schema=False)
        def _no_frontend() -> HTMLResponse:
            return HTMLResponse(
                "<h1>PerpLab API is running</h1>"
                "<p>The web UI has not been built yet:</p>"
                "<pre>cd frontend &amp;&amp; npm install &amp;&amp; npm run build</pre>"
                "<p>Then restart <code>perplab serve</code>. "
                'API docs are at <a href="/api/docs">/api/docs</a>.</p>',
                status_code=503,
            )

        return

    app.mount(
        "/assets", StaticFiles(directory=dist / "assets"), name="assets"
    )

    # `response_model=None` because the return is a union of two Response classes and
    # FastAPI would otherwise try to build a Pydantic model from it.
    @app.get("/", include_in_schema=False, response_model=None)
    @app.get("/{path:path}", include_in_schema=False, response_model=None)
    def _spa(path: str = "") -> FileResponse | HTMLResponse | JSONResponse:
        # An unknown path *under `/api`* is a 404, not the app shell. The catch-all only
        # runs for routes that do not exist, so a renamed or mistyped endpoint fell through
        # to it and came back `200 text/html` — and the client then failed parsing JSON,
        # which is exactly the "a blanket answer makes a real failure unreadable" problem
        # the typed error handlers exist to avoid.
        if path == "api" or path.startswith("api/"):
            return JSONResponse(
                {"detail": f"no such endpoint: /{path}"},
                status_code=status.HTTP_404_NOT_FOUND,
            )
        # Single-page app: any other path that is not a real file inside `dist` returns
        # index.html so the client router handles it.
        candidate = safe_asset(dist, path)
        if candidate is not None:
            return FileResponse(candidate)
        return HTMLResponse(index.read_text(encoding="utf-8"))


def _install_error_handlers(app: FastAPI) -> None:
    """Map library exceptions to status codes once, rather than in every route.

    The distinction between 404, 409 and 400 is not decoration: the frontend shows a
    different thing for each, and a blanket 500 would make "you already have a strategy
    with that name" indistinguishable from a crash.
    """

    @app.exception_handler(NotFound)
    def _not_found(_request: Request, exc: NotFound) -> JSONResponse:
        return JSONResponse({"detail": str(exc)}, status_code=status.HTTP_404_NOT_FOUND)

    @app.exception_handler(RunNotFound)
    def _run_not_found(_request: Request, exc: RunNotFound) -> JSONResponse:
        return JSONResponse({"detail": str(exc)}, status_code=status.HTTP_404_NOT_FOUND)

    @app.exception_handler(LabNotFound)
    def _lab_not_found(_request: Request, exc: LabNotFound) -> JSONResponse:
        return JSONResponse({"detail": str(exc)}, status_code=status.HTTP_404_NOT_FOUND)

    @app.exception_handler(NameInUse)
    def _name_in_use(_request: Request, exc: NameInUse) -> JSONResponse:
        return JSONResponse({"detail": str(exc)}, status_code=status.HTTP_409_CONFLICT)

    @app.exception_handler(DeleteBlocked)
    def _delete_blocked(_request: Request, exc: DeleteBlocked) -> JSONResponse:
        return JSONResponse(
            {"detail": str(exc)}, status_code=status.HTTP_412_PRECONDITION_FAILED
        )

    @app.exception_handler(LibraryError)
    def _library_error(_request: Request, exc: LibraryError) -> JSONResponse:
        return JSONResponse({"detail": str(exc)}, status_code=status.HTTP_400_BAD_REQUEST)

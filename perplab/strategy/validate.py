"""Import-time validation (spec 5.5).

Six stages, in order, failing at the first that produces an error:

1. **Parse** — `ast.parse`, so a syntax error is a line and column rather than a traceback.
2. **Scan** — the static rejection rules in `strategy.scan`.
3. **Structure** — exactly one `Strategy` subclass; `on_bar` or `on_tick` present.
4. **Params** — `params` and `requires` well-formed, defaults inside their bounds.
5. **Smoke run** — 500+ synthetic bars in a sandboxed worker under a 10 s timeout.
6. **Determinism probe** — run twice, compare event-log hashes.

Stages 3 to 6 run in a **subprocess**, not here. Stage 2 has to pass before any strategy
code executes, so the ordering is not cosmetic: it is what stops `os.system(...)` at module
level from running during the validation that was supposed to reject it.

**The determinism probe runs the two smoke runs in two separate interpreters with different
`PYTHONHASHSEED` values.** Running twice in one process would prove only that the process
is deterministic, which it is; the failure spec 5.5 names -- "usually a set/dict iteration
order" -- is invisible without varying the hash seed, because a `set` iterates in the same
order all through a single interpreter's life and in a different one the next time the
platform starts. A strategy that iterates a `set` of symbols passes the one-process probe
and produces two different equity curves in production.

**The worker's environment is built from a whitelist, and its memory is bounded.** Both are
about the same thing: what the child gets to have. The environment because the parent's copy
contains `PERPLAB_PASSWORD` and whatever exchange keys the operator exported (spec 11) --
handing it wholesale to a process whose entire job is to execute code that was, moments ago,
merely a file upload, put the credentials one `print(os.environ)` away from a strategy that
never had to defeat a single rule to read them. The memory bound because a 10 s timeout
bounds time and nothing else, and a strategy that allocates until the machine swaps takes
the platform down with it while staying comfortably inside its ten seconds.
"""

from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from perplab.strategy.dryrun import SMOKE_BARS, SMOKE_SEED
from perplab.strategy.scan import Diagnostic, scan_source

__all__ = [
    "SMOKE_TIMEOUT_S",
    "SMOKE_MEMORY_BYTES",
    "WORKER_ENV_ALLOWLIST",
    "ValidationResult",
    "worker_env",
    "validate_code",
]

SMOKE_TIMEOUT_S = 10.0
"""Spec 5.5: "a sandboxed worker with a 10 s timeout"."""

SMOKE_MEMORY_BYTES = 1 << 30
"""1 GiB of committed memory for the validation worker (finding M54).

Chosen from what the worker actually needs: a bare interpreter plus `perplab`, `numpy` and
`pyarrow` settles around 150-250 MB, and the smoke run's synthetic bars add a few more. A
gigabyte is four to six times headroom for legitimate work and still small enough that the
machine does not notice a strategy hitting it.

Enforced as a hard ceiling on Windows and a coarser one elsewhere -- see `_limit_memory`.
"""

WORKER_ENV_ALLOWLIST: frozenset[str] = frozenset(
    {
        # Windows: without SystemRoot the interpreter cannot initialise sockets or load
        # most of the C runtime, and `python -c pass` fails outright. The rest are what a
        # venv interpreter and `tempfile` need to find themselves.
        "SYSTEMROOT",
        "WINDIR",
        "SYSTEMDRIVE",
        "COMSPEC",
        "PATHEXT",
        "LOCALAPPDATA",
        "APPDATA",
        "PROGRAMDATA",
        "NUMBER_OF_PROCESSORS",
        "PROCESSOR_ARCHITECTURE",
        # Both platforms.
        "PATH",
        "TEMP",
        "TMP",
        "TMPDIR",
        "USERPROFILE",
        "HOME",
        "LANG",
        "LC_ALL",
        "LC_CTYPE",
        # Interpreter configuration. Not secrets, and dropping them breaks a checkout that
        # relies on PYTHONPATH to find `perplab` rather than on an installed distribution.
        "PYTHONPATH",
        "PYTHONHOME",
        "PYTHONIOENCODING",
        "PYTHONUTF8",
    }
)
"""Environment variable names the validation worker is allowed to inherit (finding C3).

**A whitelist, because the deny-list version of this cannot be written.** The child used to
get `dict(os.environ)` -- the parent's entire environment, including `PERPLAB_PASSWORD`, any
`BINANCE_API_SECRET` the operator exported into the shell they launched from, and every
other credential that happens to live in a developer's environment. The process receiving it
executes freshly-uploaded strategy code, so the shortest exfiltration path in the whole
platform was `print(os.environ)` in a bundle's module body: no rule defeated, no escape
needed, and on the default localhost bind no password required to submit the bundle either.
Naming the variables to *remove* would mean knowing every secret an operator might have
exported, which is not knowable.

Compared case-insensitively, because Windows environment keys are.
"""

_PROBE_HASH_SEEDS = ("1", "2", "3", "5", "8", "13")
"""Candidate `PYTHONHASHSEED` values for the determinism probe's second run.

Tried in order until one produces a **different string-set iteration order** from the first
run, which the worker reports as `probe_order`.

Fixing on two seeds was not enough, and the failure was worse than it sounds. For a
two-element set the two interpreters agree about half the time, and `{"long", "short"}` --
about the most likely thing a strategy iterates -- happened to be one of the agreeing
pairs. A strategy looping over it passed the probe cleanly and produced two different
equity curves in production. Selecting on the observed order removes the coincidence:
either the probe found two interpreters that really do order strings differently, or it
reports that it could not and says so, instead of passing by default.
"""

_PROBE_BASE_SEED = "0"
"""The first run's seed. `0` disables hash randomisation entirely, which makes it the
stable reference the others are compared against."""


@dataclass(frozen=True)
class ValidationResult:
    """Everything the editor needs to render the outcome of a save."""

    ok: bool
    diagnostics: tuple[Diagnostic, ...] = ()
    class_name: str | None = None
    params: tuple[dict[str, Any], ...] = ()
    requires: dict[str, Any] | None = None
    hooks: tuple[str, ...] = ()
    warmup: int | None = None
    indicator_warmup: int | None = None
    bars: int | None = None
    orders: int | None = None
    event_hash: str | None = None
    stdout: str = ""

    @property
    def errors(self) -> tuple[Diagnostic, ...]:
        return tuple(d for d in self.diagnostics if d.severity == "error")

    @property
    def warnings(self) -> tuple[Diagnostic, ...]:
        return tuple(d for d in self.diagnostics if d.severity == "warning")

    def to_json(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "diagnostics": [d.to_json() for d in self.diagnostics],
            "class_name": self.class_name,
            "params": list(self.params),
            "requires": self.requires,
            "hooks": list(self.hooks),
            "warmup": self.warmup,
            "indicator_warmup": self.indicator_warmup,
            "bars": self.bars,
            "orders": self.orders,
            "event_hash": self.event_hash,
            "stdout": self.stdout,
        }


def _failed(*diagnostics: Diagnostic) -> ValidationResult:
    return ValidationResult(ok=False, diagnostics=tuple(diagnostics))


def validate_code(
    code: str,
    *,
    filename: str = "<strategy>",
    run_sandbox: bool = True,
    bars: int = SMOKE_BARS,
    seed: int = SMOKE_SEED,
    timeout_s: float = SMOKE_TIMEOUT_S,
) -> ValidationResult:
    """Validate strategy source. Returns diagnostics; raises only on platform faults.

    `run_sandbox=False` stops after the two static stages. It exists for the editor's
    as-you-type path, where spawning two interpreters per keystroke would be absurd, and
    for tests that only care about the static rules. A save always runs the full pipeline.
    """
    if not isinstance(code, str):
        raise TypeError(f"code must be str, got {type(code).__name__}")

    # ------------------------------------------------------------------ 1. parse
    try:
        tree = ast.parse(code, filename=filename)
    except SyntaxError as exc:
        line = exc.lineno or 1
        column = exc.offset or 1
        end_line = getattr(exc, "end_lineno", None) or line
        end_column = getattr(exc, "end_offset", None) or column + 1
        return _failed(
            Diagnostic(
                severity="error",
                stage="parse",
                code="syntax-error",
                message=f"{exc.msg}",
                line=line,
                column=max(column, 1),
                end_line=end_line,
                end_column=max(end_column, column + 1),
            )
        )
    except ValueError as exc:
        # `ast.parse` raises ValueError, not SyntaxError, for source containing a null
        # byte. Rare, but it comes from a paste out of a binary file and the bare
        # traceback is unhelpful.
        return _failed(
            Diagnostic(
                severity="error",
                stage="parse",
                code="unparseable",
                message=f"the file could not be parsed: {exc}",
            )
        )

    # ------------------------------------------------------------------- 2. scan
    diagnostics = list(scan_source(tree))
    if any(d.severity == "error" for d in diagnostics):
        # Hard stop. The next stage executes the module, and executing code that was just
        # found to call `subprocess` in order to tell the author not to call `subprocess`
        # would be a memorable way to learn the ordering matters.
        return ValidationResult(ok=False, diagnostics=tuple(diagnostics))

    if not run_sandbox:
        return ValidationResult(ok=True, diagnostics=tuple(diagnostics))

    # ------------------------------------------------------- 3-5. sandboxed execution
    first = _run_worker(
        code, filename=filename, seed=seed, bars=bars, timeout_s=timeout_s,
        hash_seed=_PROBE_BASE_SEED,
    )
    if isinstance(first, Diagnostic):
        return ValidationResult(ok=False, diagnostics=(*diagnostics, first))

    diagnostics.extend(_diagnostics_from(first))
    result = ValidationResult(
        ok=not any(d.severity == "error" for d in diagnostics),
        diagnostics=tuple(diagnostics),
        class_name=first.get("class_name"),
        params=tuple(first.get("params") or ()),
        requires=first.get("requires"),
        hooks=tuple(first.get("hooks") or ()),
        warmup=first.get("warmup"),
        indicator_warmup=first.get("indicator_warmup"),
        bars=first.get("bars"),
        orders=first.get("orders"),
        event_hash=first.get("event_hash"),
        stdout=first.get("stdout", ""),
    )
    if not result.ok or not result.event_hash:
        return result

    # ----------------------------------------------------------- 6. determinism probe
    # Every candidate seed, not one — stopping at the first hash mismatch.
    #
    # Two seeds were not enough, and the reason is arithmetic. Whether two interpreters
    # order a *given* set identically is close to a coin flip, and it depends on the set:
    # knowing that seeds 0 and 1 order a six-element probe differently says nothing about
    # how they order `{"long", "short"}`. That two-element set — about the most likely thing
    # a strategy iterates — is one of the pairs seeds 0 and 1 happen to agree on, so a
    # strategy looping over it passed cleanly and produced two different equity curves in
    # production.
    #
    # Six extra seeds drops the odds of a two-element set slipping through from one in two
    # to about one in sixty-four, and the cost lands in the right place: a nondeterministic
    # strategy usually fails on the first or second, while a clean one pays a few hundred
    # milliseconds on a save that already spawns a worker.
    second: dict[str, Any] | None = None
    for candidate in _PROBE_HASH_SEEDS:
        attempt = _run_worker(
            code, filename=filename, seed=seed, bars=bars, timeout_s=timeout_s,
            hash_seed=candidate,
        )
        if isinstance(attempt, Diagnostic):
            return ValidationResult(
                ok=False,
                diagnostics=(*result.diagnostics, attempt),
                class_name=result.class_name,
            )
        second = attempt
        if attempt.get("event_hash") != result.event_hash:
            break

    assert second is not None
    if second.get("event_hash") != result.event_hash:
        return ValidationResult(
            ok=False,
            diagnostics=(
                *result.diagnostics,
                Diagnostic(
                    severity="error",
                    stage="determinism",
                    code="nondeterministic",
                    message=(
                        "two runs of the same seed produced different event logs "
                        f"({result.event_hash[:12]} vs "
                        f"{str(second.get('event_hash'))[:12]}). Identical inputs must "
                        "produce an identical log (spec 12.1). The two runs differed only "
                        "in PYTHONHASHSEED, so the usual cause is iterating a `set` — its "
                        "order changes between processes. Sort it, or use a list or dict. "
                        "Other causes: `id()`, an object's default `repr`, or reading "
                        "anything outside `ctx`."
                    ),
                ),
            ),
            class_name=result.class_name,
            params=result.params,
            requires=result.requires,
            hooks=result.hooks,
            warmup=result.warmup,
            indicator_warmup=result.indicator_warmup,
            bars=result.bars,
            orders=result.orders,
            event_hash=result.event_hash,
            stdout=result.stdout,
        )

    return result


def _diagnostics_from(payload: dict[str, Any]) -> list[Diagnostic]:
    out: list[Diagnostic] = []
    for raw in payload.get("diagnostics", ()):
        out.append(
            Diagnostic(
                severity=raw.get("severity", "error"),
                stage=raw.get("stage", "smoke"),
                code=raw.get("code", "unknown"),
                message=raw.get("message", ""),
                line=int(raw.get("line", 1)),
                column=int(raw.get("column", 1)),
                end_line=int(raw.get("end_line", raw.get("line", 1))),
                end_column=int(raw.get("end_column", 2)),
            )
        )
    return out


def worker_env(hash_seed: str) -> dict[str, str]:
    """The environment for one validation worker: the whitelist, plus what it is told.

    Built from `os.environ` rather than from nothing so that a machine-specific `PATH` or a
    non-default `TEMP` still reaches the child -- the whitelist decides *which* variables
    travel, not what their values are.
    """
    env = {
        name: value
        for name, value in os.environ.items()
        if name.upper() in WORKER_ENV_ALLOWLIST
    }
    env["PYTHONHASHSEED"] = hash_seed
    # Strategy code may `print`; keeping the worker's own streams unbuffered and
    # separate means a crash message is not lost to buffering when we kill it.
    env["PYTHONUNBUFFERED"] = "1"
    return env


def _limit_memory(process: subprocess.Popen[str]) -> Callable[[], None]:
    """Cap the worker's memory at `SMOKE_MEMORY_BYTES`. Returns a release callable.

    **Windows: a Job Object with `JOB_OBJECT_LIMIT_PROCESS_MEMORY`.** The child is assigned
    to a job whose committed-memory ceiling it cannot raise, so an allocation past the limit
    fails with `MemoryError` inside the strategy rather than by pushing the operator's
    machine into swap. `KILL_ON_JOB_CLOSE` is set as well, which makes the release callable
    a backstop: anything the child spawned dies with the job even if the child itself is
    already gone.

    **Nothing here is allowed to fail the validation.** Every Win32 call is checked and a
    failure returns a no-op release rather than raising -- a validator that refuses to run
    because a job object could not be created has turned a hardening measure into an outage.
    The cost of that choice is stated plainly: when the limit cannot be applied, the run
    proceeds unbounded, exactly as it did before this existed.

    **Elsewhere: `RLIMIT_AS`, at four times the ceiling.** The two numbers differ because
    the two limits count different things. Windows counts committed memory; `RLIMIT_AS`
    counts reserved address space, and `numpy` and `pyarrow` reserve address space by the
    hundreds of megabytes without touching it, so a 1 GiB `RLIMIT_AS` fails the *import*
    rather than the runaway allocation. Four gigabytes still bounds a leak and leaves
    ordinary work alone. This platform is Windows (spec 2.1); the POSIX branch is a
    courtesy, and it is the coarser of the two.
    """
    if sys.platform != "win32":  # pragma: no cover - platform branch
        return lambda: None

    import ctypes
    from ctypes import wintypes

    class _IoCounters(ctypes.Structure):
        _fields_ = [(name, ctypes.c_uint64) for name in (
            "ReadOperationCount", "WriteOperationCount", "OtherOperationCount",
            "ReadTransferCount", "WriteTransferCount", "OtherTransferCount",
        )]

    class _BasicLimits(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", ctypes.c_int64),
            ("PerJobUserTimeLimit", ctypes.c_int64),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class _ExtendedLimits(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", _BasicLimits),
            ("IoInfo", _IoCounters),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]

    job_object_extended_limit_information = 9
    limit_process_memory = 0x00000100
    limit_kill_on_job_close = 0x00002000

    try:
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        # Return and argument types are declared because they must be: the default
        # `restype` is `c_int`, which truncates a 64-bit HANDLE to its low half and hands
        # back a handle that belongs to nothing.
        kernel32.CreateJobObjectW.restype = wintypes.HANDLE
        kernel32.CreateJobObjectW.argtypes = [wintypes.LPVOID, wintypes.LPCWSTR]
        kernel32.SetInformationJobObject.restype = wintypes.BOOL
        kernel32.SetInformationJobObject.argtypes = [
            wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD
        ]
        kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
        kernel32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
        kernel32.CloseHandle.restype = wintypes.BOOL
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]

        job = kernel32.CreateJobObjectW(None, None)
        if not job:
            return lambda: None

        limits = _ExtendedLimits()
        limits.BasicLimitInformation.LimitFlags = (
            limit_process_memory | limit_kill_on_job_close
        )
        limits.ProcessMemoryLimit = SMOKE_MEMORY_BYTES
        ok = kernel32.SetInformationJobObject(
            job,
            job_object_extended_limit_information,
            ctypes.byref(limits),
            ctypes.sizeof(limits),
        )
        if not ok or not kernel32.AssignProcessToJobObject(job, int(process._handle)):
            kernel32.CloseHandle(job)
            return lambda: None

        return lambda: kernel32.CloseHandle(job)
    except (OSError, AttributeError, ValueError):  # pragma: no cover - hardening only
        return lambda: None


def _child_preexec() -> Callable[[], None] | None:
    """`preexec_fn` applying `RLIMIT_AS` on POSIX; `None` on Windows.

    The limit has to be set between `fork` and `exec`, which is what `preexec_fn` is, so
    unlike the Windows job it cannot be applied after the process exists.
    """
    if sys.platform == "win32":
        return None

    try:  # pragma: no cover - platform branch
        import resource
    except ImportError:  # pragma: no cover
        return None

    def apply() -> None:  # pragma: no cover - runs in the child, between fork and exec
        ceiling = SMOKE_MEMORY_BYTES * 4
        soft, hard = resource.getrlimit(resource.RLIMIT_AS)
        if hard != resource.RLIM_INFINITY:
            ceiling = min(ceiling, hard)
        resource.setrlimit(resource.RLIMIT_AS, (ceiling, hard))

    return apply


def _run_worker(
    code: str,
    *,
    filename: str,
    seed: int,
    bars: int,
    timeout_s: float,
    hash_seed: str,
) -> dict[str, Any] | Diagnostic:
    """Run one sandbox pass. Returns its payload, or a `Diagnostic` if the process failed."""
    with tempfile.TemporaryDirectory(prefix="perplab-validate-") as tmp:
        request_path = Path(tmp) / "request.json"
        response_path = Path(tmp) / "response.json"
        request_path.write_text(
            json.dumps(
                {"code": code, "filename": filename, "seed": seed, "bars": bars},
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        # `Popen` rather than `subprocess.run`, because the memory limit needs the process
        # handle and `run` never exposes one. Everything else about the call is unchanged.
        process = subprocess.Popen(  # noqa: S603 - fixed argv, our own interpreter
            [
                sys.executable,
                "-m",
                "perplab.strategy.sandbox",
                "--request",
                str(request_path),
                "--response",
                str(response_path),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=worker_env(hash_seed),
            cwd=tmp,
            preexec_fn=_child_preexec(),  # noqa: PLW1509 - POSIX-only, sets RLIMIT_AS
        )
        release = _limit_memory(process)
        with process:
            try:
                stdout, stderr = process.communicate(timeout=timeout_s)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate()
                return Diagnostic(
                    severity="error",
                    stage="smoke",
                    code="timeout",
                    message=(
                        f"the strategy did not finish {bars} synthetic bars within "
                        f"{timeout_s:.0f} s. The usual cause is an unbounded loop, or work "
                        "inside `on_bar` that scales with the number of bars seen so far — "
                        "over a real range that becomes hours. Indicators keep their own "
                        "state; recomputing over `.series(n)` every bar does not."
                    ),
                )
            finally:
                release()

        if not response_path.exists():
            detail = (stderr or stdout or "").strip()[-2000:]
            return Diagnostic(
                severity="error",
                stage="smoke",
                code="worker-died",
                message=(
                    f"the validation worker exited with code {process.returncode} "
                    "without reporting. This usually means the strategy exhausted memory "
                    "or called something that ends the process."
                    + (f"\n\n{detail}" if detail else "")
                ),
            )

        try:
            return json.loads(response_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            return Diagnostic(
                severity="error",
                stage="smoke",
                code="worker-unreadable",
                message=f"the validation worker wrote an unreadable response: {exc}",
            )

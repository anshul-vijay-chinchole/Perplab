"""Shared backend budgets. Windows jobs bound *commit*, not working-set guesses.

The registry is scheduling/diagnostics; named kernel jobs are the hard enforcement.
No credentials or strategy contents enter the registry. Queued processes heartbeat too.
"""
from __future__ import annotations

import contextlib
import ctypes
import hashlib
import json
import os
import sqlite3
import sys
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable

GiB = 1024 ** 3
MiB = 1024 ** 2
RESEARCH = frozenset({"backtest", "lab", "lab-point", "shadow", "backfill", "query", "cli", "validation"})


class ResourceLimitExceeded(RuntimeError):
    pass


@dataclass(frozen=True)
class ResourcePolicy:
    research_gib: float = 6
    normal_gib: float = 8
    hard_gib: float = 10
    reserve_gib: float = 4
    worker_gib: float = 2
    research_workers: int = 1
    spill_gib: float = 32

    def __post_init__(self) -> None:
        import math
        if any(not math.isfinite(value) for value in asdict(self).values()):
            raise ValueError("resource budgets must be finite")
        if not (0.25 <= self.worker_gib <= self.research_gib <= self.normal_gib <= self.hard_gib <= 10):
            raise ValueError("require worker <= research <= normal <= hard <= 10 GiB")
        if self.worker_gib > 2 or self.research_gib > 6 or self.normal_gib > 8 or self.spill_gib > 32:
            raise ValueError("maximums are 2 GiB per worker, 6 GiB research, 8 GiB normal and 32 GiB temporary disk")
        if self.reserve_gib < 4 or self.research_workers not in (1, 2) or self.spill_gib <= 0:
            raise ValueError("keep at least 4 GiB available; allow one or two research workers")

    @classmethod
    def load(cls, root: Path) -> ResourcePolicy:
        try:
            stored = json.loads((root / "settings.json").read_text(encoding="utf-8"))
            return cls(**{name: stored.get("resource_" + name, default)
                          for name, default in asdict(cls()).items()})
        except FileNotFoundError:
            return cls()
        except (OSError, ValueError, TypeError) as exc:
            # A broken policy must never silently open an unlimited worker.
            raise ResourceLimitExceeded(f"resource policy could not be loaded: {exc}") from exc


def _atomic(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    tmp.write_text(json.dumps(value, ensure_ascii=True, default=str), encoding="utf-8")
    os.replace(tmp, path)


if sys.platform == "win32":
    from ctypes import wintypes

    class _Basic(ctypes.Structure):
        _fields_ = [("user", ctypes.c_int64), ("job_user", ctypes.c_int64),
                    ("flags", wintypes.DWORD), ("min_ws", ctypes.c_size_t),
                    ("max_ws", ctypes.c_size_t), ("processes", wintypes.DWORD),
                    ("affinity", ctypes.c_size_t), ("priority", wintypes.DWORD),
                    ("scheduling", wintypes.DWORD)]

    class _Extended(ctypes.Structure):
        _fields_ = [("basic", _Basic), ("io", ctypes.c_uint64 * 6),
                    ("process_memory", ctypes.c_size_t), ("job_memory", ctypes.c_size_t),
                    ("peak_process", ctypes.c_size_t), ("peak_job", ctypes.c_size_t)]

    class _Memory(ctypes.Structure):
        _fields_ = [("size", wintypes.DWORD), ("load", wintypes.DWORD)] + [
            (name, ctypes.c_uint64) for name in
            ("total", "available", "page_total", "page_available", "virtual", "virtual_available", "extended")]

    class _ProcessMemory(ctypes.Structure):
        _fields_ = [("size", wintypes.DWORD), ("faults", wintypes.DWORD)] + [
            (name, ctypes.c_size_t) for name in
            ("peak_rss", "rss", "peak_paged", "paged", "peak_nonpaged", "nonpaged", "commit", "peak_commit", "private")]

    _kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    for name, args, result in (
        ("CreateJobObjectW", [wintypes.LPVOID, wintypes.LPCWSTR], wintypes.HANDLE),
        ("OpenJobObjectW", [wintypes.DWORD, wintypes.BOOL, wintypes.LPCWSTR], wintypes.HANDLE),
        ("SetInformationJobObject", [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD], wintypes.BOOL),
        ("QueryInformationJobObject", [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD, ctypes.c_void_p], wintypes.BOOL),
        ("AssignProcessToJobObject", [wintypes.HANDLE, wintypes.HANDLE], wintypes.BOOL),
        ("IsProcessInJob", [wintypes.HANDLE, wintypes.HANDLE, ctypes.POINTER(wintypes.BOOL)], wintypes.BOOL),
        ("GetCurrentProcess", [], wintypes.HANDLE),
        ("OpenProcess", [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD], wintypes.HANDLE),
        ("CloseHandle", [wintypes.HANDLE], wintypes.BOOL),
        ("GlobalMemoryStatusEx", [ctypes.c_void_p], wintypes.BOOL),
        ("GetProcessTimes", [wintypes.HANDLE] + [ctypes.c_void_p] * 4, wintypes.BOOL),
    ):
        fn = getattr(_kernel, name)
        fn.argtypes, fn.restype = args, result
    _psapi = ctypes.WinDLL("psapi", use_last_error=True)
    _psapi.GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.c_void_p, wintypes.DWORD]
    _psapi.GetProcessMemoryInfo.restype = wintypes.BOOL


def process_memory(pid: int | None = None) -> dict[str, int]:
    pid = pid or os.getpid()
    if sys.platform == "win32":
        handle = _kernel.OpenProcess(0x1000 | 0x0010, False, pid)
        if not handle:
            return {}
        try:
            mem = _ProcessMemory()
            mem.size = ctypes.sizeof(mem)
            if not _psapi.GetProcessMemoryInfo(handle, ctypes.byref(mem), mem.size):
                return {}
            creation, exit_time, kernel_time, user_time = (ctypes.c_uint64() for _ in range(4))
            _kernel.GetProcessTimes(handle, *(ctypes.byref(t) for t in (creation, exit_time, kernel_time, user_time)))
            return {"private_bytes": int(mem.private), "rss_bytes": int(mem.rss),
                    "peak_bytes": int(mem.peak_commit), "identity": int(creation.value)}
        finally:
            _kernel.CloseHandle(handle)
    try:  # Linux diagnostics; hard aggregate enforcement is intentionally unavailable.
        values = dict(line.split(":", 1) for line in Path(f"/proc/{pid}/status").read_text().splitlines() if ":" in line)
        rss = int(values["VmRSS"].split()[0]) * 1024
        return {"private_bytes": rss, "rss_bytes": rss,
                "peak_bytes": int(values["VmHWM"].split()[0]) * 1024,
                "identity": int(Path(f"/proc/{pid}/stat").read_text().split()[21])}
    except (OSError, KeyError, ValueError):
        return {}


def system_memory() -> dict[str, int]:
    if sys.platform == "win32":
        mem = _Memory()
        mem.size = ctypes.sizeof(mem)
        if not _kernel.GlobalMemoryStatusEx(ctypes.byref(mem)):
            raise OSError(ctypes.get_last_error(), "cannot read system RAM")
        return {"total_bytes": int(mem.total), "available_bytes": int(mem.available)}
    values = dict(line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())
    return {"total_bytes": int(values["MemTotal"].split()[0]) * 1024,
            "available_bytes": int(values["MemAvailable"].split()[0]) * 1024}


def memory_job(name: str | None, *, total: int = 0, process: int = 0,
               handle: Any = None, kill_on_close: bool = False) -> Any:
    """Apply the validator's native limit, now checked and failing closed."""
    if sys.platform != "win32":
        raise ResourceLimitExceeded("hard aggregate memory enforcement requires Windows Job Objects")
    job = _kernel.CreateJobObjectW(None, name)
    if not job:
        raise ResourceLimitExceeded(f"cannot create memory job: Windows error {ctypes.get_last_error()}")
    try:
        limits = _Extended()
        existing = _Extended()
        if name and _kernel.QueryInformationJobObject(job, 9, ctypes.byref(existing), ctypes.sizeof(existing), None):
            # Other launchers cannot relax a ceiling while the shared job is alive.
            if existing.basic.flags & 0x200 and total:
                total = min(total, existing.job_memory)
            if existing.basic.flags & 0x100 and process:
                process = min(process, existing.process_memory)
        limits.basic.flags = (0x200 if total else 0) | (0x100 if process else 0) | (0x2000 if kill_on_close else 0)
        limits.job_memory, limits.process_memory = total, process
        if not _kernel.SetInformationJobObject(job, 9, ctypes.byref(limits), ctypes.sizeof(limits)):
            raise OSError(ctypes.get_last_error(), "cannot set memory limits")
        target = handle if handle is not None else _kernel.GetCurrentProcess()
        member = wintypes.BOOL()
        if not _kernel.IsProcessInJob(target, job, ctypes.byref(member)):
            raise OSError(ctypes.get_last_error(), "cannot verify job membership")
        if not member.value and not _kernel.AssignProcessToJobObject(job, target):
            raise OSError(ctypes.get_last_error(), "cannot assign memory job")
        return job
    except Exception as exc:
        _kernel.CloseHandle(job)
        raise ResourceLimitExceeded(f"memory enforcement failed: {exc}") from exc


def close_job(handle: Any) -> None:
    if sys.platform == "win32" and handle:
        _kernel.CloseHandle(handle)


def job_name() -> str:
    return "Local\\PerpLab-" + hashlib.sha256(str(Path(__file__).resolve().parent.parent).casefold().encode()).hexdigest()[:20]


def job_memory(handle: Any) -> dict[str, int]:
    if sys.platform != "win32" or not handle:
        return {}
    values = (ctypes.c_uint64 * 2)()
    if not _kernel.QueryInformationJobObject(handle, 28, ctypes.byref(values), ctypes.sizeof(values), None):
        raise ResourceLimitExceeded("memory enforcement failed: cannot read native job memory")
    return {"current_bytes": int(values[0]), "peak_bytes": int(values[1])}


def job_limits(handle: Any) -> dict[str, int]:
    if sys.platform != "win32" or not handle:
        return {}
    info = _Extended()
    if not _kernel.QueryInformationJobObject(handle, 9, ctypes.byref(info), ctypes.sizeof(info), None):
        raise ResourceLimitExceeded("memory enforcement failed: cannot read effective limits")
    return {"process_bytes": int(info.process_memory) if info.basic.flags & 0x100 else 0,
            "tree_bytes": int(info.job_memory) if info.basic.flags & 0x200 else 0}


def shared_memory() -> dict[str, dict[str, int]]:
    result = {}
    if sys.platform == "win32":
        for role in ("backend", "research"):
            handle = _kernel.OpenJobObjectW(4, False, job_name() + "-" + role)
            if handle:
                try:
                    result[role] = job_memory(handle)
                finally:
                    close_job(handle)
    return result


def spill_usage(root: Path) -> int:
    total = 0
    for name in ("_duckdb_tmp", "_replay/sorted", "_replay/prepared"):
        for path in (root / name).rglob("*"):
            try:
                if path.is_file():
                    total += path.stat().st_size
            except FileNotFoundError:
                pass
    return total


def _registry(root: Path) -> sqlite3.Connection:
    # Alternate CLI data roots cannot bypass the platform's shared budget.
    root = Path(__file__).resolve().parent.parent / "userdata" / "_resources"
    root.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(root / "resources.sqlite3", timeout=10)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("CREATE TABLE IF NOT EXISTS leases (pid INTEGER PRIMARY KEY, identity INTEGER, updated REAL, state TEXT)")
    return con


def _read_leases(con: sqlite3.Connection) -> list[dict[str, Any]]:
    result = []
    for pid, identity, updated, state in con.execute("SELECT * FROM leases"):
        mem = process_memory(pid)
        if not mem or mem.get("identity") != identity:
            con.execute("DELETE FROM leases WHERE pid=?", (pid,))
            continue
        row = json.loads(state)
        row.update(mem)
        row["updated"] = updated
        result.append(row)
    return result


def admission_reason(policy: ResourcePolicy, leases: list[dict[str, Any]], available: int,
                     allowance: int, *, nested: bool = False,
                     backend_bytes: int = 0, research_bytes: int = 0) -> str | None:
    active = [r for r in leases if r.get("status") == "running" and r.get("role") in RESEARCH]
    if any(r.get("warning") for r in leases):
        return "a worker is at its memory warning threshold"
    if research_bytes >= policy.research_gib * GiB * .8:
        return "the shared research job is at its memory warning threshold"
    if not nested and sum(not r.get("nested", False) for r in active) >= policy.research_workers:
        return "waiting for a research worker slot"
    if sum(r["allowance_bytes"] for r in active) + allowance > policy.research_gib * GiB:
        return "waiting for the shared research budget"
    # Subtract allocations still promised to running jobs as well as the new allowance.
    promised = sum(max(0, r["allowance_bytes"] - r.get("private_bytes", 0)) for r in active)
    total = max(backend_bytes, sum(r.get("private_bytes", 0) for r in leases))
    if total + promised + allowance > policy.normal_gib * GiB:
        return "waiting for backend memory headroom"
    if available - promised - allowance < policy.reserve_gib * GiB:
        return "waiting to keep 4 GiB of system RAM available"
    return None


def snapshot(root: Path) -> dict[str, Any]:
    policy = ResourcePolicy.load(root)
    with contextlib.closing(_registry(root)) as con, con:
        leases = _read_leases(con)
    native = shared_memory()
    return {"policy": asdict(policy), "system": system_memory(), "processes": leases,
            "native": native,
            "backend_bytes": native.get("backend", {}).get("current_bytes", sum(r.get("private_bytes", 0) for r in leases)),
            "research_bytes": native.get("research", {}).get("current_bytes", sum(r.get("private_bytes", 0) for r in leases if r.get("role") in RESEARCH)),
            "enforcement": "windows_job_objects" if sys.platform == "win32" else "unavailable",
            "enforced": bool(leases) and all(r.get("enforced") for r in leases)}


def reserve_spill(directory: Path) -> int:
    """Reserve a disjoint disk allowance before DuckDB is allowed to spill.

    The query cache and DuckDB divide this allowance; dead PIDs release it. This
    bounds aggregate native spill even while a SQL sort cannot run checkpoints.
    """
    guard = current_guard()
    budget = int((guard.policy.spill_gib if guard else 32) * GiB)
    quota = min(budget, (1 if guard and guard.role in {"api", "session", "collector"} else 8) * GiB)
    with contextlib.closing(_registry(directory)) as con, con:
        con.execute("CREATE TABLE IF NOT EXISTS spill (pid INTEGER PRIMARY KEY, identity INTEGER, quota INTEGER, directory TEXT)")
        con.execute("BEGIN IMMEDIATE")
        rows = list(con.execute("SELECT pid, identity, quota FROM spill"))
        used = 0
        for pid, identity, amount in rows:
            mem = process_memory(pid)
            if not mem or mem.get("identity") != identity:
                con.execute("DELETE FROM spill WHERE pid=?", (pid,))
            elif pid != os.getpid():
                used += amount
        if used + quota > budget:
            raise ResourceLimitExceeded("spill_limit: no shared temporary-disk allowance available")
        con.execute("INSERT OR REPLACE INTO spill VALUES (?, ?, ?, ?)",
                    (os.getpid(), process_memory().get("identity", 0), quota, str(directory)))
    return quota


@contextlib.contextmanager
def cache_reservation(root: Path, desired: int, *, target: Path | None = None):
    """Atomically reserve writes from the cache half of the shared disk budget.

    DuckDB instances divide the other half using reserve_spill. Closed sorted
    inputs and prepared bars are evictable, including caches from alternate roots.
    Concurrent cursors have separate leases; an abandoned writer releases its
    reservation when its process identity disappears.
    """
    import uuid
    guard = current_guard()
    budget = int((guard.policy.spill_gib if guard else 32) * GiB) // 2
    quota = min(desired, budget)
    token = uuid.uuid4().hex
    with contextlib.closing(_registry(root)) as con, con:
        con.execute("CREATE TABLE IF NOT EXISTS cache_roots_v2 (path TEXT PRIMARY KEY)")
        con.execute("CREATE TABLE IF NOT EXISTS cache_files (path TEXT PRIMARY KEY, size INTEGER, touched INTEGER)")
        con.execute("CREATE TABLE IF NOT EXISTS cache_writes_v2 (token TEXT PRIMARY KEY, pid INTEGER, identity INTEGER, quota INTEGER, target TEXT)")
        con.execute("BEGIN IMMEDIATE")
        active = 0
        for key, pid, identity, amount, abandoned in list(con.execute("SELECT * FROM cache_writes_v2")):
            mem = process_memory(pid)
            if not mem or mem.get("identity") != identity:
                if abandoned and Path(abandoned).exists():
                    stat = Path(abandoned).stat()
                    con.execute("INSERT OR REPLACE INTO cache_files VALUES (?, ?, ?)", (abandoned, stat.st_size, stat.st_mtime_ns))
                con.execute("DELETE FROM cache_writes_v2 WHERE token=?", (key,))
            else:
                active += amount
        indexed = con.execute("SELECT 1 FROM cache_roots_v2 WHERE path=?", (str(root.resolve()),)).fetchone()
        if not indexed:
            directory = root
            for name, pattern in (("sorted", "*.parquet"), ("prepared", "*.sqlite3")):
                for path in (Path(directory) / "_replay" / name).glob(pattern):
                    if path.name.startswith("."):
                        continue
                    try:
                        stat = path.stat()
                        con.execute("INSERT OR REPLACE INTO cache_files VALUES (?, ?, ?)", (str(path.resolve()), stat.st_size, stat.st_mtime_ns))
                    except FileNotFoundError:
                        pass
            con.execute("INSERT INTO cache_roots_v2 VALUES (?)", (str(root.resolve()),))
        used = con.execute("SELECT coalesce(sum(size), 0) FROM cache_files").fetchone()[0]
        for raw_path, size in con.execute("SELECT path, size FROM cache_files ORDER BY touched"):
            if used + active + quota <= budget:
                break
            path = Path(raw_path)
            try:
                path.unlink(missing_ok=True)
                con.execute("DELETE FROM cache_files WHERE path=?", (raw_path,))
                if path.parent.name == "prepared":
                    path.with_suffix(".json").unlink(missing_ok=True)
                used -= size
            except OSError:
                continue  # a cache being read on Windows cannot be evicted
        if used + active + quota > budget:
            raise ResourceLimitExceeded("spill_limit: shared replay-cache disk allowance unavailable")
        con.execute("INSERT INTO cache_writes_v2 VALUES (?, ?, ?, ?, ?)",
                    (token, os.getpid(), process_memory().get("identity", 0), quota, str(target.resolve()) if target else None))
    try:
        yield quota
    finally:
        with contextlib.closing(_registry(root)) as con, con:
            con.execute("BEGIN IMMEDIATE")
            con.execute("DELETE FROM cache_writes_v2 WHERE token=?", (token,))
            if target and target.exists():
                stat = target.stat()
                con.execute("INSERT OR REPLACE INTO cache_files VALUES (?, ?, ?)", (str(target.resolve()), stat.st_size, stat.st_mtime_ns))


_current: ResourceGuard | None = None


def checkpoint(stage: str | None = None) -> None:
    if _current is not None:
        if stage:
            _current.stage = stage
        if _current.reason and _current.role in RESEARCH:
            raise ResourceLimitExceeded(_current.reason)


def current_guard() -> ResourceGuard | None:
    return _current


class ResourceGuard:
    def __init__(self, root: Path, role: str, *, directory: Path | None = None,
                 heartbeat: Callable[[], None] | None = None, nested: bool = False):
        self.root, self.role, self.directory = Path(root), role, directory
        self.policy = ResourcePolicy.load(self.root)
        self.allowance = int(self.policy.worker_gib * GiB) if role in RESEARCH or role == "session" else 2 * GiB
        self.heartbeat, self.nested = heartbeat, nested
        self.reason: str | None = None
        self.failure_stage: str | None = None
        self.stage, self.status, self.queue_reason = "admission", "queued", None
        self.warning = False
        self._jobs: list[Any] = []
        self._done = threading.Event()
        self._stop_at: float | None = None
        self._thread: threading.Thread | None = None
        self._spill_checked = 0.0

    def _state(self) -> dict[str, Any]:
        return {"pid": os.getpid(), "role": self.role, "status": self.status,
                "root": str(self.root.resolve()),
                "stage": self.stage, "queue_reason": self.queue_reason,
                "allowance_bytes": self.allowance, "policy": asdict(self.policy),
                "warning": self.warning, "termination_reason": self.reason,
                "failure_stage": self.failure_stage,
                "effective_limits": [job_limits(job) for job in self._jobs],
                "nested": self.nested, "enforced": bool(self._jobs), **process_memory()}

    def _publish(self) -> list[dict[str, Any]]:
        state = self._state()
        with contextlib.closing(_registry(self.root)) as con, con:
            con.execute("BEGIN IMMEDIATE")
            rows = _read_leases(con)
            con.execute("INSERT OR REPLACE INTO leases VALUES (?, ?, ?, ?)",
                        (os.getpid(), state.get("identity", 0), time.time(), json.dumps(state)))
        if self.directory:
            _atomic(self.directory / "resources.json", state)
        if self.heartbeat:
            self.heartbeat()
        return [r for r in rows if r["pid"] != os.getpid()]

    def __enter__(self) -> ResourceGuard:
        global _current
        name = job_name()
        try:
            self._jobs.append(memory_job(name + "-backend", total=int(self.policy.hard_gib * GiB), process=2 * GiB))
            if self.role in RESEARCH:
                self._jobs.append(memory_job(name + "-research", total=int(self.policy.research_gib * GiB)))
            # An unnamed leaf ensures the process tree dies on forced exit. Each
            # descendant has this process limit as well, plus the shared research cap.
            if self.role in RESEARCH or self.role == "session":
                tree_allowance = int(self.policy.research_gib * GiB) if self.role in {"lab", "cli"} else self.allowance
                self._jobs.append(memory_job(None, total=tree_allowance, process=self.allowance, kill_on_close=True))
            _current = self
            while True:
                with contextlib.closing(_registry(self.root)) as con, con:
                    con.execute("BEGIN IMMEDIATE")
                    rows = [r for r in _read_leases(con) if r["pid"] != os.getpid()]
                    native = shared_memory()
                    self.queue_reason = admission_reason(
                        self.policy, rows, system_memory()["available_bytes"], self.allowance,
                        nested=self.nested,
                        backend_bytes=native.get("backend", {}).get("current_bytes", 0),
                        research_bytes=native.get("research", {}).get("current_bytes", 0),
                    ) if self.role in RESEARCH else None
                    self.status = "queued" if self.queue_reason else "running"
                    state = self._state()
                    con.execute("INSERT OR REPLACE INTO leases VALUES (?, ?, ?, ?)",
                                (os.getpid(), state.get("identity", 0), time.time(), json.dumps(state)))
                if self.directory:
                    _atomic(self.directory / "resources.json", state)
                    control = self.directory / "control.json"
                    if control.exists() and json.loads(control.read_text()).get("stop"):
                        raise ResourceLimitExceeded("cancelled while waiting for memory")
                if self.heartbeat:
                    self.heartbeat()
                if not self.queue_reason:
                    break
                self._done.wait(1)
            if self.directory and (self.directory / "spec.json").exists():
                spec_path = self.directory / "spec.json"
                spec = json.loads(spec_path.read_text(encoding="utf-8"))
                spec["resource_policy"] = asdict(self.policy)
                spec["effective_resource_limits"] = self._state()["effective_limits"]
                _atomic(spec_path, spec)
            self.stage = "starting"
            self._thread = threading.Thread(target=self._monitor, daemon=True, name="memory-governor")
            self._thread.start()
            return self
        except BaseException:
            self.__exit__(*sys.exc_info())
            raise

    def request_stop(self, reason: str) -> None:
        if self.reason:
            return
        self.reason, self._stop_at = reason, time.monotonic()
        self.failure_stage = self.stage
        if self.role == "session" and self.directory:
            # Persist before the session sends cancellations. Existing policy is the
            # sole authority for flattening; no unconditional liquidation here.
            from perplab.store.killswitch import KillSwitchStore
            with KillSwitchStore(self.root) as kill:
                kill.arm(trigger="MEMORY_LIMIT", detail=reason, run_id=int(self.directory.name),
                         flattened=False, ts_ms=int(time.time() * 1000))
            _atomic(self.directory / "control.json", {"stop": True, "reason": reason, "flatten": self._flatten()})

    def _flatten(self) -> bool:
        try:
            return bool(json.loads((self.directory / "spec.json").read_text()).get("kill_switch_flatten", False))
        except (OSError, ValueError, TypeError):
            return False

    def _monitor(self) -> None:
        while not self._done.wait(1):
            try:
                rows = self._publish()
                used = process_memory().get("private_bytes", 0)
                self.warning = used >= self.allowance * .8
                if used >= self.allowance * .9 and (self.role in RESEARCH or self.role == "session"):
                    self.request_stop("memory_limit: worker reached 90% of its committed-memory allowance")
                native = shared_memory()
                research = native.get("research", {}).get("current_bytes", 0)
                total = native.get("backend", {}).get("current_bytes", used + sum(r.get("private_bytes", 0) for r in rows))
                free = system_memory()["available_bytes"]
                sensitive_warning = any(r.get("warning") and r.get("role") not in RESEARCH for r in rows)
                if self.role in RESEARCH and research >= self.policy.research_gib * GiB * .9:
                    self.request_stop("memory_limit: shared research job reached 90% of its committed-memory allowance")
                if self.role in RESEARCH and (free < self.policy.reserve_gib * GiB or total >= self.policy.normal_gib * GiB or sensitive_warning):
                    self.request_stop("memory_pressure: research stopped to preserve backend/system headroom")
                elif self.role == "session" and (free < self.policy.reserve_gib * GiB or total >= self.policy.normal_gib * GiB) and not any(r.get("role") in RESEARCH and r.get("status") == "running" for r in rows):
                    self.request_stop("memory_pressure: system reserve exhausted after research stopped")
                if self.role in RESEARCH and time.monotonic() - self._spill_checked > 30:
                    self._spill_checked = time.monotonic()
                    if spill_usage(self.root / "market") > self.policy.spill_gib * GiB:
                        self.request_stop("spill_limit: shared temporary disk budget exhausted")
            except Exception as exc:
                reason = f"resource_monitor_failed: {type(exc).__name__}: {exc}"
                try:
                    self.request_stop(reason)
                except Exception:
                    # Enforcement must still fail closed if its diagnostic store is
                    # unavailable. Sessions also persist their halt from the pump.
                    self.reason = self.reason or reason
                self.failure_stage = self.failure_stage or self.stage
                self._stop_at = self._stop_at or time.monotonic()
            if self._stop_at and (self.role in RESEARCH or self.role == "session") and time.monotonic() - self._stop_at >= (30 if self.role == "session" else 5):
                self.status, self.stage = "failed", "forced_shutdown"
                self.reason += "; cleanup incomplete, remaining exposure requires review" if self.role == "session" else "; process tree did not stop within five seconds"
                try:
                    self._publish()
                finally:
                    os._exit(73)  # closes leaf handle and kills descendants

    def cleanup_complete(self) -> None:
        self._stop_at = None
        self.stage = "session_cleanup_complete"

    def record_failure(self, exc: BaseException) -> None:
        self.failure_stage = self.failure_stage or self.stage
        if self.reason:
            return
        kind = type(exc).__name__.lower()
        if isinstance(exc, MemoryError) or "outofmemory" in kind:
            self.reason = "memory_limit: allocation refused"
        elif isinstance(exc, TimeoutError) or "wall-clock budget" in str(exc):
            self.reason = f"timeout: {exc}"
        elif isinstance(exc, ResourceLimitExceeded):
            self.reason = str(exc)
        else:
            self.reason = f"exception: {type(exc).__name__}: {exc}"

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        global _current
        self._done.set()
        if self._thread:
            self._thread.join(timeout=2)
        if exc is not None:
            self.record_failure(exc)
        self.status = "failed" if exc or self.reason else "finished"
        self.stage = "finished"
        if self.directory:
            _atomic(self.directory / "resources.json", self._state())
        with contextlib.closing(_registry(self.root)) as con, con:
            con.execute("DELETE FROM leases WHERE pid=?", (os.getpid(),))
        _current = None
        # KILL_ON_JOB_CLOSE includes ourselves: leave the leaf handle alive until
        # process exit, while closing named shared handles is safe.
        for job in self._jobs[:-1]:
            close_job(job)


def worker_exit_reason(directory: Path, code: int) -> str:
    try:
        state = json.loads((directory / "resources.json").read_text(encoding="utf-8"))
        if state.get("termination_reason"):
            return str(state["termination_reason"])
    except (OSError, ValueError):
        pass
    return "native_crash" if code < 0 or code & 0xC0000000 == 0xC0000000 else "worker_exited_without_result"

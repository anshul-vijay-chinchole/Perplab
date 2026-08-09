"""Enforce/queue before a worker imports Arrow, DuckDB or strategy code."""
from __future__ import annotations
import importlib
import sys
from pathlib import Path
from perplab.resources import ResourceGuard


def run(module: str, role: str) -> int:
    if len(sys.argv) != 3 or sys.argv[1].startswith("-"):
        return importlib.import_module(module).main(sys.argv[1:])
    root, ident = Path(sys.argv[1]), int(sys.argv[2])
    if role == "lab":
        from perplab.store.lab import LabStore as Store
    elif role == "backfill":
        from perplab.store.ingests import IngestStore as Store
    else:
        from perplab.store.runs import RunStore as Store
    store = Store(root)
    try:
        if role == "backtest":
            spec = store.read_json(ident, "spec.json")
            if spec.get("session_kind") == "shadow":
                role = "shadow"
        with ResourceGuard(root, role, directory=store.directory(ident),
                           heartbeat=lambda: store.heartbeat(ident)):
            return importlib.import_module(module).main([str(root), str(ident)])
    except BaseException as exc:
        detail = f"{type(exc).__name__}: {exc}"
        store.fail(ident, detail)
        print(detail, file=sys.stderr)
        return 1
    finally:
        store.close()

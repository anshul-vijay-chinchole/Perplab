"""FastAPI dependencies.

Separate from `app.py` so routers can import the dependency without importing the module
that imports them. That cycle is not hypothetical -- `app.py` includes the routers at
import time, so a router reaching back into `app.py` fails on the first import with a
"partially initialized module" error that reads like a mystery.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import HTTPException, Request

from perplab.store.lab import LabStore
from perplab.store.runs import RunStore
from perplab.strategy.library import StrategyLibrary

__all__ = ["get_library", "get_runs", "get_lab", "get_root"]


def get_library(request: Request) -> StrategyLibrary:
    """The process-wide library handle, attached to app state at startup."""
    library: StrategyLibrary | None = getattr(request.app.state, "library", None)
    if library is None:  # pragma: no cover - only reachable if the app was built by hand
        raise HTTPException(status_code=500, detail="strategy library not initialised")
    return library


def get_runs(request: Request) -> RunStore:
    """The process-wide run store.

    One instance per server process, deliberately: it holds the `Popen` handles for the
    workers *this* process launched, and that is how a crashed worker is noticed at all
    (`RunStore._reap`). A store built per request would have no handles and every dead
    worker would sit in the UI claiming to be running until its heartbeat expired.
    """
    runs: RunStore | None = getattr(request.app.state, "runs", None)
    if runs is None:  # pragma: no cover - only reachable if the app was built by hand
        raise HTTPException(status_code=500, detail="run store not initialised")
    return runs


def get_lab(request: Request) -> LabStore:
    """The process-wide Lab store. One instance, for `get_runs`'s handle argument."""
    lab: LabStore | None = getattr(request.app.state, "lab", None)
    if lab is None:  # pragma: no cover - only reachable if the app was built by hand
        raise HTTPException(status_code=500, detail="lab store not initialised")
    return lab


def get_root(request: Request) -> Path:
    return Path(request.app.state.root)

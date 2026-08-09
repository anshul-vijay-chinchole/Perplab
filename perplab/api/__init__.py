"""HTTP API (spec 2.1, 2.3).

The API server **never executes strategy code** (spec 2.3). Validation spawns a worker
process; backtests will run in the job pool. That boundary is why an infinite loop in a
strategy costs one worker rather than the editor the author would use to fix it.
"""

from __future__ import annotations

from perplab.api.app import create_app

__all__ = ["create_app"]

"""In-process crash supervision for the collector.

Reconnect logic in `exchange.ws` handles *socket* death. It does nothing for *process*
death -- an unhandled exception in a parser, an OOM, a bug in a library. Without
supervision a crash at hour 4 ends a 72-hour unattended run silently, and the failure is
discovered three days later as missing data with no explanation.

Two layers cover this, and both are needed:

1. **This module** -- a catch-all around the collector coroutine that logs the traceback
   and restarts with exponential backoff. Handles everything short of interpreter death.
2. **An external watchdog** (`scripts/install_watchdog.ps1`) -- a Windows Scheduled Task
   that relaunches the process. Handles interpreter death, OOM kill, and reboot.

Neither writes the restart record. The collector does that itself on startup by reading
the state file left behind by the previous run, so a restart is reported identically
whether it came from this supervisor, the watchdog, or a power cut.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

__all__ = ["supervise", "BACKOFF_INITIAL_S", "BACKOFF_CAP_S", "HEALTHY_RUN_S"]

log = logging.getLogger("perplab.supervisor")

BACKOFF_INITIAL_S = 1.0
BACKOFF_CAP_S = 60.0

HEALTHY_RUN_S = 120.0
"""A run lasting this long is treated as healthy, and backoff resets.

Without this, a collector that crashes once a day would eventually be restarting at the
60-second cap for a fault that has nothing to do with rate limiting. With it, backoff
escalates only for genuine crash *loops* -- repeated failures in quick succession, which
is the case where hammering the exchange would actually make things worse.
"""


async def supervise(
    run_once: Callable[[], Awaitable[None]],
    stop: asyncio.Event,
    *,
    max_restarts: int | None = None,
) -> int:
    """Run `run_once` until `stop` is set, restarting it on any unhandled exception.

    Returns the number of restarts performed, so callers and tests can assert on it.

    A clean return from `run_once` is treated as an intentional shutdown, not a crash.
    `CancelledError` propagates untouched -- swallowing it would make the process
    unkillable, which is a considerably worse failure than the one being guarded against.
    """
    backoff = BACKOFF_INITIAL_S
    restarts = 0

    while not stop.is_set():
        started = asyncio.get_running_loop().time()
        try:
            await run_once()
            return restarts
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - a supervisor that filters is not a supervisor
            ran_for = asyncio.get_running_loop().time() - started
            log.exception("collector crashed after %.1fs; restarting", ran_for)

            if ran_for >= HEALTHY_RUN_S:
                backoff = BACKOFF_INITIAL_S

        if stop.is_set():
            break

        restarts += 1
        if max_restarts is not None and restarts > max_restarts:
            log.error("exceeded %d restarts; giving up", max_restarts)
            raise RuntimeError(f"collector exceeded {max_restarts} restarts")

        log.warning("restart #%d in %.1fs", restarts, backoff)
        try:
            await asyncio.wait_for(stop.wait(), timeout=backoff)
            break
        except TimeoutError:
            pass
        backoff = min(backoff * 2, BACKOFF_CAP_S)

    return restarts

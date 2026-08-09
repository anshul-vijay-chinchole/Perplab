"""What a live session holds that a paper one does not (spec 13, Phase 8).

One object, because the five things it carries are only meaningful together and in one
order. The worker builds them after the preflight has succeeded and before the session
runs, which is the ordering `ExchangeTransport` enforces by refusing construction without
a `PreflightReport` -- see `live.preflight` for why the preflight is not optional and
`live.worker._execute_live` for the one place this is assembled.

This is a container, not a manager. The session owns the tasks (`transport.run`,
`stream.run`, `reconciler.run`) and the shutdown ordering, because the shutdown ordering
*is* the hard part of a live session -- requests must drain and reports must land after
the market feed has already died -- and splitting that across two owners is how a step
gets skipped.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - import cycle; the session imports this module
    from perplab.exchange.keys import KeySession
    from perplab.exchange.signed import SignedRestClient
    from perplab.exchange.userstream import UserDataStream
    from perplab.live.exchange_transport import ExchangeTransport
    from perplab.live.preflight import PreflightReport
    from perplab.live.reconcile import Reconciler

__all__ = ["LiveStack"]


@dataclass(slots=True)
class LiveStack:
    """The live-mode order path, fill path and verification loop, ready to run.

    `keys` is the worker's own `KeySession` copy of the credential the API exported over
    stdin. It has its own idle timer (spec 11), refreshed by every signed request the
    reconciler makes -- which is `exchange.keys`' stated intended reading: a session that
    is verifying its account every sixty seconds keeps its own keys alive for as long as
    it runs, and one that has somehow gone twelve hours without signing anything has lost
    the right to keep them.

    `keys_expire_ms` is the deadline the API computed from *its* copy's remaining idle
    time at spawn. It is checked once, at startup, as a staleness guard on the handoff --
    a worker that somehow started after the parent's copy would already have been wiped
    must not begin trading on it. It is deliberately **not** a running ceiling: the
    worker's own idle timer is strictly more current than a deadline frozen at spawn, and
    a fixed 12-hour cap would end every 48-hour session at hour twelve while its
    credential was in constant, legitimate use.
    """

    keys: KeySession
    client: SignedRestClient
    transport: ExchangeTransport
    stream: UserDataStream
    reconciler: Reconciler
    preflight: PreflightReport
    keys_expire_ms: int | None = None

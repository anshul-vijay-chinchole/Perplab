"""Run analytics (spec 8).

`trades` reconstructs round-trips, `metrics` computes the ratios, `attribution` splits PnL
into the four perp-specific components. Nothing here touches the lake or the ledger: it all
runs on what a completed run recorded, so a metric can be recomputed from a stored run
without re-running it.
"""

from __future__ import annotations

__all__: list[str] = []

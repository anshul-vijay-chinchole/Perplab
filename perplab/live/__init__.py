"""Papertrading and live operation (spec 13, Phase 7).

Nothing in here is a second engine. `engine.backtest.BacktestEngine` runs a paper session
unchanged; what this package supplies is the two things spec 6.1's table permits a mode to
change -- where market data comes from (`feed`) and where orders go (`engine.transport`) --
plus the process that drives them on a wall clock (`session`), the recording that makes a
session reproducible (`engine.tape`), and the shadow backtest that measures whether the
backtester was telling the truth (`shadow`).

The order of that list is the argument. If the paper engine were written separately it would
agree with the backtester on the day it was written and drift thereafter, and the parity
report would be measuring the drift between two codebases rather than the accuracy of one
fill model. Spec 14-I8 records that exact failure from a previous platform.
"""

from __future__ import annotations

__all__: list[str] = ["PRODUCTION_LIVE_ENABLED"]

PRODUCTION_LIVE_ENABLED = False
"""Whether a live session may be started against the production endpoint.

`False` until spec 13's Phase 8 exit criterion has been met on testnet: one real order
placed, filled, streamed back and reconciled to the cent. Until that round trip has been
*observed* -- not unit-tested against hand-written payloads, observed -- every claim the
live path makes about Binance's behaviour is an inference from documentation, and the
place to find out which inferences are wrong is the venue where being wrong costs nothing.

Checked in two places on purpose: `api.routers.sessions.start_session` refuses the request
where the person who made it can read the refusal, and `live.worker` refuses again before
the preflight, because a check that lives only in one caller is a check another caller can
skip. Flip it here, once, when the testnet criterion is met -- both guards read this name.
"""

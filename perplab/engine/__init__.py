"""Execution engines (spec 6).

`clock` fixes the event ordering, `latency` and `fills` model what happens between a
signal and a trade, `executor_base` holds everything the three modes share, `source` is the
one component a mode is allowed to swap, and `backtest` holds the engine every mode runs.

Nothing here is imported eagerly. `backtest` pulls in DuckDB through the query layer, and
the collector -- which has to start on a machine where no backtest has ever run -- would
otherwise pay for it at import time.
"""

from __future__ import annotations

__all__ = ["ENGINE_VERSION"]

ENGINE_VERSION = 5
"""Bumped when a change can move a completed run's numbers.

Recorded in every run manifest (spec 12.1) and compared when a run is repeated. Spec 12.2's
regression rule -- "a stored reference run's metrics must not change across engine versions
without an explicit, reviewed changelog entry" -- needs a version to hang off, and a
version that only exists in a docstring is a version nobody bumps.

**It was not bumped for Phase 5 or Phase 6, and that was a defect.** Replaying the twelve
stored runs in `userdata/runs/` on 2026-08-03 found the seven Phase 4-era ones producing a
different event log under today's engine, which is correct and expected -- Phase 5 changed
how stops and limit orders fill, Phase 6 added the risk layer and order amendment -- but
every one of them recorded `engine_version: 4`, and so does the engine that disagrees with
them. Two of those runs had *already* recorded two different hashes for the same spec before
Phase 7 began, which is proof the behaviour moved while the number did not.

The version is what spec 12.2's rule hangs off, so a version that does not move makes the
rule unfalsifiable: nothing can be flagged as "changed across engine versions" if there is
only ever one version. Bumped to 5 for Phase 7, and the lesson is that this constant belongs
in the exit checklist of every phase that touches `engine/`.
"""

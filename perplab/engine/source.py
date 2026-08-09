"""Where a run's events come from -- the one thing spec 6.1 lets a mode change.

Spec 6.1 gives a table of what differs between backtest, paper and live, and the first row
is *data source*: Parquet replay against Binance WS. Every other row in the shared list --
accounting, fees, funding, margin, filters, risk, the event log, the metrics -- must be one
implementation. So the seam belongs exactly here, and nowhere else:

```
Engine(source=LakeSource())   # a backtest: replay the lake
Engine(source=TapeSource(d))  # a shadow backtest: replay one session's recording
Engine(source=LiveSource())   # a paper session: a wall clock pushes onto the queue
```

`Engine.step` dispatches whatever comes out through the same handlers in all three cases,
which is what makes the parity report of spec 6.7 a measurement of the fill model rather
than a measurement of two codebases that were written to agree and no longer do.

**`prepare` is handed the engine, and that is deliberate.** How far back the lake must be
read is a function of the strategy's frozen indicator set, which does not exist until
`on_start` has run; the tier decides whether book streams are opened at all; and the
open-interest scan happens only for a strategy that asked for it. A source that took a bag
of parameters instead would need the caller to compute all of that first, which is precisely
the duplication this seam exists to avoid.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from perplab.engine.clock import Event
from perplab.strategy.context import FillTier

if TYPE_CHECKING:  # pragma: no cover - import cycle; the engine imports this module
    from perplab.engine.backtest import BacktestEngine

__all__ = ["Prepared", "MarketSource", "LakeSource", "TapeSource", "TapeNotSealed"]


@dataclass(frozen=True, slots=True)
class Prepared:
    """What a source tells the engine before the first event is dispatched."""

    streams: tuple[Iterable[Event], ...]
    """Event streams, each already in total-order (spec 6.2). Registered on the queue."""

    data_start_ms: int
    """The first instant of data the run **actually reads**, warm-up included.

    Not the warm-up start it asked for. The two differ whenever the source does not reach
    back far enough, and the difference is not cosmetic: the dataset manifest is built over
    this range, and demanding partitions that were never ingested discards a finished run
    over a warm-up shortfall it has already flagged.
    """

    total_bars: int
    """Denominator for the progress bar. Zero where the end is not known in advance, which
    is every live session -- the UI shows elapsed time instead of a percentage."""

    funding_times: dict[str, list[int]] = field(default_factory=dict)
    """Settlement instants per symbol, sorted, for `ctx.funding().next_settlement_ms` and
    for `AutoFlatten.before_funding_ms`. Empty leaves both inert, so a live source that
    forgets to populate it silently disables a platform exit -- see `LiveSource`."""

    flags: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()


class MarketSource(Protocol):
    """Supplies a run's market data. The only mode-dependent component in the engine."""

    def prepare(self, engine: BacktestEngine) -> Prepared: ...

    def close(self) -> None:
        """Release handles. Called whether the run finished or raised."""


class LakeSource:
    """Replays the Parquet lake -- what every backtest uses.

    Holds the lake-bound half of what used to be `BacktestEngine.run`: the bar/mark/funding
    load, the book streams, the open-interest scan and the funding schedule. It reads a good
    deal of engine state (the frozen warm-up, the resolved tier, the declared datasets)
    because all of it is decided by the strategy rather than by the caller, and it writes
    only through the `Prepared` it returns.
    """

    def __init__(self) -> None:
        self._engine: BacktestEngine | None = None

    def prepare(self, engine: BacktestEngine) -> Prepared:
        # Imported here rather than at module scope: `backtest` imports this module for the
        # `MarketSource` annotation, so a top-level import back into it is a cycle.
        from perplab.engine.feed import (
            bar_events,
            funding_events,
            load_bars,
            load_funding,
            load_marks,
            mark_events,
            warmup_start_ms,
        )
        from perplab.engine.ticks import depth_events, trade_events

        self._engine = engine
        config = engine.config
        warmup = max(engine.warmup_bars, 0)
        data_start = warmup_start_ms(config.start_ms, config.timeframe, warmup)

        steps, bar_warnings = load_bars(
            engine.root, config.symbols, config.timeframe, data_start, config.end_ms
        )
        marks, mark_warnings = load_marks(
            engine.root, config.symbols, data_start, config.end_ms
        )
        funding, funding_warnings = load_funding(
            engine.root, config.symbols, data_start, config.end_ms
        )

        flags: list[str] = []
        warnings = [*bar_warnings, *mark_warnings, *funding_warnings]
        if bar_warnings or mark_warnings:
            flags.append("GAP_SKIPPED")
        if funding_warnings:
            flags.append("FUNDING_MISSING")

        # The lake may simply not reach far enough back. The run stays *correct* -- the
        # warm-up gate is on the bar count, so nothing trades under-warmed -- but the first
        # trade then happens later than the start date the user chose, and that has to be
        # said rather than discovered by wondering why a January backtest starts in March.
        before_start = sum(1 for step in steps if step.close_time < config.start_ms)
        if before_start < warmup:
            short = warmup - before_start
            flags.append("WARMUP_SHORT")
            warnings.append(
                f"only {before_start} of the {warmup} warm-up bars this strategy "
                f"needs exist before {config.start_ms}; the first {short} bar(s) of "
                f"the requested range are spent warming up and cannot trade. Move the "
                f"start date later, or ingest more history."
            )

        data_start_ms = min(
            steps[0].bars[0].open_time, marks[0].close_time - _MARK_BAR_MS + 1
        )

        engine.open_book_streams(data_start_ms)
        engine.load_open_interest(data_start_ms)
        engine.load_macro(data_start_ms)

        bar_prints = engine.tier is FillTier.BAR_CLOSE
        streams: list[Iterable[Event]] = [
            bar_events(steps, prints=bar_prints),
            mark_events(marks),
            funding_events(funding),
        ]
        if not bar_prints:
            engine._trade_stream = trade_events(
                engine.root, config.symbols, data_start_ms, config.end_ms
            )
            streams.append(engine._trade_stream)
        if engine.depth_as_events:
            engine._depth_stream = depth_events(
                engine.root, config.symbols, data_start_ms, config.end_ms
            )
            streams.append(engine._depth_stream)

        return Prepared(
            streams=tuple(streams),
            data_start_ms=data_start_ms,
            total_bars=len(steps),
            funding_times=_funding_schedule(funding),
            flags=tuple(flags),
            warnings=tuple(warnings),
        )

    def close(self) -> None:
        """Nothing of its own: the engine owns the stream handles it opened."""


class TapeNotSealed(RuntimeError):
    """The session that wrote this tape did not stop cleanly."""


class TapeSource:
    """Replays one paper session's recording -- what makes the shadow backtest honest.

    Spec 6.7.1: *"Every paper/live session records its exact market-data inputs. On session
    end, a backtest is automatically re-run over that window with the same strategy version,
    seed, and params."* The parity report that follows is only a measurement of the fill
    model if the two runs saw the *same market*, and the only way to guarantee that is to
    replay the session's own recording rather than to re-query the lake.

    Re-querying would have been easier and is wrong in three separate ways. The bulk archive
    lags roughly a day, so the range a session just traded is not in it yet. The collector's
    lake is downsampled and de-duplicated on its own schedule, so it is not what the session
    saw either. And a WebSocket delivers what it delivers -- a dropped frame the session
    genuinely never received would reappear in a lake replay, and the parity report would
    blame the fill model for a difference that was really a missing observation.

    **Everything comes from the tape, nothing is recomputed.** The fill tier, the warm-up and
    the data start are read from `meta.json`. In particular the tier must *not* be re-derived
    with `tiers.resolve_tier`: that function judges coverage from the lake, the lake does not
    yet contain this window, and a shadow demoted to `BAR_CLOSE` would be compared against a
    `BOOK_WALK` session and the difference reported as fill-model divergence. That is the
    single most likely way this report gets quietly ruined.
    """

    def __init__(self, run_dir: Path | str, *, allow_unsealed: bool = False) -> None:
        from perplab.engine.tape import TapeReader

        self._reader = TapeReader(run_dir)
        self._allow_unsealed = allow_unsealed

    @property
    def reader(self) -> Any:
        return self._reader

    def prepare(self, engine: BacktestEngine) -> Prepared:
        meta = self._reader.meta()
        flags: list[str] = []
        warnings: list[str] = []

        if not self._reader.sealed:
            if not self._allow_unsealed:
                raise TapeNotSealed(
                    f"the tape in {self._reader.directory} has no seal, so the session that "
                    "wrote it crashed rather than stopping. Its last rows may be truncated "
                    "and its counts are unknown, which would make a parity report a "
                    "comparison against an unknown quantity. Replay it with "
                    "allow_unsealed=True to accept that, and the run will say so."
                )
            flags.append("TAPE_UNSEALED")
            warnings.append(
                "this shadow replayed an unsealed tape: the session that recorded it did "
                "not stop cleanly, so the recording may end mid-observation and the parity "
                "report's final-PnL delta includes whatever was lost."
            )

        late = int(meta.get("counts", {}).get("late_dropped", 0) or 0)
        if late:
            flags.append("TAPE_LATE_FRAMES")
            warnings.append(
                f"{late} frame(s) arrived after the session's reorder window had already "
                "passed their timestamp and were dropped rather than dispatched out of "
                "order. The shadow replays what was dispatched, so both runs agree -- but "
                "neither saw those observations, and a divergence against the lake would."
            )

        funding: dict[str, list[int]] = {}
        for symbol, times in (meta.get("funding_times") or {}).items():
            funding[str(symbol)] = sorted(int(t) for t in times)

        return Prepared(
            streams=(self._reader.events(),),
            data_start_ms=int(meta["data_start_ms"]),
            total_bars=int(meta.get("counts", {}).get("bar_closes", 0) or 0),
            funding_times=funding,
            flags=tuple(flags),
            warnings=tuple(warnings),
        )

    def close(self) -> None:
        """The reader streams line by line and holds no handle between iterations."""


_MARK_BAR_MS = 60_000
"""One mark bar, for turning a close time back into the instant its window opened.

Duplicated from `backtest` rather than imported, because importing it would be a cycle for a
constant that is a property of Binance's `markPriceKlines` cadence and cannot drift.
"""


def _funding_schedule(points: Sequence[Any]) -> dict[str, list[int]]:
    """Each symbol's settlement instants, sorted.

    Not look-ahead. Binance publishes `nextFundingTime` continuously and a strategy trading
    around funding knows it in advance; withholding it would model an information
    disadvantage that does not exist. What is *not* exposed is the next *rate*, which
    genuinely is unknown.
    """
    by_symbol: dict[str, list[int]] = {}
    for point in points:
        by_symbol.setdefault(point.symbol, []).append(point.ts_ms)
    return {symbol: sorted(times) for symbol, times in by_symbol.items()}

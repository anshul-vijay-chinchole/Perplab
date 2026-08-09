"""The reproducibility record (spec 12.1) and the reference data a run resolves.

Spec 12.1: *"Every run stores: strategy code hash + version id, engine version, full param
set, seed, dataset manifest, reference-snapshot ids, fill model tier, latency model config,
risk limits, and the platform's git commit. **Invariant:** identical inputs -> identical
event-log SHA-256."*

That invariant is only meaningful if "inputs" is a closed list, which is what `RunSpec` is:
everything in it can move the answer, and nothing outside it may. A field added to the
engine that changes behaviour and is not recorded here turns the hash from a proof into a
coincidence.

**Reference snapshots, and why a run can proceed without the right one.** Spec 3.2 wants a
2023 backtest validated against the 2023 `exchangeInfo`. Snapshotting on this deployment
began on 2026-08-01, so no historical range has one, and there are only three options:
refuse every historical backtest, use today's filters silently, or use today's filters and
say so. Spec 3.2 names the third -- `FILTERS_APPROXIMATE` -- so that is what happens, with
the snapshot actually used recorded separately from the one that *should* have applied. The
difference matters: BTCUSDT's tick size has been 0.10 throughout, so the approximation is
almost certainly harmless, and "almost certainly" is a judgement the person reading the run
gets to make rather than one the engine makes for them.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence

from perplab.core.margin import BracketTable, load_bracket_snapshot
from perplab.data.reference import latest_snapshot, load_snapshot
from perplab.data.schemas import normalise_symbol, partition_key
from perplab.exchange.filters import (
    SymbolFilters,
    assert_supported_quote,
    parse_exchange_info,
)

__all__ = [
    "SPEC_VERSION",
    "RunSpec",
    "ReferenceResolution",
    "resolve_filters",
    "resolve_brackets",
    "platform_commit",
    "code_hash",
]

SPEC_VERSION = 4
"""On-disk shape of a stored `RunSpec`. Refused rather than best-effort parsed if unknown.

Version 4 adds Phase 7's inputs: `source` (lake or a named tape), `endpoint`,
`reorder_buffer_ms` and `session_kind`. The same forward-direction argument that justified
the version-3 bump applies with more force here -- a version-3 reader handed a paper run's
spec would accept it, silently drop `source`, and replay a session's recording out of the
lake instead, which is precisely the substitution the shadow backtest exists to avoid.

Version 2 added the requested fill tier and moved the fill model's parameters into a
per-tier object -- version 1 carried a bare `slippage_bps`, which is the `BAR_CLOSE` model's
only knob and means nothing to the other three.

Version 3 adds the risk layer: `risk_limits`, `auto_flatten` and `kill_switch_flatten`
(spec 7, and spec 12.1's list of inputs). **The bump matters in the forward direction, not
the backward one.** A version-2 file reads correctly here -- the three fields default to
empty, which is exactly what those runs had. But leaving the number at 2 meant a *new* file
also declared 2, so a Phase 5 reader accepted it without complaint and silently dropped the
limits: a run executed under a 5x cap with a 15% drawdown halt would have been replayed as
unconstrained. Refusing to parse an unknown version is this field's entire job, and it can
only do it if the number moves.

Old files stay readable (`_upgrade_v1`, `_upgrade_v2`). Both upgrades are exact rather than
guesses: every version-1 spec was a `BAR_CLOSE` run, and every version-2 spec ran with no
risk layer, because there was none to run with.
"""


def code_hash(code: str) -> str:
    """SHA-256 of the strategy source, matching what the library stores per version."""
    return hashlib.sha256(code.encode("utf-8")).hexdigest()


def platform_commit() -> str | None:
    """The working tree's commit, or `None` when there is no repository.

    `None` rather than a placeholder. Spec 12.1 asks for the commit so that a run's numbers
    can be tied to the code that produced them; a string like `"unknown"` sitting in that
    field looks like a recorded value and is not one, and a reader comparing two runs would
    conclude the platform had not changed between them.
    """
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=Path(__file__).resolve().parents[2],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):  # pragma: no cover - no git binary
        return None
    if result.returncode != 0:
        return None
    commit = result.stdout.strip()
    return commit or None


@dataclass(frozen=True, slots=True)
class ReferenceResolution:
    """Which snapshot a run actually used, and whether it was the right one."""

    used: str | None
    """Date stem of the snapshot the engine loaded, e.g. `2026-08-02`."""
    in_force: str | None
    """The snapshot that *should* have applied at the range's start, per spec 3.2.

    `None` when nothing that old exists. When `used` and `in_force` differ -- or `in_force`
    is `None` and `used` is not -- the run is approximate and carries the flag.
    """
    flags: tuple[str, ...]


def _resolve_snapshot(
    userdata: Path,
    kind: str,
    start_ms: int,
    missing_flag: str,
) -> tuple[Path | None, ReferenceResolution]:
    """Pick a snapshot for the range, preferring the one in force at its start."""
    start_date = partition_key(start_ms)
    in_force = latest_snapshot(userdata, kind, on_or_before=start_date)
    if in_force is not None:
        return in_force, ReferenceResolution(in_force.stem, in_force.stem, ())

    # Nothing that old. Fall back to the oldest snapshot that exists, which is the closest
    # in time to the range and therefore the least wrong available -- not the newest, which
    # would be the *most* distant from a historical range.
    directory = userdata / "reference" / kind
    candidates = sorted(directory.glob("*.json")) if directory.is_dir() else []
    if not candidates:
        return None, ReferenceResolution(None, None, (missing_flag,))
    chosen = candidates[0]
    return chosen, ReferenceResolution(chosen.stem, None, (missing_flag,))


def resolve_filters(
    userdata: Path | str, symbols: Sequence[str], start_ms: int
) -> tuple[dict[str, SymbolFilters], ReferenceResolution]:
    """Exchange filters for the run's symbols (spec 3.2)."""
    root = Path(userdata)
    path, resolution = _resolve_snapshot(
        root, "exchangeInfo", start_ms, "FILTERS_APPROXIMATE"
    )
    if path is None:
        raise FileNotFoundError(
            "no exchangeInfo snapshot exists in userdata/reference/. Order quantisation, "
            "minimum notional and the tick grid would all have to be guessed at, and spec "
            "3.2 forbids inventing them. Run `perplab snapshot-reference` first."
        )
    parsed = parse_exchange_info(load_snapshot(path))
    wanted = [normalise_symbol(s) for s in symbols]
    missing = [s for s in wanted if s not in parsed]
    if missing:
        raise KeyError(
            f"the {path.stem} exchangeInfo snapshot has no entry for {missing}"
        )
    resolved = {s: parsed[s] for s in wanted}
    # The last point before a symbol can reach the ledger, and the only one where the
    # currency it settles in is still visible. `core.account` has no asset dimension at
    # all, so a non-USDT contract would not fail there -- its PnL would simply be added to
    # a USDT wallet as though the two were the same money.
    for filters in resolved.values():
        assert_supported_quote(filters)
    return resolved, resolution


def resolve_brackets(
    userdata: Path | str, symbols: Sequence[str], start_ms: int
) -> tuple[dict[str, BracketTable], ReferenceResolution]:
    """Leverage bracket tables for the run's symbols (spec 3.6).

    Required, not optional. `Account(require_brackets=True)` refuses to open a position on
    a symbol it cannot price a liquidation for, and that refusal is the point: a run with no
    bracket table never liquidates, so it reports an equity curve with no downside bound --
    which spec 4.2 calls out as the failure mode that looks like a *good* result.
    """
    root = Path(userdata)
    path, resolution = _resolve_snapshot(
        root, "leverageBracket", start_ms, "BRACKETS_APPROXIMATE"
    )
    if path is None:
        raise FileNotFoundError(
            "no leverageBracket snapshot exists in userdata/reference/. Without a bracket "
            "table nothing can be liquidated and the run would report an unbounded "
            "downside (spec 3.6, 4.2)."
        )
    # `load_bracket_snapshot` parses with `parse_float=Decimal`. Reading the file with the
    # default float handling instead would put a binary float into every maintenance rate
    # before it ever reached the accounting layer, and no amount of `Decimal` downstream
    # recovers a number that was wrong on the way in.
    return (
        {
            normalise_symbol(s): load_bracket_snapshot(path, normalise_symbol(s))
            for s in symbols
        },
        resolution,
    )


@dataclass(frozen=True, slots=True)
class RunSpec:
    """Everything that decides a run's answer (spec 12.1). Serialised beside its results."""

    strategy_id: int
    version_id: int
    version_no: int
    strategy_name: str
    code: str
    class_name: str | None
    params: Mapping[str, Any]
    symbols: tuple[str, ...]
    timeframe: str
    start_ms: int
    end_ms: int
    seed: int
    opening_balance: str
    leverage: int
    maker_rate: str
    taker_rate: str
    fee_source: str
    latency: Mapping[str, Any]
    fill_tier: str
    """The tier this run **asked** for. What it executed at is resolved from the lake at run
    time and recorded separately -- see `engine.tiers`. Both belong in spec 12.1's input
    list, but only this one is an input: the other is a fact about the data."""
    fill_model: Mapping[str, Any]
    """The tier's model parameters, keyed by `tier`. See `fills.fill_model_from_json`."""
    liquidation_recovery_pct: str
    timeout_s: float
    engine_version: int
    risk_limits: Mapping[str, Any] = field(default_factory=dict)
    """Spec 12.1 lists risk limits among a run's inputs (spec 7).

    **An empty mapping means "no risk layer", not "the defaults".** Every Phase 4 and
    Phase 5 run stored one, and those runs genuinely had no limits; reading them back as
    spec 7's table would rewrite their history, reporting a run that never rejected an
    order as one that ran under a 5x cap and happened never to reach it. `RiskLimits`
    encodes that reading -- `from_json({})` is `unlimited()`.
    """

    auto_flatten: Mapping[str, Any] = field(default_factory=dict)
    """Platform-enforced exits. Empty means none, on the same argument as `risk_limits`."""

    kill_switch_flatten: bool = False
    """Whether a halt closes positions. Spec 7.3's default is cancel-only."""

    hedge_mode: bool = False
    """Whether the run holds a long **and** a short position per symbol (spec 3.3 extended).

    **The default is exact, not a guess**, for the same reason `margin_mode` below is: the
    ledger was one-way-only until hedge mode was built, so every run recorded before this
    field existed genuinely was one-way, and reading `False` back rewrites no history.

    Account state at the exchange rather than a strategy preference -- Binance holds one
    `dualSidePosition` flag for the whole account -- so `live.preflight.configure_account`
    refuses a session whose ledger and account disagree, in either direction.
    """

    margin_mode: str = "ISOLATED"
    """Which balance pool backs a position (spec 3.7). `ISOLATED` or `CROSSED`.

    **The default is exact, not a guess.** Reading an older spec that predates this field as
    `ISOLATED` rewrites nothing: the ledger has never implemented anything else, so every
    run ever recorded genuinely was isolated. That is the distinction `risk_limits` makes
    above in the other direction -- there, an empty mapping had to mean "no risk layer"
    rather than "the defaults", because those runs really did have no limits.

    Stored as a string rather than a `MarginMode` so the spec stays JSON-shaped like every
    other field here; `core.types.MarginMode` is where the value is interpreted, and
    `CROSSED` is refused there rather than silently priced as isolated.
    """

    source: str = ""
    """Where market data comes from. Empty means the lake; `tape:<run_id>` a recording.

    An input in spec 12.1's sense, and the most consequential one a shadow backtest has: two
    runs identical in every other field but reading different data are not reproductions of
    each other. Stored as a string rather than a flag so the manifest says *which* recording,
    which is what someone re-deriving a parity report a year later needs.
    """

    endpoint: str = ""
    """`testnet`, `production`, or empty for a run that touched no exchange.

    Testnet prices diverge from production, so a paper session's numbers are only comparable
    with another session on the same venue. Recording it stops that being something a reader
    has to remember.
    """

    reorder_buffer_ms: int = 0
    """The live reordering window (spec 6.2 applied to a wall clock). Zero for a backtest.

    It changes what the strategy saw and when, so it belongs in the input list: a session run
    with a 250 ms window and replayed with a 50 ms one would dispatch a different order.
    """

    session_kind: str = ""
    """Empty for a backtest, `paper` for a live session, `shadow` for its replay.

    Read by the worker to decide two things that must not apply to a shadow: the fill tier is
    taken from the tape rather than re-resolved against a lake that does not hold this window
    yet, and no trial is recorded -- a shadow is a re-execution of a session, not a new
    evaluation, and counting it would inflate spec 8.5's multiple-testing `N`.
    """

    @property
    def code_sha256(self) -> str:
        return code_hash(self.code)

    def to_json(self) -> dict[str, Any]:
        return {
            "spec_version": SPEC_VERSION,
            "strategy_id": self.strategy_id,
            "version_id": self.version_id,
            "version_no": self.version_no,
            "strategy_name": self.strategy_name,
            "code_sha256": self.code_sha256,
            "class_name": self.class_name,
            "params": dict(self.params),
            "symbols": list(self.symbols),
            "timeframe": self.timeframe,
            "start_ms": self.start_ms,
            "end_ms": self.end_ms,
            "seed": self.seed,
            "opening_balance": self.opening_balance,
            "leverage": self.leverage,
            "fees": {
                "maker_rate": self.maker_rate,
                "taker_rate": self.taker_rate,
                "source": self.fee_source,
            },
            "latency": dict(self.latency),
            "fill_tier": self.fill_tier,
            "fill_model": dict(self.fill_model),
            "liquidation_recovery_pct": self.liquidation_recovery_pct,
            "timeout_s": self.timeout_s,
            "engine_version": self.engine_version,
            "risk_limits": dict(self.risk_limits),
            "auto_flatten": dict(self.auto_flatten),
            "kill_switch_flatten": self.kill_switch_flatten,
            "hedge_mode": self.hedge_mode,
            "margin_mode": self.margin_mode,
            "source": self.source,
            "endpoint": self.endpoint,
            "reorder_buffer_ms": self.reorder_buffer_ms,
            "session_kind": self.session_kind,
        }

    def to_storage(self) -> dict[str, Any]:
        """`to_json` plus the source itself, for the worker to compile.

        The code travels *with* the spec rather than being re-read from the library at run
        time. A run started against version 7 and executed after version 8 was saved would
        otherwise silently backtest the wrong code, and its `code_sha256` would say so only
        to someone who thought to check.
        """
        return {**self.to_json(), "code": self.code}

    @classmethod
    def from_storage(cls, obj: Mapping[str, Any]) -> RunSpec:
        version = obj.get("spec_version")
        if version == 1:
            obj = _upgrade_v3(_upgrade_v2(_upgrade_v1(obj)))
        elif version == 2:
            obj = _upgrade_v3(_upgrade_v2(obj))
        elif version == 3:
            obj = _upgrade_v3(obj)
        elif version != SPEC_VERSION:
            raise ValueError(
                f"run spec version {version!r} cannot be read by this build (expected "
                f"{SPEC_VERSION})"
            )
        fees = obj.get("fees", {})
        return cls(
            strategy_id=int(obj["strategy_id"]),
            version_id=int(obj["version_id"]),
            version_no=int(obj["version_no"]),
            strategy_name=str(obj["strategy_name"]),
            code=str(obj["code"]),
            class_name=obj.get("class_name"),
            params=dict(obj.get("params", {})),
            symbols=tuple(obj["symbols"]),
            timeframe=str(obj["timeframe"]),
            start_ms=int(obj["start_ms"]),
            end_ms=int(obj["end_ms"]),
            seed=int(obj["seed"]),
            opening_balance=str(obj["opening_balance"]),
            leverage=int(obj["leverage"]),
            maker_rate=str(fees["maker_rate"]),
            taker_rate=str(fees["taker_rate"]),
            fee_source=str(fees.get("source", "explicit")),
            latency=dict(obj.get("latency", {})),
            fill_tier=str(obj["fill_tier"]),
            fill_model=dict(obj["fill_model"]),
            liquidation_recovery_pct=str(obj.get("liquidation_recovery_pct", "0")),
            timeout_s=float(obj.get("timeout_s", 900.0)),
            engine_version=int(obj["engine_version"]),
            risk_limits=dict(obj.get("risk_limits", {})),
            auto_flatten=dict(obj.get("auto_flatten", {})),
            kill_switch_flatten=bool(obj.get("kill_switch_flatten", False)),
            hedge_mode=bool(obj.get("hedge_mode", False)),
            margin_mode=str(obj.get("margin_mode", "ISOLATED")),
            source=str(obj.get("source", "")),
            endpoint=str(obj.get("endpoint", "")),
            reorder_buffer_ms=int(obj.get("reorder_buffer_ms", 0) or 0),
            session_kind=str(obj.get("session_kind", "")),
        )


def _upgrade_v1(obj: Mapping[str, Any]) -> dict[str, Any]:
    """Read a Phase 4 spec file as a Phase 5 one.

    Exact, not approximate. Every version-1 spec was written by an engine that had a single
    fill model at a single tier, so `slippage_bps` *is* the `BAR_CLOSE` model's parameter and
    `BAR_CLOSE` *is* the tier that run asked for. Nothing is guessed; a field that had one
    possible value is being written down.
    """
    upgraded = dict(obj)
    upgraded["spec_version"] = 2
    upgraded["fill_tier"] = "BAR_CLOSE"
    upgraded["fill_model"] = {
        "tier": "BAR_CLOSE",
        "slippage_bps": str(obj.get("slippage_bps", "1.0")),
    }
    return upgraded


def _upgrade_v2(obj: Mapping[str, Any]) -> dict[str, Any]:
    """Read a Phase 4/5 spec file as a Phase 6 one.

    Exact, not approximate. Every version-2 spec was written by an engine with no risk
    layer, so "no limits, no auto-flatten, cancel-only" is what that run had rather than a
    default being applied to it. `RiskLimits.from_json({})` encodes the same reading.
    """
    upgraded = dict(obj)
    upgraded["spec_version"] = 3
    upgraded.setdefault("risk_limits", {})
    upgraded.setdefault("auto_flatten", {})
    upgraded.setdefault("kill_switch_flatten", False)
    return upgraded


def _upgrade_v3(obj: Mapping[str, Any]) -> dict[str, Any]:
    """Read a Phase 4/5/6 spec file as a Phase 7 one.

    Exact, like its predecessors. Every version-3 spec was written before papertrading
    existed, so it read the lake, touched no exchange, buffered nothing and was not a
    session: the empty defaults are what those runs *were*, not a guess about them.
    """
    upgraded = dict(obj)
    upgraded["spec_version"] = SPEC_VERSION
    upgraded.setdefault("source", "")
    upgraded.setdefault("endpoint", "")
    upgraded.setdefault("reorder_buffer_ms", 0)
    upgraded.setdefault("session_kind", "")
    return upgraded

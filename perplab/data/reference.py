"""Dated reference snapshots (spec 3.2, Phase 0 exit criterion).

Exchange filters and leverage brackets change over time. A backtest over 2023 data must
use the 2023 snapshot, not today's -- otherwise it validates orders against rules that
did not exist yet. That only works if snapshots are taken continuously starting now, so
this runs from day one even though nothing consumes the output until Phase 2.

Snapshots are written raw and never overwritten. A snapshot that had already been through
our parser would be worth much less on the day the parser turns out to be wrong.

**Leverage brackets no longer require API keys.** The documented endpoint
(`GET /fapi/v1/leverageBracket`) is signed and returns HTTP 401 `-2014` unsigned, which
was recorded as finding F3 and left the Phase 0 exit criterion half-met. The same table is
served unauthenticated by the endpoint behind Binance's public leverage-bracket page
(`exchange.rest.BRACKETS_PUBLIC_URL`), so brackets are now snapshotted from day one like
everything else and F3 is closed. Nothing in this process holds a credential.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

from perplab.core.margin import validate_bracket_document
from perplab.exchange.filters import parse_exchange_info
from perplab.exchange.rest import BRACKETS_PUBLIC_URL, PublicRestClient

__all__ = [
    "snapshot_exchange_info",
    "snapshot_leverage_brackets",
    "snapshot_reference",
    "latest_snapshot",
    "load_snapshot",
]

log = logging.getLogger("perplab.reference")


def _reference_dir(root: Path, kind: str) -> Path:
    return Path(root) / "reference" / kind


def _write_snapshot(root: Path, kind: str, date: str, payload: Any) -> Path:
    """Write a dated snapshot, refusing to overwrite an existing one.

    Immutability is the point: a run's manifest records which snapshot it used, and that
    reference is worthless if the file behind it can change (spec 4.6).
    """
    directory = _reference_dir(root, kind)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{date}.json"

    if path.exists():
        log.info("%s snapshot for %s already exists; leaving it untouched", kind, date)
        return path

    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    os.replace(tmp, path)
    return path


async def snapshot_exchange_info(root: Path, client: PublicRestClient) -> Path:
    """Fetch and store today's `exchangeInfo`, keyed by the exchange's own server date.

    The exchange's clock decides the date, not ours. Naming a snapshot by local date
    would misfile it whenever the machine's timezone or clock disagrees with Binance,
    which is exactly the situation where you most want the record to be trustworthy.
    """
    payload = await client.exchange_info()
    server_ms = int(payload.get("serverTime", 0))
    date = _utc_date(server_ms)

    path = _write_snapshot(root, "exchangeInfo", date, payload)

    parsed = parse_exchange_info(payload)
    total = len(payload.get("symbols", []))
    if len(parsed) < total:
        log.warning("parsed %d of %d symbols from exchangeInfo", len(parsed), total)
    log.info("exchangeInfo snapshot -> %s (%d symbols)", path, len(parsed))
    return path


def _write_snapshot_text(root: Path, kind: str, date: str, text: str) -> Path:
    """Write a dated snapshot from undecoded text, refusing to overwrite.

    Separate from `_write_snapshot` because bracket rates are bare JSON numbers: routing
    them through `json.loads`/`json.dumps` to store them would either lose the published
    precision (default float parsing) or fail outright (`Decimal` is not JSON-serialisable).
    Writing the exchange's own bytes sidesteps both and is what "byte-faithful" in this
    module's docstring actually requires.
    """
    directory = _reference_dir(root, kind)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{date}.json"

    if path.exists():
        log.info("%s snapshot for %s already exists; leaving it untouched", kind, date)
        return path

    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)
    return path


async def snapshot_leverage_brackets(root: Path, client: PublicRestClient) -> Path:
    """Fetch and store today's leverage bracket table (spec 3.6, Phase 0 exit criterion).

    Validated before it is written. A snapshot is reference data that a backtest years from
    now will price margin against, and an HTTP 200 carrying an error envelope or a renamed
    schema would otherwise be filed as though it were the real table -- discovered only
    when a liquidation price came out wrong. The check is deliberately about *usability*:
    the payload must parse with `parse_float=Decimal` and at least one symbol must survive
    `brackets_from_payload`, including its ordering and contiguity invariants.

    Dated by Binance's clock, not ours, for the reason `snapshot_exchange_info` gives: a
    snapshot misfiled by a local timezone is least trustworthy exactly when it matters
    most. This endpoint carries no `serverTime`, so the futures API's `/time` is asked
    separately.
    """
    text = await client.leverage_brackets_text()

    # Validation lives behind the accounting seam: the rates are bare JSON numbers and must
    # be read with parse_float=Decimal, and `Decimal` is confined by test to the accounting
    # modules (spec 3.1). Handing the raw text over keeps both facts in one place.
    try:
        symbols = validate_bracket_document(text)
    except ValueError as exc:
        raise ValueError(
            f"leverage bracket endpoint ({BRACKETS_PUBLIC_URL}) returned an unusable "
            f"payload: {exc}"
        ) from exc

    date = _utc_date(await client.server_time_ms())
    path = _write_snapshot_text(root, "leverageBracket", date, text)
    log.info(
        "leverageBracket snapshot -> %s (%d symbols, %d bytes)",
        path,
        len(symbols),
        len(text),
    )
    return path


async def snapshot_reference(root: Path, client: PublicRestClient) -> dict[str, Path]:
    """Take every reference snapshot Phase 0 requires, reporting per-kind outcomes.

    Both are attempted even if one fails. They are independent reference sets, and losing
    today's `exchangeInfo` because an undocumented bracket endpoint moved would be a strictly
    worse outcome than recording one and reporting the other as missing -- a day of filters
    that cannot be recovered later, traded for one that can be re-fetched tomorrow.

    Raises only if *both* fail, since that is a connectivity problem rather than a schema one.
    """
    results: dict[str, Path] = {}
    failures: dict[str, Exception] = {}

    for kind, coro in (
        ("exchangeInfo", snapshot_exchange_info),
        ("leverageBracket", snapshot_leverage_brackets),
    ):
        try:
            results[kind] = await coro(root, client)
        except Exception as exc:  # noqa: BLE001 - one kind failing must not stop the other
            failures[kind] = exc
            log.error("%s snapshot failed: %s", kind, exc)

    if not results:
        raise RuntimeError(
            "every reference snapshot failed: "
            + "; ".join(f"{k}: {v}" for k, v in failures.items())
        )
    return results


def latest_snapshot(root: Path, kind: str, on_or_before: str | None = None) -> Path | None:
    """Most recent snapshot at or before `on_or_before` (ISO date), if any.

    Returns None rather than falling back to the newest available. Silently using today's
    filters for a 2023 backtest is precisely what spec 3.2 forbids; the caller is expected
    to flag the run `FILTERS_APPROXIMATE` instead.
    """
    directory = _reference_dir(root, kind)
    if not directory.is_dir():
        return None
    candidates = sorted(p for p in directory.glob("*.json") if p.is_file())
    if on_or_before is not None:
        candidates = [p for p in candidates if p.stem <= on_or_before]
    return candidates[-1] if candidates else None


def load_snapshot(path: Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _utc_date(ms: int) -> str:
    from perplab.data.schemas import partition_key

    return partition_key(ms)

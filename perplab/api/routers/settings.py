"""Settings (spec 10.3): the defaults the platform applies when the user does not choose.

Stored as one JSON file at `userdata/settings.json`, written atomically like every other
artefact. A file rather than a database table because settings are read at form-open and
written on save — there is nothing to query — and a file the user can read in an editor
is the honest representation of "what will my next run default to".

**Settings are defaults, never silent overrides.** The run form initialises from them and
the user can change anything per run; the stored spec records what the run actually used
(spec 12.1). Nothing here reaches into a running session.

The model is `extra="forbid"`: a misspelled key in a PUT is refused with the key named,
because a setting that silently does nothing is the risk-limit failure mode (spec 7)
wearing a preferences dialog.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from perplab.api.deps import get_root
from perplab.core.money import parse_money

router = APIRouter(tags=["settings"])

SETTINGS_FILENAME = "settings.json"


class Settings(BaseModel, extra="forbid"):
    """Everything spec 10.3's Settings tab persists server-side.

    Theme is deliberately absent: it is a property of the browser (`localStorage`),
    not of the account, and a server round-trip per toggle buys nothing.
    """

    default_leverage: int = Field(10, ge=1, le=125)
    maker_rate: str = "0.0002"
    taker_rate: str = "0.0005"
    latency_model: str = Field("lognormal", pattern="^(fixed|lognormal)$")
    submit_ms: int = Field(120, ge=0, le=60_000)

    # Spec 7's table, as the run form's starting point.
    risk_enabled: bool = True
    max_leverage: str | None = "5"
    max_daily_loss_pct: str | None = "0.02"
    max_drawdown_pct: str | None = "0.15"
    max_open_orders: int | None = Field(10, ge=1)
    max_orders_per_minute: int | None = Field(30, ge=1)
    min_equity_pct: str | None = "0.50"

    kill_switch_flatten: bool = False
    """Spec 7.3: cancel-only by default. The kill dialog states the armed behaviour in
    words before anything is sent; this is where that behaviour is chosen."""

    sweep_workers: int | None = Field(None, ge=1, le=64)
    """Walk-forward/sweep pool size. `None` uses cores minus one, leaving a core for the
    collector — see `lab.sweep.default_workers`."""

    def validated_decimals(self) -> None:
        """Refuse unparseable decimal strings at PUT time, naming the field.

        Stored as strings (the platform's exact-decimal convention), so pydantic cannot
        check them numerically; a fee of `"0.00o2"` would otherwise sit in the file until
        a run form crashed on it weeks later.

        **`parse_money`, not a local `Decimal(...)`.** This module is an API router, not
        the ledger, and `test_money.py` enforces that `decimal` is imported only inside
        the accounting seam -- a rule worth keeping, because a router that reaches for
        `Decimal` is one step from doing arithmetic with it. Calling the seam's own parser
        also means these fields are accepted here exactly when the engine will accept them
        later, rather than by a second, nearly-identical rule.
        """
        for name in (
            "maker_rate", "taker_rate", "max_leverage", "max_daily_loss_pct",
            "max_drawdown_pct", "min_equity_pct",
        ):
            value = getattr(self, name)
            if value is None:
                continue
            try:
                parse_money(str(value))
            except ValueError:
                raise ValueError(f"{name} is not a number: {value!r}") from None


def settings_path(root: Path) -> Path:
    return root / SETTINGS_FILENAME


def load_settings(root: Path) -> tuple[Settings, str | None]:
    """The stored settings, plus a complaint if the file could not be used as written.

    **Never raises.** `extra="forbid"` is right for a PUT -- a misspelled key must not be
    silently dropped -- and wrong for a read: a stray key, a hand-edit typo, or a file
    written by a newer build would otherwise turn the whole Settings tab into a 500 with
    no way back except deleting a file the docstring above invites the user to edit.

    The defaults are returned instead, with the reason, so the panel can say what it is
    showing and why. What is *not* done is a partial merge of the good keys: a settings
    object half from disk and half from defaults is a state no one chose, and the user
    could not tell which half they were looking at.
    """
    path = settings_path(root)
    if not path.exists():
        return Settings(), None
    try:
        stored = Settings(**json.loads(path.read_text(encoding="utf-8")))
    except (OSError, ValueError) as exc:
        return Settings(), (
            f"{path.name} could not be read and platform defaults are being shown "
            f"instead; saving from this page will overwrite it. ({exc})"
        )
    try:
        stored.validated_decimals()
    except ValueError as exc:
        return stored, (
            f"{path.name} holds a value that is not a number ({exc}). It is shown as "
            f"stored; fix it here and save."
        )
    return stored, None


@router.get("/settings")
def get_settings(root: Path = Depends(get_root)) -> dict[str, Any]:
    settings, problem = load_settings(root)
    return {"settings": settings.model_dump(), "problem": problem}


@router.put("/settings")
def put_settings(update: Settings, root: Path = Depends(get_root)) -> dict[str, Any]:
    try:
        update.validated_decimals()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    path = settings_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / f".{path.name}.tmp"
    tmp.write_text(
        json.dumps(update.model_dump(), indent=2) + "\n", encoding="utf-8"
    )
    os.replace(tmp, path)
    return {"settings": update.model_dump(), "problem": None}

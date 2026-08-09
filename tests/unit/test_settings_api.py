"""The settings store (spec 10.3): defaults out, atomic writes in, typos refused."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from perplab.api.app import create_app
from perplab.store import db


@pytest.fixture()
def client(tmp_path: Path):
    db.connect(tmp_path).close()
    app = create_app(tmp_path)
    with TestClient(app) as handle:
        yield handle, tmp_path


def test_defaults_are_served_before_anything_is_saved(client) -> None:
    handle, root = client
    settings = handle.get("/api/settings").json()["settings"]
    assert settings["default_leverage"] == 10
    assert settings["kill_switch_flatten"] is False  # spec 7.3: cancel-only default
    assert settings["max_daily_loss_pct"] == "0.02"
    assert not (root / "settings.json").exists()  # nothing written by a read


def test_put_round_trips_and_persists_atomically(client) -> None:
    handle, root = client
    settings = handle.get("/api/settings").json()["settings"]
    settings["default_leverage"] = 3
    settings["kill_switch_flatten"] = True
    settings["max_drawdown_pct"] = "0.10"
    response = handle.put("/api/settings", json=settings)
    assert response.status_code == 200
    stored = json.loads((root / "settings.json").read_text(encoding="utf-8"))
    assert stored["default_leverage"] == 3
    assert handle.get("/api/settings").json()["settings"]["kill_switch_flatten"] is True


def test_a_misspelled_key_is_refused_not_silently_dropped(client) -> None:
    """A setting that silently does nothing is the spec 7 failure mode in a dialog."""
    handle, _ = client
    settings = handle.get("/api/settings").json()["settings"]
    settings["max_dialy_loss_pct"] = "0.05"
    assert handle.put("/api/settings", json=settings).status_code == 422


def test_an_unparseable_decimal_is_refused_naming_the_field(client) -> None:
    handle, _ = client
    settings = handle.get("/api/settings").json()["settings"]
    settings["taker_rate"] = "0.00o5"
    response = handle.put("/api/settings", json=settings)
    assert response.status_code == 400
    assert "taker_rate" in response.json()["detail"]


def test_out_of_range_values_are_refused(client) -> None:
    handle, _ = client
    settings = handle.get("/api/settings").json()["settings"]
    settings["default_leverage"] = 500
    assert handle.put("/api/settings", json=settings).status_code == 422

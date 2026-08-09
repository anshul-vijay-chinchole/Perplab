"""The HTTP surface, and the Phase 3 exit criterion end to end.

`test_the_phase_3_exit_criterion` is the one that matters: spec 13 asks that a strategy can
be written, saved, validated and versioned entirely in-browser, and everything the browser
does it does through these endpoints. If that test passes, the criterion is met at the
server; the frontend is the other half and is exercised by hand.

The field-name assertions exist because `frontend/src/api.ts` mirrors these shapes by hand.
A rename here should break a Python test, not a React runtime.
"""

from __future__ import annotations

import warnings
from pathlib import Path

import pytest

# Starlette's TestClient warns that httpx support is deprecated, and `pyproject.toml` turns
# DeprecationWarning into an error to keep the accounting-seam test loud. Filtered to this
# one message rather than globally, so a real deprecation in our own code still fails.
warnings.filterwarnings(
    "ignore", message=".*httpx.*starlette.testclient.*", category=DeprecationWarning
)

from fastapi.testclient import TestClient  # noqa: E402

from perplab.api.app import API_HOST_DEFAULTS, create_app  # noqa: E402
from perplab.cli import API_DEFAULT_HOST, API_DEFAULT_PORT  # noqa: E402
from perplab.strategy.template import NEW_STRATEGY_TEMPLATE  # noqa: E402


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    with TestClient(create_app(tmp_path)) as test_client:
        yield test_client


class TestSystem:
    def test_health_reports_the_root_and_version(self, client: TestClient) -> None:
        payload = client.get("/api/health").json()
        assert payload["status"] == "ok"
        assert payload["exposed"] is False

    def test_the_cli_defaults_match_the_api_defaults(self) -> None:
        """`cmd_serve` imports FastAPI lazily so the collector does not pay for it, which
        means the host and port literals are duplicated. This is what stops them drifting."""
        assert (API_DEFAULT_HOST, API_DEFAULT_PORT) == API_HOST_DEFAULTS

    def test_binding_beyond_loopback_without_a_password_is_refused(
        self, tmp_path: Path
    ) -> None:
        """Spec 11: exposure is opt-in and a password is then mandatory, "enforced in code,
        not documentation". A trading platform reachable from a hotel network with no auth
        is not a configuration mistake."""
        with pytest.raises(ValueError, match="password is mandatory"):
            create_app(tmp_path, host="0.0.0.0")

    def test_a_password_gates_every_route_but_health(self, tmp_path: Path) -> None:
        app = create_app(tmp_path, host="0.0.0.0", password="hunter2")
        with TestClient(app) as guarded:
            assert guarded.get("/api/health").status_code == 200
            assert guarded.get("/api/strategies").status_code == 401
            assert (
                guarded.get(
                    "/api/strategies", headers={"Authorization": "Bearer hunter2"}
                ).status_code
                == 200
            )
            assert (
                guarded.get(
                    "/api/strategies", headers={"Authorization": "Bearer hunter3"}
                ).status_code
                == 401
            )

    def test_the_template_endpoint_serves_something_that_validates(
        self, client: TestClient
    ) -> None:
        payload = client.get("/api/template").json()
        assert payload["template"] == NEW_STRATEGY_TEMPLATE
        assert client.post("/api/validate", json={"code": payload["template"]}).json()["ok"]


class TestValidateEndpoint:
    def test_quick_mode_returns_static_findings_only(self, client: TestClient) -> None:
        code = "import time\nfrom perplab import Strategy\n"
        payload = client.post(
            "/api/validate", json={"code": code + "x = time.time()", "quick": True}
        ).json()
        assert {d["stage"] for d in payload["diagnostics"]} == {"scan"}

    def test_diagnostics_carry_the_monaco_marker_fields(self, client: TestClient) -> None:
        payload = client.post(
            "/api/validate", json={"code": "def broken(:", "quick": True}
        ).json()
        diagnostic = payload["diagnostics"][0]
        assert set(diagnostic) == {
            "severity",
            "stage",
            "code",
            "message",
            "line",
            "column",
            "end_line",
            "end_column",
        }


class TestStrategyRoutes:
    def test_the_phase_3_exit_criterion(self, client: TestClient) -> None:
        """Write, save, validate, and version a strategy entirely in-browser (spec 13).

        Every step below is one HTTP call the editor makes. Nothing here touches the
        filesystem, a terminal, or a Python import outside the server.
        """
        # Write: create from the template.
        created = client.post("/api/strategies", json={"name": "EMACross"})
        assert created.status_code == 201
        strategy = created.json()["strategy"]
        assert created.json()["validation"]["ok"]
        assert created.json()["version"]["version_no"] == 1

        # Save + validate: an edit that fails validation is still stored.
        broken = client.post(
            f"/api/strategies/{strategy['id']}/save",
            json={"code": "from perplab import Strategy\nclass S(Strategy):\n  pass\n"},
        ).json()
        assert broken["created"]
        assert broken["version"]["version_no"] == 2
        assert not broken["validation"]["ok"]
        assert not broken["version"]["valid"]

        # Save again, valid this time.
        good = client.post(
            f"/api/strategies/{strategy['id']}/save",
            json={"code": NEW_STRATEGY_TEMPLATE, "message": "back to the template"},
        ).json()
        assert good["validation"]["ok"]
        assert good["version"]["version_no"] == 3

        # Version: history is complete, ordered, and diffable.
        versions = client.get(f"/api/strategies/{strategy['id']}/versions").json()["versions"]
        assert [v["version_no"] for v in versions] == [3, 2, 1]
        assert [v["valid"] for v in versions] == [True, False, True]
        assert "code" not in versions[0]  # the list omits code; the detail route has it

        diff = client.get(
            f"/api/strategies/{strategy['id']}/diff", params={"left": 2, "right": 3}
        ).json()["diff"]
        assert "+class MyStrategy(Strategy):" in diff

        single = client.get(f"/api/strategies/{strategy['id']}/versions/1").json()["version"]
        assert single["code"] == NEW_STRATEGY_TEMPLATE

    def test_saving_identical_code_reports_no_change(self, client: TestClient) -> None:
        strategy = client.post("/api/strategies", json={"name": "S"}).json()["strategy"]
        again = client.post(
            f"/api/strategies/{strategy['id']}/save",
            json={"code": strategy["head"]["code"]},
        ).json()
        assert again["created"] is False
        assert again["version"]["version_no"] == 1

    def test_a_duplicate_name_is_a_conflict_not_a_crash(self, client: TestClient) -> None:
        client.post("/api/strategies", json={"name": "S"})
        assert client.post("/api/strategies", json={"name": "s"}).status_code == 409

    def test_an_unknown_strategy_is_a_404(self, client: TestClient) -> None:
        assert client.get("/api/strategies/999").status_code == 404

    def test_a_bad_name_is_a_400_with_the_reason(self, client: TestClient) -> None:
        response = client.post("/api/strategies", json={"name": "bad/slash"})
        assert response.status_code == 400
        assert "not a usable strategy name" in response.json()["detail"]

    def test_archive_hides_and_restores(self, client: TestClient) -> None:
        strategy = client.post("/api/strategies", json={"name": "S"}).json()["strategy"]
        client.post(f"/api/strategies/{strategy['id']}/archive", json={"archived": True})
        assert client.get("/api/strategies").json()["strategies"] == []
        assert len(client.get("/api/strategies?archived=true").json()["strategies"]) == 1
        client.post(f"/api/strategies/{strategy['id']}/archive", json={"archived": False})
        assert len(client.get("/api/strategies").json()["strategies"]) == 1

    def test_delete_needs_the_typed_name(self, client: TestClient) -> None:
        strategy = client.post("/api/strategies", json={"name": "EMACross"}).json()["strategy"]
        wrong = client.request(
            "DELETE", f"/api/strategies/{strategy['id']}", params={"confirm_name": "ema"}
        )
        assert wrong.status_code == 400
        right = client.request(
            "DELETE", f"/api/strategies/{strategy['id']}", params={"confirm_name": "EMACross"}
        )
        assert right.status_code == 200
        assert right.json()["removed"] == {"runs": 0, "versions": 1}
        assert client.get(f"/api/strategies/{strategy['id']}").status_code == 404

    def test_delete_blocked_by_a_run_is_a_412(self, client: TestClient) -> None:
        strategy = client.post("/api/strategies", json={"name": "S"}).json()["strategy"]
        library = client.app.state.library
        with library._db:
            library._db.execute(
                "INSERT INTO runs (strategy_id, version_id, mode, status, created_ms) "
                "VALUES (?, ?, 'BACKTEST', 'DONE', 1)",
                (strategy["id"], strategy["head"]["id"]),
            )
        blocked = client.request(
            "DELETE", f"/api/strategies/{strategy['id']}", params={"confirm_name": "S"}
        )
        assert blocked.status_code == 412
        assert "unreproducible" in blocked.json()["detail"]

    def test_metadata_patch_does_not_mint_a_version(self, client: TestClient) -> None:
        strategy = client.post("/api/strategies", json={"name": "S"}).json()["strategy"]
        patched = client.patch(
            f"/api/strategies/{strategy['id']}",
            json={"name": "Renamed", "tags": ["btc"], "notes": "why"},
        ).json()["strategy"]
        assert patched["name"] == "Renamed"
        assert patched["tags"] == ["btc"]
        assert patched["version_count"] == 1


class TestBundleRoutes:
    def test_export_then_import_round_trips(self, client: TestClient) -> None:
        strategy = client.post(
            "/api/strategies", json={"name": "EMACross", "tags": ["trend"]}
        ).json()["strategy"]

        exported = client.get(f"/api/strategies/{strategy['id']}/export")
        assert exported.status_code == 200
        assert exported.headers["content-type"] == "application/zip"
        assert "EMACross-v1.perplab" in exported.headers["content-disposition"]

        imported = client.post(
            "/api/strategies/import",
            files={"file": ("EMACross-v1.perplab", exported.content, "application/zip")},
        )
        assert imported.status_code == 201
        payload = imported.json()
        assert payload["strategy"]["name"] == "EMACross (2)"
        assert payload["strategy"]["tags"] == ["trend"]
        assert payload["validation"]["ok"]
        assert payload["warnings"] == []

    def test_a_plain_py_upload_is_accepted(self, client: TestClient) -> None:
        source = (
            "from perplab import Strategy\n\n\n"
            "class Idea(Strategy):\n"
            '    requires = {"symbols": ["BTCUSDT"], "timeframe": "1h", "history": 1}\n\n'
            "    def on_bar(self, ctx, bar):\n        pass\n"
        )
        response = client.post(
            "/api/strategies/import",
            files={"file": ("my_idea.py", source.encode(), "text/x-python")},
        )
        assert response.status_code == 201
        assert response.json()["strategy"]["name"] == "my_idea"

    def test_a_junk_upload_is_a_400(self, client: TestClient) -> None:
        response = client.post(
            "/api/strategies/import",
            files={"file": ("x.bin", b"\xff\xfe\x00binary", "application/octet-stream")},
        )
        assert response.status_code == 400

    def test_a_content_disposition_survives_a_spaced_name(self, client: TestClient) -> None:
        """An unquoted header value is truncated at the space by some clients, so the
        filename has both a quoted form and the RFC 5987 UTF-8 form."""
        strategy = client.post(
            "/api/strategies", json={"name": "Mean Reversion (BTC)"}
        ).json()["strategy"]
        headers = client.get(f"/api/strategies/{strategy['id']}/export").headers
        assert 'filename="Mean-Reversion-BTC-v1.perplab"' in headers["content-disposition"]
        assert "filename*=UTF-8''" in headers["content-disposition"]


class TestFrontendMount:
    def test_the_api_is_not_shadowed_by_the_spa_fallback(self, client: TestClient) -> None:
        """The catch-all route returns index.html for unknown paths. Mounted after `/api`,
        so an unknown API path is still a JSON 404 rather than a page."""
        assert client.get("/api/strategies/999").status_code == 404
        assert client.get("/api/strategies/999").headers["content-type"].startswith(
            "application/json"
        )

    def test_the_static_mount_cannot_escape_its_directory(self, client: TestClient) -> None:
        """Over HTTP. Starlette normalises `..` before routing, so this passes even with
        the guard removed — which is why the guard is also tested directly below."""
        for path in ("/../../perplab/cli.py", "/%2e%2e/perplab/cli.py"):
            response = client.get(path)
            assert "PerpLab command line entry point" not in response.text

    def test_the_asset_guard_refuses_a_traversal_it_is_handed_directly(
        self, tmp_path: Path
    ) -> None:
        """The guard itself, not the layer above it.

        Starlette collapses `..` out of a URL before the route sees it, so over HTTP this
        code path never receives one and a mutation deleting the check survives every
        request-level test. Defence that depends on a layer above it continuing to behave
        is not defence — and the failure mode is a static mount that serves the whole disk.

        `resolve()` before the comparison is the load-bearing half: `dist / "../secret"`
        compares as *inside* `dist` until it is resolved, because `Path` does not collapse
        `..` on its own.
        """
        from perplab.api.app import safe_asset

        dist = tmp_path / "dist"
        (dist / "assets").mkdir(parents=True)
        (dist / "assets" / "app.js").write_text("inside", encoding="utf-8")
        (tmp_path / "secret.txt").write_text("outside", encoding="utf-8")

        assert safe_asset(dist, "assets/app.js") == (dist / "assets" / "app.js").resolve()
        assert safe_asset(dist, "../secret.txt") is None
        assert safe_asset(dist, "assets/../../secret.txt") is None
        assert safe_asset(dist, "") is None
        assert safe_asset(dist, "assets") is None  # a directory is not an asset
        assert safe_asset(dist, "missing.js") is None

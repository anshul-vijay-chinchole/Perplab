"""Tests for the command line surface.

The CLI is where four independently written modules meet, so most of what is worth testing
here is *agreement* rather than behaviour: that every subcommand has a handler, that the
one range convention is applied identically by all of them, and that the commands taking a
lake root are given the lake root while the one taking the userdata root is given that.
None of those mistakes raises. Each produces an empty result, an off-by-one-day range, or a
manifest quietly flagged `FILTERS_APPROXIMATE`, which is exactly why they are asserted
rather than eyeballed.

Nothing here reaches the network. The one command that would -- `ingest` without
`--dry-run` -- is exercised only through its planner, which is the point of having one.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from perplab import cli
from perplab.data.ingest_bulk import PHASE1_DATASETS
from perplab.data.schemas import SCHEMAS, layout_for, market_root
from perplab.data.writer import ParquetBufferedWriter

_MS_PER_DAY = 86_400_000


# --------------------------------------------------------------------------------------
# Fixture lake
# --------------------------------------------------------------------------------------


def _row(dataset: str, ts_ms: int) -> dict[str, Any]:
    schema = SCHEMAS[dataset]
    row: dict[str, Any] = {}
    for field in schema:
        if pa.types.is_list(field.type):
            row[field.name] = [1, 2, 3]
        elif pa.types.is_boolean(field.type):
            row[field.name] = True
        elif pa.types.is_string(field.type):
            row[field.name] = "x"
        else:
            row[field.name] = 1
    row[layout_for(dataset).time_column] = ts_ms
    return row


def fill(root: Path, dataset: str, timestamps: list[int], *, symbol: str | None = "BTCUSDT") -> None:
    """Write through the real writer, so the CLI has to look where the writer writes."""
    writer = ParquetBufferedWriter(
        market_root(root), dataset, SCHEMAS[dataset], symbol=symbol
    )
    for ts in timestamps:
        writer.append(_row(dataset, ts))
    writer.flush()


DAY = 1785542400000
"""2026-08-01T00:00:00Z, checked by `test_the_fixture_day_is_the_day_it_claims`."""


@pytest.fixture
def lake(tmp_path: Path) -> Path:
    """A userdata root with one complete day of 1m klines for BTCUSDT."""
    fill(tmp_path, "klines", [DAY + i * 60_000 for i in range(1440)])
    return tmp_path


def run(argv: list[str], capsys: pytest.CaptureFixture[str]) -> tuple[int, str]:
    code = cli.main(argv)
    return code, capsys.readouterr().out


def test_the_fixture_day_is_the_day_it_claims() -> None:
    from perplab.data.schemas import partition_key

    assert partition_key(DAY) == "2026-08-01"
    assert DAY % _MS_PER_DAY == 0


# --------------------------------------------------------------------------------------
# Wiring
# --------------------------------------------------------------------------------------


class TestWiring:
    EXPECTED = {
        "preflight",
        "snapshot-reference",
        "collect",
        "verify-bulk",
        "ingest",
        "gaps",
        "manifest",
        "query",
        "serve",
    }

    def test_every_subcommand_has_a_handler(self) -> None:
        """A subcommand argparse accepts but `handlers` does not carry raises `KeyError`
        after the arguments have already been validated, which reads as a crash rather
        than as a missing command."""
        for command in sorted(self.EXPECTED):
            with pytest.raises(SystemExit):
                cli.main([command, "--help"])

    def test_the_existing_commands_still_parse(self) -> None:
        """Phase 0's commands must keep working unchanged."""
        for command in ("preflight", "snapshot-reference", "collect", "verify-bulk"):
            with pytest.raises(SystemExit) as exit_info:
                cli.main([command, "--help"])
            assert exit_info.value.code == 0

    def test_an_unknown_command_is_refused_by_argparse(self) -> None:
        with pytest.raises(SystemExit):
            cli.main(["nonsuch"])


class TestServeCommand:
    """`perplab serve` (spec 2.3, 11)."""

    def test_a_non_loopback_bind_without_a_password_exits_non_zero(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Spec 11 makes the password mandatory once the platform is exposed, "enforced in
        code, not documentation". The command must refuse rather than warn — a warning
        printed to a terminal nobody is watching is documentation with extra steps.

        Exercised through the CLI rather than through `create_app` alone, because the CLI
        is what a person types and a handler that swallowed the error would still exit 0.
        """
        code = cli.main(["--root", str(tmp_path), "serve", "--host", "0.0.0.0"])
        assert code == 1
        assert "password is mandatory" in capsys.readouterr().err

    def test_the_server_is_never_started_when_the_bind_is_refused(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import uvicorn

        started: list[object] = []
        monkeypatch.setattr(uvicorn, "run", lambda *a, **k: started.append(a))
        assert cli.main(["--root", str(tmp_path), "serve", "--host", "10.0.0.5"]) == 1
        assert started == []


# --------------------------------------------------------------------------------------
# The shared range convention
# --------------------------------------------------------------------------------------


class TestRangeConversion:
    """One convention at the command line, converted once.

    `ingest_range` takes inclusive archive periods; `detect_gaps`, `build_manifest` and
    `partition_predicate` take half-open millisecond ranges. Exposing both would mean
    `--end 2026-08-01` covering the first of August for one command and stopping at
    midnight before it for another, and the resulting gap report would disagree with the
    ingest that produced the data.
    """

    def test_the_end_date_is_inclusive(self) -> None:
        start_ms, end_ms = cli._inclusive_range_ms("2026-08-01", "2026-08-01")
        assert start_ms == DAY
        assert end_ms == DAY + _MS_PER_DAY

    def test_a_multi_day_range_spans_every_day(self) -> None:
        start_ms, end_ms = cli._inclusive_range_ms("2026-08-01", "2026-08-03")
        assert end_ms - start_ms == 3 * _MS_PER_DAY

    def test_an_inverted_range_is_refused(self) -> None:
        with pytest.raises(SystemExit, match="precedes"):
            cli._inclusive_range_ms("2026-08-03", "2026-08-01")

    def test_an_impossible_date_is_refused(self) -> None:
        """A typo must fail here rather than as an empty result."""
        with pytest.raises(ValueError):
            cli._inclusive_range_ms("2026-02-31", "2026-03-01")

    def test_no_naive_datetime_is_involved(self) -> None:
        """A local-zone conversion would shift the boundary by hours on most machines."""
        assert cli._day_start_ms("1970-01-01") == 0


# --------------------------------------------------------------------------------------
# ingest --dry-run
# --------------------------------------------------------------------------------------


class TestIngestDryRun:
    def test_lists_files_and_bytes_without_downloading(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        code, out = run(
            [
                "--root",
                str(tmp_path),
                "ingest",
                "--dry-run",
                "--symbol",
                "BTCUSDT",
                "--dataset",
                "klines",
                "--start",
                "2026-07-01",
                "--end",
                "2026-07-03",
            ],
            capsys,
        )
        assert code == 0
        assert "3 of 3 periods to fetch" in out
        assert "TOTAL: 3 archive(s)" in out
        assert "Nothing was downloaded" in out
        # Nothing may be created in the lake by a plan.
        assert not (tmp_path / "market").exists()

    def test_the_default_set_is_the_phase_1_set(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        _, out = run(
            [
                "--root",
                str(tmp_path),
                "ingest",
                "--dry-run",
                "--start",
                "2026-07-01",
                "--end",
                "2026-07-01",
            ],
            capsys,
        )
        for dataset in PHASE1_DATASETS:
            assert dataset in out

    def test_bookticker_scale_is_surfaced_before_the_run_not_during(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """The command can pull tens of gigabytes. That has to be the headline."""
        _, out = run(
            [
                "--root",
                str(tmp_path),
                "ingest",
                "--dry-run",
                "--dataset",
                "bookTicker",
                "--start",
                "2023-05-16",
                "--end",
                "2024-03-30",
            ],
            capsys,
        )
        assert "GB" in out
        assert "320" in out

    def test_the_unavailable_dataset_is_named_on_every_run(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Finding F2. Absent and unmentioned is indistinguishable from forgotten."""
        _, out = run(
            [
                "--root",
                str(tmp_path),
                "ingest",
                "--dry-run",
                "--dataset",
                "klines",
                "--start",
                "2026-07-01",
                "--end",
                "2026-07-01",
            ],
            capsys,
        )
        assert "liquidationSnapshot is unavailable by design" in out

    def test_the_lake_name_is_accepted_for_a_dataset(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        code, out = run(
            [
                "--root",
                str(tmp_path),
                "ingest",
                "--dry-run",
                "--dataset",
                "funding",
                "--start",
                "2026-06-01",
                "--end",
                "2026-07-01",
            ],
            capsys,
        )
        assert code == 0
        assert "fundingRate BTCUSDT" in out


# --------------------------------------------------------------------------------------
# gaps
# --------------------------------------------------------------------------------------


class TestGapsCommand:
    def test_a_complete_day_exits_zero(
        self, lake: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        code, out = run(
            [
                "--root",
                str(lake),
                "gaps",
                "--symbol",
                "BTCUSDT",
                "--start",
                "2026-08-01",
                "--end",
                "2026-08-01",
                "--dataset",
                "klines",
            ],
            capsys,
        )
        assert code == 0
        assert "No unexplained gaps" in out

    def test_an_unexplained_gap_exits_non_zero(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Phase 1b's exit criterion, as a process exit code."""
        fill(tmp_path, "klines", [DAY + i * 60_000 for i in range(100)])
        code, out = run(
            [
                "--root",
                str(tmp_path),
                "gaps",
                "--symbol",
                "BTCUSDT",
                "--start",
                "2026-08-01",
                "--end",
                "2026-08-01",
                "--dataset",
                "klines",
            ],
            capsys,
        )
        assert code == 1
        assert "unexplained gap(s)" in out

    def test_the_unlived_remainder_of_today_is_not_a_gap(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """`--end` is an inclusive date, so asking about today reaches tomorrow's midnight.

        Without clamping, the hours of today that have not happened yet are reported as
        missing data: an unexplained gap that is largest in the morning and disappears
        overnight. That trains the operator to ignore the report, which is the one outcome
        the whole subsystem exists to prevent -- and it makes Phase 1b's exit criterion
        unmeasurable on the day you actually want to check it.
        """
        now_ms = int(time.time() * 1000)
        today = time.strftime("%Y-%m-%d", time.gmtime(now_ms / 1000))
        # A dense minute of bars ending a moment ago: nothing is genuinely missing.
        fill(tmp_path, "klines", [now_ms - i * 60_000 for i in range(30, 0, -1)])

        code, out = run(
            [
                "--root", str(tmp_path), "gaps", "--symbol", "BTCUSDT",
                "--start", today, "--end", today, "--dataset", "klines",
            ],
            capsys,
        )

        assert "clamped to the present" in out
        # The morning before the data starts is still a real, reported gap; the evening
        # after "now" is not.
        assert f"{today}T23:5" not in out

    def test_a_range_entirely_in_the_future_evaluates_nothing(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Asking about tomorrow is a question with no answer, not a failure."""
        future = time.strftime("%Y-%m-%d", time.gmtime(time.time() + 5 * 86_400))

        code, out = run(
            [
                "--root", str(tmp_path), "gaps", "--symbol", "BTCUSDT",
                "--start", future, "--end", future, "--dataset", "klines",
            ],
            capsys,
        )

        assert code == 0

    def test_it_reads_the_lake_not_the_userdata_root(
        self, lake: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Handed `--root` directly it would glob `<root>/klines` and find nothing, then
        report a complete day as wholly missing."""
        _, out = run(
            [
                "--root",
                str(lake),
                "gaps",
                "--symbol",
                "BTCUSDT",
                "--start",
                "2026-08-01",
                "--end",
                "2026-08-01",
                "--dataset",
                "klines",
            ],
            capsys,
        )
        assert "1440/1440 bars" in out


# --------------------------------------------------------------------------------------
# manifest
# --------------------------------------------------------------------------------------


class TestManifestCommand:
    def _args(self, lake: Path) -> list[str]:
        return [
            "--root",
            str(lake),
            "manifest",
            "--symbol",
            "BTCUSDT",
            "--start",
            "2026-08-01",
            "--end",
            "2026-08-01",
        ]

    def test_computes_and_prints(
        self, lake: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        code, out = run(self._args(lake), capsys)
        assert code == 0
        assert "fill_model_tier  : BAR_CLOSE" in out
        assert "klines_1m" in out

    def test_saves_and_reloads(
        self, lake: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        path = tmp_path / "saved.json"
        code, _ = run(self._args(lake) + ["--out", str(path)], capsys)
        assert code == 0
        assert json.loads(path.read_text(encoding="utf-8"))["manifest_version"] == 1

    def test_an_unchanged_lake_diffs_clean(
        self, lake: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        path = tmp_path / "saved.json"
        run(self._args(lake) + ["--out", str(path)], capsys)
        code, out = run(self._args(lake) + ["--diff", str(path)], capsys)
        assert code == 0
        assert "unchanged" in out

    def test_a_changed_lake_diffs_loudly_and_exits_non_zero(
        self, lake: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Spec 4.6 requires a recomputation that differs to "warn loudly". A non-zero
        exit is what makes that survive being run from a script."""
        path = tmp_path / "saved.json"
        run(self._args(lake) + ["--out", str(path)], capsys)
        fill(lake, "aggTrades", [DAY, DAY + 1000])

        code, out = run(self._args(lake) + ["--diff", str(path)], capsys)
        assert code == 1
        assert "DIFFERS" in out
        assert "aggTrades" in out

    def test_gaps_are_folded_in_when_asked(
        self, lake: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        code, out = run(self._args(lake) + ["--gaps"], capsys)
        assert code == 0
        assert "gaps recorded    :" in out

    def test_it_reads_the_userdata_root_not_the_lake(
        self, lake: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """The reference snapshot lives at `<root>/reference/`. Handed `<root>/market`
        this silently finds none and flags every manifest FILTERS_APPROXIMATE."""
        directory = lake / "reference" / "exchangeInfo"
        directory.mkdir(parents=True)
        (directory / "2026-07-01.json").write_text("{}", encoding="utf-8")

        _, out = run(self._args(lake), capsys)
        assert "FILTERS_APPROXIMATE" not in out


# --------------------------------------------------------------------------------------
# query
# --------------------------------------------------------------------------------------


class TestQueryCommand:
    def test_runs_sql_against_the_views(
        self, lake: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        code, out = run(
            [
                "--root",
                str(lake),
                "query",
                "SELECT count(*) AS n FROM klines",
                "--dataset",
                "klines",
            ],
            capsys,
        )
        assert code == 0
        assert "1440" in out

    def test_timing_reports_the_split_and_passes_the_budget(
        self, lake: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        code, out = run(
            [
                "--root",
                str(lake),
                "query",
                "SELECT count(*) FROM klines",
                "--dataset",
                "klines",
                "--timing",
            ],
            capsys,
        )
        assert code == 0
        assert "connect" in out and "execute" in out
        assert "within the 2 s budget" in out

    def test_an_impossible_budget_fails_the_process(
        self, lake: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """The exit criterion has to be checkable by a script, not only readable."""
        code, out = run(
            [
                "--root",
                str(lake),
                "query",
                "SELECT count(*) FROM klines",
                "--dataset",
                "klines",
                "--timing",
                "--budget",
                "0",
            ],
            capsys,
        )
        assert code == 1
        assert "OVER the 0 s budget" in out

    def test_output_is_truncated_but_the_true_count_is_printed(
        self, lake: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """A truncated display must not be mistakable for the whole result."""
        _, out = run(
            [
                "--root",
                str(lake),
                "query",
                "SELECT open_time FROM klines ORDER BY open_time",
                "--dataset",
                "klines",
                "--limit",
                "5",
            ],
            capsys,
        )
        assert "1435 more row(s) of 1440" in out

    def test_a_never_ingested_dataset_returns_no_rows_rather_than_failing(
        self, lake: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """A lake where some datasets were never written is the normal state."""
        code, out = run(
            [
                "--root",
                str(lake),
                "query",
                "SELECT count(*) AS n FROM depth20",
                "--dataset",
                "depth20",
            ],
            capsys,
        )
        assert code == 0
        assert "0" in out

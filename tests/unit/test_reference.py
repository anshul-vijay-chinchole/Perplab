"""Tests for reference snapshotting, and for reading the public bracket shape.

`leverageBracket` was the one part of the Phase 0 exit criterion that could not be met:
its documented endpoint is signed, so brackets were skipped with a warning (finding F3).
They are served unauthenticated by the endpoint behind Binance's public leverage-bracket
page, which closes F3 -- but that endpoint spells every field differently and publishes
rates as bare JSON numbers, so the parse path is what these tests are mostly about.

The float guard is the load-bearing one. `0.0333` has no exact binary representation, and
a bracket rate that is wrong in the seventeenth digit multiplies a six-figure notional
inside every liquidation solve, where spec 3.10 forbids the epsilon that would hide it.
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from perplab.core.margin import (
    bracket_symbols,
    brackets_from_payload,
    load_bracket_snapshot,
    validate_bracket_document,
)
from perplab.data.reference import _write_snapshot_text, latest_snapshot

# A real BTCUSDT response from the public endpoint, trimmed to four tiers
# (captured 2026-08-02). Note the bare numbers: this is the shape as served.
PUBLIC_PAYLOAD = """
{"code":"000000","message":null,"messageDetail":null,"data":{"brackets":[
 {"symbol":"BTCUSDT","updateTime":1755585384254,"notionalLimit":100,"riskBrackets":[
  {"bracketSeq":1,"bracketNotionalFloor":0,"bracketNotionalCap":300000,
   "bracketMaintenanceMarginRate":0.004,"cumFastMaintenanceAmount":0,
   "minOpenPosLeverage":101,"maxOpenPosLeverage":150},
  {"bracketSeq":2,"bracketNotionalFloor":300000,"bracketNotionalCap":800000,
   "bracketMaintenanceMarginRate":0.005,"cumFastMaintenanceAmount":300,
   "minOpenPosLeverage":76,"maxOpenPosLeverage":100},
  {"bracketSeq":3,"bracketNotionalFloor":800000,"bracketNotionalCap":3000000,
   "bracketMaintenanceMarginRate":0.0065,"cumFastMaintenanceAmount":1500,
   "minOpenPosLeverage":51,"maxOpenPosLeverage":75},
  {"bracketSeq":4,"bracketNotionalFloor":3000000,"bracketNotionalCap":12000000,
   "bracketMaintenanceMarginRate":0.01,"cumFastMaintenanceAmount":12000,
   "minOpenPosLeverage":26,"maxOpenPosLeverage":50}]},
 {"symbol":"ETHUSDT","updateTime":1755585384254,"notionalLimit":100,"riskBrackets":[
  {"bracketSeq":1,"bracketNotionalFloor":0,"bracketNotionalCap":50000,
   "bracketMaintenanceMarginRate":0.0075,"cumFastMaintenanceAmount":0,
   "minOpenPosLeverage":76,"maxOpenPosLeverage":100}]}]}}
"""

DOCUMENTED_PAYLOAD = """
[{"symbol":"BTCUSDT","brackets":[
  {"bracket":1,"initialLeverage":150,"notionalCap":300000,"notionalFloor":0,
   "maintMarginRatio":0.004,"cum":0}]}]
"""


def parsed(text: str) -> object:
    return json.loads(text, parse_float=Decimal)


class TestPublicBracketShape:
    def test_parses_the_public_shape(self) -> None:
        table = brackets_from_payload(parsed(PUBLIC_PAYLOAD), "BTCUSDT")

        assert len(table.brackets) == 4
        assert table.brackets[0].notional_cap == Decimal("300000")
        assert table.brackets[3].maintenance_amount == Decimal("12000")

    def test_rate_precision_survives_the_parse(self) -> None:
        """`0.0065` and `0.0333` are not representable as binary floats.

        This is the whole reason the payload is read with `parse_float=Decimal` and the
        reason a float reaching the parser is refused rather than converted.
        """
        table = brackets_from_payload(parsed(PUBLIC_PAYLOAD), "BTCUSDT")

        assert table.brackets[2].mmr == Decimal("0.0065")
        assert table.brackets[2].mmr != Decimal(0.0065)

    def test_a_float_payload_is_refused(self) -> None:
        """Default `json.loads` loses the precision before the parser is reached.

        Accepting it would make the loss invisible -- the table would build, every number
        would look right, and liquidation prices would be wrong in a digit nobody prints.
        """
        with pytest.raises(ValueError, match="arrived as a float"):
            brackets_from_payload(json.loads(PUBLIC_PAYLOAD), "BTCUSDT")

    def test_max_leverage_comes_from_the_max_not_the_min(self) -> None:
        """`maxOpenPosLeverage`, not `minOpenPosLeverage`.

        Both exist in the public payload and both are plausible integers, so picking the
        wrong one produces a table that parses cleanly and permits 101x where the exchange
        permits 150x -- or, in the top tiers, refuses a position the exchange would allow.
        """
        table = brackets_from_payload(parsed(PUBLIC_PAYLOAD), "BTCUSDT")

        assert [b.max_leverage for b in table.brackets] == [150, 100, 75, 50]

    def test_the_documented_shape_still_parses(self) -> None:
        """Both endpoints must converge on one parse path."""
        table = brackets_from_payload(parsed(DOCUMENTED_PAYLOAD), "BTCUSDT")

        assert table.brackets[0].max_leverage == 150
        assert table.brackets[0].mmr == Decimal("0.004")

    def test_symbols_are_listed_from_either_shape(self) -> None:
        assert bracket_symbols(parsed(PUBLIC_PAYLOAD)) == ("BTCUSDT", "ETHUSDT")
        assert bracket_symbols(parsed(DOCUMENTED_PAYLOAD)) == ("BTCUSDT",)

    def test_an_absent_symbol_is_named_in_the_error(self) -> None:
        with pytest.raises(ValueError, match="DOGEUSDT not present"):
            brackets_from_payload(parsed(PUBLIC_PAYLOAD), "DOGEUSDT")

    def test_cum_values_are_continuous_across_tiers(self) -> None:
        """Binance's `cum` makes the maintenance-margin curve continuous at each boundary.

        Checked against the published numbers rather than assumed, because dropping or
        mis-mapping `cum` overstates maintenance margin on every position past tier 1 --
        phantom liquidations at exactly the sizes worth trading.
        """
        table = brackets_from_payload(parsed(PUBLIC_PAYLOAD), "BTCUSDT")

        for lower, upper in zip(table.brackets, table.brackets[1:]):
            boundary = lower.notional_cap
            assert boundary * lower.mmr - lower.maintenance_amount == (
                boundary * upper.mmr - upper.maintenance_amount
            )


class TestValidateBracketDocument:
    def test_accepts_a_good_document_and_reports_symbols(self) -> None:
        assert validate_bracket_document(PUBLIC_PAYLOAD) == ("BTCUSDT", "ETHUSDT")

    def test_rejects_undecodable_json(self) -> None:
        with pytest.raises(ValueError, match="not decodable JSON"):
            validate_bracket_document("<html>rate limited</html>")

    def test_rejects_an_error_envelope_carrying_no_entries(self) -> None:
        """An HTTP 200 with an error body must not be archived as reference data.

        This is the failure that would otherwise be discovered years later, when a backtest
        priced margin against a snapshot that was never a bracket table.
        """
        with pytest.raises(ValueError, match="no symbol entries"):
            validate_bracket_document('{"code":"000002","message":"illegal parameter"}')

    def test_rejects_a_document_whose_tiers_do_not_form_a_table(self) -> None:
        """Decoding is not enough; the tiers must survive the ordering/contiguity checks."""
        broken = PUBLIC_PAYLOAD.replace('"bracketNotionalFloor":300000', '"bracketNotionalFloor":400000')

        with pytest.raises(ValueError, match="gap between brackets"):
            validate_bracket_document(broken)

    def test_validation_reads_floats_as_decimals(self) -> None:
        """The validator must not be the lossy path the loader then rejects."""
        assert validate_bracket_document(PUBLIC_PAYLOAD)


class TestSnapshotWriting:
    def test_writes_bytes_verbatim(self, tmp_path: Path) -> None:
        """Re-encoding would either lose the rates or fail outright on `Decimal`."""
        path = _write_snapshot_text(tmp_path, "leverageBracket", "2026-08-02", PUBLIC_PAYLOAD)

        assert path.read_text(encoding="utf-8") == PUBLIC_PAYLOAD

    def test_refuses_to_overwrite(self, tmp_path: Path) -> None:
        """A run's manifest names the snapshot it used; that reference is worthless if the
        file behind it can change (spec 4.6)."""
        _write_snapshot_text(tmp_path, "leverageBracket", "2026-08-02", PUBLIC_PAYLOAD)
        _write_snapshot_text(tmp_path, "leverageBracket", "2026-08-02", "{}")

        path = tmp_path / "reference" / "leverageBracket" / "2026-08-02.json"
        assert path.read_text(encoding="utf-8") == PUBLIC_PAYLOAD

    def test_round_trips_through_the_loader(self, tmp_path: Path) -> None:
        path = _write_snapshot_text(tmp_path, "leverageBracket", "2026-08-02", PUBLIC_PAYLOAD)

        table = load_bracket_snapshot(path, "BTCUSDT")

        assert table.snapshot_date == "2026-08-02"
        assert table.brackets[2].mmr == Decimal("0.0065")

    def test_temp_file_is_not_mistaken_for_a_snapshot(self, tmp_path: Path) -> None:
        """`latest_snapshot` globs `*.json`; a stray `.json.tmp` must not win the sort."""
        _write_snapshot_text(tmp_path, "leverageBracket", "2026-08-02", PUBLIC_PAYLOAD)
        (tmp_path / "reference" / "leverageBracket" / "2026-08-03.json.tmp").write_text("{}")

        found = latest_snapshot(tmp_path, "leverageBracket")

        assert found is not None and found.name == "2026-08-02.json"

    def test_dated_lookup_never_reaches_forward(self, tmp_path: Path) -> None:
        """Spec 3.2: a 2023 backtest may not silently use today's brackets."""
        _write_snapshot_text(tmp_path, "leverageBracket", "2026-08-02", PUBLIC_PAYLOAD)

        assert latest_snapshot(tmp_path, "leverageBracket", "2026-08-01") is None


class TestRealSnapshot:
    """Against the snapshot this install actually took, when one is present."""

    def _path(self) -> Path | None:
        return latest_snapshot(Path("userdata"), "leverageBracket")

    def test_real_snapshot_parses_btcusdt(self) -> None:
        path = self._path()
        if path is None:
            pytest.skip("no leverageBracket snapshot in userdata/")

        table = load_bracket_snapshot(path, "BTCUSDT")

        assert table.brackets[0].max_leverage >= 1
        assert table.brackets[0].notional_floor == 0
        # Ordering and contiguity are enforced by BracketTable itself, so reaching here
        # means the real published table satisfies both.
        assert len(table.brackets) >= 5

"""Bracket tables, margin arithmetic, and the ways a snapshot can be wrong (spec 3.6).

The liquidation algebra is covered by `tests/golden/test_liquidation.py`. What is here is
everything around it: parsing the published payload without losing precision, refusing a
malformed table, and the boundary behaviour of the two margin formulas.
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from perplab.core.margin import (
    BracketTable,
    LeverageBracket,
    bankruptcy_price,
    brackets_from_payload,
    initial_margin,
    liquidation_price,
    load_bracket_snapshot,
    maintenance_margin,
)
from tests.support import graduated_bracket_table, single_bracket_table

PAYLOAD_TEXT = """
[{"symbol": "BTCUSDT", "brackets": [
    {"bracket": 1, "initialLeverage": 125, "notionalCap": 50000, "notionalFloor": 0,
     "maintMarginRatio": 0.004, "cum": 0.0},
    {"bracket": 2, "initialLeverage": 100, "notionalCap": 600000, "notionalFloor": 50000,
     "maintMarginRatio": 0.005, "cum": 50.0}
]}]
"""


class TestPayloadParsing:
    def test_parses_with_decimal_float_hook(self) -> None:
        payload = json.loads(PAYLOAD_TEXT, parse_float=Decimal)
        table = brackets_from_payload(payload, "BTCUSDT")

        assert len(table.brackets) == 2
        assert table.brackets[0].mmr == Decimal("0.004")
        assert table.brackets[1].maintenance_amount == Decimal("50.0")

    def test_a_float_is_refused_rather_than_converted(self) -> None:
        """The single most important line in `margin.py`, and the least obvious.

        `leverageBracket` sends numbers as JSON *numbers*, unlike every other futures
        endpoint, which sends decimal strings. A default `json.loads` therefore turns
        `0.004` into an IEEE-754 double before this code sees it, and
        `Decimal(0.004)` is `0.004000000000000000083266726...` -- not a rounding error
        that appears later, one that is already baked in.

        That value multiplies a six-figure notional inside every liquidation solve, and
        spec 3.10 forbids the epsilon that would be needed to hide the result. Converting
        it silently is worse than failing, because the failure is then invisible; so the
        float is rejected with the fix in the message.
        """
        payload = json.loads(PAYLOAD_TEXT)  # no parse_float -- the mistake being caught
        with pytest.raises(ValueError, match="parse_float=Decimal"):
            brackets_from_payload(payload, "BTCUSDT")

    def test_unknown_symbol_names_what_was_available(self) -> None:
        payload = json.loads(PAYLOAD_TEXT, parse_float=Decimal)
        with pytest.raises(ValueError, match="ETHUSDT not present"):
            brackets_from_payload(payload, "ETHUSDT")

    def test_single_symbol_response_is_a_bare_object(self) -> None:
        """`GET /fapi/v1/leverageBracket?symbol=BTCUSDT` returns an object, not a list."""
        payload = json.loads(PAYLOAD_TEXT, parse_float=Decimal)[0]
        table = brackets_from_payload(payload, "BTCUSDT")
        assert len(table.brackets) == 2

    def test_snapshot_round_trip(self, tmp_path: Path) -> None:
        path = tmp_path / "2026-08-02.json"
        path.write_text(PAYLOAD_TEXT, encoding="utf-8")

        table = load_bracket_snapshot(path, "BTCUSDT")
        assert table.snapshot_date == "2026-08-02"
        assert table.brackets[0].mmr == Decimal("0.004")

    def test_snapshot_reads_through_a_wrapper_envelope(self, tmp_path: Path) -> None:
        """Reference snapshots wrap the payload; the raw endpoint response does not."""
        path = tmp_path / "2026-08-02.json"
        path.write_text(
            json.dumps({"fetched_at": 1, "payload": json.loads(PAYLOAD_TEXT)}),
            encoding="utf-8",
        )
        assert len(load_bracket_snapshot(path, "BTCUSDT").brackets) == 2


class TestTableValidation:
    def test_empty_table_is_refused(self) -> None:
        with pytest.raises(ValueError, match="empty"):
            BracketTable(symbol="BTCUSDT", brackets=())

    def test_out_of_order_brackets_are_refused(self) -> None:
        table = graduated_bracket_table()
        with pytest.raises(ValueError, match="ordered by notional floor"):
            BracketTable(symbol="BTCUSDT", brackets=tuple(reversed(table.brackets)))

    def test_a_gap_between_tiers_is_refused(self) -> None:
        """A notional band with no tier would silently fall through to the highest rate.

        `resolve` walks tiers in order and returns the first whose cap covers the notional,
        so a hole between 50 000 and 60 000 does not raise at lookup time -- it quietly
        applies the *top* bracket's MMR to a mid-sized position. Catching it when the table
        is built means a truncated snapshot fails on load rather than months later, on one
        position, in one direction.
        """
        with pytest.raises(ValueError, match="gap between brackets"):
            BracketTable(
                symbol="BTCUSDT",
                brackets=(
                    LeverageBracket(1, 125, Decimal(0), Decimal(50000), Decimal("0.004"), Decimal(0)),
                    LeverageBracket(2, 100, Decimal(60000), Decimal(600000), Decimal("0.005"), Decimal(50)),
                ),
            )

    def test_overlapping_tiers_are_refused(self) -> None:
        """The constructor rejected gaps but permitted overlaps, and overlaps fail worse.

        `resolve` returns the first tier whose cap covers the notional, so every
        notional inside an overlap silently took the *lower* tier's MMR and solved a
        liquidation price further from the mark than the exchange's -- wrong in the
        "you survived" direction, which is the direction nobody notices. A real
        `leverageBracket` payload is exactly contiguous (each floor is the previous
        cap), so an overlap is malformed reference data and must fail on load, exactly
        as a gap does.
        """
        with pytest.raises(ValueError, match="overlap"):
            BracketTable(
                symbol="BTCUSDT",
                brackets=(
                    LeverageBracket(1, 125, Decimal(0), Decimal(50000), Decimal("0.004"), Decimal(0)),
                    LeverageBracket(2, 100, Decimal(40000), Decimal(600000), Decimal("0.005"), Decimal(50)),
                ),
            )

    @pytest.mark.parametrize(
        "kwargs, message",
        [
            ({"notional_cap": Decimal(0)}, "must exceed"),
            ({"mmr": Decimal(0)}, "not a fraction"),
            ({"mmr": Decimal(1)}, "not a fraction"),
            ({"max_leverage": 0}, "max leverage"),
        ],
    )
    def test_malformed_brackets_are_refused(self, kwargs: dict, message: str) -> None:
        base = {
            "bracket": 1,
            "max_leverage": 125,
            "notional_floor": Decimal(0),
            "notional_cap": Decimal(50000),
            "mmr": Decimal("0.004"),
            "maintenance_amount": Decimal(0),
        }
        with pytest.raises(ValueError, match=message):
            LeverageBracket(**{**base, **kwargs})


class TestResolve:
    def test_boundary_belongs_to_the_lower_tier(self) -> None:
        table = graduated_bracket_table()
        assert table.resolve(Decimal(50000)).bracket == 1
        assert table.resolve(Decimal("50000.01")).bracket == 2

    def test_above_the_top_cap_uses_the_top_bracket(self) -> None:
        """Binance refuses the position at that size; the top rate is the closest honest
        answer, and it is the conservative one."""
        table = graduated_bracket_table()
        assert table.resolve(Decimal(10) ** 12).bracket == len(table.brackets)

    def test_negative_notional_is_refused(self) -> None:
        with pytest.raises(ValueError, match="non-negative"):
            graduated_bracket_table().resolve(Decimal(-1))

    def test_below_the_lowest_floor_is_refused(self) -> None:
        """Notional floors are honoured, not decorative.

        With contiguity enforced at construction, the lowest floor is the only floor a
        notional can fall outside of. `resolve` used to scan caps alone, so a notional
        below a truncated table's first floor was silently served that tier's rate --
        a maintenance rate the table never defined for that size. Every published table
        starts at zero, so this is purely a malformed-snapshot guard, and it should be
        loud for the same reason the gap check is.
        """
        table = BracketTable(
            symbol="BTCUSDT",
            brackets=(
                LeverageBracket(
                    1, 125, Decimal(1000), Decimal(50000), Decimal("0.004"), Decimal(0)
                ),
            ),
        )
        with pytest.raises(ValueError, match="below the lowest bracket"):
            table.resolve(Decimal(500))

    def test_max_leverage_falls_with_size(self) -> None:
        table = graduated_bracket_table()
        assert table.max_leverage_for(Decimal(10000)) == 125
        assert table.max_leverage_for(Decimal(1000000)) == 50


class TestMarginFormulas:
    def test_initial_margin(self) -> None:
        assert initial_margin(Decimal(5000), 10) == Decimal(500)

    def test_initial_margin_refuses_sub_one_leverage(self) -> None:
        with pytest.raises(ValueError, match="at least 1"):
            initial_margin(Decimal(5000), 0)

    def test_maintenance_margin_uses_the_deduction(self) -> None:
        table = graduated_bracket_table()
        notional = Decimal(100000)
        bracket = table.resolve(notional)
        assert bracket.bracket == 2
        # 100 000 x 0.005 - 50 = 450, not 500.
        assert maintenance_margin(notional, bracket) == Decimal(450)

    def test_maintenance_margin_clamps_at_zero(self) -> None:
        """The deduction can exceed the charge on a very small position in a high tier.

        A negative maintenance requirement would mean the exchange owes you margin for
        holding a position. Nothing downstream is built for that, and the true answer is
        zero.
        """
        bracket = LeverageBracket(
            bracket=2,
            max_leverage=100,
            notional_floor=Decimal(0),
            notional_cap=Decimal(10) ** 9,
            mmr=Decimal("0.005"),
            maintenance_amount=Decimal(1000),
        )
        assert maintenance_margin(Decimal(100), bracket) == Decimal(0)


class TestDegenerateInputs:
    def test_bankruptcy_price_of_a_flat_position_is_undefined(self) -> None:
        with pytest.raises(ValueError, match="flat position"):
            bankruptcy_price(Decimal(0), Decimal(50000), Decimal(5000))

    def test_liquidation_price_of_a_flat_position_is_undefined(self) -> None:
        with pytest.raises(ValueError, match="flat position"):
            liquidation_price(
                qty=Decimal(0),
                entry_price=Decimal(50000),
                margin=Decimal(5000),
                mark_price=Decimal(50000),
                table=single_bracket_table(),
            )

    def test_non_positive_mark_is_refused(self) -> None:
        with pytest.raises(ValueError, match="mark price must be positive"):
            liquidation_price(
                qty=Decimal(1),
                entry_price=Decimal(50000),
                margin=Decimal(5000),
                mark_price=Decimal(0),
                table=single_bracket_table(),
            )

    def test_the_mark_seed_cannot_steer_the_solved_tier(self) -> None:
        """The mark passed to `liquidation_price` seeds the tier iteration; it must not
        pick the answer.

        The liquidation probe checks a bar's low and high, so the solve is routinely
        seeded with a mark far from the eventual fixed point -- an audit finding claimed
        that a probe at the bar extreme could settle a boundary-straddling position into
        a lower tier and a further `P_liq`. It cannot, and this pins why: acceptance
        requires the *solved price's own notional* to resolve to the tier it was solved
        under, and for any table whose `cum` makes maintenance margin continuous the
        trigger equation `W + Q*(P - Pe) - MM(q*P)` is strictly monotonic in `P`, so it
        has exactly one root and any accepted answer is that root. The seed only decides
        how many iterations the walk takes. (A table whose `cum` breaks continuity can
        carry two self-consistent roots -- and the iteration then oscillates and raises
        `BracketConvergenceError` rather than letting the seed flip a coin; see the
        non-convergence golden test.)
        """
        table = graduated_bracket_table()
        seeds = (Decimal(45000), Decimal(50500), Decimal(60000), Decimal(80000))
        # Margins chosen so the solved price sweeps across the 50 000 tier boundary.
        cases = [(Decimal(1), Decimal(60000), m) for m in
                 (Decimal(9700), Decimal(10100), Decimal(10200), Decimal(10300))]
        cases += [(Decimal(-1), Decimal(50000), Decimal(5000))]

        for qty, entry, margin in cases:
            solved = {
                (s.price, s.bracket.bracket)
                for s in (
                    liquidation_price(
                        qty=qty, entry_price=entry, margin=margin,
                        mark_price=seed, table=table,
                    )
                    for seed in seeds
                )
            }
            assert len(solved) == 1, (qty, entry, margin, solved)

    def test_zero_margin_long_solves_above_the_entry_price(self) -> None:
        """Documenting the shape of the answer, because it is the one that looks like a bug.

        With no margin, `P_liq = Pe / (1 - MMR)`, which is *above* the entry -- the position
        is already gone. `Account` never reaches this: `Position.margin_exhausted` is
        checked first and liquidates without solving. Asserted here so that if it ever does
        reach it, the reason is written down.
        """
        solution = liquidation_price(
            qty=Decimal(1),
            entry_price=Decimal(50000),
            margin=Decimal(0),
            mark_price=Decimal(50000),
            table=single_bracket_table(),
        )
        assert solution.price > Decimal(50000)

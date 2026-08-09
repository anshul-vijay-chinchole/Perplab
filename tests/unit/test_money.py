"""Tests for the numeric seam (spec 3.1).

These are the first tests in the project on purpose. Every balance PerpLab ever computes
passes through this module, so an error here is invisible everywhere and fatal
everywhere.
"""

from __future__ import annotations

import ast
from decimal import Decimal
from pathlib import Path

import pytest
from hypothesis import given
from hypothesis import strategies as st

from perplab.core.money import (
    SCALE,
    SCALE_EXP,
    decimal_to_scaled,
    from_scaled,
    is_multiple,
    quantize_price,
    quantize_qty,
    scaled_to_str,
    to_scaled,
)
from perplab.core.types import Side


class TestToScaled:
    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("0", 0),
            ("1", SCALE),
            ("50000.10", 5_000_010_000_000),
            ("0.014", 1_400_000),
            ("0.00000001", 1),
            (".5", 50_000_000),
            ("5.", 500_000_000),
            ("-1.5", -150_000_000),
            ("+1.5", 150_000_000),
            ("  1.5  ", 150_000_000),
            # Trailing zeros beyond 8dp carry no information and must not be treated as
            # a precision overflow.
            ("1.000000000000", SCALE),
        ],
    )
    def test_parses_exactly(self, text: str, expected: int) -> None:
        assert to_scaled(text) == expected

    def test_scientific_notation(self) -> None:
        assert to_scaled("1e-8") == 1
        assert to_scaled("1.5E2") == 150 * SCALE

    def test_rejects_excess_precision(self) -> None:
        """Rounding here would corrupt data permanently and invisibly (spec 1.4)."""
        with pytest.raises(ValueError, match="decimal places"):
            to_scaled("0.000000001")

    def test_rejects_garbage(self) -> None:
        with pytest.raises(ValueError):
            to_scaled("")
        with pytest.raises(ValueError):
            to_scaled("abc")

    @pytest.mark.parametrize(
        "text", ["not-a-price", "e", "1e", "1.2e", "eee", "1e2e3", "--1e5"]
    )
    def test_garbage_on_the_decimal_branch_is_still_a_ValueError(self, text: str) -> None:
        """Every rejection is a `ValueError`, including the ones routed via `Decimal`.

        Anything containing an `e` takes the `Decimal` branch, and raw `Decimal` answers
        malformed input with `decimal.InvalidOperation` -- an `ArithmeticError`, not a
        `ValueError`. That distinction is invisible until it costs a diagnosis: every
        caller of this seam names its column by catching `ValueError` and re-raising with
        context, so an `ArithmeticError` escapes with no column, no archive and no line
        number, surfacing as a bare `[<class 'ConversionSyntax'>]` in the middle of a
        2400-file backfill.

        `"not-a-price"` is the case that made this concrete: it looks nothing like a
        number and takes the scientific-notation path purely because it ends in `e`.
        """
        with pytest.raises(ValueError):
            to_scaled(text)

    def test_rejects_a_literal_that_overflows_when_scaled(self) -> None:
        """`1E999999` is finite and parses cleanly; scaling it by 10^8 does not.

        `scaleb` raises `decimal.Overflow`, an `ArithmeticError`, which would escape every
        caller filtering on `ValueError` for the same reason `InvalidOperation` did.
        """
        with pytest.raises(ValueError, match="cannot be scaled to an int64"):
            to_scaled("1E999999")

    @pytest.mark.parametrize("text", ["--1.5", "--1e5", "+-1", "-+1", "-", "+", "."])
    def test_a_second_sign_is_refused_rather_than_applied_twice(self, text: str) -> None:
        """`--1.5` used to return +0.5: wrong magnitude, wrong sign, no error.

        The leading sign is stripped before parsing and re-applied afterwards, so a second
        one was applied twice and cancelled itself. Nothing crashed, and nothing could
        have -- `int("-1")` is perfectly happy -- so a malformed field in a third-party
        archive would have scaled to a plausible price of the opposite sign. `"-"` and
        `"."` are here for the neighbouring case: with no digits at all they parsed as a
        confident zero.
        """
        with pytest.raises(ValueError):
            to_scaled(text)

    @pytest.mark.parametrize("text", ["1 .5", "1.-5", "1.2.3", "1,5", "0x10"])
    def test_refuses_input_int_would_have_accepted(self, text: str) -> None:
        """`int()` tolerates whitespace and its own sign; a decimal literal does not."""
        with pytest.raises(ValueError):
            to_scaled(text)

    def test_negative_exponents_still_parse(self) -> None:
        """The sign check must not catch the legitimate sign inside an exponent."""
        assert to_scaled("1e-8") == 1
        assert to_scaled("-1e-8") == -1
        assert to_scaled("+1E+2") == 100 * SCALE

    def test_never_uses_float(self) -> None:
        """The canonical float-parsing failure: float('0.07') != 0.07.

        If this module ever routes through float, 0.07 scales to 6999999 or 7000001
        instead of 7000000 and every downstream sum inherits the error.
        """
        assert to_scaled("0.07") == 7_000_000
        assert to_scaled("0.1") + to_scaled("0.2") == to_scaled("0.3")


class TestRoundTrip:
    @pytest.mark.parametrize(
        "text", ["0", "1", "50000.10", "0.014", "0.00000001", "-1.5", "999999.99999999"]
    )
    def test_scaled_round_trip(self, text: str) -> None:
        assert from_scaled(to_scaled(text)) == Decimal(text)

    @given(st.integers(min_value=-(10**15), max_value=10**15))
    def test_from_scaled_inverts_decimal_to_scaled(self, scaled: int) -> None:
        assert decimal_to_scaled(from_scaled(scaled)) == scaled

    def test_decimal_to_scaled_rejects_excess_precision(self) -> None:
        with pytest.raises(ValueError, match="decimal places"):
            decimal_to_scaled(Decimal("0.000000001"))

    def test_rejects_int64_overflow(self) -> None:
        """Parquet's int64 is bounded even though Python's int is not."""
        with pytest.raises(ValueError, match="int64"):
            decimal_to_scaled(Decimal(10) ** 30)

    @pytest.mark.parametrize("text", ["nan", "Infinity", "-Infinity", "sNaN"])
    def test_rejects_non_finite_decimals_as_ValueError(self, text: str) -> None:
        """NaN and the infinities are refused, and refused as `ValueError`.

        Without the guard `NaN` trips the precision check -- a misleading complaint about
        decimal places -- and `Infinity` reaches `int()` and raises `OverflowError`, an
        `ArithmeticError` that escapes every caller filtering on `ValueError`.
        """
        with pytest.raises(ValueError, match="finite"):
            decimal_to_scaled(Decimal(text))


class TestScaledToStr:
    @pytest.mark.parametrize(
        ("scaled", "expected"),
        [(0, "0"), (SCALE, "1"), (5_000_010_000_000, "50000.1"), (1, "1E-8")],
    )
    def test_renders_readably(self, scaled: int, expected: str) -> None:
        # Integers must not come back as Decimal('1E+2') -- unreadable in a log line.
        assert scaled_to_str(scaled) == expected


class TestQuantizePrice:
    """Spec 3.1: prices round *against* the trader. Buy down, sell up."""

    def test_buy_rounds_down(self) -> None:
        assert quantize_price(Decimal("50000.19"), Decimal("0.10"), Side.BUY) == Decimal(
            "50000.10"
        )

    def test_sell_rounds_up(self) -> None:
        assert quantize_price(
            Decimal("50000.11"), Decimal("0.10"), Side.SELL
        ) == Decimal("50000.20")

    def test_exact_multiple_is_unchanged_either_side(self) -> None:
        price, tick = Decimal("50000.10"), Decimal("0.10")
        assert quantize_price(price, tick, Side.BUY) == price
        assert quantize_price(price, tick, Side.SELL) == price

    def test_rejects_invalid_inputs(self) -> None:
        with pytest.raises(ValueError, match="tick_size"):
            quantize_price(Decimal("1"), Decimal("0"), Side.BUY)
        with pytest.raises(ValueError, match="price"):
            quantize_price(Decimal("-1"), Decimal("0.1"), Side.BUY)

    @given(
        price=st.decimals(
            min_value=Decimal("0.01"), max_value=Decimal("1000000"), places=8
        ),
        side=st.sampled_from(Side),
    )
    def test_result_is_always_an_exact_tick_multiple(
        self, price: Decimal, side: Side
    ) -> None:
        """Invariant I6 (spec 3.10), the property version."""
        tick = Decimal("0.10")
        assert is_multiple(quantize_price(price, tick, side), tick)

    @given(
        price=st.decimals(
            min_value=Decimal("0.01"), max_value=Decimal("1000000"), places=8
        )
    )
    def test_rounding_is_never_favourable(self, price: Decimal) -> None:
        """The direction is the entire point: quantisation must never improve a price."""
        tick = Decimal("0.10")
        assert quantize_price(price, tick, Side.BUY) <= price
        assert quantize_price(price, tick, Side.SELL) >= price


class TestQuantizeQty:
    def test_always_rounds_down(self) -> None:
        step = Decimal("0.001")
        assert quantize_qty(Decimal("0.13847362"), step) == Decimal("0.138")
        # Spec 3.2 opens with exactly this case: a backtest that fills 0.13847362 BTC
        # when stepSize is 0.001 is fiction.
        assert quantize_qty(Decimal("0.1389"), step) == Decimal("0.138")

    def test_rounds_down_regardless_of_side(self) -> None:
        """Unlike prices, quantities have no side-dependent direction."""
        assert quantize_qty(Decimal("1.9999"), Decimal("1")) == Decimal("1")

    def test_can_round_to_zero(self) -> None:
        """Sub-minimum sizes quantise to zero; rejecting them is minQty's job, not ours."""
        assert quantize_qty(Decimal("0.0009"), Decimal("0.001")) == 0

    def test_rejects_invalid_inputs(self) -> None:
        with pytest.raises(ValueError, match="step_size"):
            quantize_qty(Decimal("1"), Decimal("0"))
        with pytest.raises(ValueError, match="qty"):
            quantize_qty(Decimal("-1"), Decimal("0.001"))

    @given(qty=st.decimals(min_value=Decimal(0), max_value=Decimal("10000"), places=8))
    def test_never_rounds_up(self, qty: Decimal) -> None:
        step = Decimal("0.001")
        result = quantize_qty(qty, step)
        assert result <= qty
        assert is_multiple(result, step)


class TestIsMultiple:
    def test_exact(self) -> None:
        assert is_multiple(Decimal("50000.10"), Decimal("0.10"))
        assert not is_multiple(Decimal("50000.15"), Decimal("0.10"))

    def test_no_epsilon_tolerance(self) -> None:
        """If this ever needs a tolerance, a float has leaked in (spec 3.10)."""
        assert not is_multiple(Decimal("0.30000001"), Decimal("0.1"))


# Modules permitted to import `decimal`. Everything else in the package must work in
# scaled integers.
_DECIMAL_ALLOWLIST = {
    "perplab/core/money.py",  # the seam itself
    "perplab/core/account.py",  # Phase 2 -- the ledger
    "perplab/core/margin.py",  # Phase 2
    "perplab/core/funding.py",  # Phase 2
    "perplab/core/invariants.py",  # Phase 2
    "perplab/core/sizing.py",  # Phase 3 -- balances and prices in, order quantity out
    # Phase 4. These three do exact money arithmetic and their results are *reported*
    # numbers, not chart decoration: `fills` prices an execution to the tick, `trades`
    # reconstructs realised PnL per round-trip, and `attribution` has to satisfy spec 8.4's
    # identity exactly (its four components must sum to net PnL with no epsilon). Everything
    # downstream of them -- `analytics/metrics.py`, the equity curve, the API -- is float,
    # which is spec 3.1's other half and is checked by the same rule continuing to hold there.
    "perplab/engine/fills.py",
    "perplab/analytics/trades.py",
    "perplab/analytics/attribution.py",
    # Phase 6. The risk layer compares notionals, equity and drawdown fractions against
    # limits an operator wrote down, and a limit evaluated in binary floating point is a
    # limit that fires or does not at the boundary depending on the bit pattern. It is in
    # `core` for the same reason `margin` is: spec 7's first sentence puts risk checks in
    # the shared core so that a limit stopping a backtest stops live trading identically.
    "perplab/core/risk.py",
}


def test_decimal_is_confined_to_the_accounting_seam() -> None:
    """Structural guard against float/Decimal leakage (spec 3.1).

    Cheap to enforce now and near-impossible to retrofit: once `Decimal` is spread across
    the data path, backtest throughput is gone, and once `float` is in the ledger the
    exact-equality invariants of spec 3.10 cannot hold. Asserting the boundary as a test
    means the seam stays a seam without anyone having to remember it exists.
    """
    package_root = Path(__file__).resolve().parents[2] / "perplab"
    offenders: list[str] = []

    for path in package_root.rglob("*.py"):
        rel = path.relative_to(package_root.parent).as_posix()
        if rel in _DECIMAL_ALLOWLIST:
            continue

        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or ""]
            else:
                continue
            if any(n == "decimal" or n.startswith("decimal.") for n in names):
                offenders.append(f"{rel}:{node.lineno}")

    assert not offenders, (
        "`decimal` imported outside the accounting seam: "
        + ", ".join(offenders)
        + "\nMarket-data code must use scaled integers (perplab.core.money). "
        "If this module genuinely belongs to the ledger, add it to _DECIMAL_ALLOWLIST."
    )


def test_scale_exponent_covers_the_finest_usdm_tick() -> None:
    """The 1000-prefixed meme pairs quote to 1e-8; nothing on USD-M is finer."""
    assert SCALE_EXP >= 8
    assert to_scaled("0.00000001") == 1
    assert SCALE == 10**SCALE_EXP

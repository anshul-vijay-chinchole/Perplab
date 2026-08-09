"""The numeric seam between market data and accounting.

Spec 3.1 mandates `Decimal` for money and `float64` for indicators, but does not say
where the conversion happens. Left implicit, one of two failures is certain: `Decimal`
leaks into the data path and destroys backtest throughput, or `float` leaks into the
ledger and breaks the exact-equality invariants in spec 3.10 -- which explicitly forbid
an epsilon, so a leaked float shows up as an unfixable reconciliation failure rather than
a clean error.

This module is that seam, and it is the only place the two representations meet.

**Storage representation: scaled int64.**

Market data is stored as integers scaled by 10^8. Integers are exact, fast, and
Parquet-native, and they make invariant I6 (every price an exact multiple of `tickSize`)
checkable with a modulo rather than a tolerance.

The scale is a *fixed* 10^8 for every symbol and both prices and quantities, rather than
each symbol's own `pricePrecision`/`quantityPrecision`. Three reasons:

1. The collector can start recording without first resolving per-symbol precision, so
   data capture is not blocked on the reference-snapshot path.
2. A stored value's meaning does not change if Binance revises a symbol's precision.
   Per-symbol scaling would silently reinterpret every historical row already on disk.
3. It covers the full USD-M range with room to spare. The finest tick on any USD-M perp
   is 1e-8 (the 1000-prefixed meme pairs), and int64 holds a 10^6 price at this scale
   with ten orders of magnitude left over.

Values finer than 1e-8 are rejected rather than rounded. Silently discarding precision at
the ingest boundary would corrupt data permanently and invisibly, which spec 1.4
("fail loudly") rules out.
"""

from __future__ import annotations

from decimal import (
    ROUND_CEILING,
    ROUND_FLOOR,
    ROUND_HALF_EVEN,
    Context,
    Decimal,
    localcontext,
)
from typing import Any

from perplab.core.types import Side

__all__ = [
    "SCALE_EXP",
    "SCALE",
    "ACCOUNTING_PREC",
    "ACCOUNTING_CONTEXT",
    "accounting",
    "Money",
    "to_scaled",
    "decimal_to_scaled",
    "from_scaled",
    "scaled_to_str",
    "parse_money",
    "money_to_str",
    "quantize_price",
    "quantize_qty",
    "quantize_money",
    "quantize_entry_price",
    "is_multiple",
]

Money = Decimal
"""Alias re-exported so modules outside the seam can *name* an exact quantity.

`tests/unit/test_money.py::test_decimal_is_confined_to_the_accounting_seam` forbids
`import decimal` outside the five accounting modules, and it does so by inspecting import
statements rather than usage. That is the right check -- usage analysis would be
defeatable and this one is not -- but it leaves a module like `strategy/params.py` unable
to *annotate* a value it legitimately holds, having obtained it from a function here.

Exporting the alias resolves that without weakening anything. The rule the test enforces
is "do not do decimal arithmetic outside the seam", and a module that imports `Money` to
write `default: Money` is still obeying it: every such value was constructed by
`parse_money` below and is only ever handed onward to the accounting layer. Widening the
allowlist instead would have granted those modules the whole `decimal` module, including
the context mutation and the float constructor this file exists to keep out.
"""

SCALE_EXP = 8
SCALE = 10**SCALE_EXP

ACCOUNTING_PREC = 50
"""Significant digits for `Decimal` arithmetic in the accounting layer (spec 3.10).

The default `decimal` context carries 28. That is comfortably enough for any *single*
balance, and not enough for the exact-equality invariants: spec 3.10 states them without
tolerance and adds that reaching for an epsilon means a float has leaked. But `Decimal`
addition rounds to the context precision like any other operation, so with 28 digits a
wallet in the 10^4 range accumulating a realized PnL carrying 16 decimal places is one
operation away from a rounded sum -- and then `W` and `W0 + sum(realized) - sum(fees) +
sum(funding)` disagree in the last place, which reads exactly like the float leak the
invariant exists to catch.

50 digits leaves roughly 20 orders of magnitude of headroom over the worst realistic
operand (an eight-decimal quantity times an eight-decimal price times an eight-decimal fee
rate, at a six-figure notional), so every accounting operation is exact and the invariants
mean what they say. Nothing in the accounting layer is hot enough for the cost to matter:
spec 3.1 notes accounting events are rare compared to data events.
"""

ACCOUNTING_CONTEXT = Context(prec=ACCOUNTING_PREC, rounding=ROUND_HALF_EVEN)
"""Used via `decimal.localcontext(ACCOUNTING_CONTEXT)`, never by mutating the global one.

A library that reassigns the process-wide decimal context changes the arithmetic of every
other library in the interpreter, which is both rude and untraceable when it goes wrong.
"""

def accounting() -> Any:
    """`with accounting():` -- run a block under `ACCOUNTING_CONTEXT`.

    Exists so a module can do exact arithmetic on values it obtained from this seam without
    importing `decimal` itself. `test_decimal_is_confined_to_the_accounting_seam` inspects
    *import statements*, which is the right check -- usage analysis would be defeatable --
    but it means a module like `engine.backtest`, which legitimately sums fees and slippage
    it was handed, cannot reach `localcontext` without being added to the allowlist and
    thereby granted the float constructor and the global context mutation this file exists
    to keep out. One re-exported context manager grants exactly the arithmetic and nothing
    else.

    The default `decimal` context carries 28 significant digits, which is *not* enough here:
    see `ACCOUNTING_PREC`. An accumulator running outside this context rounds, and the
    exact-equality invariants of spec 3.10 then fail in the last place, which reads exactly
    like the float leak they exist to catch.
    """
    return localcontext(ACCOUNTING_CONTEXT)


MAX_MONEY_EXPONENT = 30
"""Decimal exponent bound for `parse_money`, at both ends.

Not a precision limit -- the mantissa may be as long as it likes. It bounds only the
*magnitude*, so that a value which renders in nine characters cannot render in a billion.
"""

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1


def _check_range(value: int, source: object) -> int:
    """Reject values that will not survive a Parquet int64 round-trip.

    Python integers are unbounded but Parquet's int64 is not. Without this check an
    oversized value would fail deep inside the writer, or worse, in a way that only
    surfaces when the affected partition is read back weeks later.
    """
    if not _INT64_MIN <= value <= _INT64_MAX:
        raise ValueError(f"scaled value out of int64 range: {source!r} -> {value}")
    return value


def to_scaled(text: str) -> int:
    """Parse a Binance decimal string into a scaled int64. Exact; never touches float.

    Binance sends every price and quantity as a decimal string precisely so clients can
    avoid binary floating point. Parsing via `float()` -- the obvious shortcut -- would
    throw that away at the first opportunity: `float("0.07")` is not 0.07, and the error
    compounds through every downstream aggregation.

    Raises `ValueError` on more than 8 decimal places rather than rounding, so an
    unexpected precision change from the exchange is a loud failure at ingest instead of
    silent corruption on disk.

    **Every rejection is a `ValueError`, including the ones that go via `Decimal`.** That
    uniformity is load-bearing rather than tidy: callers name the offending column by
    catching the failure and re-raising with context (`bulk_layout._to_scaled`), and a
    `decimal.InvalidOperation` -- an `ArithmeticError`, not a `ValueError` -- would slip
    past that handler and surface as a bare `[<class 'ConversionSyntax'>]` with no column,
    no archive and no line number. In a 2400-file backfill that is close to
    undiagnosable, and the trigger is unremarkable: any garbage containing an `e` takes
    the `Decimal` branch, and `"not-a-price"` ends in one.
    """
    s = text.strip()
    if not s:
        raise ValueError("empty numeric string")

    negative = s[0] == "-"
    if negative or s[0] == "+":
        s = s[1:]

    # The sign is consumed once, so anything sign-shaped still at the front is malformed.
    # This is not pedantry about input hygiene: the sign is re-applied at the end of this
    # function, so a second one was applied twice and cancelled. `"--1.5"` parsed as -1.5
    # and then came back +0.5 -- a wrong number, of the wrong sign, with no error. A price
    # feed will not send that, but this function is also the ingest path for six years of
    # third-party archives, and the failure it produces is a plausible value rather than a
    # crash.
    if not s or s[0] in "+-":
        raise ValueError(
            f"{text!r} is not a decimal number: a sign may appear once, at the front"
        )

    # Binance uses plain decimal notation, but scientific notation appears occasionally
    # in less-travelled fields. Decimal parses it exactly, so route it there rather than
    # hand-rolling exponent handling.
    if "e" in s or "E" in s:
        try:
            parsed = Decimal(s)
        except ArithmeticError:
            raise ValueError(f"{text!r} is not a decimal number") from None
        value = decimal_to_scaled(parsed)
        return -value if negative else value

    int_part, _, frac_part = s.partition(".")
    # `int()` is more permissive than a decimal literal: it accepts surrounding whitespace
    # and its own sign, so `"1 .5"` and `"1.-5"` would both come back as numbers. Both
    # halves are checked as digits here instead, which also rejects a second decimal point
    # (it lands in `frac_part`) and the bare `"."` and `"-"` that would otherwise scale to
    # a confident zero.
    if not (int_part + frac_part).isdigit():
        raise ValueError(f"{text!r} is not a decimal number")
    if len(frac_part) > SCALE_EXP:
        # Trailing zeros carry no information, so strip them before deciding this is a
        # genuine precision overflow -- "1.000000000" is representable, "1.000000001" is
        # not.
        stripped = frac_part.rstrip("0")
        if len(stripped) > SCALE_EXP:
            raise ValueError(
                f"value {text!r} has more than {SCALE_EXP} decimal places; "
                "storing it would silently lose precision"
            )
        frac_part = stripped

    # int('') raises, and both halves are legitimately empty for inputs like "5" or ".5".
    whole = int(int_part) if int_part else 0
    frac = int(frac_part.ljust(SCALE_EXP, "0")) if frac_part else 0

    value = whole * SCALE + frac
    return _check_range(-value if negative else value, text)


def decimal_to_scaled(value: Decimal) -> int:
    """Convert a `Decimal` to a scaled int64. Exact; raises rather than rounding.

    `NaN` and the infinities are refused first, and refused as `ValueError`. They are not
    hypothetical -- `Decimal("1E999999")` is a well-formed literal that overflows to
    `Infinity` on `scaleb` -- and without the guard `NaN` would trip the precision check
    (a misleading message about decimal places) while `Infinity` would reach `int()` and
    raise `OverflowError`, which is an `ArithmeticError` and would therefore escape every
    caller that filters on `ValueError`.
    """
    if not value.is_finite():
        raise ValueError(f"{value} is not a finite decimal; refusing to store it")
    try:
        shifted = value.scaleb(SCALE_EXP)
        integral = shifted.to_integral_value()
        exact = shifted == integral
        scaled = int(integral)
    except ArithmeticError as exc:
        # `Decimal("1E999999")` is finite and parses cleanly, but scaling it by 10^8
        # exceeds the default context's Emax and raises `decimal.Overflow`. Left to
        # propagate it would be an `ArithmeticError` escaping a function every caller
        # filters for `ValueError` -- the same hole the non-finite guard above closes,
        # one step further along.
        raise ValueError(f"{value} cannot be scaled to an int64: {exc}") from None

    if not exact:
        raise ValueError(
            f"value {value} has more than {SCALE_EXP} decimal places; "
            "storing it would silently lose precision"
        )
    return _check_range(scaled, value)


def from_scaled(scaled: int) -> Decimal:
    """Convert a scaled int64 back to an exact `Decimal`.

    This is the crossing point into accounting. Everything upstream is integers;
    everything downstream of a balance mutation is `Decimal`.
    """
    return Decimal(scaled).scaleb(-SCALE_EXP)


def scaled_to_str(scaled: int) -> str:
    """Render a scaled value for logs and diagnostics, without trailing-zero noise."""
    d = from_scaled(scaled)
    normalised = d.normalize()
    # normalize() renders integers in scientific notation (Decimal('1E+2')), which is
    # unreadable in a log line. quantize() back to a plain integer in that case.
    if normalised == normalised.to_integral_value():
        return str(normalised.quantize(Decimal(1)))
    return str(normalised)


def parse_money(text: str) -> Decimal:
    """Parse an exact quantity from its decimal string, for values that never hit the lake.

    `to_scaled` is the ingest path and caps precision at `SCALE_EXP` because a scaled
    int64 is what lands in Parquet. Strategy parameters are not market data -- they are
    stored as text in SQLite and only ever multiply a notional -- so that cap does not
    apply and imposing it would reject a perfectly sane `"0.000000005"` risk fraction for
    a reason that has nothing to do with the value.

    What *does* apply is the reason `to_scaled` takes a string in the first place. A param
    declared as the Python literal `0.01` is already the wrong number by the time this
    function could see it, so the type is `str` and callers are expected to have refused
    floats upstream, where the offending literal still has a line number.
    """
    s = text.strip()
    if not s:
        raise ValueError("empty numeric string")
    try:
        value = Decimal(s)
    except ArithmeticError:
        # Uniform `ValueError` for the same reason `to_scaled` documents: an
        # `InvalidOperation` is an `ArithmeticError` and escapes every caller that filters
        # for `ValueError`, surfacing as a bare `[<class 'ConversionSyntax'>]`.
        raise ValueError(f"{text!r} is not a decimal number") from None
    if not value.is_finite():
        raise ValueError(f"{text!r} is not a finite decimal number")
    if value != 0 and not -MAX_MONEY_EXPONENT <= value.adjusted() <= MAX_MONEY_EXPONENT:
        # `Decimal("1E+999999999")` is finite and parses in nine characters, and
        # `money_to_str` then renders it in a *billion* -- because `format(v, "f")` writes
        # out every digit. Rendering happens in the API response, the config form and the
        # export manifest, so a nine-character strategy param would allocate a gigabyte of
        # string in the server process. Bounded here rather than by making the renderer
        # switch notation, because the exponent is the thing that is wrong: no price,
        # quantity, rate or risk fraction lives outside 1e-30 .. 1e+30, so a value that
        # does is a typo, not a number.
        raise ValueError(
            f"{text!r} has an exponent outside 1e-{MAX_MONEY_EXPONENT} .. "
            f"1e+{MAX_MONEY_EXPONENT}; no price, size or rate is that large or small"
        )
    return value


def money_to_str(value: Decimal) -> str:
    """Render an exact quantity as plain decimal text.

    `str(Decimal)` switches to scientific notation once the adjusted exponent drops below
    -6, so `Decimal("0.0000001")` renders as `1E-7`. That round-trips perfectly and reads
    terribly: it is the value in an auto-generated config form, in an order in the event
    log, and in an exported bundle's manifest. An author who wrote `"0.00000001"` for a
    1000-prefixed meme pair's tick and is shown `1E-8` has been handed a different-looking
    number and no explanation.

    `format(value, "f")` forces fixed-point and is exact -- it is a rendering choice, not a
    rounding one, so the parsed value is unchanged either way.
    """
    return format(value, "f")


def quantize_price(price: Decimal, tick_size: Decimal, side: Side) -> Decimal:
    """Round a price to the symbol's tick, *against* the trader (spec 3.1).

    A buy rounds down and a sell rounds up. This is deliberately the unfavourable
    direction for fill probability: a buy limit placed lower is less likely to fill, so a
    backtest that rounds this way understates rather than overstates execution. Rounding
    to nearest would let quantisation nudge orders into fills they would not have got,
    which is exactly the kind of small optimism spec 1.4 rules out.
    """
    if tick_size <= 0:
        raise ValueError(f"tick_size must be positive, got {tick_size}")
    if price < 0:
        raise ValueError(f"price must be non-negative, got {price}")

    rounding = ROUND_FLOOR if side is Side.BUY else ROUND_CEILING
    steps = (price / tick_size).to_integral_value(rounding=rounding)
    return steps * tick_size


def quantize_qty(qty: Decimal, step_size: Decimal) -> Decimal:
    """Round a quantity down to the symbol's step (spec 3.1).

    Always down, never up, regardless of side. Rounding a quantity up can breach position
    limits, exceed available margin, or push an order past `maxQty` -- all of which the
    exchange would reject in live while the backtest happily filled them.
    """
    if step_size <= 0:
        raise ValueError(f"step_size must be positive, got {step_size}")
    if qty < 0:
        raise ValueError(f"qty must be non-negative, got {qty}")

    steps = (qty / step_size).to_integral_value(rounding=ROUND_FLOOR)
    return steps * step_size


_ENTRY_PRICE_QUANTUM = Decimal(1).scaleb(-SCALE_EXP)


def quantize_money(value: Decimal) -> Decimal:
    """Round a monetary amount to the storage seam's 8 decimal places.

    Applied wherever an amount is produced by a *division* and then enters the ledger or
    the margin allocation. Division is the only operation that grows a `Decimal` to the
    full context precision, and an operand carrying 44 decimal places against a six-figure
    balance sits right at the 50-digit ceiling -- so the next addition rounds, and `W` and
    `W0 + sum(realized) - sum(fees) + sum(funding)` part company in the last place. Spec
    3.10 permits no epsilon to absorb that.

    Two amounts need it: initial margin (`notional / leverage`) and the margin an
    overdrawn position surrenders on liquidation, which inherits the same division through
    the allocation. Both are real amounts of money that an exchange reports at finite
    precision; carrying them to fifty digits models nothing.
    """
    return value.quantize(_ENTRY_PRICE_QUANTUM, rounding=ROUND_HALF_EVEN)


def quantize_entry_price(price: Decimal) -> Decimal:
    """Round a VWAP entry price to the storage seam's 8 decimal places.

    Entry price is the one accounting quantity produced by a *division* -- spec 3.3's
    `Pe' = (|Q|*Pe + |f|*Pf) / (|Q| + |f|)` -- and division is where a `Decimal` grows to
    the full context precision. Left unrounded, a 50-significant-digit entry price flows
    into every subsequent realized-PnL calculation, and the operand growth eventually
    forces a rounded addition somewhere in the ledger. Spec 3.10 permits no epsilon to
    absorb that.

    Eight decimals is not an arbitrary cut. It is `SCALE_EXP`, the same precision every
    price in the Parquet lake is stored at, so a quantised entry price is exactly
    representable in the storage seam and round-trips through `decimal_to_scaled` without
    loss. Rounding is half-even rather than against the trader: entry price is a recorded
    average, not an order price, and biasing it would misstate unrealised PnL in a fixed
    direction on every position.

    The residual is real but bounded and one-directional-free: at most 5e-9 of a quote unit
    on the average price, against positions whose smallest meaningful increment is 1e-8.
    Binance's own reported `entryPrice` is likewise a rounded figure.
    """
    return price.quantize(_ENTRY_PRICE_QUANTUM, rounding=ROUND_HALF_EVEN)


def is_multiple(value: Decimal, increment: Decimal) -> bool:
    """Exact multiple test, for invariant I6 (spec 3.10).

    Exact by construction -- no epsilon. If this ever needs a tolerance, a float has
    leaked into the accounting layer and the tolerance would only hide it.
    """
    if increment <= 0:
        raise ValueError(f"increment must be positive, got {increment}")
    return value % increment == 0

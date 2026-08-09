"""Shared fixtures for the Phase 2 accounting tests.

Two things here are deliberate and worth reading before using them.

**The bracket tables are constructed, not snapshotted.** `leverageBracket` is a *signed*
endpoint and no API keys exist yet (finding F3 in `docs/DATA_AVAILABILITY.md`), so there is
no real BTCUSDT bracket table on disk to test against. Rather than transcribe remembered
values and let them read as reference data, `graduated_bracket_table` takes the tier shape
-- caps, rates, leverage caps -- and *derives* every maintenance amount from the continuity
requirement. The result is a table that is internally consistent by construction, which is
what the arithmetic under test actually needs, and which cannot be mistaken for a snapshot
of what Binance publishes.

**The spec's own worked examples assume a flat maintenance rate.** Spec 3.7's short check
(`P_liq = 55 000 / 1.004 = 54 780.88`) is computed at `MMR = 0.004` throughout, but a
54 780.88 liquidation price on 1 BTC is a 54 780 notional, which on any realistic BTCUSDT
table has already crossed out of the lowest tier. Reproducing the spec's numbers therefore
requires `single_bracket_table`; using a graduated table there would fail the golden test
for a correct reason, and that difference is itself worth a test -- see the bracket-boundary
golden case.
"""

from __future__ import annotations

from decimal import Decimal

from perplab.core.margin import BracketTable, LeverageBracket
from perplab.exchange.filters import SymbolFilters, parse_symbol

__all__ = [
    "BTCUSDT_PAYLOAD",
    "btcusdt_filters",
    "single_bracket_table",
    "graduated_bracket_table",
    "SPEC_MMR",
]

SPEC_MMR = Decimal("0.004")
"""The maintenance margin rate used throughout spec 3.7 and 3.9's worked examples."""

BTCUSDT_PAYLOAD = {
    "symbol": "BTCUSDT",
    "status": "TRADING",
    "contractType": "PERPETUAL",
    "pricePrecision": 2,
    "quantityPrecision": 3,
    "onboardDate": 1569398400000,
    "filters": [
        {"filterType": "PRICE_FILTER", "tickSize": "0.10", "minPrice": "556.80", "maxPrice": "4529764"},
        {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001", "maxQty": "1000"},
        {"filterType": "MARKET_LOT_SIZE", "stepSize": "0.001", "minQty": "0.001", "maxQty": "120"},
        {"filterType": "MIN_NOTIONAL", "notional": "50"},
        {
            "filterType": "PERCENT_PRICE",
            "multiplierUp": "1.0500",
            "multiplierDown": "0.9500",
            "multiplierDecimal": "4",
        },
        {"filterType": "MAX_NUM_ORDERS", "limit": 200},
    ],
}
"""The real BTCUSDT entry from the 2026-08-01 `exchangeInfo` snapshot, trimmed.

Real values rather than invented ones, for the reason `tests/unit/test_filters.py` gives:
two of the spec's stated assumptions turned out to be wrong against the actual payload, and
a hand-written fixture reproduces assumptions instead of reality. `tickSize` 0.10 and
`stepSize` 0.001 are what invariant I6 is checked against here.
"""


def btcusdt_filters() -> SymbolFilters:
    return parse_symbol(BTCUSDT_PAYLOAD)


def single_bracket_table(
    symbol: str = "BTCUSDT",
    mmr: Decimal = SPEC_MMR,
    max_leverage: int = 125,
) -> BracketTable:
    """One tier covering every notional -- the world the spec's worked examples live in.

    The cap is 10^12, far above anything BTCUSDT could sustain, so the fixed-point
    iteration in `margin.liquidation_price` always settles on the first pass. That is the
    point: it isolates the liquidation algebra from the bracket resolution, so a failing
    golden test here means the formula is wrong rather than the tier selection.
    """
    return BracketTable(
        symbol=symbol,
        brackets=(
            LeverageBracket(
                bracket=1,
                max_leverage=max_leverage,
                notional_floor=Decimal(0),
                notional_cap=Decimal(10) ** 12,
                mmr=mmr,
                maintenance_amount=Decimal(0),
            ),
        ),
    )


def graduated_bracket_table(
    tiers: tuple[tuple[str, str, int], ...] | None = None,
    symbol: str = "BTCUSDT",
) -> BracketTable:
    """A multi-tier table whose maintenance amounts are derived from continuity.

    `tiers` is `(notional_cap, mmr, max_leverage)` per tier, ascending. The maintenance
    amount of each tier is solved so that maintenance margin is continuous at the boundary
    with the one below it:

    ```
    N_b * MMR_i - MA_i  ==  N_b * MMR_(i-1) - MA_(i-1)
    MA_i = MA_(i-1) + N_b * (MMR_i - MMR_(i-1))
    ```

    That is what `cum` is *for* on the real endpoint, and deriving it here rather than
    quoting numbers means the fixture cannot drift into being subtly discontinuous -- which
    would make a bracket-crossing test fail for a reason having nothing to do with the code
    under test.
    """
    shape = tiers or (
        ("50000", "0.004", 125),
        ("600000", "0.005", 100),
        ("3000000", "0.01", 50),
        ("12000000", "0.025", 20),
        ("70000000", "0.05", 10),
    )

    brackets: list[LeverageBracket] = []
    previous_cap = Decimal(0)
    previous_mmr = Decimal(0)
    previous_amount = Decimal(0)

    for index, (cap, mmr_text, leverage) in enumerate(shape, start=1):
        mmr = Decimal(mmr_text)
        amount = (
            Decimal(0)
            if index == 1
            else previous_amount + previous_cap * (mmr - previous_mmr)
        )
        brackets.append(
            LeverageBracket(
                bracket=index,
                max_leverage=leverage,
                notional_floor=previous_cap,
                notional_cap=Decimal(cap),
                mmr=mmr,
                maintenance_amount=amount,
            )
        )
        previous_cap, previous_mmr, previous_amount = Decimal(cap), mmr, amount

    return BracketTable(symbol=symbol, brackets=tuple(brackets))

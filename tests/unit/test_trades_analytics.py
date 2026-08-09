"""Round-trip reconstruction and the spec 8.4 attribution identity."""

from __future__ import annotations

from decimal import Decimal

import pytest

from perplab.analytics.attribution import AttributionMismatch, build_attribution
from perplab.analytics.trades import CloseReason, TradeBuilder


def D(text: str) -> Decimal:
    return Decimal(text)


def open_long(builder: TradeBuilder, ts: int, qty: str, price: str, fee: str = "0") -> None:
    builder.fill(
        ts_ms=ts,
        symbol="BTCUSDT",
        signed_qty=D(qty),
        price=D(price),
        fee=D(fee),
        realized=D("0"),
        qty_before=D("0"),
        qty_after=D(qty),
    )


def test_a_scale_in_and_scale_out_is_one_trade_not_four() -> None:
    """Spec 8.1 rule 3. Counting legs as trades inflates trade count and distorts win rate."""
    builder = TradeBuilder()
    open_long(builder, 1000, "1", "100")
    builder.fill(ts_ms=2000, symbol="BTCUSDT", signed_qty=D("1"), price=D("110"),
                 fee=D("0"), realized=D("0"), qty_before=D("1"), qty_after=D("2"))
    builder.fill(ts_ms=3000, symbol="BTCUSDT", signed_qty=D("-1"), price=D("120"),
                 fee=D("0"), realized=D("15"), qty_before=D("2"), qty_after=D("1"))
    builder.fill(ts_ms=4000, symbol="BTCUSDT", signed_qty=D("-1"), price=D("130"),
                 fee=D("0"), realized=D("25"), qty_before=D("1"), qty_after=D("0"))
    trades = builder.finish(5000)

    assert len(trades) == 1
    trade = trades[0]
    assert trade.legs == 4
    assert trade.side == "LONG"
    assert trade.entry_ms == 1000 and trade.exit_ms == 4000
    # Entry is the VWAP of the two opening legs, exit the VWAP of the two closing ones.
    assert trade.entry_price == D("105")
    assert trade.exit_price == D("125")
    assert trade.max_qty == D("2")
    assert trade.realized_pnl == D("40")
    assert trade.close_reason == CloseReason.SIGNAL


def test_a_flip_closes_one_round_trip_and_opens_another() -> None:
    """Spec 3.3 case C. The old position realises in full; the residual is a new trade."""
    builder = TradeBuilder()
    open_long(builder, 1000, "1", "100", fee="1")
    builder.fill(ts_ms=2000, symbol="BTCUSDT", signed_qty=D("-3"), price=D("90"),
                 fee=D("27"), realized=D("-10"), qty_before=D("1"), qty_after=D("-2"))
    trades = builder.finish(3000)

    assert len(trades) == 2
    closed, residual = trades
    assert closed.side == "LONG" and closed.exit_ms == 2000
    assert closed.realized_pnl == D("-10")
    # The 27 fee covers three units; one of them closed the long, so nine belongs there.
    assert closed.fees == D("1") + D("9")
    assert residual.side == "SHORT"
    assert residual.entry_price == D("90")
    assert residual.fees == D("18")
    assert residual.is_open


def test_a_still_open_position_is_reported_open_and_never_force_closed() -> None:
    """Closing it would invent a fill the strategy never asked for -- and would break spec
    12.3's prefix property for a reason unrelated to look-ahead."""
    builder = TradeBuilder()
    open_long(builder, 1000, "1", "100")
    trades = builder.finish(9999)
    assert trades[0].is_open
    assert trades[0].exit_ms is None
    assert trades[0].exit_price is None
    assert trades[0].close_reason == CloseReason.OPEN


def test_a_liquidation_closes_the_trade_at_the_trigger_price() -> None:
    builder = TradeBuilder()
    open_long(builder, 1000, "1", "100")
    builder.liquidation(
        ts_ms=2000, symbol="BTCUSDT", closed_qty=D("-1"), price=D("80"), realized=D("-20")
    )
    trades = builder.finish(3000)
    assert trades[0].close_reason == CloseReason.LIQUIDATION
    assert trades[0].exit_price == D("80")
    assert trades[0].realized_pnl == D("-20")


def test_mae_and_mfe_track_the_trade_total_and_the_price_extremes() -> None:
    """MAE in price space is what an author reads when deciding where a stop belongs; MAE in
    PnL is what spec 8.3 asks for. Both are recorded, and they answer different questions."""
    builder = TradeBuilder()
    open_long(builder, 1000, "1", "100", fee="1")
    builder.mark(symbol="BTCUSDT", mark_price=D("95"), unrealized=D("-5"))
    builder.mark(symbol="BTCUSDT", mark_price=D("130"), unrealized=D("30"))
    builder.mark(symbol="BTCUSDT", mark_price=D("110"), unrealized=D("10"))
    trades = builder.finish(4000)

    trade = trades[0]
    assert trade.mae == D("-6")  # -5 unrealised, less the 1 fee already paid
    assert trade.mfe == D("29")
    assert trade.mae_price == D("95")
    assert trade.mfe_price == D("130")


def test_adverse_means_the_high_for_a_short() -> None:
    """Recording the raw min and max would make the two columns mean opposite things
    depending on which way the trade was facing."""
    builder = TradeBuilder()
    builder.fill(ts_ms=1000, symbol="BTCUSDT", signed_qty=D("-1"), price=D("100"),
                 fee=D("0"), realized=D("0"), qty_before=D("0"), qty_after=D("-1"))
    builder.mark(symbol="BTCUSDT", mark_price=D("120"), unrealized=D("-20"))
    builder.mark(symbol="BTCUSDT", mark_price=D("80"), unrealized=D("20"))
    trade = builder.finish(2000)[0]
    assert trade.side == "SHORT"
    assert trade.mae_price == D("120")
    assert trade.mfe_price == D("80")


def test_funding_lands_on_the_trade_that_was_open_at_the_time() -> None:
    builder = TradeBuilder()
    open_long(builder, 1000, "1", "100")
    builder.funding(symbol="BTCUSDT", cashflow=D("-0.5"))
    builder.fill(ts_ms=3000, symbol="BTCUSDT", signed_qty=D("-1"), price=D("110"),
                 fee=D("2"), realized=D("10"), qty_before=D("1"), qty_after=D("0"))
    trade = builder.finish(4000)[0]
    assert trade.funding == D("-0.5")
    assert trade.net_pnl == D("10") - D("2") + D("-0.5")


def test_funding_with_no_open_trade_is_ignored_rather_than_lost_into_a_new_one() -> None:
    builder = TradeBuilder()
    builder.funding(symbol="BTCUSDT", cashflow=D("-1"))
    assert builder.finish(1000) == ()


# ------------------------------------------------------------------------- attribution


def ledger(**overrides: str) -> dict[str, Decimal]:
    base = {
        "price_pnl": D("100"),
        "realized_pnl": D("90"),
        "unrealized_pnl": D("10"),
        "funding_pnl": D("-5"),
        "fees": D("3"),
        "liquidation_cost": D("0"),
        "net_pnl": D("92"),
    }
    base.update({k: D(v) for k, v in overrides.items()})
    return base


def test_the_four_way_split_closes_exactly() -> None:
    """Spec 8.4 requires the components to sum to the total with no tolerance.

    `price_pnl` is reported at *reference* prices, so the ledger's actual-price figure has
    the signed slippage added back; subtracting it again is what makes the identity hold.
    """
    attribution = build_attribution(
        ledger(), slippage_cost=D("7"), slippage_abs=D("9")
    )
    assert attribution.price_pnl == D("107")
    assert attribution.slippage_cost == D("7")
    assert attribution.slippage_abs == D("9")
    assert (
        attribution.price_pnl
        + attribution.funding_pnl
        - attribution.fees
        - attribution.slippage_cost
        + attribution.liquidation_cost
    ) == attribution.net_pnl


def test_a_mismatched_ledger_is_fatal_with_no_epsilon_offered() -> None:
    """Reaching for a tolerance here would hide the accumulator disagreement it exists to
    catch -- the same argument spec 3.10 makes about the ledger's own invariants."""
    with pytest.raises(AttributionMismatch, match="off by"):
        build_attribution(ledger(net_pnl="91.99"), slippage_cost=D("7"), slippage_abs=D("7"))


def test_favourable_execution_produces_a_negative_slippage_and_still_closes() -> None:
    attribution = build_attribution(
        ledger(), slippage_cost=D("-4"), slippage_abs=D("4")
    )
    assert attribution.price_pnl == D("96")
    assert attribution.slippage_cost == D("-4")
    assert attribution.net_pnl == D("92")


def test_liquidation_cost_is_its_own_column_and_stays_out_of_price_pnl() -> None:
    """A position can be liquidated *while in profit* once funding has drained its margin.

    With the penalty folded into `price_pnl`, that run reports a price leg of zero for a
    position whose price leg was positive.
    """
    attribution = build_attribution(
        ledger(price_pnl="50", liquidation_cost="-40", net_pnl="2"),
        slippage_cost=D("0"),
        slippage_abs=D("0"),
    )
    assert attribution.price_pnl == D("50")
    assert attribution.liquidation_cost == D("-40")
    assert attribution.net_pnl == D("2")

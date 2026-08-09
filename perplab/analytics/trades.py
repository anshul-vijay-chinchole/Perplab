"""Round-trip reconstruction (spec 8.3).

**A trade is flat -> flat.** Spec 8.1 is explicit and the reason is that the alternative
inflates everything downstream: *"Scale-ins and partial exits are legs within one trade, not
separate trades. Counting legs as trades inflates trade count and distorts win rate."* A
strategy that pyramids into a winner over four fills and exits in two has made **one**
trade, and its win rate should say so.

**Built as the run happens, not reconstructed from it.** Spec 8.3 requires MAE and MFE to be
*"computed from the mark price series inside the engine, not reconstructed afterwards"*, and
that is not a performance note. The mark series between two fills is thousands of samples
that no run artefact stores; reconstructing the worst excursion after the fact would mean
re-reading the lake and hoping it had not changed, which is exactly the reproducibility hole
spec 4.6 exists to close.

**A flip closes one trade and opens another in the same fill.** Spec 3.3 case C: the old
position realises in full and the residual opens fresh. The fill's commission covers both
halves, so it is split by quantity -- charging it all to either side would make one trade
look better than it was and the other worse.

**In hedge mode, "flat" is flat *per side*.** The book is keyed by `(symbol, PositionSide)`
rather than by symbol, and that is a restatement of spec 8.1 rather than a departure from
it. A strategy holding a long and a short on BTCUSDT has two round-trips running at once,
opened at different prices and closing at different times; keyed by symbol they would be one
trade whose entry price was a weighted average of two positions that never existed together,
whose MAE was the excursion of their sum, and which "closed" the moment either leg went
flat. Per side, each is exactly the trade spec 8.1 describes -- and the one-way case is
literally unchanged, because a one-way symbol has exactly one side.

Case C cannot arise on a hedged side (the ledger refuses the flip), so the residual branch
below serves one-way runs only. It is kept rather than guarded because it is still correct
there, and a `BOTH` fill reaching it means precisely what it always meant.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, localcontext
from typing import Any

from perplab.core.money import ACCOUNTING_CONTEXT, Money, money_to_str, quantize_money
from perplab.core.types import PositionSide

__all__ = ["Trade", "TradeBuilder", "CloseReason"]

TradeKey = tuple[str, PositionSide]
"""How an in-flight round-trip is addressed. Mirrors `core.account.PositionKey`, and must:
a trade is the life of a position, so the two have to agree on what a position is."""


def _sign(value: Decimal) -> int:
    return (value > 0) - (value < 0)


_SIDE_ORDER = {PositionSide.BOTH: 0, PositionSide.LONG: 1, PositionSide.SHORT: 2}


def _key_order(key: TradeKey) -> tuple[str, int]:
    """A total order over trade keys, so trade indices are reproducible.

    `sorted()` on the raw tuple would compare two `PositionSide` enums, which have no
    ordering, and a run's trade numbering would then depend on dict insertion order --
    which spec 12.1 requires it not to."""
    return (key[0], _SIDE_ORDER[key[1]])


class CloseReason:
    """Why a round-trip ended. Plain strings -- they are written to Parquet and read in the UI."""

    SIGNAL = "signal"
    LIQUIDATION = "liquidation"
    OPEN = "open"
    """Still open when the run ended.

    Reported rather than force-closed. Closing the position at the last mark would
    manufacture a fill the strategy never asked for, and -- worse for spec 12.3 -- it would
    make a run truncated at bar N differ from the full run at bar N, breaking the
    look-ahead test's prefix property for a reason that has nothing to do with look-ahead.
    """


@dataclass(frozen=True, slots=True)
class Trade:
    """One completed (or still-open) round-trip."""

    index: int
    symbol: str
    side: str
    """`LONG` or `SHORT`, taken from the first leg."""

    entry_ms: int
    exit_ms: int | None
    entry_price: Money
    """Quantity-weighted average of the legs that *opened or increased* the position."""
    exit_price: Money | None
    """Quantity-weighted average of the legs that *reduced* it. `None` while open."""

    max_qty: Money
    """Largest absolute position held during the trade -- the size that was actually at risk."""

    realized_pnl: Money
    fees: Money
    funding: Money
    mae: Money
    """Maximum adverse excursion, in quote currency, on the trade's *total* PnL.

    Spec 8.3 phrases MAE as "worst unrealised loss during the trade". For a single-leg
    round-trip the two are the same number. For a multi-leg one they are not, and total PnL
    is the honest generalisation: a trade that has already banked half its size at a profit
    is not underwater merely because the remainder is. `mae_price` carries the price-space
    figure alongside, which is the one an author reads when deciding where a stop belongs.
    """
    mfe: Money
    mae_price: Money
    """Worst mark price seen while the trade was open, from the trade's own perspective."""
    mfe_price: Money
    legs: int
    close_reason: str
    position_side: str = "BOTH"
    """Which position slot this round-trip lived in: `BOTH`, `LONG` or `SHORT`.

    Distinct from `side`, and the two always agree in hedge mode while only `side` carries
    information in one-way mode. They answer different questions: `side` is which way the
    trade faced, `position_side` is which of the symbol's two books it was in. Last in the
    field order and defaulted, so every existing construction and every stored `trades.json`
    from before hedge mode reads back as the one-way trade it was.
    """

    @property
    def net_pnl(self) -> Money:
        """Realised price PnL, less commissions, plus funding.

        Matches the wallet: `apply_fill` books `realized - fee` and `apply_funding` books
        the cashflow, so summing these three over a closed trade reproduces exactly what
        that trade did to the balance.
        """
        with localcontext(ACCOUNTING_CONTEXT):
            return self.realized_pnl - self.fees + self.funding

    @property
    def duration_ms(self) -> int | None:
        return None if self.exit_ms is None else self.exit_ms - self.entry_ms

    @property
    def is_open(self) -> bool:
        return self.exit_ms is None

    def to_json(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "symbol": self.symbol,
            "side": self.side,
            "position_side": self.position_side,
            "entry_ms": self.entry_ms,
            "exit_ms": self.exit_ms,
            "entry_price": money_to_str(self.entry_price),
            "exit_price": None if self.exit_price is None else money_to_str(self.exit_price),
            "max_qty": money_to_str(self.max_qty),
            "realized_pnl": money_to_str(self.realized_pnl),
            "fees": money_to_str(self.fees),
            "funding": money_to_str(self.funding),
            "net_pnl": money_to_str(self.net_pnl),
            "mae": money_to_str(self.mae),
            "mfe": money_to_str(self.mfe),
            "mae_price": money_to_str(self.mae_price),
            "mfe_price": money_to_str(self.mfe_price),
            "legs": self.legs,
            "duration_ms": self.duration_ms,
            "close_reason": self.close_reason,
        }


@dataclass
class _Open:
    """A round-trip in progress."""

    symbol: str
    side: str
    entry_ms: int
    position_side: PositionSide = PositionSide.BOTH
    open_qty: Decimal = Decimal(0)
    open_notional: Decimal = Decimal(0)
    close_qty: Decimal = Decimal(0)
    close_notional: Decimal = Decimal(0)
    max_qty: Decimal = Decimal(0)
    realized: Decimal = Decimal(0)
    fees: Decimal = Decimal(0)
    funding: Decimal = Decimal(0)
    legs: int = 0
    mae: Decimal | None = None
    mfe: Decimal | None = None
    mae_price: Decimal | None = None
    mfe_price: Decimal | None = None


def _extreme(trade: _Open, exit_price: Decimal | None, *, adverse: bool) -> Decimal | None:
    """The adverse or favourable price extreme, including the exit if there was one.

    "Adverse" is the low for a long and the high for a short, so the two columns mean the
    same thing whichever way the trade was facing rather than swapping meaning with the side.
    """
    recorded = trade.mae_price if adverse else trade.mfe_price
    candidates = [value for value in (recorded, exit_price) if value is not None]
    if not candidates:
        return None
    worse_is_lower = (trade.side == "LONG") == adverse
    return min(candidates) if worse_is_lower else max(candidates)


@dataclass
class TradeBuilder:
    """Accumulates round-trips as fills, funding and marks arrive.

    Driven by the engine rather than by a post-processing pass, so it sees every mark
    sample. `mark()` is the hot call -- once per mark bar per open position -- and does
    nothing at all when nothing is open.
    """

    trades: list[Trade] = field(default_factory=list)
    _open: dict[TradeKey, _Open] = field(default_factory=dict, init=False)
    _closed_net: dict[str, Decimal] = field(default_factory=dict, init=False)
    """Cumulative net PnL of *closed* round-trips per symbol, maintained as they close.

    Keyed by **symbol** rather than by `(symbol, side)`, unlike everything else here, and
    deliberately: this feeds spec 9.5's portfolio curve, which asks "what has BTCUSDT done
    for this account", and both legs of a hedge are BTCUSDT doing something. Splitting it
    per side would make a market-neutral pair look like two uncorrelated instruments in the
    correlation matrix, which is the opposite of what it is.

    Kept incrementally rather than summed on demand: the portfolio curve reads the
    per-symbol total once per mark bar, and re-summing a ten-thousand-trade list half a
    million times is O(n*m) for a number this dict holds in O(1)."""
    _open_unrealized: dict[TradeKey, Decimal] = field(default_factory=dict, init=False)
    """The unrealised PnL each open trade last saw, at its most recent mark sample.

    What makes `net_pnl_by_symbol` answerable without a price: the caller asks "where
    does this symbol stand *now*", and now is the last mark -- the same LOCF reading the
    equity curve itself is built from (spec 3.4). Keyed per side, because that is what a
    mark sample updates -- the two legs of a hedge have different unrealised PnLs at the
    same mark."""

    # ------------------------------------------------------------------------- fills

    def fill(
        self,
        *,
        ts_ms: int,
        symbol: str,
        signed_qty: Decimal,
        price: Decimal,
        fee: Decimal,
        realized: Decimal,
        qty_before: Decimal,
        qty_after: Decimal,
        position_side: PositionSide = PositionSide.BOTH,
    ) -> None:
        """Record one fill against the open round-trip, opening or closing as needed.

        `qty_before` and `qty_after` are **this side's** quantities, not the symbol's net.
        Feeding it the net would make every hedge fill look like a reduce or a flip of a
        position that does not exist.
        """
        key = (symbol, position_side)
        with localcontext(ACCOUNTING_CONTEXT):
            before_sign = _sign(qty_before)
            fill_sign = _sign(signed_qty)

            if before_sign == 0:
                self._start(ts_ms, key, fill_sign)
                self._add_open_leg(key, abs(signed_qty), price, fee, qty_after)
                self._open[key].legs += 1
                return

            trade = self._open.get(key)
            if trade is None:  # pragma: no cover - a position with no trade is I-9 territory
                self._start(ts_ms, key, before_sign)
                trade = self._open[key]

            if fill_sign == before_sign:
                self._add_open_leg(key, abs(signed_qty), price, fee, qty_after)
                trade.legs += 1
                return

            closed_qty = min(abs(signed_qty), abs(qty_before))
            residual = abs(signed_qty) - closed_qty

            # The commission covers the whole fill. Split it by quantity so a flip does not
            # charge the residual position's fee to the trade it closed.
            closing_fee = fee if residual == 0 else quantize_money(
                fee * closed_qty / abs(signed_qty)
            )
            trade.close_qty += closed_qty
            trade.close_notional += closed_qty * price
            trade.realized += realized
            trade.fees += closing_fee
            trade.legs += 1

            if qty_after == 0 or _sign(qty_after) != before_sign:
                self._finish(key, ts_ms, CloseReason.SIGNAL)

            if residual > 0:
                # One-way only: a hedged side cannot flip, so `residual` is zero there by
                # construction (`account.HedgeFlipRefused` refuses the fill upstream).
                self._start(ts_ms, key, _sign(qty_after))
                self._add_open_leg(key, residual, price, fee - closing_fee, qty_after)
                self._open[key].legs += 1

    def liquidation(
        self,
        *,
        ts_ms: int,
        symbol: str,
        closed_qty: Decimal,
        price: Decimal,
        realized: Decimal,
        position_side: PositionSide = PositionSide.BOTH,
    ) -> None:
        """A liquidation closes the round-trip at the solved `P_liq` (spec 3.7).

        One side, not the symbol: in hedge mode a liquidation destroys the leg whose own
        allocation was exhausted and leaves the other one open and trading.
        """
        key = (symbol, position_side)
        trade = self._open.get(key)
        if trade is None:  # pragma: no cover
            return
        with localcontext(ACCOUNTING_CONTEXT):
            trade.close_qty += abs(closed_qty)
            trade.close_notional += abs(closed_qty) * price
            trade.realized += realized
            trade.legs += 1
        self._finish(key, ts_ms, CloseReason.LIQUIDATION)

    def funding(
        self,
        *,
        symbol: str,
        cashflow: Decimal,
        position_side: PositionSide = PositionSide.BOTH,
    ) -> None:
        trade = self._open.get((symbol, position_side))
        if trade is None:
            return
        with localcontext(ACCOUNTING_CONTEXT):
            trade.funding += cashflow

    def mark(
        self,
        *,
        symbol: str,
        mark_price: Decimal,
        unrealized: Decimal,
        position_side: PositionSide = PositionSide.BOTH,
    ) -> None:
        """Update the excursion extremes from one mark sample.

        The excursion is measured on the trade's running total -- realised legs, less fees,
        plus funding, plus the current unrealised -- so a scaled-out trade is not recorded
        as underwater merely because its remainder is.

        `unrealized` is **this side's**. The two legs of a hedge see the same mark and have
        opposite unrealised PnLs; handing either of them the sum would give both an MAE of
        roughly zero and make a pair whose legs each swung 20% look like it never moved --
        precisely the distribution spec 8.3 says stops get sized from.
        """
        key = (symbol, position_side)
        trade = self._open.get(key)
        if trade is None:
            return
        self._open_unrealized[key] = unrealized
        with localcontext(ACCOUNTING_CONTEXT):
            running = trade.realized - trade.fees + trade.funding + unrealized
        if trade.mae is None or running < trade.mae:
            trade.mae = running
        if trade.mfe is None or running > trade.mfe:
            trade.mfe = running
        # Price extremes are recorded from the trade's own perspective: "adverse" is the
        # low for a long and the high for a short. Recording the raw min and max instead
        # would make the two columns mean opposite things depending on the side.
        adverse_is_low = trade.side == "LONG"
        if trade.mae_price is None:
            trade.mae_price = mark_price
            trade.mfe_price = mark_price
        elif adverse_is_low:
            trade.mae_price = min(trade.mae_price, mark_price)
            trade.mfe_price = max(trade.mfe_price or mark_price, mark_price)
        else:
            trade.mae_price = max(trade.mae_price, mark_price)
            trade.mfe_price = min(trade.mfe_price or mark_price, mark_price)

    def finish(self, ts_ms: int) -> tuple[Trade, ...]:
        """Close the books. Positions still open are reported as open, never force-closed."""
        for key in sorted(self._open, key=_key_order):
            self._finish(key, None, CloseReason.OPEN)
        return tuple(self.trades)

    def snapshot(self) -> tuple[Trade, ...]:
        """What `finish` would return, without ending the run.

        `finish` is destructive -- it pops every still-open round-trip out of `_open` -- so a
        live session that called it to refresh its trade table would report the position as
        closed and then never account for the rest of it. A 48-hour paper session republishes
        `trades.json` every minute, which means the same tuple has to be buildable an
        arbitrary number of times from a book that is still being written to.

        Open trades are rendered exactly as `finish` renders them: `exit_ms` and `exit_price`
        are `None` and the reason is `OPEN`, so a reader cannot mistake one for a completed
        round-trip.
        """
        closed = tuple(self.trades)
        live = tuple(
            self._render(self._open[key], None, CloseReason.OPEN, len(closed) + offset)
            for offset, key in enumerate(sorted(self._open, key=_key_order))
        )
        return closed + live

    def net_pnl_by_symbol(self) -> dict[str, Decimal]:
        """Cumulative net PnL per symbol: closed round-trips plus the open trade's
        running total at its last mark.

        The per-symbol decomposition of the account's own PnL (spec 9.5): summed across
        symbols it reproduces what the trades did to the wallet, and per symbol it is the
        curve whose correlations the portfolio report computes. A symbol that has not
        traded is absent rather than zero -- absent is "no evidence", zero is a flat
        result, and a correlation against fabricated zeros would be a statement about
        nothing.
        """
        with localcontext(ACCOUNTING_CONTEXT):
            totals = dict(self._closed_net)
            for key, trade in self._open.items():
                running = (
                    trade.realized
                    - trade.fees
                    + trade.funding
                    + self._open_unrealized.get(key, Decimal(0))
                )
                symbol = key[0]
                totals[symbol] = totals.get(symbol, Decimal(0)) + running
        return totals

    # --------------------------------------------------------------------- internals

    def _start(self, ts_ms: int, key: TradeKey, sign: int) -> None:
        symbol, position_side = key
        self._open[key] = _Open(
            symbol=symbol,
            side="LONG" if sign > 0 else "SHORT",
            entry_ms=ts_ms,
            position_side=position_side,
        )

    def _add_open_leg(
        self,
        key: TradeKey,
        qty: Decimal,
        price: Decimal,
        fee: Decimal,
        qty_after: Decimal,
    ) -> None:
        trade = self._open[key]
        trade.open_qty += qty
        trade.open_notional += qty * price
        trade.fees += fee
        if abs(qty_after) > trade.max_qty:
            trade.max_qty = abs(qty_after)

    def _finish(self, key: TradeKey, ts_ms: int | None, reason: str) -> None:
        trade = self._open.pop(key, None)
        if trade is None:  # pragma: no cover
            return
        rendered = self._render(trade, ts_ms, reason, len(self.trades))
        self.trades.append(rendered)
        symbol = key[0]
        with localcontext(ACCOUNTING_CONTEXT):
            self._closed_net[symbol] = (
                self._closed_net.get(symbol, Decimal(0)) + rendered.net_pnl
            )
        # A closed trade has no unrealised anything. Leaving the last mark's figure here
        # would let a flip's fresh position inherit the old one's excursion for a bar.
        self._open_unrealized.pop(key, None)

    def _render(
        self, trade: _Open, ts_ms: int | None, reason: str, index: int
    ) -> Trade:
        """Freeze one round-trip into its immutable record.

        Split out of `_finish` so `snapshot` can build the same object without popping the
        book. It reads `trade` and never writes to it, which is what makes calling it once a
        minute for forty-eight hours safe.
        """
        with localcontext(ACCOUNTING_CONTEXT):
            entry_price = (
                quantize_money(trade.open_notional / trade.open_qty)
                if trade.open_qty
                else Decimal(0)
            )
            exit_price = (
                quantize_money(trade.close_notional / trade.close_qty)
                if trade.close_qty
                else None
            )
            # The trade's final state, with the closing leg's realised PnL and commission
            # booked. **It is a candidate for the excursion extremes**, and leaving it out
            # was a one-directionally optimistic bug: `mark()` is the only writer of
            # `mae`/`mfe`, and the closing leg is applied by `fill()`/`liquidation()` after
            # the last mark, so a trade could report a "worst point" better than where it
            # actually ended. A long bought at 100, marked at 101 and 102, then sold at 102
            # with a 10.05 commission lost 8.05 and reported an MAE of **+0.95**. A
            # liquidation is the acute case: its realised loss has no preceding mark at all,
            # so the event that defined the trade was absent from its own worst excursion --
            # and spec 8.3 says this distribution is what stops get sized from.
            running = trade.realized - trade.fees + trade.funding
        mae = running if trade.mae is None else min(trade.mae, running)
        mfe = running if trade.mfe is None else max(trade.mfe, running)
        return Trade(
            index=index,
            symbol=trade.symbol,
            side=trade.side,
            entry_ms=trade.entry_ms,
            exit_ms=ts_ms,
            entry_price=entry_price,
            exit_price=exit_price,
            max_qty=trade.max_qty,
            realized_pnl=trade.realized,
            fees=trade.fees,
            funding=trade.funding,
            mae=mae,
            mfe=mfe,
            # The exit price is a price the trade genuinely traded at, so it is a
            # candidate for the price-space extremes too -- for the same reason and with
            # the same acute case, a liquidation whose trigger price never appeared as a
            # mark sample.
            mae_price=_extreme(trade, exit_price, adverse=True) or entry_price,
            mfe_price=_extreme(trade, exit_price, adverse=False) or entry_price,
            legs=trade.legs,
            close_reason=reason,
            position_side=trade.position_side.value,
        )

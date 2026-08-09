"""Parse `exchangeInfo` into the enforceable filter set (spec 3.2).

Orders violating these are rejected by Binance in live, so they must be rejected
identically in backtest. Spec 3.2 puts it plainly: a backtest that lets you buy
0.13847362 BTC when `stepSize` is 0.001 is fiction. This was R2, one of the two critical
findings from the review pass.

Every value is stored as a **scaled int** (see `perplab.core.money`), not `Decimal` and
never `float`. Order validation converts to `Decimal` at the accounting boundary, which
keeps this module -- read by both data and engine code -- on the integer side of the seam.

Nothing here is hardcoded. `minNotional` is commonly quoted as 5 USDT but is 100 for
BTCUSDT; funding intervals differ per symbol and have been changed on live symbols (R17).
Both are read from the payload.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from perplab.core.money import SCALE, to_scaled

__all__ = [
    "SymbolFilters",
    "OrderCheck",
    "parse_exchange_info",
    "parse_symbol",
    "validate_order",
    "SUPPORTED_QUOTE_ASSET",
    "assert_supported_quote",
]


@dataclass(frozen=True, slots=True)
class SymbolFilters:
    """Exchange-enforced constraints for one symbol, at one point in time.

    "At one point in time" is the important part: filters change, so these are always
    read from a dated snapshot (spec 3.2). A backtest over 2023 must use the 2023
    snapshot, and a run that cannot find one is flagged `FILTERS_APPROXIMATE` rather than
    silently using today's values.
    """

    symbol: str
    status: str
    contract_type: str
    price_precision: int
    quantity_precision: int
    onboard_ms: int

    tick_size: int
    min_price: int
    max_price: int

    step_size: int
    min_qty: int
    max_qty: int

    market_step_size: int
    market_min_qty: int
    market_max_qty: int
    """MARKET_LOT_SIZE is a separate filter from LOT_SIZE, and its `maxQty` is typically
    far lower -- market orders are capped well below limit orders. Collapsing the two
    lets a backtest fill a market order the exchange would have rejected outright."""

    min_notional: int

    multiplier_up: int | None
    multiplier_down: int | None
    multiplier_decimal: int | None
    """PERCENT_PRICE: limit orders further than this from mark price are rejected."""

    max_num_orders: int | None
    max_num_algo_orders: int | None
    """Algo orders (stop/TP) are capped separately from ordinary orders.

    `None` means the exchange did not publish the limit -- verified 2026-08-01, BTCUSDT
    carries MAX_NUM_ORDERS but no MAX_NUM_ALGO_ORDERS, despite spec 3.2 listing both.

    These are `None` rather than `0` because the difference is not cosmetic: a validator
    written as `if open_orders >= limit: reject` treats a missing limit of `0` as
    "no orders permitted" and rejects every order on the symbol. `None` forces the caller
    to decide explicitly what an absent limit means.
    """

    funding_interval_hours: int
    """Read per symbol. Hardcoding 8 was R17 -- Binance varies this and has changed it on
    existing symbols. A value of 0 means the payload did not carry it and the interval
    must be derived from the historical settlement record instead."""

    quote_asset: str = ""
    """The currency the contract is priced and settled in -- `USDT` for everything this
    build supports.

    Read from `exchangeInfo` and checked rather than assumed, because **the USD-M venue is
    not a USDT venue**. `BTCUSDC` and other USDC-margined perpetuals trade on the same
    `fapi` endpoint with the same payload shape, and nothing about a symbol *name*
    distinguishes them from a USDT pair. PerpLab's ledger is a single undifferentiated
    `wallet: Decimal` with no asset dimension anywhere in `core.account`, so booking a USDC
    contract's PnL into it would add two currencies together as though they were one -- and
    silently, because there is no field for the mismatch to disagree with. See
    `assert_supported_quote`.

    Empty means "the snapshot did not say", not "there is no quote asset": snapshots taken
    before this field was parsed do not carry it, and the guard treats that as unverifiable
    rather than as a pass.
    """

    margin_asset: str = ""
    """The collateral the contract consumes.

    Equal to `quote_asset` on every USD-M perpetual, and read separately anyway because they
    are different questions -- multi-assets mode is precisely the setting that would make
    them diverge, and it is the mode this build requires to stay off.
    """

    @property
    def is_trading(self) -> bool:
        return self.status == "TRADING"


def _filters_by_type(symbol_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {f["filterType"]: f for f in symbol_payload.get("filters", [])}


def _required(
    filters: dict[str, dict[str, Any]], filter_type: str, field: str, symbol: str
) -> str:
    """Fetch a filter field, failing loudly if absent (spec 1.4).

    A missing filter must never default to something permissive. Defaulting `stepSize` to
    zero or `minNotional` to nothing would let the backtester accept orders the exchange
    rejects, which is precisely the class of silent optimism the spec forbids.
    """
    try:
        return str(filters[filter_type][field])
    except KeyError as exc:
        raise ValueError(
            f"{symbol}: exchangeInfo is missing {filter_type}.{field}; "
            "refusing to guess a permissive default"
        ) from exc


def _optional_scaled(value: Any) -> int | None:
    return None if value is None else to_scaled(str(value))


def _optional_int(value: Any) -> int | None:
    return None if value is None else int(value)


def parse_symbol(payload: dict[str, Any]) -> SymbolFilters:
    """Parse one symbol entry from an `exchangeInfo` payload."""
    symbol = str(payload["symbol"])
    f = _filters_by_type(payload)

    # MARKET_LOT_SIZE is absent on some symbols; falling back to LOT_SIZE is the
    # conservative reading (it never widens what a market order may do).
    market = f.get("MARKET_LOT_SIZE", f.get("LOT_SIZE", {}))

    percent = f.get("PERCENT_PRICE", {})

    return SymbolFilters(
        symbol=symbol,
        status=str(payload.get("status", "")),
        contract_type=str(payload.get("contractType", "")),
        price_precision=int(payload["pricePrecision"]),
        quantity_precision=int(payload["quantityPrecision"]),
        onboard_ms=int(payload.get("onboardDate", 0)),
        tick_size=to_scaled(_required(f, "PRICE_FILTER", "tickSize", symbol)),
        min_price=to_scaled(_required(f, "PRICE_FILTER", "minPrice", symbol)),
        max_price=to_scaled(_required(f, "PRICE_FILTER", "maxPrice", symbol)),
        step_size=to_scaled(_required(f, "LOT_SIZE", "stepSize", symbol)),
        min_qty=to_scaled(_required(f, "LOT_SIZE", "minQty", symbol)),
        max_qty=to_scaled(_required(f, "LOT_SIZE", "maxQty", symbol)),
        market_step_size=to_scaled(str(market.get("stepSize", "0"))),
        market_min_qty=to_scaled(str(market.get("minQty", "0"))),
        market_max_qty=to_scaled(str(market.get("maxQty", "0"))),
        min_notional=to_scaled(_required(f, "MIN_NOTIONAL", "notional", symbol)),
        multiplier_up=_optional_scaled(percent.get("multiplierUp")),
        multiplier_down=_optional_scaled(percent.get("multiplierDown")),
        multiplier_decimal=_optional_scaled(percent.get("multiplierDecimal")),
        max_num_orders=_optional_int(f.get("MAX_NUM_ORDERS", {}).get("limit")),
        max_num_algo_orders=_optional_int(f.get("MAX_NUM_ALGO_ORDERS", {}).get("limit")),
        funding_interval_hours=int(payload.get("fundingIntervalHours", 0)),
        quote_asset=str(payload.get("quoteAsset", "")),
        margin_asset=str(payload.get("marginAsset", "")),
    )


SUPPORTED_QUOTE_ASSET = "USDT"
"""The only settlement currency this build's ledger can represent.

Not a preference. `core.account.Account` holds one `wallet: Decimal` and there is no asset
field anywhere in `perplab/core/` -- multi-assets mode is not disabled here, it is
*unrepresentable*. That is the strongest form the guarantee can take, and it is also why
this check has to exist: an unrepresentable concept raises no error on its own, it simply
adds numbers that should never have been added.
"""


def assert_supported_quote(filters: SymbolFilters) -> None:
    """Refuse a symbol whose PnL would land in the wrong currency.

    Called where a run resolves its filters, which is the last point before any symbol can
    reach the ledger. The check is cheap and the failure it prevents is not detectable
    afterwards: a run mixing BTCUSDT and BTCUSDC would report one equity curve that is the
    sum of two currencies, with every metric derived from it -- Sharpe, drawdown,
    attribution -- silently meaningless.

    An empty `quote_asset` is *accepted*, and that is a deliberate asymmetry. Every
    `exchangeInfo` snapshot taken before this field was parsed lacks it, and refusing those
    would make every historical snapshot unusable to punish a risk they almost certainly do
    not carry -- the platform has only ever been pointed at BTCUSDT and ETHUSDT. Raising on
    a *known-wrong* asset while passing an unknown one is the honest split: it refuses what
    it can prove is wrong and does not claim to have checked what it could not.
    """
    quote = filters.quote_asset.strip().upper()
    if quote and quote != SUPPORTED_QUOTE_ASSET:
        raise ValueError(
            f"{filters.symbol} settles in {quote}, not {SUPPORTED_QUOTE_ASSET}. PerpLab's "
            f"ledger is a single wallet with no asset dimension, so this symbol's PnL "
            f"would be added to USDT balances as though the two currencies were "
            f"interchangeable -- and nothing downstream would report the mistake. "
            f"Multi-assets mode is out of scope for this build; trade the USDT-margined "
            f"pair instead."
        )

    margin = filters.margin_asset.strip().upper()
    if margin and margin != SUPPORTED_QUOTE_ASSET:
        raise ValueError(
            f"{filters.symbol} posts margin in {margin}, not {SUPPORTED_QUOTE_ASSET}. The "
            f"ledger allocates margin out of one undifferentiated wallet, so it cannot "
            f"represent a position collateralised in a second asset."
        )


@dataclass(frozen=True, slots=True)
class OrderCheck:
    """The verdict on one prospective order against a symbol's filter set.

    `reason` is the exchange-style filter name that rejected it (`LOT_SIZE`,
    `MIN_NOTIONAL`, ...) plus the numbers, so a rejection in a backtest reads the same way
    the live rejection would and the two can be compared directly (spec 6.7 parity).
    """

    ok: bool
    reason: str = ""

    def __bool__(self) -> bool:
        return self.ok


_ACCEPTED = OrderCheck(ok=True)


def validate_order(
    filters: SymbolFilters,
    *,
    qty: int,
    price: int | None,
    mark_price: int | None = None,
    is_market: bool = False,
    partial: bool = False,
) -> OrderCheck:
    """Check a prospective order against spec 3.2's filter set. Scaled ints throughout.

    **Why this is not in the accounting layer.** These are constraints on *orders*, and
    `Account` only ever sees *fills*. The distinction is not pedantic: a partial fill of
    0.001 BTC against a larger order is completely legitimate even though its notional is
    far below `minNotional`, so enforcing that filter at fill time would reject the correct
    behaviour of the fill model spec 6.5 describes. What `Account` does enforce on a fill
    is invariant I6 -- tick and step -- because a fill price the exchange could not print
    is a fill the fill model invented.

    `partial=True` is that paragraph made enforceable. Phase 5's queue model fills a resting
    order in increments, and the whole-order checks were being applied to each one: a 0.001
    BTC sweep against a live 1 BTC bid produced an increment worth 39.99 USDT, failed
    `MIN_NOTIONAL` at 50, and *rejected the entire order* -- the exchange would have filled
    the increment and kept the rest working. The size floors (`minQty`, `minNotional`) are
    admission criteria for an order; the price grid and the lot step are properties of any
    printable execution, so those still apply to every increment.

    Everything is scaled int64 (`perplab.core.money`), matching how `SymbolFilters` stores
    its own values. That keeps this module on the integer side of the seam, so the modulo
    tests below are exact rather than tolerant, and callers on the `Decimal` side convert
    with `money.decimal_to_scaled`.

    `mark_price` is optional because `PERCENT_PRICE` cannot be evaluated without it; when
    it is absent that one filter is skipped rather than guessed at. Order-count limits
    (`MAX_NUM_ORDERS`, `MAX_NUM_ALGO_ORDERS`) are not checked here -- they are properties
    of the open-order book, not of a single order, and belong to the execution engine.

    **`price=None` is the wire shape of a market order, and it skips every price-anchored
    filter rather than inventing an anchor.** The live pre-send check (C6) validates what
    will actually ride the wire, and a market order carries no price -- the venue prices
    it at execution. Substituting the mark as a stand-in was tried and is wrong twice
    over: marks are routinely finer than the tick (63838.76999122 against a 0.1 grid), so
    `PRICE_FILTER` would refuse every market order ever sent; and a `MIN_NOTIONAL`
    computed from a stand-in can refuse an order the venue would fill, which turns a
    parity guard into a new parity break. What remains -- the lot step, `minQty`,
    `maxQty` -- is exactly the set the venue checks against the request itself, so a local
    refusal here is one the exchange was certain to issue. The simulated path never
    passes `None`: its market orders are validated at fill time against the price the
    fill model actually produced, which is the honest anchor a simulation has.
    """
    if qty <= 0:
        return OrderCheck(False, f"quantity {qty} must be positive")
    if price is not None and price <= 0:
        return OrderCheck(False, f"price {price} must be positive")

    step = filters.market_step_size if is_market else filters.step_size
    min_qty = filters.market_min_qty if is_market else filters.min_qty
    max_qty = filters.market_max_qty if is_market else filters.max_qty
    lot = "MARKET_LOT_SIZE" if is_market else "LOT_SIZE"

    # MARKET_LOT_SIZE is absent on some symbols and `parse_symbol` stores 0 for the fields
    # it could not read. Falling back keeps a missing filter from rejecting every market
    # order -- the same "absent is not zero" trap `max_num_algo_orders` documents.
    if step <= 0:
        step, min_qty, max_qty, lot = (
            filters.step_size,
            filters.min_qty,
            filters.max_qty,
            "LOT_SIZE",
        )

    if qty % step != 0:
        return OrderCheck(False, f"{lot}: quantity {qty} is not a multiple of step {step}")
    if not partial and qty < min_qty:
        return OrderCheck(False, f"{lot}: quantity {qty} is below minQty {min_qty}")
    if max_qty > 0 and qty > max_qty:
        return OrderCheck(False, f"{lot}: quantity {qty} exceeds maxQty {max_qty}")

    if price is None:
        # No price rides the wire, so nothing below can be evaluated honestly -- see the
        # docstring on why a stand-in anchor is worse than the skip.
        return _ACCEPTED

    if price % filters.tick_size != 0:
        return OrderCheck(
            False,
            f"PRICE_FILTER: price {price} is not a multiple of tick {filters.tick_size}",
        )
    if filters.min_price > 0 and price < filters.min_price:
        return OrderCheck(
            False, f"PRICE_FILTER: price {price} is below minPrice {filters.min_price}"
        )
    if filters.max_price > 0 and price > filters.max_price:
        return OrderCheck(
            False, f"PRICE_FILTER: price {price} exceeds maxPrice {filters.max_price}"
        )

    # Both operands are scaled by 10^8, so their product is scaled by 10^16 and has to come
    # back down before it can be compared with a singly-scaled minNotional. Integer
    # division truncates toward zero, which is the conservative direction here: it can only
    # make the computed notional smaller, so a borderline order is rejected rather than
    # accepted.
    notional = (qty * price) // SCALE
    if not partial and notional < filters.min_notional:
        return OrderCheck(
            False,
            f"MIN_NOTIONAL: notional {notional} is below {filters.min_notional}",
        )

    if mark_price is not None and mark_price > 0:
        if filters.multiplier_up is not None:
            ceiling = (mark_price * filters.multiplier_up) // SCALE
            if price > ceiling:
                return OrderCheck(
                    False, f"PERCENT_PRICE: price {price} is above the ceiling {ceiling}"
                )
        if filters.multiplier_down is not None:
            floor = (mark_price * filters.multiplier_down) // SCALE
            if price < floor:
                return OrderCheck(
                    False, f"PERCENT_PRICE: price {price} is below the floor {floor}"
                )

    return _ACCEPTED


def parse_exchange_info(payload: dict[str, Any]) -> dict[str, SymbolFilters]:
    """Parse a full `exchangeInfo` payload into `{symbol: SymbolFilters}`.

    Symbols that fail to parse are skipped rather than aborting the whole snapshot -- the
    payload carries 500+ symbols and one malformed entry for an instrument we will never
    trade should not block recording BTCUSDT. Skips are reported by the caller.
    """
    out: dict[str, SymbolFilters] = {}
    for entry in payload.get("symbols", []):
        try:
            parsed = parse_symbol(entry)
        except (KeyError, ValueError):
            continue
        out[parsed.symbol] = parsed
    return out

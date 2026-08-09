"""The strategy that proves the live order path works, and nothing else (spec 13, Phase 8).

Not a trading idea and deliberately not shaped like one. Phase 8's exit criterion is *one
real order placed, filled, streamed back and reconciled to the cent* -- that is a test of
the platform's plumbing, and the right instrument for it is the smallest, most predictable
round trip the venue will accept.

**Why the library's EMACross cannot be that instrument.** It declares 40 bars of 15m
history, and a live session starts cold -- no history is preloaded, for the three reasons
`live.session._PushSource` sets out (the lake does not hold the most recent bars, preloading
would break the shadow replay, and a warm-up filled from a hole is worse than an honest cold
start). So EMACross cannot place its first order for **ten hours** of wall clock, and the
first thing an operator would learn about the live path is nothing at all. This declares one
bar of 1m, so it is warm one minute in.

**Sized against the venue's own filters, read live rather than assumed.** BTCUSDT on
testnet: `stepSize` 0.0001, `minNotional` 50 USDT. At the testnet mark of ~63 800 the
default 0.002 BTC is ~128 USDT of notional -- comfortably clear of the floor with room for
the mark to move a long way before an order would be refused for being too small, and ~26
USDT of margin at 5x against a 5 000 USDT testnet wallet.

**One round trip, then it stops.** `_finished` latches after the exit, so a session left
running does not accumulate trades nobody asked for; the interesting output is the pair of
orders and the reconciliation pass that follows them, not a PnL.
"""

from perplab import Strategy


class TestnetSmoke(Strategy):
    """Buy the minimum, hold a couple of bars, close. Once."""

    params = {
        # A string default, not a float: 0.002 as a Python float is not 0.002, and this
        # number is multiplied by a price to produce a notional the exchange checks.
        "qty": {"type": "decimal", "default": "0.002", "min": "0.0001", "max": "0.05"},
        "hold_bars": {"type": "int", "default": 2, "min": 1, "max": 20},
    }

    requires = {
        "symbols": ["BTCUSDT"],
        "timeframe": "1m",
        # One bar, so `ctx.warm` is true a minute into the session rather than ten hours
        # into it. Nothing here needs history -- there is no indicator to warm.
        "history": 1,
        "datasets": ["klines"],
    }

    def on_start(self, ctx):
        self._entered = False
        self._finished = False
        self._bars_since_entry = 0

    def on_bar(self, ctx, bar):
        if not ctx.warm or self._finished:
            return

        if not self._entered:
            # Submitted exactly once. Latched on submission rather than on the position
            # appearing, because a live fill arrives asynchronously on the user-data
            # stream -- gating on `is_flat` would send a second order on every bar until
            # the first one filled, which against a real venue is a real second position.
            ctx.buy(qty=self.p.qty)
            self._entered = True
            return

        position = ctx.position()
        if position.is_flat:
            # The entry has not filled yet. Ordinary and worth waiting through: this is
            # the window the whole exercise exists to measure.
            return

        self._bars_since_entry += 1
        if self._bars_since_entry >= self.p.hold_bars:
            ctx.close()
            self._finished = True

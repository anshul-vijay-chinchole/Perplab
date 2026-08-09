"""Deliberate mutation testing for hedge-mode accounting and concurrent sessions.

`pytest` green says the code passes the tests. It says nothing about whether the tests would
notice if the code were wrong -- and on this platform that is the question that matters, since
every defect worth having a test for has been one that produced a plausible number rather than
a crash.

Each mutation below is a **specific wrong implementation**: the thing a careful person might
have written instead. It is applied to the real source, run against a targeted subset of the
suite, and reverted. A mutation that survives is not a curiosity -- it is the exact
specification of a test that is missing, because it names a way the code could be wrong that
nothing would report.

    python scripts/mutate_hedge_and_concurrency.py

Run it after any change to `core/account.py`, `core/risk.py`, `analytics/trades.py` or
`store/claims.py`. It restores every file it touches, including on a failure, but it does edit
the working tree while it runs -- so do not run it beside a `pytest` you care about.
"""

from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PY = ROOT / ".venv" / "Scripts" / "python.exe"
if not PY.exists():  # pragma: no cover - POSIX layout
    PY = ROOT / ".venv" / "bin" / "python"

LEDGER = [
    "tests/golden/",
    "tests/unit/test_account.py",
    "tests/unit/test_account_review_fixes.py",
    "tests/unit/test_hedge_engine.py",
    "tests/property/",
]
RISK = ["tests/golden/test_hedge_mode.py", "tests/unit/test_risk_limits.py"]
LIVE = [
    "tests/unit/test_reconcile.py",
    "tests/unit/test_preflight.py",
    "tests/unit/test_exchange_transport.py",
    "tests/unit/test_session_lifecycle.py",
    "tests/unit/test_hedge_engine.py",
]
CONCURRENCY = [
    "tests/unit/test_symbol_claims.py",
    "tests/unit/test_sessions_api.py",
    "tests/unit/test_runs_store.py",
]


@dataclass(frozen=True)
class Mutation:
    """One wrong implementation, and where it should be noticed."""

    name: str
    file: str
    old: str
    new: str
    tests: list[str] = field(default_factory=list)
    group: str = "hedge"


MUTATIONS: list[Mutation] = [
    # ------------------------------------------------------------------ the ledger
    Mutation(
        "I3 asserted per symbol instead of per position side",
        "perplab/core/account.py",
        "invariants.check_position_sum(qty, self._signed_fills.get(key, Decimal(0)))",
        "invariants.check_position_sum(\n            qty,\n"
        "            sum(\n"
        "                (v for (s, _), v in self._signed_fills.items() if s == key[0]),\n"
        "                Decimal(0),\n"
        "            ),\n        )",
        LEDGER,
    ),
    Mutation(
        "spec 3.3 case C allowed on a hedged side (a sell flips the long into a short)",
        "perplab/core/account.py",
        "                elif side.is_hedged:\n                    raise HedgeFlipRefused(",
        "                elif False:\n                    raise HedgeFlipRefused(",
        LEDGER,
    ),
    Mutation(
        "a hedge side may be opened by a fill in either direction",
        "perplab/core/account.py",
        "        wanted = side.opening_sign\n        if wanted and _sign(qty) != wanted:",
        "        wanted = side.opening_sign\n        if False:",
        LEDGER,
    ),
    Mutation(
        "funding settles on the first open side only",
        "perplab/core/account.py",
        "        for position in open_sides:\n"
        "            cashflow = funding_cashflow(position.qty, mark, rate)",
        "        for position in open_sides[:1]:\n"
        "            cashflow = funding_cashflow(position.qty, mark, rate)",
        LEDGER,
    ),
    Mutation(
        "funding charged to the wallet but not to the position's own margin (spec 6.2 R5)",
        "perplab/core/account.py",
        "                self.positions[position.key] = replace(\n"
        "                    position, funding_paid=position.funding_paid + cashflow\n"
        "                )",
        "                pass",
        LEDGER,
    ),
    Mutation(
        "the second leg opens at the account default leverage, not the symbol's",
        "perplab/core/account.py",
        "        for other in self.positions_for(symbol):\n            return other.leverage",
        "        pass",
        LEDGER,
    ),
    Mutation(
        "an ambiguous position lookup silently answers BOTH instead of raising",
        "perplab/core/account.py",
        '                raise ValueError(\n'
        '                    f"{symbol}: this account is in hedge mode, so it can hold a LONG and a "',
        '                return PositionSide.BOTH\n'
        '                raise ValueError(\n'
        '                    f"{symbol}: this account is in hedge mode, so it can hold a LONG and a "',
        LEDGER,
    ),
    Mutation(
        "check_liquidations stops after the first triggered side",
        "perplab/core/account.py",
        "            if triggered:\n"
        "                results.append(self._liquidate(ts_ms, position, solution.price, mark))\n"
        "\n        return results",
        "            if triggered:\n"
        "                results.append(self._liquidate(ts_ms, position, solution.price, mark))\n"
        "                break\n\n        return results",
        LEDGER,
    ),
    Mutation(
        "the I9 event-log replay keys positions by symbol alone",
        "perplab/core/account.py",
        "                    key = (event.symbol, event.position_side)",
        "                    key = (event.symbol, PositionSide.BOTH)",
        LEDGER,
    ),
    Mutation(
        "set_leverage only checks the side being asked about",
        "perplab/core/account.py",
        "        if self.has_position(symbol):",
        "        if (symbol, PositionSide.BOTH) in self.positions:",
        LEDGER,
    ),
    # -------------------------------------------------------------------- the risk layer
    Mutation(
        "gross exposure nets the two sides instead of summing them",
        "perplab/core/risk.py",
        "        growth = working.buy_qty if position_side is PositionSide.LONG else working.sell_qty\n"
        "        return abs(position_qty) + growth",
        "        growth = working.buy_qty if position_side is PositionSide.LONG else working.sell_qty\n"
        "        return position_qty + growth",
        RISK,
    ),
    Mutation(
        "the other hedge leg is excluded from the notional ceiling",
        "perplab/core/risk.py",
        "                + other_side_exposure\n            )\n            notional = projected * price",
        "                + Decimal(0)\n            )\n            notional = projected * price",
        RISK,
    ),
    Mutation(
        "a hedged side uses the one-way both-directions bound",
        "perplab/core/risk.py",
        "    if position_side is PositionSide.BOTH:\n        return projected_exposure(position_qty, working)",
        "    if True:\n        return projected_exposure(position_qty, working)",
        RISK,
    ),
    Mutation(
        "working exposure pooled across both sides of a symbol",
        "perplab/engine/backtest.py",
        "            if order.intent.position_side is not position_side:\n                continue",
        "            pass",
        # `RISK` alone left this surviving, and the survival was a fault in *this script*
        # rather than in the suite: the test that catches it lives in `test_hedge_engine.py`,
        # which `RISK` does not list. Worth recording, because a mutation harness that runs
        # the wrong tests reports a false gap and sends someone to write a test that exists.
        RISK + ["tests/unit/test_hedge_engine.py"],
    ),
    # ---------------------------------------------------------------------- analytics
    Mutation(
        "round-trips keyed by symbol, so a hedge is reconstructed as one trade",
        "perplab/analytics/trades.py",
        "        key = (symbol, position_side)\n        with localcontext(ACCOUNTING_CONTEXT):",
        "        key = (symbol, PositionSide.BOTH)\n        with localcontext(ACCOUNTING_CONTEXT):",
        LEDGER,
    ),
    Mutation(
        "mark excursions pooled across sides, so a hedge's MAE cancels out",
        "perplab/analytics/trades.py",
        "        key = (symbol, position_side)\n        trade = self._open.get(key)\n"
        "        if trade is None:\n            return\n"
        "        self._open_unrealized[key] = unrealized",
        "        key = (symbol, PositionSide.BOTH)\n        trade = self._open.get(key)\n"
        "        if trade is None:\n            return\n"
        "        self._open_unrealized[key] = unrealized",
        LEDGER,
    ),
    # ------------------------------------------------------------------ the strategy API
    Mutation(
        "the order intent drops the position side on the wire",
        "perplab/strategy/context.py",
        '            "position_side": self.position_side.value,\n            "client_id": self.client_id,',
        '            "position_side": PositionSide.BOTH.value,\n            "client_id": self.client_id,',
        LEDGER,
    ),
    Mutation(
        "reduce_only is accepted alongside a hedged side",
        "perplab/strategy/context.py",
        "        if self.position_side.is_hedged and self.reduce_only:",
        "        if False:",
        LEDGER,
    ),
    Mutation(
        "the engine routes every fill to BOTH regardless of the intent",
        "perplab/engine/backtest.py",
        "        routed = order.intent.position_side\n        held = self.account.qty(symbol, routed)",
        "        routed = PositionSide.BOTH\n        held = self.account.qty(symbol, routed)",
        LEDGER,
    ),
    # ---------------------------------------------------------------------- live guards
    Mutation(
        "a position-mode mismatch is skipped rather than refused at reconciliation",
        "perplab/live/reconcile.py",
        '    ours = "hedge" if PositionSide.BOTH not in expected else "one-way"',
        '    return\n    ours = "hedge" if PositionSide.BOTH not in expected else "one-way"',
        LIVE,
        "live",
    ),
    Mutation(
        "the exchange preflight accepts any position mode",
        "perplab/live/preflight.py",
        "    if hedge == bool(hedge_mode):\n        return hedge",
        "    if True:\n        return hedge",
        LIVE,
        "live",
    ),
    Mutation(
        "the transport stops checking the preflight's position mode",
        "perplab/live/exchange_transport.py",
        "        if report.hedge_mode != engine.account.hedge_mode:",
        "        if False:",
        LIVE,
        "live",
    ),
    Mutation(
        "the monitor reports liquidation distance as a percent again",
        "perplab/live/session.py",
        "                    distance = float(abs(mark - liq) / mark)",
        "                    distance = float(abs(mark - liq) / mark * 100)",
        LIVE,
        "live",
    ),
    Mutation(
        "the monitor emits one row per symbol rather than one per side",
        "perplab/live/session.py",
        "            for side in engine._sides:",
        "            for side in engine._sides[:1]:",
        LIVE,
        "live",
    ),
    # ---------------------------------------------------------------------- concurrency
    Mutation(
        "the claim uses a deferred transaction instead of BEGIN IMMEDIATE",
        "perplab/store/claims.py",
        'self._connection.execute("BEGIN IMMEDIATE")',
        'self._connection.execute("BEGIN DEFERRED")',
        CONCURRENCY,
        "concurrency",
    ),
    Mutation(
        "a conflicting claim is allowed through",
        "perplab/store/claims.py",
        "                if conflicts:\n                    raise SymbolConflict(",
        "                if False:\n                    raise SymbolConflict(",
        CONCURRENCY,
        "concurrency",
    ),
    Mutation(
        # The defect this guard was added to close, reintroduced verbatim: a symbol was
        # shareable as long as both sessions agreed about leverage, margin mode and position
        # mode. It is the most likely way for the guard to regress, because the reasoning
        # behind it is superficially sound -- and it is wrong for a reason no local check can
        # see, since what merges at the exchange is the position and not the settings.
        "a symbol is shareable when both sessions agree about the configuration",
        "perplab/store/claims.py",
        "                    c for c in held if c.run_id != run_id and c.symbol in wanted\n"
        "                ]",
        "                    c\n"
        "                    for c in held\n"
        "                    if c.run_id != run_id\n"
        "                    and c.symbol in wanted\n"
        "                    and (\n"
        "                        c.leverage != int(leverage)\n"
        "                        or c.margin_mode != margin_mode\n"
        "                        or c.hedge_mode != bool(hedge_mode)\n"
        "                    )\n"
        "                ]",
        CONCURRENCY,
        "concurrency",
    ),
    Mutation(
        "stale claims from dead runs are honoured forever",
        "perplab/store/claims.py",
        "        live = set(active_run_ids)\n        stale = [c for c in claims if c.run_id not in live]",
        "        live = set(active_run_ids) | {c.run_id for c in claims}\n"
        "        stale = [c for c in claims if c.run_id not in live]",
        CONCURRENCY,
        "concurrency",
    ),
    Mutation(
        "a run cannot re-claim its own symbols (the worker's re-check self-collides)",
        "perplab/store/claims.py",
        "c for c in held if c.run_id != run_id and c.symbol in wanted",
        "c for c in held if c.symbol in wanted",
        CONCURRENCY,
        "concurrency",
    ),
    Mutation(
        # Endpoint scoping is what keeps a testnet rehearsal from blocking production and,
        # more importantly, what keeps two *different* accounts from being treated as one.
        "claims are not scoped to an endpoint, so every account shares one symbol space",
        "perplab/store/claims.py",
        '"SELECT * FROM symbol_claims WHERE endpoint = ? AND released_ms IS NULL "',
        '"SELECT * FROM symbol_claims WHERE ? IS NOT NULL AND released_ms IS NULL "',
        CONCURRENCY,
        "concurrency",
    ),
]

EQUIVALENT: list[tuple[str, str]] = [
    (
        "RunStore reads lastrowid outside its own transaction",
        "`sqlite3.Cursor.lastrowid` is stamped on the cursor at execute time rather than "
        "read back from the connection, so interleaved INSERTs on one connection cannot "
        "hand two callers the same id however the read is ordered. Verified directly: three "
        "INSERTs on one connection give three cursors reporting 1, 2 and 3. This mutation "
        "survives, and survives correctly -- it changes nothing observable. Listed here "
        "rather than deleted, because 'we tried it and it is equivalent' is a different "
        "statement from 'we did not think of it', and only one of them needs revisiting.",
    ),
]
"""Mutations tried, found to be **equivalent**, and deliberately not counted as gaps.

An equivalent mutant is one that changes the source without changing behaviour, so no test
can distinguish it and writing one would mean writing a test that asserts an implementation
detail. Recording them is what stops the same false gap being rediscovered and 'fixed' with a
test that pins the wrong thing.
"""


def apply_and_run(mutation: Mutation) -> tuple[bool, str]:
    """Mutate, run the targeted tests, revert. Returns `(caught, summary)`."""
    path = ROOT / mutation.file
    original = path.read_text(encoding="utf-8")
    occurrences = original.count(mutation.old)
    if occurrences != 1:
        return False, f"ANCHOR MATCHED {occurrences} TIMES -- mutation not applied"
    path.write_text(original.replace(mutation.old, mutation.new, 1), encoding="utf-8")
    try:
        proc = subprocess.run(
            [
                str(PY), "-m", "pytest", *mutation.tests,
                "-q", "-x", "-p", "no:cacheprovider", "--no-header",
            ],
            cwd=ROOT, capture_output=True, text=True, timeout=1200,
        )
        summary = ""
        for line in reversed(proc.stdout.strip().splitlines()):
            stripped = line.strip()
            if "passed" in stripped or "failed" in stripped or "error" in stripped.lower():
                summary = stripped
                break
        return proc.returncode != 0, summary or f"exit {proc.returncode}"
    except subprocess.TimeoutExpired:
        # A hang is technically a catch, and a bad one -- it says the tests notice without
        # saying what they noticed. Reported distinctly so it is never counted as a clean one.
        return True, "TIMED OUT"
    finally:
        path.write_text(original, encoding="utf-8")


def main() -> int:
    results: list[tuple[Mutation, bool, str]] = []
    for mutation in MUTATIONS:
        caught, summary = apply_and_run(mutation)
        results.append((mutation, caught, summary))
        print(f"[{'CAUGHT  ' if caught else 'SURVIVED'}] {mutation.group:12s} {mutation.name}")
        print(f"             {summary}")
        sys.stdout.flush()

    print("\n" + "=" * 78)
    for group in ("hedge", "live", "concurrency"):
        rows = [r for r in results if r[0].group == group]
        if rows:
            print(f"{group:12s} {sum(1 for _m, c, _s in rows if c)}/{len(rows)} caught")
    caught_total = sum(1 for _m, c, _s in results if c)
    print(f"{'TOTAL':12s} {caught_total}/{len(results)} caught")

    if EQUIVALENT:
        print("\nKNOWN-EQUIVALENT (tried, changes nothing observable, not a gap):")
        for name, why in EQUIVALENT:
            print(f"  - {name}\n      {why}")

    survivors = [(m, s) for m, c, s in results if not c]
    if survivors:
        print("\nSURVIVORS -- each names a way the code could be wrong that nothing reports:")
        for mutation, summary in survivors:
            print(f"  - [{mutation.group}] {mutation.name}\n      {summary}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

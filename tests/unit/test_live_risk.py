"""The two auto-triggers that only exist once there is a socket, and the state that grows.

Spec 7 lists four auto-triggers. Phase 6 built the two a backtest can produce -- an
invariant failure and a rejection storm -- and could not build the other two, because a
replay has no socket to drop and no counterparty to disagree with. This file covers them:

- **a WS disconnect exceeding `max_disconnect_seconds` while a position is open**, where the
  qualifier is the trigger rather than a detail of it, and
- **a live-vs-exchange reconciliation mismatch** (spec 6.7.3), where the tolerance is one
  `tickSize`/`stepSize` and anything past it means PerpLab's idea of the account is wrong.

Both are tested at the boundary rather than in the middle, because a limit whose comparison
is the wrong way round still fires on a gross breach and is therefore invisible to a test
that only uses gross breaches.

**The rest of the file is about a session that does not end.** A backtest stops when the
data does, so an accumulator that grows per event is bounded by the file. A paper session
runs for the weekend, and `RiskEngine`'s breach list and rejection tally were both unbounded
-- one growing per refusal and one growing per distinct *string* the exchange sent. The tests
here pin the ceilings and, more importantly, pin what the engine still reports honestly once
it has hit them.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from perplab.core.money import parse_money
from perplab.core.risk import (
    MAX_BREACHES,
    MAX_REJECTION_REASONS,
    MS_PER_DAY,
    OTHER_REJECTION_REASON,
    RiskAction,
    RiskEngine,
    RiskLimits,
    WorkingExposure,
    normalise_rejection_reason,
)


def money(text: str) -> Decimal:
    return parse_money(text)


def engine_with(**limits: object) -> RiskEngine:
    """A `RiskEngine` with exactly the named limits in force and nothing else.

    `from_json` reads a missing key as `None`, so this is the shortest honest way to say
    "this limit and no other" -- the same idiom the Phase 6 suite uses.
    """
    return RiskEngine(
        limits=RiskLimits.from_json(limits),
        starting_equity=money("100000"),
    )


# ============================================== the disconnect trigger (spec 7, live-only)


def test_a_disconnect_shorter_than_the_ceiling_is_not_a_breach() -> None:
    """30 seconds is 30 000 ms, and 29 999 ms is inside it.

    A socket that drops and comes back is ordinary. If a reconnect inside the operator's
    own window halted the session, the operator would turn the trigger off, and a trigger
    that gets turned off protects nothing.
    """
    engine = engine_with(max_disconnect_seconds=30)
    assert engine.observe_disconnect(1_000, 29_999, True) is None
    assert engine.halted is False


def test_a_disconnect_exactly_at_the_ceiling_is_not_a_breach() -> None:
    """30 s x 1 000 = 30 000 ms, and at 30 000 ms the socket has not been down *longer*.

    Spec 7's word is "exceeding", which is the strict comparison, and it puts this limit
    with the size ceilings rather than with the counts and losses -- where "reached" is
    enough. Both rules live in the module docstring precisely because they differ.
    """
    engine = engine_with(max_disconnect_seconds=30)
    assert engine.observe_disconnect(1_000, 30_000, True) is None
    assert engine.halted is False


def test_a_disconnect_past_the_ceiling_halts_while_a_position_is_open() -> None:
    """30 001 ms against a 30 s ceiling: one millisecond past 30 000 breaches.

    The breach names the limit and the kill switch names the *trigger source*, and spec 7.5
    asks for the second. "max_disconnect_seconds" answers "which number was exceeded";
    `DISCONNECT` answers "what kind of thing went wrong", which is the question a reader of
    the run log is asking.
    """
    engine = engine_with(max_disconnect_seconds=30)
    breach = engine.observe_disconnect(1_000, 30_001, True)
    assert breach is not None
    assert breach.action is RiskAction.HALT
    assert breach.limit == "max_disconnect_seconds"
    assert breach.observed == "30001"
    assert breach.allowed == "30000", "the ceiling in the same unit as the measurement"
    assert engine.halted is True
    assert engine.kill_switch.trigger == "DISCONNECT"
    assert engine.kill_switch.tripped_at_ms == 1_000


def test_a_disconnect_over_a_flat_account_is_not_a_breach_however_long_it_runs() -> None:
    """Spec 7 says *"while a position is open"*, and the qualifier is the whole trigger.

    An hour is 3 600 000 ms, two orders of magnitude past a 30 s ceiling, and it still does
    not halt a flat account. Nothing is at risk: there is no mark to miss and no fill to
    lose. Halting here would stop a session for a data outage, which is a decision about
    data quality and not about risk.
    """
    engine = engine_with(max_disconnect_seconds=30)
    assert engine.observe_disconnect(1_000, 3_600_000, False) is None
    assert engine.halted is False
    # The same outage with something open is the breach.
    assert engine.observe_disconnect(2_000, 3_600_000, True) is not None
    assert engine.halted is True


def test_a_disconnect_ceiling_nobody_set_never_fires() -> None:
    """`None` is unlimited here as everywhere in `RiskLimits`.

    A backtest replays a file and cannot disconnect, so this is the state every backtest is
    in, and an hour of "downtime" reported by a confused caller must not halt one.
    """
    engine = engine_with(max_position_notional="1000000")
    assert engine.limits.max_disconnect_seconds is None
    assert engine.observe_disconnect(1_000, 3_600_000, True) is None
    assert engine.halted is False


def test_a_negative_downtime_is_refused_rather_than_read_as_no_downtime() -> None:
    """Two timestamps subtracted the wrong way round produce a negative duration.

    Read as "no downtime", it disables the trigger silently for the whole session, and the
    session that most needs the trigger is exactly the one whose socket is misbehaving.
    """
    engine = engine_with(max_disconnect_seconds=30)
    with pytest.raises(ValueError, match="negative"):
        engine.observe_disconnect(1_000, -1, True)


# ========================================== the reconciliation trigger (spec 6.7.3)


def test_a_disagreement_within_the_step_size_is_not_a_mismatch() -> None:
    """A 0.001 `stepSize` admits a 0.001 difference exactly.

    Ours 1.000, theirs 1.001, difference 0.001, tolerance 0.001. The tolerance is a ceiling
    on the disagreement, so equality passes -- one step of rounding is precisely the
    disagreement a `stepSize` tolerance exists to admit, and a check that halted on it would
    halt on every position the exchange rounded.
    """
    engine = engine_with(max_position_notional="1000000")
    breach = engine.observe_reconciliation(
        1_000, "position_size", money("1.000"), money("1.001"), money("0.001")
    )
    assert breach is None
    assert engine.halted is False


def test_a_disagreement_beyond_the_step_size_halts_the_session() -> None:
    """Ours 1.000 against theirs 1.002 is 0.002, which is twice a 0.001 tolerance.

    Spec 6.7.3: *"any mismatch beyond tickSize/stepSize tolerance triggers the kill
    switch"*. This is the check that catches a missed fill before it becomes an unhedged
    position, so it has no limit to switch it off and it reports the divergence rather than
    either side's number -- the divergence is what is being compared against the tolerance.
    """
    engine = engine_with(max_position_notional="1000000")
    breach = engine.observe_reconciliation(
        1_000, "position_size", money("1.000"), money("1.002"), money("0.001")
    )
    assert breach is not None
    assert breach.action is RiskAction.HALT
    assert breach.limit == "reconciliation"
    assert breach.observed == "0.00200000"
    assert breach.allowed == "0.00100000"
    assert engine.halted is True
    assert engine.kill_switch.trigger == "RECONCILIATION"


def test_a_reconciliation_mismatch_reports_the_field_and_both_sides() -> None:
    """Which of the five fields disagreed, and by what, is the whole diagnostic.

    Spec 6.7.3 compares wallet balance, position size, entry price, unrealised PnL and
    liquidation price. "The account does not reconcile" sends an operator to five places;
    "wallet_balance: ours 10 000, theirs 9 000" sends them to one.
    """
    engine = engine_with(max_position_notional="1000000")
    breach = engine.observe_reconciliation(
        7_000, "wallet_balance", money("10000"), money("9000"), money("0.01")
    )
    assert breach is not None
    assert "wallet_balance" in breach.detail
    assert "10000.00000000" in breach.detail
    assert "9000.00000000" in breach.detail
    assert breach.observed == "1000.00000000", "10 000 - 9 000"


def test_the_mismatch_does_not_care_which_side_is_larger() -> None:
    """A position the exchange thinks is *smaller* than ours is the dangerous direction.

    It means PerpLab believes it holds size the exchange has no record of, so the platform's
    own liquidation price is computed on a position that does not exist. Both directions are
    a 0.5 divergence against a 0.001 tolerance and both must halt.
    """
    ours_bigger = engine_with(max_position_notional="1000000")
    assert ours_bigger.observe_reconciliation(
        1_000, "position_size", money("2.0"), money("1.5"), money("0.001")
    ) is not None

    theirs_bigger = engine_with(max_position_notional="1000000")
    assert theirs_bigger.observe_reconciliation(
        1_000, "position_size", money("1.5"), money("2.0"), money("0.001")
    ) is not None


def test_a_negative_reconciliation_tolerance_is_refused() -> None:
    """A negative tolerance halts on an *exact* match, which reads as the check working.

    Every reconciliation would trip the kill switch, an operator would conclude the
    reconciliation is broken, and the trigger would be disabled -- from a sign error.
    """
    engine = engine_with(max_position_notional="1000000")
    with pytest.raises(ValueError, match="negative"):
        engine.observe_reconciliation(
            1_000, "wallet_balance", money("1"), money("1"), money("-0.001")
        )


def test_the_first_of_two_live_triggers_is_the_one_that_explains_the_session() -> None:
    """A disconnect that hides a fill produces a mismatch on reconnect: one incident.

    The switch keeps the first trigger, so the log says `DISCONNECT` -- the thing that
    started it -- rather than `RECONCILIATION`, the symptom that arrived second. The halt is
    one event, so the second call records no additional breach.
    """
    engine = engine_with(max_disconnect_seconds=30)
    assert engine.observe_disconnect(1_000, 45_000, True) is not None
    assert engine.observe_reconciliation(
        2_000, "position_size", money("1"), money("2"), money("0.001")
    ) is not None
    assert engine.kill_switch.trigger == "DISCONNECT"
    assert len(engine.breaches) == 1


# ================================================ bounded growth over a 48-hour session


def refuse_once(engine: RiskEngine, ts_ms: int) -> None:
    """Submit an order far past the notional ceiling, so it is refused and recorded."""
    engine.check_order(
        ts_ms=ts_ms,
        symbol="BTCUSDT",
        side="BUY",
        qty=money("1"),
        price=money("40000"),
        reduce_only=False,
        position_qty=money("0"),
        working=WorkingExposure.zero(),
        equity=money("100000"),
        open_orders=0,
    )


def test_the_breach_list_keeps_the_first_thousand_and_counts_the_rest() -> None:
    """A strategy at 30 rejections a minute for 48 hours is 86 400 breach records.

    Here it is `MAX_BREACHES + 5` refusals, so exactly five must be dropped. Each record
    carries five strings and the list is copied into the run result and serialised, so the
    growth is not merely memory -- it is a results page that stops rendering.
    """
    engine = engine_with(max_position_notional="100")
    for index in range(MAX_BREACHES + 5):
        refuse_once(engine, 1_000 + index)

    assert len(engine.breaches) == MAX_BREACHES
    assert engine.breaches_dropped == 5


def test_the_cap_keeps_the_first_breaches_rather_than_the_most_recent() -> None:
    """The first refusal explains the session; the thousandth is a consequence of it.

    A ring buffer would keep the tail, which is the shape a reader least needs: by then the
    strategy has been refused for hours and every record says the same thing. The first
    timestamp written must survive and the last must not.
    """
    engine = engine_with(max_position_notional="100")
    for index in range(MAX_BREACHES + 5):
        refuse_once(engine, 1_000 + index)

    assert engine.breaches[0].ts_ms == 1_000, "the first refusal is still there"
    assert engine.breaches[-1].ts_ms == 1_000 + MAX_BREACHES - 1
    last_submitted = 1_000 + MAX_BREACHES + 4
    assert all(b.ts_ms != last_submitted for b in engine.breaches)


def test_a_truncated_breach_list_still_reports_how_many_there_were() -> None:
    """A count derived from a capped list is a number that looks like a measurement.

    `MAX_BREACHES + 5` refusals happened, so `breach_count` is `MAX_BREACHES + 5` and
    `rejected_orders` is the same -- every one of them was a REJECT -- while the list itself
    holds `MAX_BREACHES`. Reporting 1 000 for a session that refused 86 400 orders would be
    the worst kind of wrong: plausible.
    """
    engine = engine_with(max_position_notional="100")
    for index in range(MAX_BREACHES + 5):
        refuse_once(engine, 1_000 + index)

    summary = engine.summary()
    assert summary["breach_count"] == MAX_BREACHES + 5
    assert summary["breaches_dropped"] == 5
    assert summary["rejected_orders"] == MAX_BREACHES + 5


def test_a_run_that_stays_under_the_cap_reports_nothing_dropped() -> None:
    """The ordinary case, so the new field cannot be read as "this build lost records"."""
    engine = engine_with(max_position_notional="100")
    for index in range(10):
        refuse_once(engine, 1_000 + index)

    summary = engine.summary()
    assert summary["breach_count"] == 10
    assert summary["breaches_dropped"] == 0
    assert len(engine.breaches) == 10


# ------------------------------------------------------- the rejection tally's key


def test_an_exchange_message_carrying_a_number_is_one_reason_not_one_per_order() -> None:
    """`"insufficient margin: need N"` is a fresh string on every order.

    Five hundred refusals with five hundred different numbers are one *reason*: the account
    is out of margin. Keyed by the raw text they were five hundred rows in the tally, which
    is both unbounded and unreadable.
    """
    engine = engine_with(max_consecutive_rejections=None)
    for index in range(500):
        engine.observe_rejection(1_000 + index, f"insufficient margin: need {index}.5")

    assert engine.rejections == {"INSUFFICIENT MARGIN": 500}


def test_a_number_with_no_colon_in_front_of_it_is_folded_too() -> None:
    """Not every exchange message has the `kind: instance` shape.

    `"Order 8347289347 would immediately trigger"` carries its variable part inline, so
    splitting on the colon alone leaves the order id in the key. Digits become `#`.
    """
    assert (
        normalise_rejection_reason("Order 8347289347 would immediately trigger")
        == "ORDER # WOULD IMMEDIATELY TRIGGER"
    )
    assert (
        normalise_rejection_reason("Order 991 would immediately trigger")
        == "ORDER # WOULD IMMEDIATELY TRIGGER"
    )


def test_a_filter_name_survives_normalisation_unchanged() -> None:
    """The keys that were already bounded must not be mangled into each other.

    Binance's filter names carry no digits and no colon, so they pass through as themselves
    -- and `PRICE_FILTER` and `PERCENT_PRICE` stay two reasons, because they are.
    """
    assert normalise_rejection_reason("PRICE_FILTER") == "PRICE_FILTER"
    assert normalise_rejection_reason("MARKET_LOT_SIZE") == "MARKET_LOT_SIZE"
    assert normalise_rejection_reason("PERCENT_PRICE") == "PERCENT_PRICE"


def test_normalisation_is_case_and_whitespace_insensitive() -> None:
    """One reason written three ways by three code paths is still one reason."""
    assert (
        normalise_rejection_reason("Insufficient  margin")
        == normalise_rejection_reason("insufficient margin")
        == normalise_rejection_reason("INSUFFICIENT MARGIN\n")
        == "INSUFFICIENT MARGIN"
    )


def test_reasons_past_the_cap_are_pooled_rather_than_dropped() -> None:
    """Normalisation folds numbers; it cannot fold free prose, so there is also a ceiling.

    `MAX_REJECTION_REASONS + 20` genuinely distinct alphabetic reasons, one refusal each.
    The tally holds the cap plus one pooled entry -- `MAX_REJECTION_REASONS + 1` keys -- and
    the pool holds the 20 that arrived after the cap was full. Pooling rather than dropping
    keeps the breakdown summing to the total, and a breakdown that does not add up to the
    total is worse than a coarse one.
    """
    engine = engine_with(max_consecutive_rejections=None)
    total = MAX_REJECTION_REASONS + 20
    for index in range(total):
        engine.observe_rejection(1_000 + index, f"reason {chr(97 + index // 26)}{chr(97 + index % 26)}")

    assert len(engine.rejections) == MAX_REJECTION_REASONS + 1
    assert engine.rejections[OTHER_REJECTION_REASON] == 20
    assert sum(engine.rejections.values()) == total


def test_the_pool_cannot_be_collided_with_by_a_real_reason() -> None:
    """A pool a genuine reason could fall into reports a sum of two different things.

    Normalisation upper-cases everything it returns and the pool's key is lower-case, so no
    message the exchange can send lands there.
    """
    assert OTHER_REJECTION_REASON == OTHER_REJECTION_REASON.lower()
    assert normalise_rejection_reason("<other>") == "<OTHER>"
    assert normalise_rejection_reason("<other>") != OTHER_REJECTION_REASON


def test_the_breach_still_carries_the_exchanges_own_words() -> None:
    """The tally key is bounded; the message an operator reads must not be truncated to it.

    A halt saying "INSUFFICIENT MARGIN" is a category. The operator needs the number the
    exchange asked for, and it is in the breach that stopped the session.
    """
    engine = engine_with(max_consecutive_rejections=2)
    engine.observe_rejection(1_000, "insufficient margin: need 1234.56789")
    breach = engine.observe_rejection(2_000, "insufficient margin: need 9876.54321")
    assert breach is not None
    assert "need 9876.54321" in breach.detail
    assert engine.rejections == {"INSUFFICIENT MARGIN": 2}


# ============================================== the day baseline and out-of-order samples


def test_a_replayed_sample_from_a_past_day_does_not_become_the_next_days_baseline() -> None:
    """The day key could not go backwards; the equity it left behind could.

    Day 5 opens at 10 000 and ends at 9 900. Day 6 opens on that 9 900, carried forward. A
    replayed day-5 message stamped mid-day then arrives -- ordinary in live, where a
    reconnect re-sends -- carrying 9 000, and it correctly does not roll the day. But it
    used to become `_last_equity`, so when day 7 arrived the new baseline was that 9 000
    rather than day 6's closing 9 800: day 7 would be measured against an equity the account
    held two days earlier, and a 2% limit of 200 would sit 800 the wrong side of the truth.
    """
    engine = RiskEngine(limits=RiskLimits.unlimited(), starting_equity=money("10000"))
    day5 = 5 * MS_PER_DAY

    engine.observe_equity(day5, money("10000"))
    engine.observe_equity(day5 + MS_PER_DAY - 1, money("9900"))
    engine.observe_equity(day5 + MS_PER_DAY, money("9850"))
    assert engine.day_open_equity == money("9900"), "day 6 opens on day 5's close"
    engine.observe_equity(day5 + 2 * MS_PER_DAY - 1, money("9800"))

    engine.observe_equity(day5 + 5_000, money("9000"))  # a replayed day-5 sample

    engine.observe_equity(day5 + 2 * MS_PER_DAY, money("9790"))
    assert engine.day_key == (day5 + 2 * MS_PER_DAY) // MS_PER_DAY
    assert engine.day_open_equity == money("9800"), "day 6's close, not the replayed 9 000"


def test_a_replayed_sample_within_one_day_does_not_overwrite_the_later_reading() -> None:
    """The same defect without crossing a day boundary, which is the commoner shape.

    Two samples stamped 10:00 and 10:05 arrive in that order, then the 10:00 one is
    re-delivered. The day's closing reading is the 10:05 value of 9 500, and the replay must
    not put the 10:00 value of 9 900 back in its place -- the next day would then open 400
    above where the account actually was.
    """
    engine = RiskEngine(limits=RiskLimits.unlimited(), starting_equity=money("10000"))
    day5 = 5 * MS_PER_DAY
    ten_00 = day5 + 36_000_000
    ten_05 = ten_00 + 300_000

    engine.observe_equity(day5, money("10000"))
    engine.observe_equity(ten_00, money("9900"))
    engine.observe_equity(ten_05, money("9500"))
    engine.observe_equity(ten_00, money("9900"))  # re-delivered

    engine.observe_equity(day5 + MS_PER_DAY, money("9490"))
    assert engine.day_open_equity == money("9500")


def test_two_samples_on_one_millisecond_are_ordered_by_arrival() -> None:
    """Equality has to go to the newcomer, or a same-millisecond update never lands.

    Marks and fills share a millisecond constantly. The second sample stamped `t` is the
    later of the two by arrival, so it is the one the next day opens on.
    """
    engine = RiskEngine(limits=RiskLimits.unlimited(), starting_equity=money("10000"))
    day5 = 5 * MS_PER_DAY
    engine.observe_equity(day5, money("10000"))
    engine.observe_equity(day5 + 1_000, money("9900"))
    engine.observe_equity(day5 + 1_000, money("9800"))

    engine.observe_equity(day5 + MS_PER_DAY, money("9790"))
    assert engine.day_open_equity == money("9800")


# ============================================================= the limit set itself


def test_the_disconnect_ceiling_survives_a_round_trip_through_storage() -> None:
    """A limit that does not round-trip is a limit the replay of a run did not have."""
    limits = RiskLimits(max_disconnect_seconds=30)
    stored = limits.to_json()
    assert stored["max_disconnect_seconds"] == 30
    assert RiskLimits.from_json(stored) == limits


def test_a_limit_set_stored_before_phase_seven_reads_as_not_having_had_the_trigger() -> None:
    """The rule `from_json` already applies to an empty mapping, applied to a new key.

    A Phase 6 run stored nine limits and no `max_disconnect_seconds`, because there was no
    such field and no socket. Reading it back as spec 7's default of 30 would report that
    run as having been protected by a trigger that did not exist.
    """
    phase6 = {
        "max_leverage": "5",
        "max_drawdown_pct": "0.15",
        "max_consecutive_rejections": 5,
    }
    assert RiskLimits.from_json(phase6).max_disconnect_seconds is None
    assert RiskLimits.from_json({}) == RiskLimits.unlimited()
    assert RiskLimits.from_json(None) == RiskLimits.unlimited()
    assert RiskLimits.unlimited().max_disconnect_seconds is None


def test_the_default_limits_do_not_attach_a_socket_trigger_to_a_backtest() -> None:
    """Spec 7 says the default is 30 seconds; the default *here* is off, on purpose.

    A backtest replays a file and cannot disconnect. A 30 in every backtest's stored limit
    set would say the run was subject to a trigger that could not fire, which is the same
    false history the empty-mapping rule exists to refuse. The session that owns a socket
    sets it explicitly.
    """
    assert RiskLimits().max_disconnect_seconds is None


def test_a_run_whose_only_limit_is_the_disconnect_ceiling_still_had_a_risk_layer() -> None:
    """`any_limit` and `unbounded_exposure` are independent facts, and this is both.

    Nothing bounds position size, and a limit is nonetheless in force. Reporting "no risk
    layer" for this run would put that claim above a kill switch entry.
    """
    limits = RiskLimits.from_json({"max_disconnect_seconds": 30})
    assert limits.any_limit is True
    assert limits.unbounded_exposure is True


def test_a_disconnect_ceiling_of_zero_is_refused() -> None:
    """Zero would halt on the first millisecond of the first reconnect.

    Every socket reconnects. A ceiling nobody could operate under is a value somebody typed
    by accident, and reading it literally would stop the session at the first blip.
    """
    with pytest.raises(ValueError, match="max_disconnect_seconds must be positive"):
        RiskLimits(max_disconnect_seconds=0)
    with pytest.raises(ValueError):
        RiskLimits.from_json({"max_disconnect_seconds": -1})

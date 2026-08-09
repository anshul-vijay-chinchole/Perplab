"""The shadow-backtest parity report (spec 6.7).

Every asserted figure is derived in the test's own docstring from the `metrics.json`,
`trades.json` and `events.jsonl` that the test writes a few lines above it. Nothing here is
a number that came out of running the code.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Sequence

import pytest

from perplab.analytics.parity import (
    FILL_DEVIATION_BPS_LIMIT,
    FILL_MATCH_WINDOW_MS,
    PNL_DIVERGENCE_FRACTION,
    ParityInputMalformed,
    ParityInputMissing,
    build_parity,
)
from perplab.core.money import parse_money


def _fill(
    *,
    ts_ms: int,
    price: str,
    qty: str = "1",
    side: str = "BUY",
    symbol: str = "BTCUSDT",
    order_id: str = "o1",
    tag: str | None = None,
) -> dict[str, Any]:
    """One `FILL` payload, shaped like the one `backtest._fill` emits."""
    return {
        "order_id": order_id,
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "price": price,
        "ts_ms": ts_ms,
        "is_maker": False,
        "reduce_only": False,
        "tag": tag,
    }


def _write_run(
    directory: Path,
    *,
    net_pnl: str,
    realized: Sequence[str] = (),
    fills: Sequence[dict[str, Any]] = (),
    extra_events: Sequence[dict[str, Any]] = (),
) -> Path:
    """Write the three artefacts `build_parity` reads, exactly as the worker writes them."""
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "metrics.json").write_text(
        json.dumps({"attribution": {"net_pnl": net_pnl}}), encoding="utf-8"
    )
    (directory / "trades.json").write_text(
        json.dumps({"trades": [{"realized_pnl": value} for value in realized]}),
        encoding="utf-8",
    )
    events = [
        {"seq": index, "ts_ms": payload["ts_ms"], "kind": "FILL", "payload": payload}
        for index, payload in enumerate(fills, start=1)
    ]
    events.extend(extra_events)
    events.sort(key=lambda event: (event["ts_ms"], event["seq"]))
    (directory / "events.jsonl").write_text(
        "".join(json.dumps(event, separators=(",", ":")) + "\n" for event in events),
        encoding="utf-8",
    )
    return directory


def test_two_runs_inside_both_thresholds_are_not_flagged(tmp_path: Path) -> None:
    """The report's four quantities on a session the backtest reproduced closely.

    The paper session realised 10.00 on one round-trip, so gross PnL is |10.00| = 10.00 and
    spec 6.7.2 allows a final-PnL gap of 0.05 x 10.00 = 0.50. The shadow run finished at
    9.99 against the session's 10.00, a delta of -0.01, which is inside 0.50; as a fraction
    of gross that is -0.01 / 10.00 = -0.001.

    Both fills matched. The buy filled at 100.01 in the shadow against 100.00 in the
    session, which for a buy is (100.01 - 100.00) / 100.00 x 10000 = +1 bps against the
    trader; the sell filled at 110.00 in both, which is 0 bps. The average is
    (1 + 0) / 2 = 0.5 bps, inside the 3 bps limit, and since neither deviation is negative
    the absolute average is 0.5 bps too.
    """
    paper = _write_run(
        tmp_path / "paper",
        net_pnl="10.00",
        realized=["10.00"],
        fills=[
            _fill(ts_ms=1_000, price="100.00", side="BUY"),
            _fill(ts_ms=5_000, price="110.00", side="SELL", order_id="o2"),
        ],
    )
    shadow = _write_run(
        tmp_path / "shadow",
        net_pnl="9.99",
        realized=["9.99"],
        fills=[
            _fill(ts_ms=1_500, price="100.01", side="BUY"),
            _fill(ts_ms=5_000, price="110.00", side="SELL", order_id="o2"),
        ],
    )

    report = build_parity(paper, shadow)

    assert report.paper_fills == 2
    assert report.shadow_fills == 2
    assert report.fill_count_delta == 0
    assert report.matched_fills == 2
    assert report.avg_fill_delta_bps == parse_money("0.5")
    assert report.avg_abs_fill_delta_bps == parse_money("0.5")
    assert report.net_pnl_delta == parse_money("-0.01")
    assert report.gross_pnl == parse_money("10.00")
    assert report.pnl_delta_fraction == parse_money("-0.001")
    assert report.paper_only == ()
    assert report.shadow_only == ()
    assert report.diverged is False
    assert report.reasons == ()

    payload = report.to_json()
    assert payload["pnl"]["delta"] == "-0.01"
    assert payload["fills"]["avg_delta_bps"] == "0.50000000"
    # The thresholds travel with the verdict, so a stored report can be re-derived by
    # someone who was not there when it was written.
    assert payload["thresholds"] == {
        "pnl_fraction_of_gross": "0.05",
        "avg_fill_delta_bps": "3",
        "match_window_ms": 2000,
    }


def test_a_final_pnl_gap_wider_than_five_percent_of_gross_is_flagged(
    tmp_path: Path,
) -> None:
    """Spec 6.7.2's first condition, with fills that agree so only PnL can raise it.

    The session's round-trips realised +300 and -200, so gross PnL is |300| + |-200| = 500
    and the allowance is 0.05 x 500 = 25. The shadow run finished at 70 against the
    session's 100: a delta of -30, which exceeds 25 and is -30 / 500 = -0.06 of gross.

    Both fills are at identical prices in both runs, so the average deviation is 0 bps and
    the fill threshold cannot be what flagged this.
    """
    fills = [
        _fill(ts_ms=1_000, price="100.00", side="BUY"),
        _fill(ts_ms=2_000, price="100.00", side="SELL", order_id="o2"),
    ]
    paper = _write_run(
        tmp_path / "paper", net_pnl="100", realized=["300", "-200"], fills=fills
    )
    shadow = _write_run(
        tmp_path / "shadow", net_pnl="70", realized=["270", "-200"], fills=fills
    )

    report = build_parity(paper, shadow)

    assert report.gross_pnl == parse_money("500")
    assert report.net_pnl_delta == parse_money("-30")
    assert report.pnl_delta_fraction == parse_money("-0.06")
    assert report.avg_fill_delta_bps == parse_money("0")
    assert report.diverged is True
    assert len(report.reasons) == 1
    assert "final PnL differs" in report.reasons[0]


def test_an_average_fill_deviation_wider_than_three_bps_is_flagged(
    tmp_path: Path,
) -> None:
    """Spec 6.7.2's second condition, with a PnL gap small enough not to raise the first.

    The buy filled at 100.05 in the shadow against 100.00 in the session, which for a buy is
    (100.05 - 100.00) / 100.00 x 10000 = +5 bps against the trader. The sell filled at 99.97
    against 100.00, and for a sell receiving less is worse, so the sign flips:
    -(99.97 - 100.00) / 100.00 x 10000 = +3 bps. The average is (5 + 3) / 2 = 4 bps, which
    exceeds the 3 bps limit.

    Gross PnL is |200| + |-100| = 300, so the PnL allowance is 0.05 x 300 = 15 and the
    delta of 95 - 100 = -5 is comfortably inside it.
    """
    paper = _write_run(
        tmp_path / "paper",
        net_pnl="100",
        realized=["200", "-100"],
        fills=[
            _fill(ts_ms=1_000, price="100.00", side="BUY"),
            _fill(ts_ms=2_000, price="100.00", side="SELL", order_id="o2"),
        ],
    )
    shadow = _write_run(
        tmp_path / "shadow",
        net_pnl="95",
        realized=["195", "-100"],
        fills=[
            _fill(ts_ms=1_000, price="100.05", side="BUY"),
            _fill(ts_ms=2_000, price="99.97", side="SELL", order_id="o2"),
        ],
    )

    report = build_parity(paper, shadow)

    assert report.matched_fills == 2
    assert report.avg_fill_delta_bps == parse_money("4")
    assert report.avg_abs_fill_delta_bps == parse_money("4")
    assert report.diverged is True
    assert len(report.reasons) == 1
    assert "bps" in report.reasons[0]


def test_a_deviation_of_exactly_three_bps_is_inside_the_threshold(
    tmp_path: Path,
) -> None:
    """Spec 6.7.2 flags a deviation that *exceeds* 3 bps, so 3 bps itself passes.

    The buy filled at 100.03 against 100.00: (100.03 - 100.00) / 100.00 x 10000 = +3 bps.
    The sell filled at 99.97 against 100.00: -(99.97 - 100.00) / 100.00 x 10000 = +3 bps.
    The average is exactly 3 bps. Both runs report the same final PnL, so nothing else can
    raise the flag.

    Pinned because a threshold whose boundary drifts is a threshold nobody can reason about,
    and this is the reading `core.risk` applies to every ceiling on a value.
    """
    paper = _write_run(
        tmp_path / "paper",
        net_pnl="100",
        realized=["100"],
        fills=[
            _fill(ts_ms=1_000, price="100.00", side="BUY"),
            _fill(ts_ms=2_000, price="100.00", side="SELL", order_id="o2"),
        ],
    )
    shadow = _write_run(
        tmp_path / "shadow",
        net_pnl="100",
        realized=["100"],
        fills=[
            _fill(ts_ms=1_000, price="100.03", side="BUY"),
            _fill(ts_ms=2_000, price="99.97", side="SELL", order_id="o2"),
        ],
    )

    report = build_parity(paper, shadow)

    assert report.avg_fill_delta_bps == FILL_DEVIATION_BPS_LIMIT
    assert report.diverged is False


def test_a_fill_with_no_counterpart_is_listed_rather_than_averaged_away(
    tmp_path: Path,
) -> None:
    """Spec 6.7.1's fourth quantity, and why the first one cannot stand in for it.

    Both runs filled order `o1`; the session then filled `o2` and the shadow filled `o3`
    instead, so neither of those has a counterpart. Two fills each, so the count delta is
    2 - 2 = 0 -- a run that missed one fill and invented another looks identical to one that
    reproduced both.

    Only the `o1` pair matches, at (100.02 - 100.00) / 100.00 x 10000 = +2 bps, and that
    single pair is the whole average. The `o2` and `o3` fills never enter it: with no
    counterpart there is no price to compare against, and scoring them as zero deviation
    would report clean fill parity on a run that half missed.

    Gross PnL is 50, allowing 0.05 x 50 = 2.50, and both runs finished at 50 -- so the
    verdict stays false even though two fills did not match, which is spec 6.7.2's rule
    exactly: unmatched fills are reported, and the flag is the PnL and bps thresholds.
    """
    paper = _write_run(
        tmp_path / "paper",
        net_pnl="50",
        realized=["50"],
        fills=[
            _fill(ts_ms=1_000, price="100.00", qty="1", order_id="o1"),
            _fill(ts_ms=10_000, price="200.00", qty="2", order_id="o2"),
        ],
    )
    shadow = _write_run(
        tmp_path / "shadow",
        net_pnl="50",
        realized=["50"],
        fills=[
            _fill(ts_ms=1_000, price="100.02", qty="1", order_id="o1"),
            _fill(ts_ms=20_000, price="300.00", qty="3", order_id="o3"),
        ],
    )

    report = build_parity(paper, shadow)

    assert report.fill_count_delta == 0
    assert report.matched_fills == 1
    assert report.avg_fill_delta_bps == parse_money("2")
    assert [record.order_id for record in report.paper_only] == ["o2"]
    assert [record.order_id for record in report.shadow_only] == ["o3"]
    assert report.paper_only[0].qty == parse_money("2")
    assert report.shadow_only[0].qty == parse_money("3")
    assert report.diverged is False


def test_the_window_edge_is_inclusive_for_fills_that_carry_no_order_id(
    tmp_path: Path,
) -> None:
    """The fallback rule, and the only place a timestamp decides a match at all.

    Neither fill below carries an `order_id`, so neither has the identity the engine stamps
    on its own fills and the time window is all there is. Both cases use the same paper fill
    at t = 1 000 ms and a shadow fill at the same price, so the only thing that changes is
    the gap. At t = 3 000 ms the gap is exactly 2 000 ms and the two are the same fill; at
    t = 3 001 ms it is 2 001 ms and they are two fills that happen to be the same size, each
    reported on its own side.

    An unmatched pair leaves nothing to average, so the deviation is `None` rather than
    0 bps -- reporting zero would say the fills agreed, which is the opposite of what
    happened.
    """
    assert FILL_MATCH_WINDOW_MS == 2_000

    for gap_ms, expected_matches in ((2_000, 1), (2_001, 0)):
        root = tmp_path / f"gap{gap_ms}"
        paper = _write_run(
            root / "paper",
            net_pnl="10",
            realized=["10"],
            fills=[_fill(ts_ms=1_000, price="100.00", order_id="")],
        )
        shadow = _write_run(
            root / "shadow",
            net_pnl="10",
            realized=["10"],
            fills=[_fill(ts_ms=1_000 + gap_ms, price="100.00", order_id="")],
        )

        report = build_parity(paper, shadow)

        assert report.matched_fills == expected_matches
        if expected_matches:
            assert report.avg_fill_delta_bps == parse_money("0")
            assert report.paper_only == ()
            assert report.shadow_only == ()
        else:
            assert report.avg_fill_delta_bps is None
            assert report.avg_abs_fill_delta_bps is None
            assert len(report.paper_only) == 1
            assert len(report.shadow_only) == 1
        assert report.diverged is False


def test_a_fill_whose_order_id_agrees_but_whose_tag_does_not_is_left_unmatched(
    tmp_path: Path,
) -> None:
    """An id both runs used for a different order names nothing, so nothing is compared.

    The session filled one buy of 1 at 100.00 at t = 1 000, tagged `entry`, as order `o1`.
    The shadow's `o1` is a `stop` -- same id, different intent -- which is what two runs
    whose order streams have parted company look like. It fills at 101.00 and the second
    shadow fill, `s7` at 100.02, is tagged `entry` and resembles the session's fill closely.

    Neither is taken. Matching `o1` would average a deviation between two different orders
    ((101.00 - 100.00) / 100.00 x 10000 = +100 bps, a figure about nothing), and matching
    `s7` on the strength of a shared tag is the resemblance rule that reported 50 bps on a
    correct fill model. All three fills are reported unmatched, and with nothing matched the
    average is `None` rather than a number.
    """
    paper = _write_run(
        tmp_path / "paper",
        net_pnl="10",
        realized=["10"],
        fills=[_fill(ts_ms=1_000, price="100.00", order_id="o1", tag="entry")],
    )
    shadow = _write_run(
        tmp_path / "shadow",
        net_pnl="10",
        realized=["10"],
        fills=[
            _fill(ts_ms=1_000, price="101.00", order_id="o1", tag="stop"),
            _fill(ts_ms=1_900, price="100.02", order_id="s7", tag="entry"),
        ],
    )

    report = build_parity(paper, shadow)

    assert report.matched_fills == 0
    assert report.avg_fill_delta_bps is None
    assert [record.order_id for record in report.paper_only] == ["o1"]
    assert [record.order_id for record in report.shadow_only] == ["o1", "s7"]


def test_the_order_id_decides_the_match_and_a_nearer_fill_does_not(
    tmp_path: Path,
) -> None:
    """Identity beats proximity: the shadow fill sharing the id wins over the closer one.

    Both shadow fills are untagged, as the session's fill is. Order `o5` at t = 1 900 shares
    the session's order id; order `o9` at t = 1 000 does not, but is simultaneous with it.
    `o5` wins, and the average says so: (100.02 - 100.00) / 100.00 x 10000 = +2 bps, against
    the +100 bps that matching `o9` at 101.00 would have produced.
    """
    paper = _write_run(
        tmp_path / "paper",
        net_pnl="10",
        realized=["10"],
        fills=[_fill(ts_ms=1_000, price="100.00", order_id="o5")],
    )
    shadow = _write_run(
        tmp_path / "shadow",
        net_pnl="10",
        realized=["10"],
        fills=[
            _fill(ts_ms=1_000, price="101.00", order_id="o9"),
            _fill(ts_ms=1_900, price="100.02", order_id="o5"),
        ],
    )

    report = build_parity(paper, shadow)

    assert report.matched_fills == 1
    assert report.avg_fill_delta_bps == parse_money("2")
    assert [record.order_id for record in report.shadow_only] == ["o9"]


def test_a_scale_ins_first_clip_going_missing_does_not_re_price_its_second(
    tmp_path: Path,
) -> None:
    """The false-positive half of the resemblance rule: 50 bps on a fill model with none.

    The session scales into a long with two equal clips 1 500 ms apart -- order `o1` buys 1
    at 100.00 at t = 1 000 and order `o2` buys 1 at 101.00 at t = 2 500 -- then order `o3`
    sells 2 at 110.00. The shadow never produced `o1` and reproduced `o2` and `o3` at exactly
    the session's own prices, so over the fills that exist in both runs the fill model is
    perfect: (101.00 - 101.00) / 101.00 = 0 bps on the buy and 0 bps on the sell, average 0.

    Matching on resemblance instead gave `o1` the only buy of size 1 within 2 000 ms of it --
    `o2`'s -- and scored it (101.00 - 100.00) / 100.00 x 10000 = +100 bps, then found nothing
    left for `o2`. Averaged with the sell's 0 bps that is 50 bps, over the 3 bps spec 6.7.2
    allows, and the run was reported as diverged with `o2` named as the fill the shadow
    missed. Both figures are wrong and both look plausible.

    Both runs finished at 19.00 on a gross of 19.00, so the PnL gate cannot flag this and the
    verdict below is the fill gate's alone.
    """
    paper = _write_run(
        tmp_path / "paper",
        net_pnl="19.00",
        realized=["19.00"],
        fills=[
            _fill(ts_ms=1_000, price="100.00", qty="1", side="BUY", order_id="o1"),
            _fill(ts_ms=2_500, price="101.00", qty="1", side="BUY", order_id="o2"),
            _fill(ts_ms=9_000, price="110.00", qty="2", side="SELL", order_id="o3"),
        ],
    )
    shadow = _write_run(
        tmp_path / "shadow",
        net_pnl="19.00",
        realized=["19.00"],
        fills=[
            _fill(ts_ms=2_500, price="101.00", qty="1", side="BUY", order_id="o2"),
            _fill(ts_ms=9_000, price="110.00", qty="2", side="SELL", order_id="o3"),
        ],
    )

    report = build_parity(paper, shadow)

    assert report.matched_fills == 2
    assert report.avg_fill_delta_bps == parse_money("0")
    assert report.avg_abs_fill_delta_bps == parse_money("0")
    # The fill that actually went missing, not the one that lost the competition for a
    # partner. Naming the wrong one hides the cause as thoroughly as the wrong average does.
    assert [record.order_id for record in report.paper_only] == ["o1"]
    assert report.shadow_only == ()
    assert report.diverged is False


def test_a_shadow_that_filled_909_bps_better_cannot_report_zero_deviation(
    tmp_path: Path,
) -> None:
    """The false-negative half, and the direction that flatters a strategy into being traded.

    The session bought 1 at 100.00 as order `o1` at t = 1 000 and 1 at 110.00 as order `o2`
    at t = 2 500. The shadow produced only `o2`, and filled it at 100.00 -- 10.00 cheaper on
    a 110.00 fill, which for a buy is (100.00 - 110.00) / 110.00 x 10000 = -909.0909... bps,
    rendered at the storage seam as -909.09090909. Negative is the shadow filling *better*
    than the session, so a backtest built on this model would price this strategy's entries
    at a level the exchange never gave it.

    Matching on resemblance handed the session's `o1` fill the shadow's only buy of size 1
    within 2 000 ms -- `o2`'s, at 100.00 against `o1`'s own 100.00 -- and reported
    **0.00000000 bps average deviation and `diverged=False`**. That is the exact figure this
    project's parity evidence is quoted in.

    Both runs finished at 25 on a gross of 25, allowing 0.05 x 25 = 1.25, so the PnL gate is
    silent and the flag below is the fill gate's.
    """
    paper = _write_run(
        tmp_path / "paper",
        net_pnl="25",
        realized=["25"],
        fills=[
            _fill(ts_ms=1_000, price="100.00", side="BUY", order_id="o1"),
            _fill(ts_ms=2_500, price="110.00", side="BUY", order_id="o2"),
        ],
    )
    shadow = _write_run(
        tmp_path / "shadow",
        net_pnl="25",
        realized=["25"],
        fills=[_fill(ts_ms=2_500, price="100.00", side="BUY", order_id="o2")],
    )

    report = build_parity(paper, shadow)

    assert report.matched_fills == 1
    assert report.avg_fill_delta_bps is not None
    assert report.avg_fill_delta_bps < parse_money("-909")
    assert report.to_json()["fills"]["avg_delta_bps"] == "-909.09090909"
    assert [record.order_id for record in report.paper_only] == ["o1"]
    assert report.diverged is True
    assert "the shadow filled better than the session did" in report.reasons[0]


def test_an_order_the_shadow_filled_a_minute_later_is_still_the_same_fill(
    tmp_path: Path,
) -> None:
    """Identity carries no time component, because a late arrival is not a different order.

    Order `o1` filled at t = 1 000 in the session and at t = 61 000 in the shadow -- a minute
    apart, thirty times the 2 000 ms window. It is the same order either way, and its price
    moved: (100.05 - 100.00) / 100.00 x 10000 = +5 bps against the trader, over the 3 bps
    spec 6.7.2 allows.

    Under a rule that needed the two to land within 2 000 ms of each other this pair simply
    vanished: nothing matched, the average was `None`, and a 5 bps fill-model breach was
    reported as two unmatched fills and no verdict.

    Both runs finished at 10 on a gross of 10, so the PnL gate is silent here too.
    """
    paper = _write_run(
        tmp_path / "paper",
        net_pnl="10",
        realized=["10"],
        fills=[_fill(ts_ms=1_000, price="100.00", order_id="o1")],
    )
    shadow = _write_run(
        tmp_path / "shadow",
        net_pnl="10",
        realized=["10"],
        fills=[_fill(ts_ms=61_000, price="100.05", order_id="o1")],
    )

    report = build_parity(paper, shadow)

    assert report.matched_fills == 1
    assert report.avg_fill_delta_bps == parse_money("5")
    assert report.paper_only == ()
    assert report.shadow_only == ()
    assert report.diverged is True


def test_each_increment_of_an_order_is_matched_against_the_same_increment(
    tmp_path: Path,
) -> None:
    """An order fills in pieces, and the pieces are matched in order, not by size.

    Order `o1` fills twice in each run: 0.4 at 100.00 then 0.6 at 200.00 in the session, and
    0.6 at 100.00 then 0.4 at 200.00 in the shadow. The prices agree increment for increment,
    so the deviation is (100.00 - 100.00) / 100.00 = 0 bps on the first and
    (200.00 - 200.00) / 200.00 = 0 bps on the second: the fill model priced this order
    identically and only carved it up differently.

    Pairing on `(symbol, side, quantity)` instead matched the session's 0.4 against the
    shadow's 0.4 -- the *second* increment, at 200.00 -- for
    (200.00 - 100.00) / 100.00 x 10000 = +10000 bps, and the session's 0.6 against the
    shadow's first at 100.00 for (100.00 - 200.00) / 200.00 x 10000 = -5000 bps, averaging
    (10000 + -5000) / 2 = +2500 bps out of two fills that agreed exactly.

    This also pins that quantity is *not* required to agree: the sizes differ on both
    increments and both still match, because how an order was carved up is the fill model
    differing, which is the quantity being measured rather than a reason to stop measuring.
    """
    paper = _write_run(
        tmp_path / "paper",
        net_pnl="40",
        realized=["40"],
        fills=[
            _fill(ts_ms=1_000, price="100.00", qty="0.4", order_id="o1"),
            _fill(ts_ms=1_100, price="200.00", qty="0.6", order_id="o1"),
        ],
    )
    shadow = _write_run(
        tmp_path / "shadow",
        net_pnl="40",
        realized=["40"],
        fills=[
            _fill(ts_ms=1_000, price="100.00", qty="0.6", order_id="o1"),
            _fill(ts_ms=1_100, price="200.00", qty="0.4", order_id="o1"),
        ],
    )

    report = build_parity(paper, shadow)

    assert report.matched_fills == 2
    assert report.avg_fill_delta_bps == parse_money("0")
    assert report.avg_abs_fill_delta_bps == parse_money("0")
    assert report.diverged is False
    # The increment is published, so a reader of `parity.json` can tell which piece of an
    # order an unmatched fill was without re-deriving it from the log.
    assert [fill["fill_no"] for fill in report.to_json()["fills"]["paper_only"]] == []


def test_two_id_less_fills_that_could_both_be_one_shadow_fill_are_left_unmatched(
    tmp_path: Path,
) -> None:
    """The fallback refuses an ambiguous pairing rather than ranking its way to a winner.

    Neither run's fills carry an `order_id`, so the time window is the only rule available.
    The session bought 1 at 100.00 at t = 1 000 and 1 at 101.00 at t = 2 500; the shadow
    produced one buy of 1 at 101.00 at t = 2 500, which is inside 2 000 ms of *both* -- 1 500
    ms from the first and 0 ms from the second.

    There is no evidence here that picks one, so none is picked: all three fills are reported
    unmatched and the average is `None`. Ranking always produces a winner, and the winner it
    produced was the first paper fill, scoring (101.00 - 100.00) / 100.00 x 10000 = +100 bps
    against a shadow fill that was the other clip.
    """
    paper = _write_run(
        tmp_path / "paper",
        net_pnl="10",
        realized=["10"],
        fills=[
            _fill(ts_ms=1_000, price="100.00", order_id=""),
            _fill(ts_ms=2_500, price="101.00", order_id=""),
        ],
    )
    shadow = _write_run(
        tmp_path / "shadow",
        net_pnl="10",
        realized=["10"],
        fills=[_fill(ts_ms=2_500, price="101.00", order_id="")],
    )

    report = build_parity(paper, shadow)

    assert report.matched_fills == 0
    assert report.avg_fill_delta_bps is None
    assert [record.ts_ms for record in report.paper_only] == [1_000, 2_500]
    assert [record.ts_ms for record in report.shadow_only] == [2_500]
    assert report.diverged is False


def test_a_flagged_pnl_reason_says_its_direction_rather_than_signing_its_figure(
    tmp_path: Path,
) -> None:
    """The reason has to be checkable against its own two numbers, and it is a comparison.

    Gross PnL is |300| + |-200| = 500, so spec 6.7.2 allows 0.05 x 500 = 25 and a shadow
    finishing at 70 against the session's 100 breaches it by |70 - 100| = 30.

    The two figures the sentence compares are therefore 30.00000000 and 25.00000000, and
    30 is more than 25. Printing the signed delta instead produced "differs by -30.00000000,
    which is more than ... 25.00000000" on every flagged run whose shadow finished lower --
    half of them -- which is a true statement about magnitudes in the shape of a false one
    about numbers. The sign is not dropped; it is said.
    """
    fills = [
        _fill(ts_ms=1_000, price="100.00", side="BUY"),
        _fill(ts_ms=2_000, price="100.00", side="SELL", order_id="o2"),
    ]
    paper = _write_run(
        tmp_path / "paper", net_pnl="100", realized=["300", "-200"], fills=fills
    )
    shadow = _write_run(
        tmp_path / "shadow", net_pnl="70", realized=["270", "-200"], fills=fills
    )

    report = build_parity(paper, shadow)
    reason = report.reasons[0]

    assert "final PnL differs by 30.00000000 " in reason
    assert "the shadow finished lower than the session" in reason
    assert "more than the 25.00000000 spec 6.7.2 allows" in reason
    assert "-30" not in reason


def test_a_pnl_reason_whose_figures_would_round_equal_is_printed_exactly(
    tmp_path: Path,
) -> None:
    """A breach the eight-decimal rendering cannot show is rendered at full precision.

    The session's one round-trip realised 100, so the allowance is 0.05 x 100 = 5.00 exactly.
    The shadow finished at 5.0000000000000000001 against the session's 0, which is more than
    5.00 -- by one part in 10^19, which is a real breach decided on the exact decimal strings
    this module refuses to round before comparing.

    At the house rendering of eight decimals both figures print as 5.00000000 and the reason
    read "differs by 5.00000000, which is more than ... (= 5.00000000)". Rounding is
    monotone, so it could never read backwards, but a reader who checks that sentence finds
    two equal numbers and an assertion that one exceeds the other -- and the natural
    conclusion, that the verdict is broken, is wrong. Both figures fall back to exact
    together, because rendering only the delta exactly against a rounded allowance can invert
    the pair instead of merely flattening it.
    """
    paper = _write_run(tmp_path / "paper", net_pnl="0", realized=["100"])
    shadow = _write_run(
        tmp_path / "shadow", net_pnl="5.0000000000000000001", realized=["100"]
    )

    report = build_parity(paper, shadow)
    reason = report.reasons[0]

    assert report.diverged is True
    assert "final PnL differs by 5.0000000000000000001 " in reason
    assert "more than the 5.00 spec 6.7.2 allows" in reason
    assert "5.00000000 " not in reason


def test_a_session_that_realised_nothing_and_agrees_on_pnl_is_not_flagged(
    tmp_path: Path,
) -> None:
    """Gross PnL of zero is a real state, not an error: nothing closed, so nothing realised.

    Neither run recorded a round-trip, so gross PnL is 0 and the fraction spec 6.7.2 scales
    by is undefined -- reported as `None`, because 0 would say the two runs agreed to within
    nothing and a large number would name a disagreement that does not exist. Both runs
    finished at 0, so there is nothing to flag either way.
    """
    paper = _write_run(tmp_path / "paper", net_pnl="0")
    shadow = _write_run(tmp_path / "shadow", net_pnl="0")

    report = build_parity(paper, shadow)

    assert report.gross_pnl == parse_money("0")
    assert report.pnl_delta_fraction is None
    assert report.avg_fill_delta_bps is None
    assert report.diverged is False
    assert report.reasons == ()


def test_a_pnl_gap_with_no_gross_to_scale_by_is_flagged_rather_than_divided(
    tmp_path: Path,
) -> None:
    """The zero-gross case that must not become a division: gross 0 and a real PnL gap.

    The session held one position to the end of the window, so its only round-trip realised
    0.00 and gross PnL is |0.00| = 0. The shadow run nonetheless finished at -12.50 against
    the session's 0, a delta of -12.50. There is no denominator, so the 5% rule cannot be
    evaluated -- and the difference is flagged rather than skipped, because there is no size
    of position at which a 12.50 disagreement about money would have been small.
    """
    paper = _write_run(tmp_path / "paper", net_pnl="0", realized=["0.00"])
    shadow = _write_run(tmp_path / "shadow", net_pnl="-12.50", realized=["0.00"])

    report = build_parity(paper, shadow)

    assert report.gross_pnl == parse_money("0")
    assert report.net_pnl_delta == parse_money("-12.50")
    assert report.pnl_delta_fraction is None
    assert report.diverged is True
    assert "no gross PnL to scale" in report.reasons[0]


def test_a_pnl_delta_below_float_resolution_still_decides_the_verdict(
    tmp_path: Path,
) -> None:
    """Why the PnL comes from the exact decimal string and not from any float on disk.

    The two runs finished at 1000000000.00000001 and 1000000000.00000002. Both figures need
    18 significant digits and float64 carries about 16, so both round to the same float --
    the assertion below shows it -- and a report built on `Metrics` or on `equity.parquet`
    would compute a delta of exactly 0.0 and flag nothing.

    Exactly, the delta is 0.00000001. The session's single round-trip realised 0.0000001, so
    gross PnL is 0.0000001 and the allowance is 0.05 x 0.0000001 = 0.000000005. Since
    0.00000001 > 0.000000005, this run diverged, and only the exact figure can see it.
    """
    assert float("1000000000.00000001") == float("1000000000.00000002")

    paper = _write_run(
        tmp_path / "paper", net_pnl="1000000000.00000001", realized=["0.0000001"]
    )
    shadow = _write_run(
        tmp_path / "shadow", net_pnl="1000000000.00000002", realized=["0.0000001"]
    )

    report = build_parity(paper, shadow)

    assert report.net_pnl_delta == parse_money("0.00000001")
    assert report.gross_pnl == parse_money("0.0000001")
    assert PNL_DIVERGENCE_FRACTION * report.gross_pnl == parse_money("0.000000005")
    assert report.diverged is True


def test_a_net_pnl_written_as_a_json_number_is_refused(tmp_path: Path) -> None:
    """A number in `attribution.net_pnl` has already been rounded by `json.load`.

    Refused rather than parsed from its repr: the digits are gone by the time this module
    could see them, and an exact-looking `Decimal` rebuilt from a float is the kind of wrong
    number that looks right.
    """
    paper = _write_run(tmp_path / "paper", net_pnl="10", realized=["10"])
    shadow = _write_run(tmp_path / "shadow", net_pnl="10", realized=["10"])
    (shadow / "metrics.json").write_text(
        json.dumps({"attribution": {"net_pnl": 10.0}}), encoding="utf-8"
    )

    with pytest.raises(ParityInputMalformed, match="JSON number"):
        build_parity(paper, shadow)


def test_a_fill_price_written_as_a_json_number_is_refused(tmp_path: Path) -> None:
    """The same exactness rule one layer down: a fill price is a decimal string too.

    A price that reached the log as a JSON number has already been rounded to a float, and a
    basis-point deviation measured against a rounded price is the divergence this report is
    supposed to detect rather than commit.
    """
    paper = _write_run(tmp_path / "paper", net_pnl="10", realized=["10"])
    shadow = _write_run(tmp_path / "shadow", net_pnl="10", realized=["10"])
    payload = _fill(ts_ms=1_000, price="100.00")
    payload["price"] = 100.0
    (paper / "events.jsonl").write_text(
        json.dumps({"seq": 1, "ts_ms": 1_000, "kind": "FILL", "payload": payload}) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ParityInputMalformed, match="not the exact decimal string"):
        build_parity(paper, shadow)


def test_only_fill_events_are_read_from_the_event_log(tmp_path: Path) -> None:
    """The cheap `"FILL"` substring pre-filter must not admit what it merely matches.

    The session's log holds one real `FILL`, one `NO_QUOTE_FILL` from the validation
    harness, and one strategy log line whose message contains the word. All three survive
    the substring test and only one survives the parsed check, so the report counts one
    paper fill.
    """
    paper = _write_run(
        tmp_path / "paper",
        net_pnl="10",
        realized=["10"],
        fills=[_fill(ts_ms=1_000, price="100.00")],
        extra_events=[
            {
                "seq": 90,
                "ts_ms": 900,
                "kind": "NO_QUOTE_FILL",
                "payload": {"order_id": "o1", "reason": "no ladder"},
            },
            {
                "seq": 91,
                "ts_ms": 950,
                "kind": "LOG",
                "payload": {"message": "waiting for a FILL"},
            },
        ],
    )
    shadow = _write_run(
        tmp_path / "shadow",
        net_pnl="10",
        realized=["10"],
        fills=[_fill(ts_ms=1_000, price="100.00")],
    )

    report = build_parity(paper, shadow)

    assert report.paper_fills == 1
    assert report.matched_fills == 1


def test_a_log_whose_fills_run_backwards_in_time_is_refused(tmp_path: Path) -> None:
    """The single streaming pass is only correct while each log runs forwards.

    A log that goes backwards would have fills evicted from the match window before their
    counterpart arrived, so the pairing would depend on the reordering rather than on the
    fills. Refused loudly instead of matched arbitrarily.
    """
    paper = _write_run(tmp_path / "paper", net_pnl="10", realized=["10"])
    shadow = _write_run(tmp_path / "shadow", net_pnl="10", realized=["10"])
    (paper / "events.jsonl").write_text(
        json.dumps(
            {"seq": 1, "ts_ms": 9_000, "kind": "FILL", "payload": _fill(ts_ms=9_000, price="100")}
        )
        + "\n"
        + json.dumps(
            {"seq": 2, "ts_ms": 1_000, "kind": "FILL", "payload": _fill(ts_ms=1_000, price="100")}
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ParityInputMalformed, match="goes backwards"):
        build_parity(paper, shadow)


def test_a_run_without_its_artefacts_names_the_file_it_wanted(tmp_path: Path) -> None:
    """A parity report needs two *finished* runs, and says so rather than reporting zeros."""
    paper = _write_run(tmp_path / "paper", net_pnl="10", realized=["10"])
    shadow = tmp_path / "shadow"
    shadow.mkdir()

    with pytest.raises(ParityInputMissing, match="metrics.json"):
        build_parity(paper, shadow)

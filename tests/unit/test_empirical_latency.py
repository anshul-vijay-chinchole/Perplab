"""Spec 6.3's `empirical` latency mode -- draws replayed from measured paper sessions.

The probabilistic assertions here are derived rather than observed: each docstring states
the chance of the assertion failing by luck, computed from the pool the test itself builds.
"""

from __future__ import annotations

import random

import pytest

from perplab.engine.latency import (
    EmpiricalLatency,
    EmptyLatencySamples,
    latency_from_json,
)


def test_the_same_seed_replays_the_same_latencies() -> None:
    """Determinism is the whole reproducibility contract (spec 12.1), so it is pinned first.

    Two generators seeded identically must produce the same 100 draws. A third, seeded
    differently, must not: the pool holds five distinct values, so two independent runs
    agreeing on all 100 draws has probability 5^-100, which is about 1e-70.
    """
    model = EmpiricalLatency.from_samples([80, 110, 140, 220, 900], [130, 700])

    one = random.Random(7)
    two = random.Random(7)
    other = random.Random(8)
    assert [model.submit_ms(one) for _ in range(100)] == [
        model.submit_ms(two) for _ in range(100)
    ]
    assert [model.submit_ms(one) for _ in range(100)] != [
        model.submit_ms(other) for _ in range(100)
    ]


def test_submit_and_cancel_draw_from_their_own_pools() -> None:
    """Spec 6.3's cancel latency is a separate measurement, not a second look at submits.

    The pools here share no value -- submits are always 100 ms, cancels always 900 ms -- so
    a single merged pool could not produce these 200 draws: it would return 900 for some
    submit and 100 for some cancel. That is the point. A merged pool would draw a 100 ms
    cancel from submit measurements, pull the resting order 800 ms early, and let the
    strategy escape a fill the exchange would have given it -- spec 6.3's named optimism.
    """
    model = EmpiricalLatency.from_samples([100], [900])
    rng = random.Random(1)

    assert {model.submit_ms(rng) for _ in range(100)} == {100}
    assert {model.cancel_ms(rng) for _ in range(100)} == {900}


def test_every_recorded_sample_can_be_drawn() -> None:
    """A draw is uniform over the pool -- no value is unreachable.

    Over 200 draws from a three-value pool, one specific value failing to appear has
    probability (2/3)^200, about 3e-36; all three appearing is therefore certain to any
    standard this test could be held to.
    """
    model = EmpiricalLatency.from_samples([5, 6, 7], [11])
    rng = random.Random(4)

    assert {model.submit_ms(rng) for _ in range(200)} == {5, 6, 7}


def test_an_empty_pool_is_refused_rather_than_falling_back() -> None:
    """A model calibrated on nothing must not become `lognormal` wearing another name.

    All three doors are checked -- no submits, no cancels, and a stored manifest naming
    `empirical` without samples -- because a fallback on any of them would re-price every
    fill in the run while leaving the run's identity unchanged.
    """
    with pytest.raises(EmptyLatencySamples, match="'fixed' or 'lognormal'"):
        EmpiricalLatency.from_samples([], [120])
    with pytest.raises(EmptyLatencySamples, match="'fixed' or 'lognormal'"):
        EmpiricalLatency.from_samples([120], [])
    with pytest.raises(EmptyLatencySamples, match="re-price every fill"):
        latency_from_json({"model": "empirical"})


def test_a_sub_millisecond_measurement_is_floored_rather_than_dropped() -> None:
    """A zero-latency arrival fills at the price that triggered the order.

    The pool `[0, 3, 0]` becomes `(1, 1, 3)`: the two zeros floor to 1 ms and neither is
    discarded. Dropping them would remove the fastest observations from the pool and bias
    every remaining draw slow, which is the opposite error and just as silent.
    """
    model = EmpiricalLatency.from_samples([0, 3, 0], [0])

    assert model.submit_samples == (1, 1, 3)
    assert model.cancel_samples == (1,)
    assert min(model.submit_ms(random.Random(2)) for _ in range(50)) >= 1


def test_a_negative_sample_is_refused_rather_than_clamped() -> None:
    """A negative latency is a clock that ran backwards, not a fast order.

    Refused rather than floored with the zeros, because the reading is not merely out of
    range: the session that produced it was mis-timed, and the rest of its samples are
    suspect too.
    """
    with pytest.raises(ValueError, match="clock that ran backwards"):
        EmpiricalLatency.from_samples([120, -1], [120])


def test_the_recording_order_of_the_pool_cannot_change_the_run() -> None:
    """Two sessions that measured the same latencies describe the same network.

    The pool is sorted at construction, so `[9, 1, 5]` and `[5, 9, 1]` build the same model
    and produce the same draws from the same seed. Without that, a manifest that listed its
    samples in a different order would replay a different run under spec 12.1's contract
    while claiming to be the same one.
    """
    one = EmpiricalLatency.from_samples([9, 1, 5], [4, 2])
    two = EmpiricalLatency.from_samples([5, 9, 1], [2, 4])

    assert one == two
    assert [one.submit_ms(random.Random(3)) for _ in range(20)] == [
        two.submit_ms(random.Random(3)) for _ in range(20)
    ]


def test_it_round_trips_through_its_manifest_form_carrying_every_sample() -> None:
    """The manifest holds the pool itself, because a summary of it is a different model.

    Rebuilding from a median and a p99 would give `lognormal`, whose only difference from
    this model is the tail it does not keep -- so the stored form lists all five submit
    samples, and the rebuilt model draws exactly what the original drew.
    """
    model = EmpiricalLatency.from_samples(
        [140, 80, 900, 110, 220], [700, 130], source="run 41"
    )

    payload = model.to_json()
    assert payload == {
        "model": "empirical",
        "submit_ms": [80, 110, 140, 220, 900],
        "cancel_ms": [130, 700],
        "source": "run 41",
    }

    rebuilt = latency_from_json(payload)
    assert rebuilt.to_json() == payload
    assert [rebuilt.submit_ms(random.Random(11)) for _ in range(20)] == [
        model.submit_ms(random.Random(11)) for _ in range(20)
    ]


def test_an_empirical_run_is_never_flagged_zero_latency() -> None:
    """The engine reads `name` and `is_zero` off the model; both are pinned here.

    `is_zero` decides whether the run carries the `ZERO_LATENCY` flag, and it is `False` by
    construction rather than by luck: every sample is floored to at least 1 ms, so even a
    pool measured entirely as zeros cannot produce a draw that fills at the price which
    triggered the order.
    """
    model = EmpiricalLatency.from_samples([0, 0], [0])

    assert model.name == "empirical"
    assert model.is_zero is False
    assert model.submit_ms(random.Random(0)) == 1

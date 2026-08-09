"""The latency model (spec 6.3).

```
signal generated at T
  |  submit_latency
order reaches the matching engine at T + L
  |
fill evaluated against market data at or after T + L, never before
```

**Default latency is deliberately not zero.** Spec 6.3 is explicit that zero-latency
backtests systematically overstate performance for anything reacting to fast moves. At the
Phase 4 `BAR_CLOSE` tier the effect is one of *causality* rather than of magnitude, and it
is worth being precise about which:

A signal fires at a bar's close. With **zero** latency the order arrives at exactly that
timestamp, and the most recent print the engine knows about is the bar's own close -- so it
fills at the very price the strategy just looked at, which is a fill decided on information
it did not yet have when the order would really have left. With any non-zero latency the
arrival lands after the next bar has opened, and the fill takes that open instead: a print
published *after* the decision.

The two prices are usually close. A bar closes at `...:59.999` and the next opens at
`...:00.000`, so the next bar's open is the first trade one millisecond later, not a bar's
worth of movement away -- typically a tick or two. The gap this closes is therefore small in
Phase 4 and correct in principle, and it stops being small in Phase 5, where fills are
evaluated against individual trades and "the print you already saw" versus "the next one" is
the whole difference between a queue model and a wish.

`FixedLatency(0)` remains available for golden tests and flags the run `ZERO_LATENCY`.

**Randomness comes from a dedicated stream.** `LognormalLatency` draws from an RNG the
engine seeds separately from `ctx.rng`. Sharing one would make the latency of every order
depend on how many random numbers the strategy happened to consume, so editing a line of
strategy code that draws a number would silently re-price every fill in the run.

**`empirical` replays measurements rather than fitting them.** Spec 6.3's third option is
*"sampled from latencies measured during your own paper sessions -- best available estimate,
and free once you have paper history"*, and Phase 7 is where that history first exists. It
was refused until then because a model calibrated on nothing would have been `lognormal`
wearing a more convincing name, and it draws from the recorded pool itself rather than from
a distribution fitted to it for the mirror-image reason: a fit smooths away the 900 ms
sample that came from one reconnect, and that sample is the latency a strategy reacting to a
liquidation cascade actually got. The pool is what was measured; the fit is an opinion about
it.
"""

from __future__ import annotations

import math
import random
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Mapping, Protocol

__all__ = [
    "LatencyModel",
    "FixedLatency",
    "LognormalLatency",
    "EmpiricalLatency",
    "EmptyLatencySamples",
    "DEFAULT_SUBMIT_MS",
    "DEFAULT_CANCEL_MS",
    "DEFAULT_P99_MS",
    "latency_from_json",
]

DEFAULT_SUBMIT_MS = 120
"""Spec 6.3's lognormal median, used as the fixed default too."""

DEFAULT_CANCEL_MS = 120
"""Cancels have latency as well.

Spec 6.3: *"A cancel issued at T does not protect you from a fill at T + 50 ms if cancel
latency is 120 ms."* This is live: a cancel is scheduled at `now + cancel_ms`, and a resting
order filled by a print inside that window fills. It was declared before it did anything, so
that a stored run's latency model would not gain a field later and stop comparing equal to
itself."""

DEFAULT_P99_MS = 600
"""Spec 6.3's p99."""

_Z99 = 2.3263478740408408
"""The standard normal 99th percentile.

Hardcoded rather than pulled from `statistics.NormalDist().inv_cdf(0.99)` so the constant
is visible and cannot move between Python releases -- it feeds the sigma that shapes every
sampled latency, and a run manifest that records `p99=600` must mean the same distribution
in five years' time."""


class LatencyModel(Protocol):
    """What the engine needs from a latency model."""

    @property
    def name(self) -> str: ...

    def submit_ms(self, rng: random.Random) -> int: ...

    def cancel_ms(self, rng: random.Random) -> int: ...

    def to_json(self) -> dict[str, Any]: ...


@dataclass(frozen=True, slots=True)
class FixedLatency:
    """A constant latency. Spec 6.3's `fixed` mode -- "deterministic, use for golden tests".

    It was the default while `BAR_CLOSE` was the only tier, and the reason was that the
    alternative would have been theatre: the fill price is the last print before arrival, the
    only prints are the bar open and the bar close, and every latency from 1 ms to just under
    one bar therefore produces *exactly the same fill*. Sampling a distribution to choose
    between values that cannot be told apart adds a random draw to the reproducibility
    surface and buys nothing.

    At the tick tiers two samples 200 ms apart land on different prints, so `lognormal` is
    the default again -- as spec 6.3 always said. This stays the right choice for a golden
    scenario, where an assertion about *which* print a fill took must not become an assertion
    about an RNG.
    """

    submit: int = DEFAULT_SUBMIT_MS
    cancel: int = DEFAULT_CANCEL_MS

    def __post_init__(self) -> None:
        if self.submit < 0 or self.cancel < 0:
            raise ValueError(
                f"latency cannot be negative (submit={self.submit}, cancel={self.cancel}); "
                "an order that arrives before it was sent is not a latency model"
            )

    @property
    def name(self) -> str:
        return "fixed"

    @property
    def is_zero(self) -> bool:
        """Whether this model lets an order fill at the price that triggered it.

        Surfaced so the engine can flag the run `ZERO_LATENCY` rather than let a
        systematically optimistic set of numbers pass as ordinary ones.
        """
        return self.submit == 0

    def submit_ms(self, rng: random.Random) -> int:
        return self.submit

    def cancel_ms(self, rng: random.Random) -> int:
        return self.cancel

    def to_json(self) -> dict[str, Any]:
        return {"model": "fixed", "submit_ms": self.submit, "cancel_ms": self.cancel}


@dataclass(frozen=True, slots=True)
class LognormalLatency:
    """Spec 6.3's default: lognormal, median 120 ms, p99 600 ms.

    Parameterised by the two percentiles rather than by `mu`/`sigma` because those are the
    numbers anyone can check against a ping. For a lognormal, `median = exp(mu)` exactly, so
    `mu = ln(median)`; and `p99 = exp(mu + sigma*z99)`, so `sigma = (ln(p99) - mu)/z99`.

    Samples are floored at 1 ms. A sampled zero would put the order back at the signal
    instant and fill it at the price that triggered it -- the exact optimism this module
    exists to prevent -- and it is reachable at any tail, however unlikely, which makes it a
    rare and irreproducible-looking anomaly rather than an honest model.
    """

    median: int = DEFAULT_SUBMIT_MS
    p99: int = DEFAULT_P99_MS
    cancel_median: int = DEFAULT_CANCEL_MS

    def __post_init__(self) -> None:
        if self.median <= 0:
            raise ValueError(f"median latency must be positive, got {self.median}")
        if self.p99 <= self.median:
            raise ValueError(
                f"p99 latency {self.p99} must exceed the median {self.median}; a "
                "distribution whose tail is below its centre is not a distribution"
            )
        if self.cancel_median <= 0:
            raise ValueError(
                f"cancel median must be positive, got {self.cancel_median}"
            )

    @property
    def name(self) -> str:
        return "lognormal"

    @property
    def is_zero(self) -> bool:
        return False

    @property
    def sigma(self) -> float:
        return (math.log(self.p99) - math.log(self.median)) / _Z99

    def _draw(self, rng: random.Random, median: int) -> int:
        value = rng.lognormvariate(math.log(median), self.sigma)
        # `int()` truncates, so the floor is applied after rather than relying on rounding.
        return max(1, int(value))

    def submit_ms(self, rng: random.Random) -> int:
        return self._draw(rng, self.median)

    def cancel_ms(self, rng: random.Random) -> int:
        return self._draw(rng, self.cancel_median)

    def to_json(self) -> dict[str, Any]:
        return {
            "model": "lognormal",
            "median_ms": self.median,
            "p99_ms": self.p99,
            "cancel_median_ms": self.cancel_median,
        }


class EmptyLatencySamples(ValueError):
    """An `empirical` model was asked for with nothing recorded to sample from.

    A `ValueError`, so every caller that already filters for one keeps working, and named so
    the config path can tell "you have no paper history yet" apart from "these samples are
    malformed" without matching on message text.
    """


@dataclass(frozen=True, slots=True)
class EmpiricalLatency:
    """Spec 6.3's `empirical` mode: latencies drawn from your own paper sessions.

    Each draw picks one recorded sample uniformly, with replacement. That keeps the shape of
    the measured tail intact -- including the handful of samples that came from a reconnect
    or a rate-limit pause, which are exactly the ones a fitted distribution rounds off and
    exactly the ones that decide what a strategy reacting to a fast move actually gets.

    **Submit and cancel have separate pools, and merging them is not a simplification.**
    Spec 6.3: *"A cancel issued at T does not protect you from a fill at T + 50 ms if cancel
    latency is 120 ms. This is a real and commonly-ignored source of backtest optimism."* A
    session submits far more orders than it cancels, so a merged pool is a submit pool with a
    few cancels stirred into it, and `cancel_ms` then stops being a measurement of cancels at
    all. Whichever way that pool is biased, it is biased on data from the wrong path -- and
    the *direction that flatters* is the reachable one: a modelled cancel that arrives sooner
    than the real one pulls a resting order before the print that would have filled it, and
    the strategy escapes a fill it would have got. Two pools cost one extra field and keep
    the number that decides that a number somebody measured.

    **Samples are sorted at construction** so the model is a function of the multiset and not
    of the order the samples happened to be recorded in. Two sessions that measured the same
    latencies in a different sequence describe the same network, and spec 12.1 requires them
    to reproduce the same run.
    """

    submit_samples: tuple[int, ...]
    """Measured submit latencies in milliseconds, sorted ascending. Never empty."""
    cancel_samples: tuple[int, ...]
    """Measured cancel latencies in milliseconds, sorted ascending. Never empty."""
    source: str = ""
    """Where the samples came from -- a run id or a session label, for the manifest.

    Carried because a pool of numbers with no provenance cannot be audited: "these are the
    latencies" is not a claim anyone can check, and "these are run 41's latencies" is.
    """

    def __post_init__(self) -> None:
        if not self.submit_samples or not self.cancel_samples:
            raise EmptyLatencySamples(
                f"an empirical latency model needs at least one submit sample and one "
                f"cancel sample; this one has {len(self.submit_samples)} and "
                f"{len(self.cancel_samples)}. There is nothing to sample from, and "
                "defaulting to a distribution would re-price every fill in the run with "
                "latencies nobody measured while leaving the run's identity unchanged "
                "(spec 12.1). Configure 'fixed' or 'lognormal' explicitly until a paper "
                "session has produced history."
            )
        if min(self.submit_samples) < 1 or min(self.cancel_samples) < 1:
            raise ValueError(
                "every empirical latency sample must be at least 1 ms; a zero would put "
                "the order back at the signal instant and fill it at the price that "
                "triggered it. Build the model with EmpiricalLatency.from_samples, which "
                "floors a sub-millisecond measurement rather than discarding it."
            )

    @classmethod
    def from_samples(
        cls,
        submit: Iterable[int],
        cancel: Iterable[int],
        *,
        source: str = "",
    ) -> EmpiricalLatency:
        """Build a model from raw measurements, flooring and sorting them.

        Sub-millisecond measurements floor to 1 ms rather than being dropped, for the reason
        `LognormalLatency` floors its draws: a zero-latency arrival fills at the price that
        triggered the order, which is the optimism this whole module exists to prevent.
        Dropping them instead would be worse than flooring -- it would remove the fastest
        observations from the pool and bias every remaining draw slow.

        A negative sample is refused rather than clamped, because it is not a fast
        measurement: it is a clock that went backwards, and the rest of that session's
        samples are then suspect too.
        """
        return cls(
            submit_samples=_clean_samples(submit, "submit"),
            cancel_samples=_clean_samples(cancel, "cancel"),
            source=source,
        )

    @property
    def name(self) -> str:
        return "empirical"

    @property
    def is_zero(self) -> bool:
        """Always `False` -- every sample is at least 1 ms, so no draw can reach zero."""
        return False

    def _draw(self, rng: random.Random, pool: tuple[int, ...]) -> int:
        # Indexed with `randrange` rather than `rng.choice`, which is the same draw today and
        # has not always been: `choice` used to scale a float into the range. A stored run
        # whose fills were priced by this pool has to replay identically on a later
        # interpreter (spec 12.1), so the draw is spelled as the documented uniform integer.
        return pool[rng.randrange(len(pool))]

    def submit_ms(self, rng: random.Random) -> int:
        return self._draw(rng, self.submit_samples)

    def cancel_ms(self, rng: random.Random) -> int:
        return self._draw(rng, self.cancel_samples)

    def to_json(self) -> dict[str, Any]:
        """The manifest form -- every sample, not a summary of them.

        A median and a p99 would be a third of the size and would describe a *different*
        model: rebuilding from them gives `lognormal`, whose whole difference from this one
        is the tail it does not keep. Spec 12.1's contract is that a stored run replays, so
        the pool that priced the fills travels with it.
        """
        return {
            "model": "empirical",
            "submit_ms": list(self.submit_samples),
            "cancel_ms": list(self.cancel_samples),
            "source": self.source,
        }


def _clean_samples(samples: Iterable[int], label: str) -> tuple[int, ...]:
    """Coerce, floor and sort one pool. See `EmpiricalLatency.from_samples`."""
    cleaned: list[int] = []
    for sample in samples:
        value = int(sample)
        if value < 0:
            raise ValueError(
                f"{label} latency sample {value} is negative, which is a clock that ran "
                "backwards rather than a fast order. Drop that session's measurements or "
                "fix the clock that produced them; do not clamp them, because the rest of "
                "the pool is suspect too."
            )
        cleaned.append(max(1, value))
    return tuple(sorted(cleaned))


def latency_from_json(payload: Mapping[str, Any] | None) -> LatencyModel:
    """Rebuild a model from a run manifest.

    Refuses an unknown name rather than falling back to the default. A manifest naming a
    model this build cannot construct describes a run whose fills were priced by rules that
    are not present, and silently substituting the default would produce a *different* run
    wearing the original's identity -- which is precisely the failure spec 12.1's
    reproducibility contract exists to make impossible.
    """
    if payload is None:
        return FixedLatency()
    model = payload.get("model")
    if model == "fixed":
        return FixedLatency(
            submit=int(payload.get("submit_ms", DEFAULT_SUBMIT_MS)),
            cancel=int(payload.get("cancel_ms", DEFAULT_CANCEL_MS)),
        )
    if model == "lognormal":
        return LognormalLatency(
            median=int(payload.get("median_ms", DEFAULT_SUBMIT_MS)),
            p99=int(payload.get("p99_ms", DEFAULT_P99_MS)),
            cancel_median=int(payload.get("cancel_median_ms", DEFAULT_CANCEL_MS)),
        )
    if model == "empirical":
        # Rebuilt through `from_samples` rather than the constructor so a manifest is held to
        # exactly the rules a fresh session's measurements are: the pool it names is the pool
        # the run is priced from, and a stored zero is floored here as it was there.
        return EmpiricalLatency.from_samples(
            payload.get("submit_ms") or (),
            payload.get("cancel_ms") or (),
            source=str(payload.get("source", "")),
        )
    raise ValueError(
        f"unknown latency model {model!r}; this build knows 'fixed', 'lognormal' and "
        "'empirical'. Substituting a default would re-price every fill in the run while "
        "leaving its identity unchanged (spec 12.1)."
    )

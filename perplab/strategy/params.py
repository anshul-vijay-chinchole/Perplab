"""Declared parameters and data requirements (spec 5.1).

A strategy declares `params` and `requires` as plain class-level dicts. Those two dicts do
three jobs, and the reason they are validated this strictly is that all three fail
expensively when they are wrong:

1. `params` drives the auto-generated config form in the UI. A malformed bound produces a
   form that accepts a value the strategy cannot run with.
2. `requires` lets the engine check data coverage *before* a run starts, instead of
   failing sixty per cent of the way through a backtest (spec 5.1).
3. `requires["history"]` is cross-checked against the warm-up the indicator set actually
   needs (spec 5.4 rule 4), so a 200-period EMA cannot quietly produce signals from
   twelve bars.

**Decimal params must be declared as strings.** `{"default": 0.01}` is already not 0.01 by
the time Python has finished reading the line, and that error then multiplies a notional.
The float is refused here, where the literal still has a line number, rather than absorbed
and discovered later as a reconciliation mismatch. This is the same float-guard the
bracket parser applies to exchange payloads (`core.margin.brackets_from_payload`), for the
same reason.
"""

from __future__ import annotations

import keyword
from dataclasses import dataclass
from typing import Any, Mapping

from perplab.core.money import Money, money_to_str, parse_money

__all__ = [
    "PARAM_TYPES",
    "TIMEFRAMES",
    "TIMEFRAME_MS",
    "KNOWN_DATASETS",
    "ParamError",
    "ParamSpec",
    "ParamSet",
    "Requirements",
    "parse_param_specs",
    "parse_requirements",
    "bind_params",
]

PARAM_TYPES = ("int", "float", "decimal", "bool", "str", "choice")
"""The declarable param types.

Deliberately small. Every type here maps to exactly one form control and one unambiguous
JSON representation, which is what keeps the auto-generated form honest. A `"list"` or
`"dict"` type would need a bespoke editor and would smuggle arbitrary structure into the
run manifest, where it has to be hashed and compared.
"""

TIMEFRAME_MS: dict[str, int] = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
    "3d": 259_200_000,
    "1w": 604_800_000,
}
"""Bar intervals, each a fixed number of milliseconds.

Binance also publishes `1M` (calendar month). It is excluded because it is *not* a fixed
duration, and every piece of bar arithmetic in this codebase -- boundary alignment,
warm-up counting, annualisation of realised volatility -- assumes it can multiply. A
monthly bar would silently make each of those wrong by up to 10%. If monthly bars are ever
wanted they need a calendar-aware path, not an entry in this table.
"""

TIMEFRAMES = tuple(TIMEFRAME_MS)

KNOWN_DATASETS = (
    "klines",
    "aggTrades",
    "bookTicker",
    "depth20",
    "markPrice",
    "funding",
    "metrics",
    "liquidations",
    # Phase 11 macro context. Declaring one is what makes the engine load it and what
    # makes `ctx.macro()` answer instead of raising -- see `EngineRuntime.macro`.
    "macroGlobal",
    "macroFx",
)
"""Dataset names a strategy may declare in `requires["datasets"]`.

These match the lake's partition roots (spec 4.3) so coverage checking is a lookup rather
than a translation table. `liquidations` is listed because it is a legitimate thing to ask
for, not because it is available -- see `UNAVAILABLE_DATASETS` in the collector. Asking for
it produces a warning at validation time, which is a great deal better than a run that
completes having silently never fired `on_market_liquidation`.

The two macro datasets are **supplementary**: unlike the market datasets, a run whose lake
holds none of their rows still completes, carrying a `MACRO_MISSING` flag rather than
failing (Phase 11). They are a signal input, not a dependency.
"""

UNAVAILABLE_ON_THIS_DEPLOYMENT = ("liquidations",)
"""Datasets with no source, warned about rather than rejected.

Rejecting would be wrong: the strategy is not malformed, the world is. A backtest over a
range predating the withdrawal could still be legitimate if the archive is ever recovered,
so the strategy stays saveable and the warning travels with it.
"""


class ParamError(ValueError):
    """A malformed `params` or `requires` declaration.

    Carries `key` so the validator can walk back into the AST and point the Monaco gutter
    at the offending dict entry instead of at the class statement (spec 5.5).
    """

    def __init__(self, message: str, *, key: str | None = None) -> None:
        super().__init__(message)
        self.key = key


@dataclass(frozen=True, slots=True)
class ParamSpec:
    """One declared parameter.

    `default`, `minimum` and `maximum` are stored already coerced to the declared type, so
    everything downstream -- form generation, override coercion, bound checking -- works
    on values rather than on the literals the author happened to write.
    """

    name: str
    type: str
    default: Any
    minimum: Any | None = None
    maximum: Any | None = None
    choices: tuple[str, ...] | None = None
    label: str | None = None
    help: str | None = None

    def to_json(self) -> dict[str, Any]:
        """Serialise for the config form and the run manifest.

        Decimals go out as strings. Sending `Decimal("0.01")` through `json.dumps` as a
        float would reintroduce, at the API boundary, exactly the error the string-only
        declaration rule exists to prevent -- and it would do it invisibly, because the
        form would still read "0.01".
        """
        payload: dict[str, Any] = {"name": self.name, "type": self.type}
        payload["default"] = _jsonable(self.default)
        if self.minimum is not None:
            payload["min"] = _jsonable(self.minimum)
        if self.maximum is not None:
            payload["max"] = _jsonable(self.maximum)
        if self.choices is not None:
            payload["choices"] = list(self.choices)
        if self.label is not None:
            payload["label"] = self.label
        if self.help is not None:
            payload["help"] = self.help
        return payload


def _jsonable(value: Any) -> Any:
    return money_to_str(value) if isinstance(value, Money) else value


class ParamSet:
    """Bound parameter values, reachable as `self.p.fast` inside a strategy.

    Attribute access rather than `self.params["fast"]` for one reason that matters more
    than ergonomics: a typo becomes an `AttributeError` naming the parameter and listing
    the ones that exist, at the first line that uses it. A dict lookup gives `KeyError:
    'fast'` and a `.get()` gives `None`, which then propagates into the arithmetic and
    produces a strategy that runs and is wrong.
    """

    __slots__ = ("_values",)

    def __init__(self, values: Mapping[str, Any]) -> None:
        object.__setattr__(self, "_values", dict(values))

    def __getattr__(self, name: str) -> Any:
        # `__getattr__` runs only when normal lookup fails, and normal lookup for
        # `_values` fails on a half-constructed instance -- during `copy`/`pickle`, which
        # bypass `__init__`. Without this guard that lookup would re-enter here and
        # recurse until the stack dies, turning a missing attribute into a crash with no
        # relation to the cause.
        if name == "_values":
            raise AttributeError(name)
        try:
            return self._values[name]
        except KeyError:
            known = ", ".join(sorted(self._values)) or "none declared"
            raise AttributeError(
                f"no parameter {name!r}; declared params are: {known}"
            ) from None

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError(
            "parameters are immutable during a run: assigning to self.p."
            f"{name} would make the run's recorded param set a lie (spec 12.1)"
        )

    def __contains__(self, name: str) -> bool:
        return name in self._values

    def __iter__(self):
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def as_dict(self) -> dict[str, Any]:
        return dict(self._values)

    def to_json(self) -> dict[str, Any]:
        return {name: _jsonable(value) for name, value in self._values.items()}

    def __repr__(self) -> str:
        inner = ", ".join(f"{k}={v!r}" for k, v in sorted(self._values.items()))
        return f"ParamSet({inner})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, ParamSet):
            return self._values == other._values
        return NotImplemented

    def __hash__(self) -> int:  # pragma: no cover - ParamSet is not a key anywhere
        raise TypeError("ParamSet is mutable-valued and not hashable")


@dataclass(frozen=True, slots=True)
class Requirements:
    """Parsed `requires` (spec 5.1)."""

    symbols: tuple[str, ...]
    timeframe: str
    history: int
    datasets: tuple[str, ...]

    @property
    def timeframe_ms(self) -> int:
        return TIMEFRAME_MS[self.timeframe]

    def to_json(self) -> dict[str, Any]:
        return {
            "symbols": list(self.symbols),
            "timeframe": self.timeframe,
            "history": self.history,
            "datasets": list(self.datasets),
        }


# --------------------------------------------------------------------------- params


def _reject_float(value: Any, *, name: str, field: str, type_name: str) -> None:
    if isinstance(value, float):
        raise ParamError(
            f"param {name!r}: {field} must be a string for a {type_name} param, not the "
            f"float {value!r}. Written as a Python float it is already a different number "
            f"({value!r} is stored as {value.hex()}), and that error would then multiply "
            f'a notional. Write it as "{value!r}".',
            key=name,
        )


def _finite_float(value: float, *, name: str, field: str) -> float:
    """Refuse NaN and the infinities.

    NaN is the dangerous one, and it is dangerous precisely because it looks handled: it
    passes every bound check, since `nan < min` and `nan > max` are **both** False. So a
    param declared with `min` and `max` accepted it and it went on to multiply a notional,
    silently turning every downstream number into NaN. The `decimal` branch already refused
    it (`parse_money` rejects non-finite values); this is the sibling path that did not.
    """
    if value != value:
        raise ParamError(
            f"param {name!r}: {field} is NaN. It would pass both bound checks — `nan < min`"
            " and `nan > max` are both false — and then propagate through every number it"
            " touches.",
            key=name,
        )
    if value in (float("inf"), float("-inf")):
        raise ParamError(f"param {name!r}: {field} is infinite", key=name)
    return value


def _coerce_declared(value: Any, *, name: str, field: str, type_name: str) -> Any:
    """Coerce one declared literal (`default`/`min`/`max`) to the param's type."""
    if type_name == "int":
        # bool is an int subclass, so `True` would sail through `isinstance(v, int)` and
        # become the integer 1 -- a param whose form control is a number box and whose
        # declared default renders as "true".
        if isinstance(value, bool) or not isinstance(value, int):
            raise ParamError(
                f"param {name!r}: {field} must be an int, got {type(value).__name__} "
                f"{value!r}",
                key=name,
            )
        return value

    if type_name == "float":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ParamError(
                f"param {name!r}: {field} must be a number, got "
                f"{type(value).__name__} {value!r}",
                key=name,
            )
        return _finite_float(float(value), name=name, field=field)

    if type_name == "decimal":
        _reject_float(value, name=name, field=field, type_name="decimal")
        if isinstance(value, bool) or not isinstance(value, (int, str)):
            raise ParamError(
                f"param {name!r}: {field} must be a decimal string, got "
                f"{type(value).__name__} {value!r}",
                key=name,
            )
        try:
            return parse_money(str(value))
        except ValueError as exc:
            raise ParamError(f"param {name!r}: {field} {value!r} — {exc}", key=name) from None

    if type_name == "bool":
        if not isinstance(value, bool):
            raise ParamError(
                f"param {name!r}: {field} must be true or false, got "
                f"{type(value).__name__} {value!r}",
                key=name,
            )
        return value

    if type_name in ("str", "choice"):
        if not isinstance(value, str):
            raise ParamError(
                f"param {name!r}: {field} must be a string, got "
                f"{type(value).__name__} {value!r}",
                key=name,
            )
        return value

    raise AssertionError(f"unhandled param type {type_name!r}")  # pragma: no cover


def _parse_one_spec(name: str, raw: Any) -> ParamSpec:
    if not isinstance(raw, Mapping):
        raise ParamError(
            f"param {name!r}: declaration must be a dict like "
            f'{{"type": "int", "default": 12}}, got {type(raw).__name__}',
            key=name,
        )

    unknown = set(raw) - {"type", "default", "min", "max", "choices", "label", "help"}
    if unknown:
        raise ParamError(
            f"param {name!r}: unknown key(s) {sorted(unknown)}. Allowed: type, default, "
            "min, max, choices, label, help",
            key=name,
        )

    type_name = raw.get("type")
    if type_name not in PARAM_TYPES:
        raise ParamError(
            f"param {name!r}: type must be one of {list(PARAM_TYPES)}, got {type_name!r}",
            key=name,
        )

    if "default" not in raw:
        raise ParamError(
            f"param {name!r}: a default is required — it is what the config form opens "
            "with and what a run records when the param is not overridden",
            key=name,
        )

    choices: tuple[str, ...] | None = None
    if type_name == "choice":
        raw_choices = raw.get("choices")
        if not isinstance(raw_choices, (list, tuple)) or not raw_choices:
            raise ParamError(
                f"param {name!r}: a choice param needs a non-empty 'choices' list",
                key=name,
            )
        if not all(isinstance(c, str) for c in raw_choices):
            raise ParamError(
                f"param {name!r}: every entry in 'choices' must be a string", key=name
            )
        if len(set(raw_choices)) != len(raw_choices):
            raise ParamError(f"param {name!r}: 'choices' contains duplicates", key=name)
        choices = tuple(raw_choices)
    elif "choices" in raw:
        raise ParamError(
            f"param {name!r}: 'choices' only applies to type 'choice', not {type_name!r}",
            key=name,
        )

    default = _coerce_declared(raw["default"], name=name, field="default", type_name=type_name)

    minimum = maximum = None
    bounded = type_name in ("int", "float", "decimal")
    for field, key in (("min", "minimum"), ("max", "maximum")):
        if field not in raw:
            continue
        if not bounded:
            raise ParamError(
                f"param {name!r}: '{field}' has no meaning for a {type_name} param",
                key=name,
            )
        value = _coerce_declared(raw[field], name=name, field=field, type_name=type_name)
        if key == "minimum":
            minimum = value
        else:
            maximum = value

    if minimum is not None and maximum is not None and minimum > maximum:
        raise ParamError(
            f"param {name!r}: min {minimum} is above max {maximum}", key=name
        )
    if minimum is not None and default < minimum:
        raise ParamError(
            f"param {name!r}: default {default} is below min {minimum}", key=name
        )
    if maximum is not None and default > maximum:
        raise ParamError(
            f"param {name!r}: default {default} is above max {maximum}", key=name
        )
    if choices is not None and default not in choices:
        raise ParamError(
            f"param {name!r}: default {default!r} is not among choices {list(choices)}",
            key=name,
        )

    for field in ("label", "help"):
        value = raw.get(field)
        if value is not None and not isinstance(value, str):
            raise ParamError(f"param {name!r}: '{field}' must be a string", key=name)

    return ParamSpec(
        name=name,
        type=type_name,
        default=default,
        minimum=minimum,
        maximum=maximum,
        choices=choices,
        label=raw.get("label"),
        help=raw.get("help"),
    )


def parse_param_specs(raw: Any) -> tuple[ParamSpec, ...]:
    """Validate a strategy's `params` declaration.

    Order is preserved from the declaration, because that is the order the form renders in
    and the author grouped them deliberately.
    """
    if raw is None:
        return ()
    if not isinstance(raw, Mapping):
        raise ParamError(
            f"`params` must be a dict of name -> declaration, got {type(raw).__name__}"
        )

    specs: list[ParamSpec] = []
    for name, decl in raw.items():
        # `isidentifier()` alone is not enough: `"class"`, `"lambda"` and `"None"` all
        # return True, and `self.p.class` is a SyntaxError. The check is here to guarantee
        # the name is reachable, so it has to exclude the words that are not.
        if not isinstance(name, str) or not name.isidentifier() or keyword.iskeyword(name):
            raise ParamError(
                f"param name {name!r} must be a valid Python identifier and not a keyword "
                "— it is reached as self.p.<name>"
            )
        if name.startswith("_"):
            raise ParamError(
                f"param name {name!r} must not start with an underscore", key=name
            )
        specs.append(_parse_one_spec(name, decl))
    return tuple(specs)


def _coerce_override(spec: ParamSpec, value: Any) -> Any:
    """Coerce a value arriving from JSON (the config form, or a saved run) to the spec.

    More permissive than `_coerce_declared` in exactly one direction: JSON has no decimal
    type, so a decimal param arrives as a string and an int param may arrive as `12.0`
    from a JavaScript number. Neither of those is an authoring mistake. A float carrying an
    actual fraction still is.
    """
    name = spec.name
    if spec.type == "int":
        if isinstance(value, bool):
            raise ParamError(f"param {name!r}: expected an int, got {value!r}", key=name)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            if value.is_integer():
                return int(value)
            raise ParamError(
                f"param {name!r}: expected a whole number, got {value!r}", key=name
            )
        if isinstance(value, str):
            try:
                return int(value.strip(), 10)
            except ValueError:
                raise ParamError(
                    f"param {name!r}: {value!r} is not an integer", key=name
                ) from None
        raise ParamError(
            f"param {name!r}: expected an int, got {type(value).__name__}", key=name
        )

    if spec.type == "float":
        if isinstance(value, bool):
            raise ParamError(f"param {name!r}: expected a number, got {value!r}", key=name)
        if isinstance(value, (int, float)):
            return _finite_float(float(value), name=name, field="value")
        if isinstance(value, str):
            try:
                parsed = float(value.strip())
            except ValueError:
                raise ParamError(
                    f"param {name!r}: {value!r} is not a number", key=name
                ) from None
            # `float("nan")` and `float("inf")` both parse, so the string path needs the
            # same guard the numeric one does.
            return _finite_float(parsed, name=name, field="value")
        raise ParamError(
            f"param {name!r}: expected a number, got {type(value).__name__}", key=name
        )

    if spec.type == "decimal":
        _reject_float(value, name=name, field="value", type_name="decimal")
        if isinstance(value, bool) or not isinstance(value, (int, str)):
            raise ParamError(
                f"param {name!r}: expected a decimal string, got "
                f"{type(value).__name__}",
                key=name,
            )
        try:
            return parse_money(str(value))
        except ValueError as exc:
            raise ParamError(f"param {name!r}: {value!r} — {exc}", key=name) from None

    if spec.type == "bool":
        if not isinstance(value, bool):
            raise ParamError(
                f"param {name!r}: expected true or false, got {value!r}", key=name
            )
        return value

    if not isinstance(value, str):
        raise ParamError(
            f"param {name!r}: expected a string, got {type(value).__name__}", key=name
        )
    if spec.choices is not None and value not in spec.choices:
        raise ParamError(
            f"param {name!r}: {value!r} is not among choices {list(spec.choices)}",
            key=name,
        )
    return value


def bind_params(
    specs: tuple[ParamSpec, ...], overrides: Mapping[str, Any] | None = None
) -> ParamSet:
    """Apply overrides to declared defaults, checking types and bounds.

    Unknown override keys are an error rather than a silent no-op. A run configured with
    `{"fastt": 8}` that quietly used `fast=12` would produce a result attributed to the
    wrong parameters, which is worse than a failed run.
    """
    values: dict[str, Any] = {spec.name: spec.default for spec in specs}
    if not overrides:
        return ParamSet(values)

    by_name = {spec.name: spec for spec in specs}
    unknown = set(overrides) - set(by_name)
    if unknown:
        known = ", ".join(sorted(by_name)) or "none declared"
        raise ParamError(
            f"unknown param(s) {sorted(unknown)}; declared params are: {known}"
        )

    for name, raw in overrides.items():
        spec = by_name[name]
        value = _coerce_override(spec, raw)
        if spec.minimum is not None and value < spec.minimum:
            raise ParamError(
                f"param {name!r}: {value} is below min {spec.minimum}", key=name
            )
        if spec.maximum is not None and value > spec.maximum:
            raise ParamError(
                f"param {name!r}: {value} is above max {spec.maximum}", key=name
            )
        values[name] = value
    return ParamSet(values)


# ----------------------------------------------------------------------- requires


def parse_requirements(raw: Any) -> Requirements:
    """Validate a strategy's `requires` declaration (spec 5.1)."""
    if not isinstance(raw, Mapping):
        raise ParamError(
            "`requires` must be a dict declaring symbols, timeframe, history and "
            f"datasets, got {type(raw).__name__}"
        )

    unknown = set(raw) - {"symbols", "timeframe", "history", "datasets"}
    if unknown:
        raise ParamError(
            f"`requires` has unknown key(s) {sorted(unknown)}. Allowed: symbols, "
            "timeframe, history, datasets"
        )

    symbols_raw = raw.get("symbols")
    if isinstance(symbols_raw, str):
        raise ParamError(
            '`requires["symbols"]` must be a list, not a string — '
            f'write ["{symbols_raw}"]',
            key="symbols",
        )
    if not isinstance(symbols_raw, (list, tuple)) or not symbols_raw:
        raise ParamError(
            '`requires["symbols"]` must be a non-empty list of symbols', key="symbols"
        )
    symbols: list[str] = []
    for symbol in symbols_raw:
        if not isinstance(symbol, str) or not symbol.strip():
            raise ParamError(
                f'`requires["symbols"]` contains {symbol!r}, which is not a symbol',
                key="symbols",
            )
        upper = symbol.strip().upper()
        if upper in symbols:
            raise ParamError(
                f'`requires["symbols"]` lists {upper} twice', key="symbols"
            )
        symbols.append(upper)

    timeframe = raw.get("timeframe")
    if timeframe not in TIMEFRAME_MS:
        raise ParamError(
            f'`requires["timeframe"]` must be one of {list(TIMEFRAMES)}, got '
            f"{timeframe!r}",
            key="timeframe",
        )

    history = raw.get("history", 0)
    if isinstance(history, bool) or not isinstance(history, int):
        raise ParamError(
            '`requires["history"]` must be an int number of warm-up bars, got '
            f"{type(history).__name__}",
            key="history",
        )
    if history < 0:
        raise ParamError(
            '`requires["history"]` cannot be negative', key="history"
        )

    datasets_raw = raw.get("datasets", ["klines"])
    if isinstance(datasets_raw, str):
        raise ParamError(
            '`requires["datasets"]` must be a list, not a string — '
            f'write ["{datasets_raw}"]',
            key="datasets",
        )
    if not isinstance(datasets_raw, (list, tuple)) or not datasets_raw:
        raise ParamError(
            '`requires["datasets"]` must be a non-empty list', key="datasets"
        )
    datasets: list[str] = []
    for dataset in datasets_raw:
        if dataset not in KNOWN_DATASETS:
            raise ParamError(
                f'`requires["datasets"]` contains {dataset!r}; known datasets are '
                f"{list(KNOWN_DATASETS)}",
                key="datasets",
            )
        if dataset in datasets:
            raise ParamError(
                f'`requires["datasets"]` lists {dataset!r} twice', key="datasets"
            )
        datasets.append(dataset)

    return Requirements(
        symbols=tuple(symbols),
        timeframe=timeframe,
        history=history,
        datasets=tuple(datasets),
    )

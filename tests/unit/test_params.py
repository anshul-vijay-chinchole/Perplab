"""Declared params and requirements (spec 5.1).

The float-rejection tests are the ones worth reading. A `decimal` param declared as the
Python literal `0.01` is already a different number before any PerpLab code sees it, and
that number then multiplies a notional -- so it is refused where the literal still has a
line number, exactly as `core.margin` refuses a float-parsed bracket payload.
"""

from __future__ import annotations

import pytest

from perplab.core.money import parse_money
from perplab.strategy.params import (
    ParamError,
    ParamSet,
    bind_params,
    parse_param_specs,
    parse_requirements,
)

GOOD_REQUIRES = {
    "symbols": ["BTCUSDT"],
    "timeframe": "15m",
    "history": 400,
    "datasets": ["klines", "funding"],
}


class TestParamSpecs:
    def test_parses_the_spec_example(self) -> None:
        specs = parse_param_specs(
            {
                "fast": {"type": "int", "default": 12, "min": 2, "max": 200},
                "slow": {"type": "int", "default": 26, "min": 3, "max": 400},
                "risk": {"type": "decimal", "default": "0.01", "min": "0.001", "max": "0.05"},
            }
        )
        assert [s.name for s in specs] == ["fast", "slow", "risk"]
        assert specs[0].default == 12
        assert specs[2].default == parse_money("0.01")

    def test_declaration_order_is_preserved(self) -> None:
        """It is the order the config form renders in, and the author grouped them."""
        specs = parse_param_specs(
            {name: {"type": "int", "default": 1} for name in ("zeta", "alpha", "mu")}
        )
        assert [s.name for s in specs] == ["zeta", "alpha", "mu"]

    def test_a_float_decimal_default_is_refused_with_its_own_value(self) -> None:
        with pytest.raises(ParamError) as caught:
            parse_param_specs({"risk": {"type": "decimal", "default": 0.01}})
        message = str(caught.value)
        assert "float" in message
        assert '"0.01"' in message  # tells the author exactly what to write instead

    def test_a_decimal_declared_as_a_string_keeps_full_precision(self) -> None:
        specs = parse_param_specs(
            {"r": {"type": "decimal", "default": "0.000000000000001"}}
        )
        assert specs[0].default == parse_money("1e-15")

    def test_a_tiny_decimal_renders_as_the_author_wrote_it(self) -> None:
        """`str(Decimal)` flips to scientific notation below 1e-7, so a tick of
        `"0.00000001"` on a 1000-prefixed pair would come back as `1E-8` in the config
        form and in the export manifest. Exact either way, unreadable one way."""
        specs = parse_param_specs({"r": {"type": "decimal", "default": "0.00000001"}})
        assert specs[0].to_json()["default"] == "0.00000001"

    def test_a_bool_is_not_an_int(self) -> None:
        """`True` passes `isinstance(v, int)` and would silently become 1 in a number box."""
        with pytest.raises(ParamError, match="must be an int"):
            parse_param_specs({"n": {"type": "int", "default": True}})

    def test_a_default_outside_its_bounds_is_refused(self) -> None:
        with pytest.raises(ParamError, match="below min"):
            parse_param_specs({"n": {"type": "int", "default": 1, "min": 2}})
        with pytest.raises(ParamError, match="above max"):
            parse_param_specs({"n": {"type": "int", "default": 9, "max": 5}})

    def test_inverted_bounds_are_refused(self) -> None:
        with pytest.raises(ParamError, match="above max"):
            parse_param_specs({"n": {"type": "int", "default": 3, "min": 5, "max": 2}})

    def test_a_missing_default_is_refused(self) -> None:
        with pytest.raises(ParamError, match="default is required"):
            parse_param_specs({"n": {"type": "int", "min": 1}})

    def test_an_unknown_type_names_the_valid_ones(self) -> None:
        with pytest.raises(ParamError, match="must be one of"):
            parse_param_specs({"n": {"type": "integer", "default": 1}})

    def test_an_unknown_key_is_refused(self) -> None:
        """A typo like `defualt` would otherwise pair with a missing-default error that
        does not mention the typo."""
        with pytest.raises(ParamError, match="unknown key"):
            parse_param_specs({"n": {"type": "int", "default": 1, "step": 2}})

    def test_choice_requires_choices_and_a_member_default(self) -> None:
        with pytest.raises(ParamError, match="non-empty 'choices'"):
            parse_param_specs({"m": {"type": "choice", "default": "a"}})
        with pytest.raises(ParamError, match="not among choices"):
            parse_param_specs(
                {"m": {"type": "choice", "default": "z", "choices": ["a", "b"]}}
            )
        specs = parse_param_specs(
            {"m": {"type": "choice", "default": "b", "choices": ["a", "b"]}}
        )
        assert specs[0].choices == ("a", "b")

    def test_choices_on_a_non_choice_type_is_refused(self) -> None:
        with pytest.raises(ParamError, match="only applies to type 'choice'"):
            parse_param_specs({"n": {"type": "int", "default": 1, "choices": ["a"]}})

    def test_bounds_on_a_bool_are_refused(self) -> None:
        with pytest.raises(ParamError, match="no meaning"):
            parse_param_specs({"b": {"type": "bool", "default": True, "min": 0}})

    def test_a_name_that_is_not_an_identifier_is_refused(self) -> None:
        """It is reached as `self.p.<name>`."""
        with pytest.raises(ParamError, match="valid Python identifier"):
            parse_param_specs({"fast period": {"type": "int", "default": 2}})

    def test_json_renders_decimals_as_strings(self) -> None:
        """Sending `Decimal("0.01")` through `json.dumps` as a float would reintroduce at
        the API boundary the exact error the string-only rule prevents."""
        specs = parse_param_specs({"r": {"type": "decimal", "default": "0.01", "min": "0"}})
        payload = specs[0].to_json()
        assert payload["default"] == "0.01"
        assert payload["min"] == "0"


class TestBinding:
    def _specs(self):
        return parse_param_specs(
            {
                "fast": {"type": "int", "default": 12, "min": 2, "max": 200},
                "risk": {"type": "decimal", "default": "0.01", "min": "0.001", "max": "0.05"},
                "long_only": {"type": "bool", "default": True},
            }
        )

    def test_defaults_apply_when_nothing_is_overridden(self) -> None:
        bound = bind_params(self._specs())
        assert bound.fast == 12
        assert bound.long_only is True

    def test_overrides_are_coerced_and_bound_checked(self) -> None:
        bound = bind_params(self._specs(), {"fast": "30", "risk": "0.02"})
        assert bound.fast == 30
        assert bound.risk == parse_money("0.02")

    def test_an_override_outside_bounds_is_refused(self) -> None:
        with pytest.raises(ParamError, match="above max"):
            bind_params(self._specs(), {"fast": 500})

    def test_a_javascript_whole_float_is_accepted_for_an_int(self) -> None:
        """JSON has one number type; a form sends `30` as `30.0`. That is not an authoring
        mistake, so it is coerced -- while `30.5` still is one."""
        assert bind_params(self._specs(), {"fast": 30.0}).fast == 30
        with pytest.raises(ParamError, match="whole number"):
            bind_params(self._specs(), {"fast": 30.5})

    def test_a_float_override_for_a_decimal_is_still_refused(self) -> None:
        with pytest.raises(ParamError, match="float"):
            bind_params(self._specs(), {"risk": 0.02})

    def test_an_unknown_override_is_an_error_not_a_no_op(self) -> None:
        """A run configured with `{"fastt": 8}` that quietly used `fast=12` would attribute
        its result to parameters it did not use."""
        with pytest.raises(ParamError, match="unknown param"):
            bind_params(self._specs(), {"fastt": 8})

    def test_a_typo_lists_the_declared_names(self) -> None:
        bound = bind_params(self._specs())
        with pytest.raises(AttributeError) as caught:
            _ = bound.fst
        assert "fast" in str(caught.value)

    def test_params_cannot_be_reassigned_during_a_run(self) -> None:
        """A mutated param set would make the run's recorded configuration a lie."""
        bound = bind_params(self._specs())
        with pytest.raises(AttributeError, match="immutable"):
            bound.fast = 99  # type: ignore[misc]

    def test_paramset_survives_a_missing_internal_attribute(self) -> None:
        """`__getattr__` must not recurse when `_values` is absent, which is the state
        `copy`/`pickle` leave a half-built instance in."""
        empty = ParamSet.__new__(ParamSet)
        with pytest.raises(AttributeError):
            _ = empty.anything


class TestRequirements:
    def test_parses_and_normalises(self) -> None:
        requires = parse_requirements({**GOOD_REQUIRES, "symbols": ["btcusdt"]})
        assert requires.symbols == ("BTCUSDT",)
        assert requires.timeframe_ms == 900_000

    def test_a_string_symbol_list_gets_a_targeted_message(self) -> None:
        """`"symbols": "BTCUSDT"` iterates as characters and would otherwise become seven
        one-letter symbols."""
        with pytest.raises(ParamError, match=r'write \["BTCUSDT"\]'):
            parse_requirements({**GOOD_REQUIRES, "symbols": "BTCUSDT"})

    def test_a_monthly_timeframe_is_refused(self) -> None:
        """`1M` is not a fixed duration, and every piece of bar arithmetic here multiplies
        -- boundary alignment, warm-up counting, volatility annualisation."""
        with pytest.raises(ParamError, match="must be one of"):
            parse_requirements({**GOOD_REQUIRES, "timeframe": "1M"})

    def test_an_unknown_dataset_lists_the_known_ones(self) -> None:
        with pytest.raises(ParamError, match="known datasets are"):
            parse_requirements({**GOOD_REQUIRES, "datasets": ["orderflow"]})

    def test_negative_history_is_refused(self) -> None:
        with pytest.raises(ParamError, match="cannot be negative"):
            parse_requirements({**GOOD_REQUIRES, "history": -1})

    def test_duplicate_symbols_are_refused(self) -> None:
        with pytest.raises(ParamError, match="twice"):
            parse_requirements({**GOOD_REQUIRES, "symbols": ["BTCUSDT", "btcusdt"]})

    def test_an_unknown_requires_key_is_refused(self) -> None:
        with pytest.raises(ParamError, match="unknown key"):
            parse_requirements({**GOOD_REQUIRES, "warmup": 10})

    def test_datasets_default_to_klines(self) -> None:
        requires = parse_requirements(
            {"symbols": ["BTCUSDT"], "timeframe": "1h", "history": 0}
        )
        assert requires.datasets == ("klines",)

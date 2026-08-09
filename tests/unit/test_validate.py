"""Import-time validation (spec 5.5).

Two groups. `TestStaticRules` runs with `run_sandbox=False` and is fast; every case is a
construct the AST scanner must reject and a reason it must give. `TestFullPipeline` spawns
the real worker processes, so it is slower and covers the parts that only exist once code
has actually run.

The alias tests matter more than they look. `import time as t; t.time()` is not somebody
evading the rule, it is ordinary code — and a scanner that only matched the literal string
`time.time` would pass it while being trusted to catch it, which is worse than having no
scanner at all.
"""

from __future__ import annotations

import pytest

from perplab.strategy.template import EMA_CROSS_EXAMPLE, NEW_STRATEGY_TEMPLATE
from perplab.strategy.validate import validate_code

MINIMAL = '''\
from perplab import Strategy


class S(Strategy):
    requires = {"symbols": ["BTCUSDT"], "timeframe": "1h", "history": 1}

    def on_bar(self, ctx, bar):
        __BODY__
'''


def body(source: str) -> str:
    """Wrap one statement in the smallest valid strategy.

    A literal placeholder rather than `str.format`, because the template is full of the
    `{` and `}` that `requires` needs and escaping them all would make every test in this
    file harder to read than the thing it tests.
    """
    return MINIMAL.replace("__BODY__", source)


def codes(code: str, *, sandbox: bool = False) -> list[str]:
    result = validate_code(code, filename="s.py", run_sandbox=sandbox)
    return [d.code for d in result.diagnostics if d.severity == "error"]


class TestStaticRules:
    def test_a_syntax_error_is_a_line_and_column(self) -> None:
        result = validate_code("def broken(:\n    pass", run_sandbox=False)
        assert not result.ok
        (diagnostic,) = result.diagnostics
        assert diagnostic.stage == "parse"
        assert diagnostic.line == 1
        assert diagnostic.column > 1

    @pytest.mark.parametrize(
        "source",
        [
            "import time\n" + body("x = time.time()"),
            "import time as t\n" + body("x = t.time()"),
            "from time import time\n" + body("x = time()"),
            "from time import time as now\n" + body("x = now()"),
            "import datetime\n" + body("x = datetime.datetime.now()"),
            "from datetime import datetime\n" + body("x = datetime.now()"),
            "from datetime import datetime as dt\n" + body("x = dt.utcnow()"),
            "import datetime\n" + body("x = datetime.date.today()"),
        ],
        ids=[
            "module",
            "module-alias",
            "from-import",
            "from-import-alias",
            "datetime-module",
            "datetime-class",
            "datetime-alias",
            "date-today",
        ],
    )
    def test_every_way_of_reading_the_wall_clock_is_caught(self, source: str) -> None:
        assert "wall-clock" in codes(source)

    def test_the_wall_clock_message_explains_why(self) -> None:
        result = validate_code("import time\n" + body("x = time.time()"), run_sandbox=False)
        message = result.diagnostics[0].message
        assert "ctx.now" in message
        assert "look-ahead" in message

    @pytest.mark.parametrize(
        "source",
        [
            "import random\n" + body("pass"),
            "from random import gauss\n" + body("x = gauss(0, 1)"),
            "import numpy as np\n" + body("x = np.random.rand()"),
            "import secrets\n" + body("pass"),
        ],
        ids=["module", "from-import", "numpy", "secrets"],
    )
    def test_unseeded_randomness_is_caught_and_points_at_ctx_rng(self, source: str) -> None:
        result = validate_code(source, run_sandbox=False)
        assert any(d.code == "unseeded-random" for d in result.diagnostics)
        assert any("ctx.rng" in d.message for d in result.diagnostics)

    def test_a_rejected_name_is_flagged_at_the_import_even_if_never_used(self) -> None:
        """The diagnostic has to land on the line the author can delete.

        Every other test here imports *and calls*, which the name-binding path catches on
        its own — so the check on the import statement itself was unpinned. It matters:
        `from time import time` cannot be used for anything legitimate, so the import is
        already the mistake, and pointing at a call site that does not exist yet is no
        help at all.
        """
        result = validate_code(
            "from time import time\n" + body("pass"), run_sandbox=False
        )
        wall_clock = [d for d in result.diagnostics if d.code == "wall-clock"]
        assert wall_clock and wall_clock[0].line == 1

    def test_ctx_rng_itself_is_not_flagged(self) -> None:
        """The rule points at `ctx.rng`, so flagging `ctx.rng.random()` would be telling
        the author to use the thing it just rejected."""
        assert codes(body("x = ctx.rng.random()")) == []

    def test_a_local_named_random_is_not_mistaken_for_the_module(self) -> None:
        """`random = ctx.rng` then `random.random()` is code doing exactly what the rule
        asks. Flagging it for importing a module it never imported is a false positive on
        the compliant case."""
        assert codes(body("random = ctx.rng\n        x = random.random()")) == []

    @pytest.mark.parametrize(
        "source",
        [
            "import requests\n" + body("pass"),
            "import urllib.request\n" + body("pass"),
            "import socket\n" + body("pass"),
            "from http.client import HTTPSConnection\n" + body("pass"),
        ],
        ids=["requests", "urllib", "socket", "http"],
    )
    def test_network_access_is_caught(self, source: str) -> None:
        assert "network" in codes(source)

    @pytest.mark.parametrize(
        "source,expected",
        [
            (body("f = open('/etc/passwd')"), "filesystem"),
            (body("eval('1+1')"), "dynamic-code"),
            (body("exec('x=1')"), "dynamic-code"),
            (body("__import__('os')"), "dynamic-code"),
            ("import subprocess\n" + body("pass"), "subprocess"),
            ("import os\n" + body("os.system('dir')"), "subprocess"),
            ("import os\n" + body("x = os.environ['KEY']"), "credentials"),
            (body("input('go?')"), "blocking"),
            ("import time\n" + body("time.sleep(5)"), "blocking"),
        ],
        ids=[
            "open", "eval", "exec", "dunder-import", "subprocess",
            "os-system", "os-environ", "input", "sleep",
        ],
    )
    def test_the_remaining_rules(self, source: str, expected: str) -> None:
        assert expected in codes(source)

    @pytest.mark.parametrize(
        "source",
        [
            "import os\n" + body("x = os.environ['KEY']"),
            "import os\n" + body("x = os.getenv('KEY')"),
            "from os import environ\n" + body("x = environ['KEY']"),
        ],
        ids=["environ", "getenv", "from-import"],
    )
    def test_reading_the_environment_is_a_credential_finding_not_a_file_one(
        self, source: str
    ) -> None:
        """Finding L24. The environment is where the exchange secret and
        `PERPLAB_PASSWORD` live (spec 11), so "this is not reproducible" is the wrong
        sentence: an author who believes that fixes it by caching the value at import time
        and has now written the exfiltration bug on purpose. The message has to name the
        boundary it is about.
        """
        result = validate_code(source, run_sandbox=False)
        finding = next(d for d in result.diagnostics if d.severity == "error")
        assert finding.code == "credentials"
        assert "credential" in finding.message
        assert "secret" in finding.message
        # Spec 11 is the credential section; 12.1 is the reproducibility invariant the
        # filesystem rule cites. Citing 12.1 here is the miscategorisation itself.
        assert "spec 11" in finding.message
        assert "12.1" not in finding.message

    def test_a_variable_named_open_is_not_a_file_handle(self) -> None:
        assert codes(body("open = bar.open\n        x = open")) == []

    def test_os_path_is_allowed(self) -> None:
        """`os` is not banned outright — `os.path.join` is harmless and common. Only its
        dangerous members are listed."""
        assert codes("import os\n" + body("x = os.path.sep")) == []

    def test_a_relative_import_is_refused(self) -> None:
        assert "relative-import" in codes("from .helpers import thing\n" + body("pass"))

    def test_a_star_import_is_a_warning_not_an_error(self) -> None:
        result = validate_code(
            "from math import *\n" + body("x = sqrt(4)"), run_sandbox=False
        )
        assert result.ok
        assert any(d.code == "star-import" for d in result.diagnostics)

    def test_one_finding_per_rule_per_line(self) -> None:
        """An attribute chain matches from the outside in, so `numpy.random.default_rng()`
        would otherwise place two markers on one line for one mistake."""
        result = validate_code(
            "import numpy as np\n" + body("g = np.random.default_rng(1)"), run_sandbox=False
        )
        randomness = [d for d in result.diagnostics if d.code == "unseeded-random"]
        assert len(randomness) == 1

    def test_diagnostics_are_ordered_by_position(self) -> None:
        result = validate_code(
            "import time\nimport socket\n" + body("x = time.time()"), run_sandbox=False
        )
        lines = [d.line for d in result.diagnostics]
        assert lines == sorted(lines)

    def test_columns_are_one_based_for_monaco(self) -> None:
        """Python's AST is 0-based in column; Monaco is 1-based. Converted once, at the
        diagnostic, so the underline sits under the thing it underlines."""
        result = validate_code("import time\n" + body("x = time.time()"), run_sandbox=False)
        offender = next(d for d in result.diagnostics if d.code == "wall-clock")
        assert offender.column >= 1
        assert offender.end_column > offender.column

    def test_the_scan_stops_before_anything_executes(self) -> None:
        """The next stage execs the module. Executing code that was just found to call
        `subprocess`, in order to report that it calls `subprocess`, would be memorable."""
        result = validate_code(
            "import subprocess\nsubprocess.run(['echo', 'hi'])\n" + body("pass"),
            run_sandbox=True,
        )
        assert not result.ok
        assert {d.stage for d in result.diagnostics} == {"scan"}
        assert result.class_name is None


class TestFullPipeline:
    def test_the_new_strategy_template_validates_clean(self) -> None:
        """The template is what every author meets first. If it does not pass the validator
        they are about to meet, the tool has contradicted itself on turn one."""
        result = validate_code(NEW_STRATEGY_TEMPLATE, filename="new.py")
        assert result.ok, [d.message for d in result.diagnostics]
        assert result.class_name == "MyStrategy"

    def test_the_worked_example_validates_clean(self) -> None:
        result = validate_code(EMA_CROSS_EXAMPLE, filename="ema.py")
        assert result.ok, [d.message for d in result.diagnostics]
        assert result.orders and result.orders > 0

    def test_a_runtime_error_reports_the_strategy_line_not_a_platform_traceback(self) -> None:
        code = body("if bar.close > 0:\n            undefined_name()")
        result = validate_code(code, filename="s.py")
        assert not result.ok
        failure = next(d for d in result.diagnostics if d.code == "runtime-failure")
        # Line 9 of the wrapped source is `undefined_name()`, inside the strategy — not a
        # frame in `dryrun.py` or `sandbox.py`, which is where the traceback actually ends.
        assert code.splitlines()[failure.line - 1].strip() == "undefined_name()"
        assert "on_bar" in failure.message

    def test_two_strategy_classes_are_refused(self) -> None:
        code = (
            "from perplab import Strategy\n"
            "class A(Strategy):\n    def on_bar(self, ctx, bar): pass\n"
            "class B(Strategy):\n    def on_bar(self, ctx, bar): pass\n"
        )
        result = validate_code(code)
        assert "multiple-strategies" in {d.code for d in result.diagnostics}

    def test_no_strategy_class_is_refused(self) -> None:
        result = validate_code("x = 1\n")
        assert "no-strategy" in {d.code for d in result.diagnostics}

    def test_a_strategy_with_no_entry_hook_is_refused(self) -> None:
        code = "from perplab import Strategy\nclass S(Strategy):\n    def on_start(self, ctx): pass\n"
        result = validate_code(code)
        assert "no-entry-hook" in {d.code for d in result.diagnostics}

    def test_a_float_decimal_param_fails_at_the_params_stage(self) -> None:
        code = (
            "from perplab import Strategy\n"
            "class S(Strategy):\n"
            '    params = {"risk": {"type": "decimal", "default": 0.01}}\n'
            '    requires = {"symbols": ["BTCUSDT"], "timeframe": "1h", "history": 1}\n'
            "    def on_bar(self, ctx, bar): pass\n"
        )
        result = validate_code(code)
        failure = next(d for d in result.diagnostics if d.code == "bad-params")
        assert failure.line == 3

    def test_declared_history_shorter_than_the_indicators_is_an_error(self) -> None:
        """Spec 5.4 rule 4. Without the cross-check a 200-period EMA quietly produces
        signals from twelve bars."""
        code = (
            "from perplab import Strategy\n"
            "class S(Strategy):\n"
            '    requires = {"symbols": ["BTCUSDT"], "timeframe": "1h", "history": 5}\n'
            "    def on_start(self, ctx):\n        self.e = ctx.indicators.ema(200)\n"
            "    def on_bar(self, ctx, bar): pass\n"
        )
        result = validate_code(code)
        assert "warmup-too-short" in {d.code for d in result.diagnostics}
        assert not result.ok

    def test_declaring_more_history_than_needed_is_information_not_failure(self) -> None:
        result = validate_code(EMA_CROSS_EXAMPLE, filename="ema.py")
        generous = next(d for d in result.diagnostics if d.code == "warmup-generous")
        assert generous.severity == "info"
        assert result.ok

    def test_an_unbounded_loop_is_killed_by_the_timeout(self) -> None:
        result = validate_code(body("while True:\n            pass"), timeout_s=5.0)
        assert "timeout" in {d.code for d in result.diagnostics}

    def test_set_iteration_order_is_caught_by_the_determinism_probe(self) -> None:
        """The probe's whole reason for existing (spec 5.5 step 6).

        Two runs in one interpreter would agree — a `set` iterates identically all through
        one process's life. Only running them under different `PYTHONHASHSEED` values
        exposes it, and this strategy is otherwise perfectly ordinary code.
        """
        code = body(
            'for tag in {"alpha", "beta", "gamma", "delta", "epsilon"}:\n'
            "            ctx.log.info(tag)"
        )
        result = validate_code(code)
        assert not result.ok
        failure = next(d for d in result.diagnostics if d.code == "nondeterministic")
        assert "set" in failure.message
        assert "PYTHONHASHSEED" in failure.message

    def test_sorting_the_same_set_is_deterministic(self) -> None:
        """The fix the message suggests has to actually work."""
        code = body(
            'for tag in sorted({"alpha", "beta", "gamma", "delta", "epsilon"}):\n'
            "            ctx.log.info(tag)"
        )
        result = validate_code(code)
        assert result.ok, [d.message for d in result.diagnostics]

    def test_an_ordinary_strategy_hashes_identically_across_hash_seeds(self) -> None:
        first = validate_code(EMA_CROSS_EXAMPLE, filename="ema.py")
        second = validate_code(EMA_CROSS_EXAMPLE, filename="ema.py")
        assert first.event_hash == second.event_hash

    def test_a_strategy_that_never_trades_gets_a_warning_not_an_error(self) -> None:
        result = validate_code(body("pass"))
        assert result.ok
        assert "no-orders" in {d.code for d in result.diagnostics}

    def test_requiring_liquidations_warns_that_no_source_exists(self) -> None:
        """Not an error: the strategy is not malformed, the world is (finding F4). It saves
        and runs; the warning travels with it so nobody wonders why the hook never fired."""
        code = (
            "from perplab import Strategy\n"
            "class S(Strategy):\n"
            '    requires = {"symbols": ["BTCUSDT"], "timeframe": "1h", "history": 1,\n'
            '                "datasets": ["klines", "liquidations"]}\n'
            "    def on_bar(self, ctx, bar): pass\n"
        )
        result = validate_code(code)
        assert result.ok
        assert "dataset-unavailable" in {d.code for d in result.diagnostics}

    def test_strategy_stdout_is_captured_not_mixed_into_the_protocol(self) -> None:
        """The worker's result travels in a file precisely so a `print("{")` cannot make
        the response unparseable and look like a platform bug."""
        result = validate_code(body('print("hello from the strategy")'))
        assert result.ok
        assert "hello from the strategy" in result.stdout

    def test_quick_mode_spawns_nothing(self) -> None:
        result = validate_code(body("while True: pass"), run_sandbox=False)
        assert result.ok  # static stages have nothing to say about an infinite loop
        assert result.class_name is None

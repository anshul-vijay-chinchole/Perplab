"""The sandboxed worker that executes strategy code (spec 5.5 steps 3-6).

Run as `python -m perplab.strategy.sandbox --request R --response W`. It is a separate
process for one reason that is worth stating plainly: **the API server must never execute
strategy code.** A syntax-clean strategy with an infinite loop at import time, a runaway
allocation, or a `sys.setrecursionlimit` stunt would otherwise take down the process that
serves the editor the author is trying to fix it in.

Everything the caller needs comes back as JSON in a **file**, not on stdout. Strategy code
prints; a protocol that shared a channel with `print()` would be one `print("{")` away from
being unparseable, and the failure would look like a platform bug.

The 10 s timeout, the process kill, and the two-run determinism probe all live in
`validate.py`, which owns the subprocess. This module runs exactly once and reports what
happened.
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import sys
import traceback
from pathlib import Path
from typing import Any

from perplab.strategy.dryrun import SMOKE_BARS, SMOKE_SEED, smoke_run
from perplab.strategy.loader import (
    MODULE_NAME,
    StrategyLoadError,
    execute_module,
    find_strategy_class,
    prime_linecache,
)
from perplab.strategy.params import ParamError, parse_param_specs, parse_requirements
from perplab.strategy.scan import Diagnostic

__all__ = ["run_request", "main"]
_MAX_CAPTURED_OUTPUT = 8_000

PROBE_SET = ("long", "short", "flat", "alpha", "beta", "delta")
"""A canonical string set whose iteration order the worker reports back.

The determinism probe compares two runs under different `PYTHONHASHSEED` values, and the
whole probe rests on those two interpreters *actually* ordering string sets differently.
For a small set they frequently do not: `{"long", "short"}` -- the single most common shape
in this domain -- iterates identically under seeds 0 and 1, so a strategy looping over it
produced matching hashes and passed, while seeds 2 and 5 gave a different order and a
different result in production.

Reporting the order turns the probe from a coin flip into a guarantee: `validate.py` keeps
starting workers until it has two whose `probe_order` differs, and only then compares their
event logs. Two seeds that order strings identically prove nothing and are not used.
"""


def _probe_order() -> list[str]:
    return list(set(PROBE_SET))


def _diag(
    severity: str, stage: str, code: str, message: str, line: int = 1
) -> dict[str, Any]:
    return Diagnostic(
        severity=severity,
        stage=stage,
        code=code,
        message=message,
        line=line,
        column=1,
        end_line=line,
        end_column=2,
    ).to_json()


def _find_line(tree_source: str, needle: str) -> int:
    """Best-effort line number for a class-level declaration such as `params = {`.

    Used only to place a diagnostic. A wrong line is a cosmetic problem; refusing to report
    the finding because the line is unknown would not be.
    """
    for index, text in enumerate(tree_source.splitlines(), start=1):
        if text.lstrip().startswith(needle):
            return index
    return 1


def run_request(request: dict[str, Any]) -> dict[str, Any]:
    """Execute one validation request. Never raises for a strategy-side failure."""
    code: str = request["code"]
    filename: str = request.get("filename", "<strategy>")
    seed: int = int(request.get("seed", SMOKE_SEED))
    bars: int = int(request.get("bars", SMOKE_BARS))

    diagnostics: list[dict[str, Any]] = []
    response: dict[str, Any] = {
        "ok": False,
        "probe_order": _probe_order(),
        "diagnostics": diagnostics,
        "class_name": None,
        "params": [],
        "requires": None,
        "hooks": [],
        "indicator_warmup": None,
        "warmup": None,
        "bars": None,
        "orders": None,
        "event_hash": None,
        "stdout": "",
    }

    prime_linecache(code, filename)

    captured = io.StringIO()
    try:
        with contextlib.redirect_stdout(captured), contextlib.redirect_stderr(captured):
            _execute(code, filename, seed, bars, diagnostics, response)
    except BaseException as exc:  # noqa: BLE001 - the worker reports, it does not crash
        diagnostics.append(
            _diag(
                "error",
                "smoke",
                "worker-failure",
                f"the validation worker failed: {type(exc).__name__}: {exc}\n"
                + "".join(traceback.format_exception(exc))[-2000:],
            )
        )

    text = captured.getvalue()
    response["stdout"] = (
        text
        if len(text) <= _MAX_CAPTURED_OUTPUT
        else text[:_MAX_CAPTURED_OUTPUT] + "\n... (truncated)"
    )
    response["ok"] = not any(d["severity"] == "error" for d in diagnostics)
    return response


def _execute(
    code: str,
    filename: str,
    seed: int,
    bars: int,
    diagnostics: list[dict[str, Any]],
    response: dict[str, Any],
) -> None:
    # --------------------------------------------------------------- import the module
    # Execution and class discovery both live in `strategy.loader`, shared with the
    # backtest worker. Two copies of "which class is the strategy" is the divergence spec
    # 14-I8 was about: the validator would accept a file the runner refused, or the two
    # would pick different classes out of one source and the run's `code_sha256` would
    # describe something other than what executed.
    try:
        module = execute_module(code, filename)
    except BaseException as exc:  # noqa: BLE001
        line = _traceback_line(exc, filename)
        diagnostics.append(
            _diag(
                "error",
                "structure",
                "import-failure",
                f"the module failed while being imported: {type(exc).__name__}: {exc}",
                line=line or 1,
            )
        )
        return

    # ------------------------------------------------------------------ exactly one class
    try:
        cls = find_strategy_class(module)
    except StrategyLoadError as exc:
        diagnostics.append(
            _diag(
                "error",
                "structure",
                exc.code,
                exc.message,
                line=_find_line(code, exc.needle) if exc.needle else 1,
            )
        )
        return

    response["class_name"] = cls.__name__

    hooks = cls.implemented_hooks()
    response["hooks"] = sorted(hooks)
    if not ({"on_bar", "on_tick"} & hooks):
        diagnostics.append(
            _diag(
                "error",
                "structure",
                "no-entry-hook",
                f"{cls.__name__} implements neither `on_bar` nor `on_tick`, so nothing "
                "would ever drive it. Add one.",
                line=_find_line(code, f"class {cls.__name__}"),
            )
        )
        return

    # ------------------------------------------------------------------------- params
    try:
        specs = cls.param_specs()
    except ParamError as exc:
        diagnostics.append(
            _diag("error", "params", "bad-params", str(exc), line=_find_line(code, "params"))
        )
        return
    except BaseException as exc:  # noqa: BLE001
        diagnostics.append(
            _diag("error", "params", "bad-params", f"{type(exc).__name__}: {exc}")
        )
        return
    response["params"] = [spec.to_json() for spec in specs]

    try:
        requirements = cls.requirements()
    except ParamError as exc:
        diagnostics.append(
            _diag(
                "error", "params", "bad-requires", str(exc), line=_find_line(code, "requires")
            )
        )
        return
    except BaseException as exc:  # noqa: BLE001
        diagnostics.append(
            _diag("error", "params", "bad-requires", f"{type(exc).__name__}: {exc}")
        )
        return
    response["requires"] = requirements.to_json()

    from perplab.strategy.params import UNAVAILABLE_ON_THIS_DEPLOYMENT

    for dataset in requirements.datasets:
        if dataset in UNAVAILABLE_ON_THIS_DEPLOYMENT:
            diagnostics.append(
                _diag(
                    "warning",
                    "params",
                    "dataset-unavailable",
                    f"`{dataset}` has no source on this deployment, so the hooks that "
                    "consume it will never fire. The strategy still saves and runs; it "
                    "will simply behave as though that stream were permanently silent. "
                    "See docs/DATA_AVAILABILITY.md.",
                    line=_find_line(code, "requires"),
                )
            )

    # -------------------------------------------------------------------- instantiate
    try:
        strategy = cls()
    except BaseException as exc:  # noqa: BLE001
        line = _traceback_line(exc, filename)
        diagnostics.append(
            _diag(
                "error",
                "smoke",
                "init-failure",
                f"{cls.__name__}() failed: {type(exc).__name__}: {exc}",
                line=line or 1,
            )
        )
        return

    # ---------------------------------------------------------------------- smoke run
    result = smoke_run(strategy, requirements, bars=bars, seed=seed, filename=filename)
    response["indicator_warmup"] = result.indicator_warmup
    response["warmup"] = result.warmup
    response["bars"] = result.bars
    response["orders"] = result.orders
    response["event_hash"] = result.hash

    if not result.ok:
        diagnostics.append(
            _diag(
                "error",
                "smoke",
                "runtime-failure",
                f"the strategy raised during `{result.hook}` after {result.bars} bars: "
                f"{result.error}\n\n{result.traceback or ''}".rstrip(),
                line=result.error_line or 1,
            )
        )
        return

    # ------------------------------------------------- warm-up cross-check (spec 5.4/4)
    declared = requirements.history
    if declared < result.indicator_warmup:
        diagnostics.append(
            _diag(
                "error",
                "params",
                "warmup-too-short",
                f"requires['history'] is {declared} but the indicator set needs "
                f"{result.indicator_warmup} bars before every indicator has a value. The "
                "run would produce signals from a shorter window than the strategy "
                "declares (spec 5.4).",
                line=_find_line(code, "requires"),
            )
        )
    elif declared > result.indicator_warmup:
        diagnostics.append(
            _diag(
                "info",
                "params",
                "warmup-generous",
                f"requires['history'] is {declared}; the indicator set is ready after "
                f"{result.indicator_warmup} bars. The extra "
                f"{declared - result.indicator_warmup} bars are still consumed as warm-up "
                "and cannot produce signals.",
                line=_find_line(code, "requires"),
            )
        )

    if not result.reached_warm:
        from perplab.strategy.dryrun import MAX_SMOKE_BARS

        diagnostics.append(
            _diag(
                "error",
                "params",
                "warmup-exceeds-smoke-run",
                f"the smoke run is capped at {MAX_SMOKE_BARS} bars and this strategy needs "
                f"{result.warmup} of warm-up, so it never left warm-up and no trading logic "
                "ran at all. Validation cannot tell you anything about a strategy it could "
                "not exercise — and the determinism probe would be comparing two empty "
                "event logs. Reduce requires['history'], or shorten the slowest indicator.",
                line=_find_line(code, "requires"),
            )
        )
        return

    if result.orders == 0:
        diagnostics.append(
            _diag(
                "warning",
                "smoke",
                "no-orders",
                f"the strategy placed no orders across {result.bars} synthetic bars. That "
                "may be correct for a selective strategy, but it also means the smoke run "
                "never executed the entry path — the most common cause is a condition "
                "that cannot be true.",
            )
        )


def _traceback_line(exc: BaseException, filename: str) -> int | None:
    summary = traceback.TracebackException.from_exception(exc)
    if isinstance(exc, SyntaxError) and exc.filename == filename:
        return exc.lineno
    line = None
    for frame in summary.stack:
        if frame.filename == filename:
            line = frame.lineno
    return line


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="PerpLab strategy validation worker")
    parser.add_argument("--request", required=True)
    parser.add_argument("--response", required=True)
    args = parser.parse_args(argv)

    request = json.loads(Path(args.request).read_text(encoding="utf-8"))
    response = run_request(request)
    Path(args.response).write_text(
        json.dumps(response, ensure_ascii=False), encoding="utf-8"
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - process entry point
    sys.exit(main())

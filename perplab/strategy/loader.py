"""Turning strategy source into a class -- once, for every process that has to do it.

Two processes execute strategy code: the validator's sandbox (spec 5.5) and the backtest
worker (spec 2.3). Both have to answer the same question -- *what is the strategy class in
this file* -- and if they ever answer it differently, the validator accepts a file the runner
refuses, or worse, they pick different classes out of the same source and the run's
`code_sha256` describes something other than what executed.

Spec 6.1's rule is stated about execution engines but applies just as well here: if a piece
of logic could live in the shared core, it must. This module is that core, and it is
deliberately small -- it executes and it discovers. Parameter validation, diagnostics and
smoke running all stay with the callers, because those genuinely differ: the sandbox turns a
failure into a gutter marker and the worker turns it into a failed run.

**Exactly one class per file**, and the reason is spec 12.1. A run records the code it
executed by hash; if a file held two strategies, that hash would not say which one ran, and
two runs with identical inputs could legitimately produce different results.

## The runtime guard, and what it is worth

Strategy code executes here against a **curated `__builtins__`** (`SAFE_BUILTINS`) and a
**guarded `__import__`** that re-checks `scan.BANNED_MODULES` against the module actually
being imported. The scan in `strategy.scan` reads source; this reads intent as the
interpreter executes it, so `import io as _`, `importlib.import_module("soc" + "ket")` and
`getattr(builtins, "open")` are all refused by the same rule the author saw named in the
editor, whatever they were spelled as.

**This is defence in depth. It is not a security boundary, and nothing in this process can
be one.** Strategy code runs in the interpreter that also holds the exchange API secret
during a live session (spec 11), and Python gives any object graph traversal a path back to
everything:

    ().__class__.__base__.__subclasses__()

reaches every class the interpreter has loaded, and from there the real `builtins`, the real
`os`, the frames holding the key. No curated `__builtins__` closes that; it is a property of
the language, not an oversight here, and the honest thing is to say so rather than to keep
extending the list and implying otherwise. `sys.modules` and `gc.get_objects()` are the same
hole with shorter names -- both are on the banned list, and both are reachable by an
adversary who already has the traversal above.

So the boundary this actually draws:

- **Against mistakes** -- the common case, since strategies are normally the operator's own
  code -- it is effective. A `datetime.now()`, an accidental `open()`, a stray `requests`
  import: caught, with a sentence explaining which invariant it breaks.
- **Against a hostile *imported* bundle** it raises the cost from "one line of indirection"
  to "a deliberate sandbox escape", and it removes the drive-by version entirely. It does
  not make importing an untrusted bundle safe. **Do not import a strategy bundle you would
  not run as a script on this machine**, because that is exactly what importing it does.

The remaining hardening -- the one that would make the claim real -- is **privilege
separation**: move exchange signing into its own process that holds the API secret and
exposes only "sign this request", so that a strategy escaping the guard finds no key in the
interpreter it escaped into. That is a structural change to `perplab.live` and
`perplab.exchange` and is not attempted here; it is the recommended follow-up, and until it
lands the mitigation for a key read out of memory is that `socket`, `importlib` and friends
are unreachable at runtime, which leaves a thief holding a secret and no way to post it.
"""

from __future__ import annotations

import builtins
import linecache
import sys
import types
from dataclasses import dataclass
from typing import Any, Sequence

from perplab.strategy.base import Strategy
from perplab.strategy.scan import (
    BANNED_ATTRIBUTE_STEMS,
    BANNED_ATTRIBUTES,
    banned_module_rule,
)

__all__ = [
    "MODULE_NAME",
    "SAFE_BUILTINS",
    "StrategyImportRefused",
    "StrategyLoadError",
    "guarded_import",
    "prime_linecache",
    "strategy_builtins",
    "execute_module",
    "find_strategy_class",
    "load_strategy_class",
]

MODULE_NAME = "perplab_user_strategy"
"""Module name given to executed strategy source.

Fixed, and used as a filter below: only classes *defined in this module* count as
candidates. Without that filter, `from perplab import Strategy` followed by any import that
happens to expose another `Strategy` subclass would make the file ambiguous, and importing
a shared base class from a helper file -- a perfectly reasonable thing to do -- would break
discovery.
"""


@dataclass(frozen=True, slots=True)
class StrategyLoadError(Exception):
    """The source does not contain exactly one usable strategy.

    Carries a machine-readable `code` and an anchor `needle` so a caller can place a
    diagnostic without re-deriving why the load failed. `Exception` subclasses are not
    usually frozen dataclasses; this one is because it is *data* that the sandbox turns
    into JSON and the worker turns into a run failure, and neither should be parsing a
    message string to tell the cases apart.
    """

    code: str
    message: str
    needle: str = ""
    """Source prefix to point a line number at, e.g. `class ` -- may be empty."""

    def __str__(self) -> str:
        return self.message


class StrategyImportRefused(ImportError):
    """A strategy imported a module on `scan.BANNED_MODULES`.

    An `ImportError` rather than a bespoke exception, because that is what it is and
    because every caller already handles one: the sandbox turns it into an `import-failure`
    diagnostic pointing at the line, and the backtest worker fails the run with the message
    intact. A new exception type would have needed catching in three places to say the same
    thing.
    """


SAFE_BUILTINS: frozenset[str] = frozenset(
    {
        # Constants and singletons.
        "Ellipsis", "NotImplemented", "__debug__",
        # Types and constructors.
        "bool", "bytearray", "bytes", "complex", "dict", "float", "frozenset", "int",
        "list", "memoryview", "object", "set", "slice", "str", "tuple", "type",
        # Sequence, numeric and iteration helpers -- the bulk of what a strategy uses.
        "abs", "all", "any", "ascii", "bin", "chr", "divmod", "enumerate", "filter",
        "format", "hex", "iter", "len", "map", "max", "min", "next", "oct", "ord",
        "pow", "range", "repr", "reversed", "round", "sorted", "sum", "zip",
        # Attribute and object protocol. `getattr` is deliberately here: it is ordinary in
        # strategy code, and taking it away would not close the hole it opens anyway --
        # see the module docstring.
        "callable", "classmethod", "delattr", "dir", "getattr", "hasattr", "hash", "id",
        "isinstance", "issubclass", "locals", "property", "setattr", "staticmethod",
        "super",
        # Class creation and imports: the interpreter looks both of these up in this dict,
        # so a `class` statement or an `import` fails outright without them. `__import__`
        # is the guarded one below, not CPython's.
        "__build_class__", "__import__",
        # Strategy output. `print` is captured by the sandbox and shown in the editor.
        "print",
    }
)
"""Names copied from `builtins` into the namespace strategy code runs against.

An allow-list, not a deny-list, because the deny-list version of this is unmaintainable:
CPython adds builtins and each new one is allowed by default until somebody notices.

Every exception class is added on top of this set in `strategy_builtins` -- an `except
ValueError` is a global lookup like any other, and a strategy that could not name the
exception it catches would be broken by the guard rather than constrained by it.

**Absent, deliberately:** `open`, `eval`, `exec`, `compile`, `input`, `breakpoint`, `vars`,
`globals`, `help`, `exit`, `quit`. Each is on `scan.BANNED_BUILTINS`, so the author is told
about it in the editor with a line number; removing it here is what makes the rule hold when
the name is assembled at runtime rather than written down. `getattr(builtins, "e" + "val")`
now fails on the `builtins` import; `__builtins__["eval"]` fails on the missing key.
"""


def guarded_import(
    name: str,
    globals: dict[str, Any] | None = None,  # noqa: A002 - CPython's parameter names
    locals: dict[str, Any] | None = None,  # noqa: A002
    fromlist: Sequence[str] = (),
    level: int = 0,
) -> types.ModuleType:
    """`__import__` for strategy code: refuses `scan.BANNED_MODULES`, by any spelling.

    The scan cannot see `importlib.import_module("soc" + "ket")` and never will -- the name
    exists only once the expression has run. This does see it, because by the time an import
    happens the module has a name, and that is the only place the check is spelling-proof.

    Both halves of the statement are checked: the module, and each `fromlist` entry against
    the module *and* the banned-attribute rules -- so `from os import environ` is refused
    here as well as in the editor, rather than being the one form of the rule that runtime
    enforcement forgot.

    Refusing at the import, not at first use, matters for the same reason it does in the
    scan: the module object is the capability. Handing back `socket` and hoping to catch
    `socket.socket()` later would be trusting the strategy not to bind a name to it.
    """
    _refuse_if_banned(name)
    for item in fromlist or ():
        if item != "*":
            _refuse_if_banned(f"{name}.{item}")
    return builtins.__import__(name, globals, locals, fromlist or (), level)


def _refuse_if_banned(dotted: str) -> None:
    rule = banned_module_rule(dotted) or BANNED_ATTRIBUTES.get(dotted)
    if rule is None:
        for stem, stem_rule in BANNED_ATTRIBUTE_STEMS.items():
            if dotted.startswith(stem):
                rule = stem_rule
                break
    if rule is None:
        return
    _, reason = rule
    raise StrategyImportRefused(
        f"`{dotted}` is not available to strategy code: {reason}", name=dotted
    )


def strategy_builtins() -> dict[str, Any]:
    """A fresh `__builtins__` mapping for one strategy module.

    Fresh per module rather than a shared constant, so a strategy that assigns to its own
    `__builtins__` -- or mutates the mapping, which it can, it is an ordinary dict -- cannot
    reach into the next strategy loaded in the same process. The lab sweeps load hundreds in
    one worker (`lab.sweep`), and one shared dict would make that a channel between them.
    """
    namespace: dict[str, Any] = {
        name: getattr(builtins, name) for name in SAFE_BUILTINS if hasattr(builtins, name)
    }
    # Every exception the strategy might raise or catch. A `raise ValueError` resolves the
    # name through this dict, so omitting them would turn the guard into a syntax-level
    # restriction on error handling, which is not what it is for.
    for name in dir(builtins):
        candidate = getattr(builtins, name)
        if isinstance(candidate, type) and issubclass(candidate, BaseException):
            namespace[name] = candidate
    namespace["__import__"] = guarded_import
    return namespace


def prime_linecache(code: str, filename: str) -> None:
    """Make tracebacks raised inside the strategy show its source.

    `compile(code, filename)` produces frames naming a file that does not exist on disk, so
    without this the author gets a traceback with the code lines blank -- the least useful
    possible rendering of the one thing they need to look at.
    """
    linecache.cache[filename] = (len(code), None, code.splitlines(True), filename)


def execute_module(code: str, filename: str) -> types.ModuleType:
    """Compile and execute the source in a fresh module. Import-time errors propagate.

    `__builtins__` is set **before** the `exec`, and that ordering is the whole guard:
    CPython inserts the real `builtins` module into any globals mapping that does not
    already carry the key, so a namespace populated afterwards is a namespace that ran with
    full builtins first. Functions defined by this source close over `module.__dict__` as
    their globals, so the curated mapping applies to `on_bar` and everything else the
    strategy defines, not just to code at module level.

    It does **not** apply to platform code the strategy calls. `ctx.indicators.ema(...)`
    runs in `perplab`'s own frames with ordinary builtins, which is correct -- the guard
    constrains the code that was written by the author, not the code it is allowed to use.
    """
    module = types.ModuleType(MODULE_NAME)
    module.__file__ = filename
    module.__dict__["__builtins__"] = strategy_builtins()
    # **`dont_inherit=True`, or the author's annotations are silently not theirs.**
    # `compile` defaults to inheriting the *calling module's* compiler flags -- including
    # this file's own `from __future__ import annotations` -- so every strategy was
    # compiled under PEP 563 stringised annotations whether its author asked for them or
    # not. That was invisible right up until it wasn't: `@dataclass` resolves stringised
    # `ClassVar`/`InitVar` markers through `sys.modules[cls.__module__]`, that name was
    # never registered, and a strategy holding a perfectly ordinary dataclass crashed on
    # load with an `AttributeError` from inside `dataclasses`. Strategy code gets the
    # interpreter's own default semantics; an author who writes the future import
    # themselves still gets it, which is why the registration below exists too.
    compiled = compile(code, filename, "exec", dont_inherit=True)
    # **Registered under its own name for exactly the duration of the exec.** Class
    # decorators that run at definition time -- `@dataclass` first among them -- look the
    # defining module up by `cls.__module__`, and a module that exists nowhere makes that
    # lookup `None`. Scoped rather than permanent because every strategy shares
    # `MODULE_NAME`: a lasting entry would have each load silently replace the previous
    # strategy's module for any later `get_type_hints` on the old classes. Loads are
    # per-process-single-threaded (the validator is its own subprocess, the worker loads
    # once), so the swap cannot race another load.
    previous = sys.modules.get(MODULE_NAME)
    sys.modules[MODULE_NAME] = module
    try:
        exec(compiled, module.__dict__)  # noqa: S102 - executing the user's strategy is the job
    finally:
        if previous is None:
            sys.modules.pop(MODULE_NAME, None)
        else:
            sys.modules[MODULE_NAME] = previous
    return module


def find_strategy_class(module: types.ModuleType) -> type[Strategy]:
    """The one `Strategy` subclass defined in `module`, or a `StrategyLoadError`.

    Candidates are deduplicated **by identity**. A module namespace maps names to objects,
    so one class bound to two names -- `Alias = MyStrat`, an ordinary export -- appears
    twice, and counting names rather than classes refused a legal file with the
    self-contradicting message "found 2 Strategy subclasses (MyStrat, MyStrat)". Because
    both the validator and the backtest worker call this, such a file was neither saveable
    as valid nor runnable.
    """
    seen: dict[int, type[Strategy]] = {}
    for obj in vars(module).values():
        if (
            isinstance(obj, type)
            and issubclass(obj, Strategy)
            and obj is not Strategy
            and obj.__module__ == MODULE_NAME
        ):
            seen.setdefault(id(obj), obj)
    candidates = list(seen.values())
    if not candidates:
        raise StrategyLoadError(
            "no-strategy",
            "no Strategy subclass found. A strategy file defines exactly one class "
            "inheriting from `perplab.Strategy`.",
        )
    if len(candidates) > 1:
        names = ", ".join(sorted(c.__name__ for c in candidates))
        raise StrategyLoadError(
            "multiple-strategies",
            f"found {len(candidates)} Strategy subclasses ({names}). A file defines "
            "exactly one, so that a run can name the code it executed without ambiguity "
            "(spec 12.1).",
            "class ",
        )
    return candidates[0]


def load_strategy_class(code: str, filename: str = "<strategy>") -> type[Strategy]:
    """Execute the source and return its strategy class."""
    prime_linecache(code, filename)
    return find_strategy_class(execute_module(code, filename))

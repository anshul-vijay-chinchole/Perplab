"""Static rejection rules for strategy code (spec 5.5 step 2).

Every rule here exists because the construct it rejects breaks one of four guarantees, and
each message says which:

- **Look-ahead** — reading a clock other than `ctx.now` lets a strategy know something the
  engine's event ordering says it cannot know yet (spec 6.2).
- **Reproducibility** — unseeded randomness, network reads, and file reads make the same
  code produce different results from the same inputs, which spec 12.1 states as an
  invariant rather than an aspiration.
- **Stability** — a subprocess, a socket read or a `sleep` inside a worker turns a strategy
  bug into a stuck job (spec 2.3).
- **Credential boundary** — the process running a live session holds the exchange secret in
  its own memory (spec 11). Reading the environment, or reaching a module that can open a
  socket, is not a reproducibility problem, it is a key-exfiltration path, and the message
  has to say so or the author "fixes" it by caching the value instead.

**What this is, and what it is not.** This scan is the *first* line: it is fast, it runs
before any strategy code executes, and its job is to give the author a line number and a
sentence. It is not the enforcement boundary. `getattr(builtins, "e" + "val")` and
`().__class__.__base__.__subclasses__()` defeat every rule below, and no amount of AST
matching closes that -- the expression is assembled at runtime, where an AST cannot see it.

Enforcement, such as it is, lives in `strategy.loader`: strategy code executes against a
curated `__builtins__` and an `__import__` that re-checks `BANNED_MODULES` by whatever name
the module is reached under. That guard catches at runtime what this file catches by
spelling, which is why the two lists must stay the same list -- `loader` imports
`BANNED_MODULES` from here rather than keeping a copy that could drift.

Read `strategy.loader`'s module docstring before trusting either of them with code you did
not write: in-process Python is defence in depth, not a security boundary.

Alias tracking exists for two different reasons and it is worth keeping them apart.
`import time as t; t.time()` is not an evasion attempt, it is ordinary code, and a scanner
that only matched the literal string `time.time` would pass it and then be trusted.
`import os; e = os; e.environ` *is* an evasion attempt, and it is tractable to follow
because the assignment is right there in the tree -- so it is followed. Neither is a claim
that alias tracking is complete; `e = globals()["os"]` is not, and cannot be.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from typing import Any

__all__ = [
    "Diagnostic",
    "BANNED_MODULES",
    "BANNED_ATTRIBUTES",
    "BANNED_ATTRIBUTE_STEMS",
    "BANNED_BUILTINS",
    "banned_module_rule",
    "scan_source",
]


@dataclass(frozen=True, slots=True)
class Diagnostic:
    """One finding, shaped for a Monaco marker.

    Monaco's `IMarkerData` is 1-based in both line and column; Python's AST is 1-based in
    line and **0-based** in column. The conversion happens once, here, at construction --
    doing it in the frontend instead would put an off-by-one between the underline and the
    thing it underlines, which is worse than no underline.
    """

    severity: str
    stage: str
    code: str
    message: str
    line: int = 1
    column: int = 1
    end_line: int = 1
    end_column: int = 2

    @classmethod
    def at(
        cls,
        node: ast.AST,
        *,
        severity: str,
        stage: str,
        code: str,
        message: str,
    ) -> Diagnostic:
        line = getattr(node, "lineno", 1) or 1
        col = getattr(node, "col_offset", 0) or 0
        end_line = getattr(node, "end_lineno", None) or line
        end_col = getattr(node, "end_col_offset", None)
        if end_col is None:
            end_col = col + 1
        return cls(
            severity=severity,
            stage=stage,
            code=code,
            message=message,
            line=line,
            column=col + 1,
            end_line=end_line,
            end_column=end_col + 1,
        )

    def to_json(self) -> dict[str, Any]:
        return {
            "severity": self.severity,
            "stage": self.stage,
            "code": self.code,
            "message": self.message,
            "line": self.line,
            "column": self.column,
            "end_line": self.end_line,
            "end_column": self.end_column,
        }


_WALL_CLOCK = (
    "reads the wall clock. The engine's clock is `ctx.now` (epoch ms). A strategy that "
    "reads real time behaves differently in a backtest than in live, and in a backtest it "
    "is reading a time the data has not reached yet — the definition of look-ahead "
    "(spec 5.3)."
)
_NETWORK = (
    "reaches the network. A run must be reproducible from its recorded inputs alone "
    "(spec 12.1); a strategy that fetches something produces a different result every time "
    "the other end changes, with nothing in the run record to show it. Data the strategy "
    "needs belongs in `requires['datasets']`."
)
_RANDOMNESS = (
    "uses unseeded randomness. Use `ctx.rng`, a `random.Random` derived from the run's seed "
    "(spec 5.5), so the same seed reproduces the same run. Module-level `random` shares "
    "state with everything else in the worker process, so even seeding it is not enough."
)
_FILESYSTEM = (
    "touches the filesystem. Whatever it reads is not part of the run's recorded inputs, so "
    "the run cannot be reproduced from them (spec 12.1), and whatever it writes escapes the "
    "run directory."
)
_SUBPROCESS = (
    "starts a process. Strategy isolation is a stability boundary (spec 2.3): a child "
    "process outlives the worker that spawned it and is not covered by the run's timeout."
)
_DYNAMIC = (
    "executes code built at runtime. Nothing about the run can be validated ahead of time "
    "if the code that runs is assembled while it runs, and the version hash recorded with "
    "the run would no longer describe what executed (spec 12.1)."
)
_CREDENTIALS = (
    "reads the process environment, which is a credential boundary rather than a "
    "reproducibility one. The process that runs a live session holds the exchange API "
    "secret, and `PERPLAB_PASSWORD` and any exported exchange keys are in that environment "
    "(spec 11) — a strategy that reads it is reading the key, whatever it does next. "
    "Configuration a strategy needs is declared in `params`; data it needs is declared in "
    "`requires['datasets']`. Neither of them travels in an environment variable."
)
_INTROSPECTION = (
    "reaches around the module system. Every module the interpreter has ever loaded is "
    "reachable from here — including `socket` — so it re-opens by another name every rule "
    "below (spec 11, spec 2.3). Whatever the strategy needs, `ctx` has it or the platform "
    "is missing a feature; say which."
)
_BLOCKING = (
    "blocks the worker. A backtest worker has a wall-clock timeout; sleeping inside it "
    "spends that budget doing nothing and makes a run's duration depend on the strategy "
    "rather than on the data."
)

BANNED_MODULES: dict[str, tuple[str, str]] = {
    "random": ("unseeded-random", _RANDOMNESS),
    "secrets": ("unseeded-random", _RANDOMNESS),
    "requests": ("network", _NETWORK),
    "httpx": ("network", _NETWORK),
    "urllib": ("network", _NETWORK),
    "urllib.request": ("network", _NETWORK),
    "http": ("network", _NETWORK),
    "http.client": ("network", _NETWORK),
    "socket": ("network", _NETWORK),
    "ftplib": ("network", _NETWORK),
    "smtplib": ("network", _NETWORK),
    "asyncio": ("network", _NETWORK),
    "websockets": ("network", _NETWORK),
    "socketserver": ("network", _NETWORK),
    "subprocess": ("subprocess", _SUBPROCESS),
    "multiprocessing": ("subprocess", _SUBPROCESS),
    "shutil": ("filesystem", _FILESYSTEM),
    "pathlib": ("filesystem", _FILESYSTEM),
    "tempfile": ("filesystem", _FILESYSTEM),
    "sqlite3": ("filesystem", _FILESYSTEM),
    "pickle": ("filesystem", _FILESYSTEM),
    "io": ("filesystem", _FILESYSTEM),
    "marshal": ("dynamic-code", _DYNAMIC),
    "runpy": ("dynamic-code", _DYNAMIC),
    "ctypes": ("subprocess", _SUBPROCESS),
    "builtins": ("dynamic-code", _INTROSPECTION),
    "importlib": ("dynamic-code", _INTROSPECTION),
    "inspect": ("dynamic-code", _INTROSPECTION),
    "gc": ("dynamic-code", _INTROSPECTION),
    "pdb": ("blocking", "stops the worker in a debugger nobody is attached to"),
}
"""Modules rejected on import, by `banned_module_rule` and again at runtime.

Rejected at the `import` rather than at the call, so the diagnostic lands on the line the
author can delete. `os` is deliberately absent -- `os.path.join` is harmless and common --
and its dangerous members are listed in `BANNED_ATTRIBUTES` instead.

The entries that are not about reproducibility are here because each one is a *rename* of
something else on this list, and a list that can be defeated by renaming is decoration:

- `io.open` and `builtins.open` are `open`, which `BANNED_BUILTINS` rejects.
- `importlib.import_module("socket")` is `import socket`, spelled as a string so no static
  rule can read it. Banning the module is the only spelling-independent answer available
  to a scanner, and `loader`'s import guard is what makes it stick.
- `inspect` and `gc` walk frames and the object graph, which is how a strategy sharing an
  interpreter with a live session's API secret would reach it (spec 11). Neither has any
  business in a strategy.
- `marshal` and `runpy` execute code from bytes and from a module path respectively.
- `socketserver` is `socket` with a loop around it.
- `pdb` blocks the worker on a prompt, and `pdb.run` execs a string besides.

This list is shared, not copied: `strategy.loader` imports it and re-checks it inside a
guarded `__import__`, so a module reached under any alias is refused at runtime too. Adding
a name here therefore fixes both halves at once, which is the point.
"""

BANNED_ATTRIBUTES: dict[str, tuple[str, str]] = {
    "time.time": ("wall-clock", _WALL_CLOCK),
    "time.time_ns": ("wall-clock", _WALL_CLOCK),
    "time.monotonic": ("wall-clock", _WALL_CLOCK),
    "time.monotonic_ns": ("wall-clock", _WALL_CLOCK),
    "time.perf_counter": ("wall-clock", _WALL_CLOCK),
    "time.perf_counter_ns": ("wall-clock", _WALL_CLOCK),
    "time.localtime": ("wall-clock", _WALL_CLOCK),
    "time.gmtime": ("wall-clock", _WALL_CLOCK),
    "time.sleep": ("blocking", _BLOCKING),
    "datetime.datetime.now": ("wall-clock", _WALL_CLOCK),
    "datetime.datetime.today": ("wall-clock", _WALL_CLOCK),
    "datetime.datetime.utcnow": ("wall-clock", _WALL_CLOCK),
    "datetime.date.today": ("wall-clock", _WALL_CLOCK),
    "os.system": ("subprocess", _SUBPROCESS),
    "os.popen": ("subprocess", _SUBPROCESS),
    "os.remove": ("filesystem", _FILESYSTEM),
    "os.unlink": ("filesystem", _FILESYSTEM),
    "os.rmdir": ("filesystem", _FILESYSTEM),
    "os.makedirs": ("filesystem", _FILESYSTEM),
    "os.environ": ("credentials", _CREDENTIALS),
    "os.environb": ("credentials", _CREDENTIALS),
    "os.getenv": ("credentials", _CREDENTIALS),
    "os.getenvb": ("credentials", _CREDENTIALS),
    "os.putenv": ("credentials", _CREDENTIALS),
    "os.unsetenv": ("credentials", _CREDENTIALS),
    "numpy.random": ("unseeded-random", _RANDOMNESS),
    "sys.exit": ("blocking", "ends the worker process rather than the run"),
    "sys.modules": ("dynamic-code", _INTROSPECTION),
}

BANNED_ATTRIBUTE_STEMS: dict[str, tuple[str, str]] = {
    "os.exec": ("subprocess", _SUBPROCESS),
    "os.spawn": ("subprocess", _SUBPROCESS),
    "os.posix_spawn": ("subprocess", _SUBPROCESS),
    "os.fork": ("subprocess", _SUBPROCESS),
    "os.kill": ("subprocess", _SUBPROCESS),
}
"""Whole families of `os` process calls, matched by name prefix.

Listing members one at a time is how `os.execv` came to be rejected while `os.execve`,
`os.execvp`, `os.spawnv` and `os.posix_spawn` were not -- eleven ways to start a process,
one of them on the list, and the list read as though it covered them. A prefix covers the
family including whatever CPython adds next, which is the only version of this that stays
true. Nothing legitimate in `os` begins with `exec`, `spawn`, `fork` or `kill`.
"""

BANNED_BUILTINS: dict[str, tuple[str, str]] = {
    "open": ("filesystem", _FILESYSTEM),
    "eval": ("dynamic-code", _DYNAMIC),
    "exec": ("dynamic-code", _DYNAMIC),
    "compile": ("dynamic-code", _DYNAMIC),
    "__import__": ("dynamic-code", _DYNAMIC),
    "vars": ("dynamic-code", _INTROSPECTION),
    "globals": ("dynamic-code", _INTROSPECTION),
    "input": ("blocking", "waits for input that will never arrive in a worker process"),
    "breakpoint": ("blocking", "stops the worker in a debugger nobody is attached to"),
}
"""Builtins rejected by name, and *absent* from the dict strategy code runs against.

`vars` and `globals` are here because `vars(os)["environ"]` and `globals()["os"]` reach
exactly what `BANNED_ATTRIBUTES` rejects, through a call whose argument no attribute rule
can see. `getattr(os, "environ")` still works and always will -- `getattr` cannot be taken
away from a strategy that legitimately needs it -- which is the honest measure of how far
this list goes. It removes the easy spellings; it does not remove the capability.
"""


def banned_module_rule(module: str) -> tuple[str, str] | None:
    """The `BANNED_MODULES` rule that refuses `module`, or `None`.

    One function, called by the scanner on an `import` statement and by `loader`'s guarded
    `__import__` on the module actually being imported, so "is this module allowed" has a
    single answer in the two places that ask.

    **Every dotted prefix and every segment counts.** A prefix because `urllib.request` is
    `urllib`; a segment because `numpy.random` is `random`, and a check that only walked
    prefixes let `import numpy.random` through while rejecting `import random` -- the same
    module, reached one dot deeper. The cost is that a package with an unluckily-named
    submodule (`scipy.io`) is refused for the wrong reason; the cases where that is a false
    positive are ones where the submodule really does do file I/O, so it is a cost worth
    paying.
    """
    parts = [part for part in module.split(".") if part]
    for depth in range(1, len(parts) + 1):
        rule = BANNED_MODULES.get(".".join(parts[:depth]))
        if rule is not None:
            return rule
    for part in parts:
        rule = BANNED_MODULES.get(part)
        if rule is not None:
            return rule
    return None


class _Scanner(ast.NodeVisitor):
    """A scoped walk over the module.

    **Scopes are a stack, not a flat set.** An earlier draft tracked local bindings in one
    module-wide set, which was wrong in both directions and badly so. A perfectly ordinary
    `open = bar.open` in one method disabled the `open()` rule for every *later* method in
    the file — a scanner that silently stops scanning is worse than no scanner, because it
    is still trusted. And in the other direction, only `=`, `:=`-free annotations and `for`
    targets were treated as bindings, so a comprehension variable, a `with ... as`, an
    `except ... as`, a `lambda` parameter or a nested `def` named after a rejected builtin
    was reported as a violation.

    Both are the same defect: bindings have scope and this has to model it. Python's own
    rules are followed — a `def` or `lambda` opens a scope, a class body opens one that
    nested functions do not inherit, and a comprehension has its own — so a name is "local"
    only where Python would agree it is.
    """

    def __init__(self) -> None:
        self.diagnostics: list[Diagnostic] = []
        self._module_aliases: dict[str, str] = {}
        self._name_bindings: dict[str, str] = {}
        # Innermost last. The module scope is index 0, is never popped, and is not a class.
        self._scopes: list[tuple[set[str], bool]] = [(set(), False)]

    # -------------------------------------------------------------------- scoping

    def _bind(self, name: str) -> None:
        """Record `name` as a local in the current scope."""
        self._scopes[-1][0].add(name)

    def _is_local(self, name: str) -> bool:
        """Python's own resolution order, including the class-body exception.

        A function does **not** see the names of an enclosing *class* body: a `class`
        attribute called `open` does not make `open(...)` inside a method refer to it. The
        innermost scope always counts (a class body sees its own names), and outer class
        scopes are skipped — which is exactly what CPython does, and getting it wrong here
        would suppress a real finding in every method of a class with an unluckily-named
        attribute.
        """
        for index in range(len(self._scopes) - 1, -1, -1):
            names, is_class = self._scopes[index]
            if is_class and index != len(self._scopes) - 1:
                continue
            if name in names:
                return True
        return False

    def _push_scope(self, *, is_class: bool = False) -> None:
        self._scopes.append((set(), is_class))

    def _pop_scope(self) -> None:
        self._scopes.pop()

    def _bind_targets(self, target: ast.AST) -> None:
        """Bind every `Name` written by an assignment target.

        Only `Store`/`Del` contexts count. Walking every `Name` regardless -- as an earlier
        version did -- meant `self.buf[np.float64(x)] = 1` bound `np` as a local and
        disabled the numpy rules from there on, because `np` appears inside the target
        subscript in a `Load` context.
        """
        for node in ast.walk(target):
            if isinstance(node, ast.Name) and isinstance(node.ctx, (ast.Store, ast.Del)):
                self._bind(node.id)

    # ------------------------------------------------------------------ reporting

    def _report(
        self, node: ast.AST, code: str, subject: str, reason: str, *, severity: str = "error"
    ) -> None:
        self.diagnostics.append(
            Diagnostic.at(
                node,
                severity=severity,
                stage="scan",
                code=code,
                message=f"`{subject}` {reason}",
            )
        )

    # -------------------------------------------------------------------- imports

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            if alias.asname:
                local = alias.asname
                self._module_aliases[local] = alias.name
            else:
                # `import a.b` binds the *root* name `a`, not `a.b`.
                local = alias.name.split(".")[0]
                self._module_aliases[local] = local
            # An import binds a name like any other statement. Without this, `import json
            # as open` leaves `open` looking like the builtin and every later use of it is
            # reported as a filesystem access.
            self._bind(local)
            self._check_module(node, alias.name)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if node.level:
            self.diagnostics.append(
                Diagnostic.at(
                    node,
                    severity="error",
                    stage="scan",
                    code="relative-import",
                    message=(
                        "a strategy is a single standalone module, so a relative import "
                        "has no package to resolve against. Import from `perplab` or from "
                        "the standard library."
                    ),
                )
            )
            self.generic_visit(node)
            return

        module = node.module or ""
        self._check_module(node, module)
        for alias in node.names:
            if alias.name == "*":
                self.diagnostics.append(
                    Diagnostic.at(
                        node,
                        severity="warning",
                        stage="scan",
                        code="star-import",
                        message=(
                            f"`from {module} import *` hides which names the strategy "
                            "actually uses, so neither this scanner nor a reader can tell "
                            "whether a rejected one is among them."
                        ),
                    )
                )
                continue
            local = alias.asname or alias.name
            self._name_bindings[local] = f"{module}.{alias.name}" if module else alias.name
            self._bind(local)
            # Checked at the import as well as at every use. `from time import time` cannot
            # be used for anything legitimate, so the import line is already the mistake and
            # it is the line the author can delete.
            self._check_attribute_path(node, self._name_bindings[local])

    def _check_module(self, node: ast.AST, module: str) -> None:
        if not module:
            return
        rule = banned_module_rule(module)
        if rule is not None:
            code, reason = rule
            self._report(node, code, module, reason)

    # ------------------------------------------------------------------- bindings

    def visit_Assign(self, node: ast.Assign) -> None:
        # The value is visited first: `time = time.time()` must still flag the call on the
        # right of the `=` before the name on the left becomes a local.
        self.visit(node.value)
        for target in node.targets:
            self._bind_targets(target)
        self._track_alias(node.targets, node.value)

    def _track_alias(self, targets: list[ast.expr], value: ast.expr) -> None:
        """Follow `e = os` so that `e.environ` resolves to `os.environ`.

        Without this, every rule keyed on a module name was one line of indirection away
        from being switched off -- `import os` passes, `e = os` passes, and `e.environ`
        looked like an attribute of an unknown local. That is not a theoretical evasion; it
        is the shortest one, and a scanner defeated by an assignment is a scanner that
        reports "clean" about code it did not understand.

        Only the tractable shape: exactly one target, and that target a plain name bound to
        a module reference the scanner is already following. `a, b = os, sys` and
        `e = mods["os"]` are not followed, and `loader`'s import guard is what covers those
        -- it re-checks the module by identity at import time, whatever it is later called.

        A name assigned something that is *not* a module reference has its alias dropped, so
        `os = compute()` later in the file stops resolving to the module. Getting that
        backwards would report findings against a name that no longer refers to a module,
        which is a false positive on innocent code -- worse than the miss it prevents.
        """
        if len(targets) != 1 or not isinstance(targets[0], ast.Name):
            return
        name = targets[0].id
        source = self._alias_source(value)
        if source is not None:
            self._module_aliases[name] = source
        else:
            self._module_aliases.pop(name, None)
            self._name_bindings.pop(name, None)

    def _alias_source(self, value: ast.expr) -> str | None:
        """The dotted module path `value` refers to, if it refers to one at all."""
        if isinstance(value, (ast.Name, ast.Attribute)):
            dotted = value.id if isinstance(value, ast.Name) else _dotted(value)
            if dotted is None:
                return None
            root = dotted.partition(".")[0]
            if root in self._module_aliases or root in self._name_bindings:
                return self._resolve(dotted)
        return None

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if node.value is not None:
            self.visit(node.value)
        self._bind_targets(node.target)

    def visit_AugAssign(self, node: ast.AugAssign) -> None:
        self.visit(node.value)
        self._bind_targets(node.target)

    def visit_NamedExpr(self, node: ast.NamedExpr) -> None:
        self.visit(node.value)
        self._bind_targets(node.target)

    def visit_For(self, node: ast.For) -> None:
        self._check_set_iteration(node.iter)
        self.visit(node.iter)
        self._bind_targets(node.target)
        for statement in node.body + node.orelse:
            self.visit(statement)

    visit_AsyncFor = visit_For

    def _check_set_iteration(self, iterable: ast.expr) -> None:
        """Warn when a loop iterates a `set`, which has no order.

        The static half of the determinism check, and it exists because the dynamic half
        cannot be made airtight. The probe compares runs under different `PYTHONHASHSEED`
        values, but whether two interpreters order a *particular* small set differently is
        close to a coin flip — so a two-element set, the most likely shape, can slip
        through however many seeds are tried.

        This catches the shape directly and it is the shape that matters: iterating a set
        is where the order becomes *observable*, because it decides what the strategy does
        first. Building one, testing membership, or taking a union are all order-free and
        are not flagged.

        A warning rather than an error. A loop whose body is order-independent — summing,
        counting, setting a flag — is perfectly correct, and there is no way to tell from
        here. `sorted()` costs nothing and removes the question.
        """
        if isinstance(iterable, (ast.Set, ast.SetComp)):
            kind = "set literal" if isinstance(iterable, ast.Set) else "set comprehension"
        elif (
            isinstance(iterable, ast.Call)
            and isinstance(iterable.func, ast.Name)
            and iterable.func.id in ("set", "frozenset")
            and not self._is_local(iterable.func.id)
        ):
            kind = f"{iterable.func.id}()"
        else:
            return
        self.diagnostics.append(
            Diagnostic.at(
                iterable,
                severity="warning",
                stage="scan",
                code="set-iteration",
                message=(
                    f"iterating a {kind}: a set has no order, and its iteration order "
                    "changes between processes because Python salts string hashing. Two "
                    "runs of the same seed would then do things in different orders, which "
                    "spec 12.1 states as an invariant they must not. Wrap it in `sorted()`, "
                    "or use a list. Harmless if the loop body is order-independent."
                ),
            )
        )

    def visit_With(self, node: ast.With) -> None:
        for item in node.items:
            self.visit(item.context_expr)
            if item.optional_vars is not None:
                self._bind_targets(item.optional_vars)
        for statement in node.body:
            self.visit(statement)

    visit_AsyncWith = visit_With

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:
        if node.type is not None:
            self.visit(node.type)
        if node.name:
            self._bind(node.name)
        for statement in node.body:
            self.visit(statement)

    def visit_Global(self, node: ast.Global) -> None:
        for name in node.names:
            self._bind(name)

    visit_Nonlocal = visit_Global  # type: ignore[assignment]

    def _visit_scoped_function(
        self, node: ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda
    ) -> None:
        # Decorators and default values are evaluated in the *enclosing* scope.
        for decorator in getattr(node, "decorator_list", []):
            self.visit(decorator)
        args = node.args
        for default in [*args.defaults, *[d for d in args.kw_defaults if d is not None]]:
            self.visit(default)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            self._bind(node.name)

        self._push_scope()
        try:
            for arg in [
                *args.posonlyargs,
                *args.args,
                *args.kwonlyargs,
                *([args.vararg] if args.vararg else []),
                *([args.kwarg] if args.kwarg else []),
            ]:
                self._bind(arg.arg)
            if isinstance(node, ast.Lambda):
                self.visit(node.body)
            else:
                for statement in node.body:
                    self.visit(statement)
        finally:
            self._pop_scope()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._visit_scoped_function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._visit_scoped_function(node)

    def visit_Lambda(self, node: ast.Lambda) -> None:
        self._visit_scoped_function(node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        for decorator in node.decorator_list:
            self.visit(decorator)
        for base in [*node.bases, *[kw.value for kw in node.keywords]]:
            self.visit(base)
        self._bind(node.name)
        # A class body is a scope, and one that nested functions do *not* inherit. Pushing
        # it means `params = {...}` at class level does not make `params` a local inside
        # `on_bar`, which is what Python does too.
        self._push_scope(is_class=True)
        try:
            for statement in node.body:
                self.visit(statement)
        finally:
            self._pop_scope()

    def _visit_comprehension(
        self, node: ast.ListComp | ast.SetComp | ast.DictComp | ast.GeneratorExp
    ) -> None:
        # The outermost iterable is evaluated in the enclosing scope; everything else
        # belongs to the comprehension's own.
        generators = node.generators
        if generators:
            self.visit(generators[0].iter)
        if generators:
            self._check_set_iteration(generators[0].iter)
        self._push_scope()
        try:
            for index, generator in enumerate(generators):
                self._bind_targets(generator.target)
                if index:
                    self.visit(generator.iter)
                for condition in generator.ifs:
                    self.visit(condition)
            if isinstance(node, ast.DictComp):
                self.visit(node.key)
                self.visit(node.value)
            else:
                self.visit(node.elt)
        finally:
            self._pop_scope()

    def visit_ListComp(self, node: ast.ListComp) -> None:
        self._visit_comprehension(node)

    def visit_SetComp(self, node: ast.SetComp) -> None:
        self._visit_comprehension(node)

    def visit_DictComp(self, node: ast.DictComp) -> None:
        self._visit_comprehension(node)

    def visit_GeneratorExp(self, node: ast.GeneratorExp) -> None:
        self._visit_comprehension(node)

    # ------------------------------------------------------------------ references

    def visit_Attribute(self, node: ast.Attribute) -> None:
        dotted = _dotted(node)
        if dotted is not None:
            root = dotted.partition(".")[0]
            self._check_attribute_path(
                node,
                self._resolve(dotted),
                # Only treat a leading segment as a module when it was actually imported
                # as one. Without this, a strategy that writes `random = ctx.rng` and then
                # `random.random()` is flagged for using the `random` *module* it never
                # imported -- a false positive on code doing exactly what the rule asks.
                module_rooted=root in self._module_aliases or root in self._name_bindings,
            )
        self.generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:
        if not isinstance(node.ctx, ast.Load):
            self.generic_visit(node)
            return
        bound = self._name_bindings.get(node.id)
        if bound is not None:
            self._check_attribute_path(node, bound)
        elif node.id in BANNED_BUILTINS and not self._is_local(node.id):
            code, reason = BANNED_BUILTINS[node.id]
            self._report(node, code, node.id, reason)
        self.generic_visit(node)

    def _resolve(self, dotted: str) -> str:
        root, _, rest = dotted.partition(".")
        base = self._name_bindings.get(root) or self._module_aliases.get(root)
        if base is None:
            return dotted
        return f"{base}.{rest}" if rest else base

    def _check_attribute_path(
        self, node: ast.AST, path: str, *, module_rooted: bool = True
    ) -> None:
        rule = BANNED_ATTRIBUTES.get(path)
        if rule is not None:
            code, reason = rule
            self._report(node, code, path, reason)
            return
        # A banned attribute reached through a longer path: `numpy.random.default_rng`
        # resolves to `numpy.random.default_rng`, whose prefix `numpy.random` is banned.
        for banned, (code, reason) in BANNED_ATTRIBUTES.items():
            if path.startswith(banned + "."):
                self._report(node, code, path, reason)
                return
        # Whole families -- `os.exec*`, `os.spawn*` -- matched on the name, not the dot.
        for stem, (code, reason) in BANNED_ATTRIBUTE_STEMS.items():
            if path.startswith(stem):
                self._report(node, code, path, reason)
                return
        if not module_rooted:
            return
        parts = path.split(".")
        for depth in range(1, len(parts)):
            prefix = ".".join(parts[:depth])
            module_rule = BANNED_MODULES.get(prefix)
            if module_rule is not None:
                code, reason = module_rule
                self._report(node, code, path, reason)
                return


def _dotted(node: ast.AST) -> str | None:
    """Flatten an attribute chain to `a.b.c`, or `None` if it is not a plain chain."""
    parts: list[str] = []
    current = node
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if isinstance(current, ast.Name):
        parts.append(current.id)
        return ".".join(reversed(parts))
    return None


def scan_source(tree: ast.AST) -> list[Diagnostic]:
    """Run every static rule over a parsed strategy module.

    Diagnostics come back sorted by position rather than by rule, because that is the order
    the author reads the file in and the order the gutter shows them.
    """
    scanner = _Scanner()
    scanner.visit(tree)

    # One finding per (line, rule). An attribute chain reports from the outside in, so
    # `numpy.random.default_rng()` matches the banned prefix at the outer node and again at
    # the inner `numpy.random` -- two markers on one line for one mistake, of which the
    # first carries the fuller path. Visit order is outermost-first, so keeping the first
    # keeps the better message.
    seen: set[tuple[int, str]] = set()
    unique: list[Diagnostic] = []
    for diagnostic in scanner.diagnostics:
        key = (diagnostic.line, diagnostic.code)
        if key in seen:
            continue
        seen.add(key)
        unique.append(diagnostic)
    return sorted(unique, key=lambda d: (d.line, d.column, d.code))

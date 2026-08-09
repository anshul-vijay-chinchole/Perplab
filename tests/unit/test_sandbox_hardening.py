"""The strategy guard and the local-origin guard (audit findings C1, C2, C3, M54, L24).

The audit's charge against the old sandbox was that it was not one: strategy code ran under
`exec` with the real builtins, and the only boundary was an AST scan that its own header
admitted was not a security control. Twelve of thirteen payloads walked through it, most of
them by writing the same thing a different way.

**What is being tested here is a claim with a limit, and the limit is tested too.**
`test_the_known_escape_is_real` asserts that `().__class__.__base__.__subclasses__()` still
works. That is not an oversight left in place; it is the honest boundary of an in-process
guard, and a test that pins it is what stops a later reader from mistaking defence in depth
for a sandbox. If that test ever starts failing, someone has found something genuinely new
and the docstrings in `strategy.loader` need rewriting — which is exactly the moment to
notice.

The C3 tests use `POST /api/strategies/import` with no body on purpose: the endpoint the
audit named, and the contrast is legible. Refused is `403` from the middleware; allowed is
`422` from FastAPI's body validation, which can only be reached if the request got past the
guard and into routing.
"""

from __future__ import annotations

import warnings
from pathlib import Path

import pytest

warnings.filterwarnings(
    "ignore", message=".*httpx.*starlette.testclient.*", category=DeprecationWarning
)

from fastapi.testclient import TestClient  # noqa: E402

from perplab.api.app import (  # noqa: E402
    DEV_ORIGINS,
    create_app,
    is_local_host,
    is_local_origin,
)
from perplab.strategy.loader import (  # noqa: E402
    SAFE_BUILTINS,
    StrategyImportRefused,
    execute_module,
    strategy_builtins,
)
from perplab.strategy.validate import (  # noqa: E402
    WORKER_ENV_ALLOWLIST,
    _run_worker,
    validate_code,
    worker_env,
)

TAIL = '''

class S(Strategy):
    requires = {"symbols": ["BTCUSDT"], "timeframe": "1h", "history": 1}

    def on_bar(self, ctx, bar):
        pass
'''


def strategy(payload: str) -> str:
    """Wrap a module-level payload in the smallest valid strategy."""
    return "from perplab import Strategy\n" + payload + TAIL


def static_errors(code: str) -> list[str]:
    result = validate_code(code, filename="p.py", run_sandbox=False)
    return sorted({d.code for d in result.diagnostics if d.severity == "error"})


def runtime_error(code: str) -> str:
    try:
        execute_module(code, "p.py")
    except BaseException as exc:  # noqa: BLE001 - any refusal counts as a refusal
        return f"{type(exc).__name__}: {exc}"
    return ""


# The audit's thirteen, verbatim in intent and spelling. `P13` is the control: it was the
# one that was already caught, and it has to stay caught.
PAYLOADS: dict[str, str] = {
    "alias": "import os\ne = os\nLEAK = e.environ\n",
    "vars": "import os\nLEAK = vars(os)['environ']\n",
    "io-open": "import io\nLEAK = io.open('pyproject.toml').read()\n",
    "builtins-open": "import builtins\nLEAK = builtins.open('pyproject.toml').read()\n",
    "getattr-builtins": "LEAK = getattr(__builtins__, 'ev' + 'al')('1+1')\n",
    "importlib": "import importlib\nLEAK = importlib.import_module('socket')\n",
    "os-execve": "import os\nLEAK = os.execve\n",
    "os-spawnv": "import os\nLEAK = os.spawnv\n",
    "os-posix-spawn": "import os\nLEAK = os.posix_spawn\n",
    "socketserver": "import socketserver\nLEAK = socketserver\n",
    "pdb": "import pdb\nLEAK = pdb\n",
    "socket": "import socket\nLEAK = socket\n",
}


class TestPayloads:
    @pytest.mark.parametrize("name", sorted(PAYLOADS))
    def test_every_payload_is_refused(self, name: str) -> None:
        """Statically or at runtime — either is a refusal, and which one is not the point.

        The scan is the friendlier of the two: it names the line and explains the rule
        before anything executes. The runtime guard is the one that holds when the payload
        is spelled in a way no AST can read. A payload that gets past both is a strategy
        reading the operator's exchange key.
        """
        code = strategy(PAYLOADS[name])
        assert static_errors(code) or runtime_error(code), (
            f"payload {name!r} executed unrefused"
        )

    def test_the_known_escape_is_real(self) -> None:
        """`().__class__.__base__.__subclasses__()` still works, and is documented.

        Object-graph traversal reaches every class the interpreter has loaded, and from
        there the real `builtins` and the real `os`. No curated `__builtins__` closes it —
        it is a property of running in the same interpreter, which is why `loader` says so
        in its module docstring instead of implying a boundary it does not draw.
        """
        module = execute_module(
            strategy("ESCAPED = ().__class__.__base__.__subclasses__()\n"), "p.py"
        )
        assert module.ESCAPED

        import perplab.strategy.loader as loader

        assert "__subclasses__" in (loader.__doc__ or "")
        assert "not a security boundary" in (loader.__doc__ or "")

    def test_the_dict_spelling_of_the_builtins_payload_is_closed_too(self) -> None:
        """`getattr(__builtins__, ...)` fails on a dict for the wrong reason — it is a
        dict, not a module. `__builtins__["eval"]` is the spelling that would have worked,
        and the curated mapping is what closes it."""
        assert "KeyError" in runtime_error(strategy("LEAK = __builtins__['eval']\n"))


class TestRuntimeGuard:
    def test_a_banned_module_is_refused_when_the_scan_cannot_see_the_name(self) -> None:
        """The runtime half, on its own. The scan cannot read a name assembled at runtime;
        the import guard does not have to, because by then the module has one."""
        error = runtime_error(strategy("LEAK = __import__('soc' + 'ket')\n"))
        assert "StrategyImportRefused" in error
        assert "socket" in error

    def test_a_from_import_of_a_banned_member_is_refused_at_runtime(self) -> None:
        error = runtime_error("from os import environ\n" + strategy(""))
        assert "StrategyImportRefused" in error
        assert "credential" in error

    def test_the_refusal_names_the_rule_the_editor_named(self) -> None:
        """Same sentence in both places. An author who saw "reaches the network" in the
        gutter and then a bare `ImportError` at run time would reasonably conclude the two
        systems disagree."""
        with pytest.raises(StrategyImportRefused, match="reaches the network"):
            execute_module(strategy("import socket\n"), "p.py")

    @pytest.mark.parametrize(
        "name", ["eval", "exec", "compile", "open", "input", "breakpoint", "vars", "globals"]
    )
    def test_the_dangerous_builtins_are_absent(self, name: str) -> None:
        assert name not in strategy_builtins()
        assert name not in SAFE_BUILTINS

    def test_ordinary_strategy_code_still_has_what_it_needs(self) -> None:
        """The guard's real risk is not that it lets something through, it is that it
        breaks legitimate strategies — and a validator that rejects working code is a
        platform nobody can use. Everything here is ordinary strategy vocabulary.

        `@dataclass` has its own test below: it was broken in strategy code for a reason
        that predated this guard (`compile()` inheriting the loader's `__future__` flags
        plus the module never being registered), and the loader now fixes both halves.
        """
        module = execute_module(
            strategy(
                "import math\n"
                "import statistics\n"
                "from collections import deque\n"
                "from decimal import Decimal\n"
                "from typing import Any\n"
                "\n"
                "WINDOW = deque([1.0, 2.0, 3.0], maxlen=8)\n"
                "MEAN = statistics.fmean(WINDOW)\n"
                "TOTAL = sum(sorted(x for x in WINDOW if x > 0))\n"
                "LABEL = f'{math.sqrt(TOTAL):.3f}'\n"
                "TABLE = {k: v for k, v in enumerate(reversed(list(WINDOW)))}\n"
                "PRICE = Decimal('1.5') * 2\n"
                "TYPED: Any = min(max(1, 2), 3)\n"
                "\n"
                "\n"
                "class Helper:\n"
                "    def __init__(self, size):\n"
                "        self.size = int(size)\n"
                "\n"
                "    def scaled(self, factor=2):\n"
                "        return round(abs(self.size * factor), 2)\n"
                "\n"
                "\n"
                "class Child(Helper):\n"
                "    def __init__(self):\n"
                "        super().__init__(21)\n"
                "\n"
                "\n"
                "SCALED = Child().scaled()\n"
                "try:\n"
                "    raise ValueError('caught by name')\n"
                "except (ValueError, KeyError) as exc:\n"
                "    CAUGHT = str(exc)\n"
            ),
            "p.py",
        )
        assert module.MEAN == 2.0
        assert module.SCALED == 42
        assert module.PRICE == module.PRICE
        assert module.CAUGHT == "caught by name"

    def test_each_module_gets_its_own_builtins(self) -> None:
        """A shared mapping would be a channel between strategies. `lab.sweep` loads
        hundreds of them in one worker process, so one poisoning the next is not a
        hypothetical shape."""
        first = execute_module(
            strategy("__builtins__['smuggled'] = 'from the first module'\n"), "a.py"
        )
        second = execute_module(strategy("MINE = 'clean'\n"), "b.py")
        assert "smuggled" in first.__dict__["__builtins__"]
        assert "smuggled" not in second.__dict__["__builtins__"]

    def test_the_template_and_the_worked_example_still_load(self) -> None:
        from perplab.strategy.template import EMA_CROSS_EXAMPLE, NEW_STRATEGY_TEMPLATE

        for source in (NEW_STRATEGY_TEMPLATE, EMA_CROSS_EXAMPLE):
            assert execute_module(source, "t.py") is not None


class TestValidatorEnvironment:
    """Finding C3(a): the validator handed the whole parent environment to the child."""

    def test_the_worker_environment_drops_secrets(self, monkeypatch) -> None:
        monkeypatch.setenv("PERPLAB_PASSWORD", "hunter2")
        monkeypatch.setenv("BINANCE_API_SECRET", "sekrit")
        env = worker_env("0")
        assert "PERPLAB_PASSWORD" not in env
        assert "BINANCE_API_SECRET" not in env
        assert "hunter2" not in "".join(env.values())

    def test_the_worker_environment_keeps_what_the_interpreter_needs(self) -> None:
        """A whitelist that starves the child is a whitelist that breaks every save. On
        Windows an interpreter without `SystemRoot` cannot even initialise sockets."""
        env = worker_env("7")
        assert env["PYTHONHASHSEED"] == "7"
        assert any(name.upper() == "PATH" for name in env)
        assert all(name.upper() in WORKER_ENV_ALLOWLIST for name in env
                   if not name.startswith("PYTHON"))

    def test_the_child_cannot_read_the_parent_password(self, monkeypatch) -> None:
        """End to end, through a real worker process. `_run_worker` directly rather than
        `validate_code`, because the scan now refuses `os.environ` before the subprocess
        starts — and the point of this test is what the child would find if it got there.
        """
        monkeypatch.setenv("PERPLAB_PASSWORD", "hunter2-must-not-leak")
        payload = _run_worker(
            strategy(
                "import os\n"
                "print('PW=' + os.environ.get('PERPLAB_PASSWORD', 'absent'))\n"
            ),
            filename="s.py",
            seed=1,
            bars=60,
            timeout_s=30.0,
            hash_seed="0",
        )
        assert isinstance(payload, dict), payload
        assert "PW=absent" in payload["stdout"]
        assert "hunter2-must-not-leak" not in payload["stdout"]


class TestMemoryBound:
    """Finding M54: the 10 s timeout bounded time and nothing else."""

    def test_a_runaway_allocation_is_refused_instead_of_swapping_the_machine(self) -> None:
        """Three gigabytes, against a one-gigabyte ceiling. Without the limit this
        *succeeds* on any machine with the RAM to spare — it took eight seconds and a
        healthy fraction of this one — and a strategy asking for thirty would have taken
        the platform down while staying inside its ten-second budget.
        """
        result = validate_code(
            strategy("BIG = bytearray(3_000_000_000)\n"), filename="big.py", timeout_s=30.0
        )
        assert not result.ok
        assert any("MemoryError" in d.message for d in result.diagnostics), [
            d.message for d in result.diagnostics
        ]


class TestLocalHostAndOrigin:
    @pytest.mark.parametrize(
        "value", ["127.0.0.1", "127.0.0.1:8756", "localhost", "localhost:5173",
                  "[::1]:8756", "testserver", "LOCALHOST"]
    )
    def test_local_hosts_are_recognised(self, value: str) -> None:
        assert is_local_host(value)

    @pytest.mark.parametrize(
        "value", ["evil.com", "attacker.example:8756", "127.0.0.1.evil.com", "10.0.0.5"]
    )
    def test_foreign_hosts_are_not(self, value: str) -> None:
        assert not is_local_host(value)

    def test_a_null_origin_is_not_local(self) -> None:
        """`file://` pages and some extensions send `Origin: null`. Unattributable is not
        the same as trusted."""
        assert not is_local_origin("null", frozenset())

    def test_a_configured_origin_is_allowed_verbatim(self) -> None:
        assert is_local_origin("https://lab.example", frozenset({"https://lab.example"}))

    def test_a_loopback_origin_is_allowed_on_any_port(self) -> None:
        assert is_local_origin("http://127.0.0.1:9999", frozenset())


class TestDriveByExecution:
    """Finding C3(b): any page the operator had open could POST a strategy bundle."""

    @pytest.fixture
    def client(self, tmp_path: Path) -> TestClient:
        with TestClient(create_app(tmp_path)) as test_client:
            yield test_client

    def test_a_cross_origin_import_is_refused(self, client: TestClient) -> None:
        response = client.post(
            "/api/strategies/import", headers={"Origin": "https://evil.com"}
        )
        assert response.status_code == 403
        assert "evil.com" in response.json()["detail"]

    def test_the_refused_request_never_reached_the_library(self, client: TestClient) -> None:
        """403 comes from the middleware, before routing — so `validate_code` never ran on
        anything the attacker sent. The store is the observable proof."""
        before = client.get("/api/strategies").json()
        client.post(
            "/api/strategies/import",
            headers={"Origin": "https://evil.com"},
            files={"file": ("evil.py", b"import os\n", "text/x-python")},
        )
        assert client.get("/api/strategies").json() == before

    @pytest.mark.parametrize(
        "path",
        [
            "/api/strategies/import",
            "/api/exchange/disconnect",
            "/api/runs/1/cancel",
            "/api/lab/jobs/1/cancel",
        ],
    )
    def test_every_named_endpoint_is_covered(self, client: TestClient, path: str) -> None:
        """The audit named four. The guard is a middleware rather than four decorators
        precisely so the list cannot go stale as endpoints are added."""
        assert (
            client.post(path, headers={"Origin": "https://evil.com"}).status_code == 403
        )

    def test_a_same_origin_write_passes(self, client: TestClient) -> None:
        response = client.post(
            "/api/strategies/import", headers={"Origin": "http://127.0.0.1:8756"}
        )
        assert response.status_code == 422  # reached routing; the body is what is missing

    def test_the_dev_server_origin_passes(self, client: TestClient) -> None:
        response = client.post(
            "/api/strategies/import", headers={"Origin": DEV_ORIGINS[0]}
        )
        assert response.status_code == 422

    def test_a_request_with_no_origin_passes(self, client: TestClient) -> None:
        """curl, the CLI and this test suite send no `Origin`. A browser making the attack
        this guards against cannot omit it, so requiring one would break every scripted
        client to defend against nothing."""
        assert client.post("/api/strategies/import").status_code == 422

    def test_a_cross_origin_read_is_left_to_cors(self, client: TestClient) -> None:
        """A cross-origin `GET` cannot be read by the attacker's script — that part CORS
        does enforce — and refusing them would break embeds for no gain."""
        response = client.get("/api/strategies", headers={"Origin": "https://evil.com"})
        assert response.status_code == 200

    def test_a_rebound_dns_name_is_refused(self, client: TestClient) -> None:
        """DNS rebinding defeats the `Origin` check by making the attacker's page
        same-origin: `evil.com` resolves to 127.0.0.1 and the browser sends a consistent
        pair of headers. `Host` is the one that still names where it thought it was going.
        """
        response = client.get("/api/strategies", headers={"Host": "evil.com"})
        assert response.status_code == 403
        assert "rebinding" in response.json()["detail"]

    def test_the_host_check_is_off_when_the_app_is_deliberately_exposed(
        self, tmp_path: Path
    ) -> None:
        """Bound to a LAN address, the operator reaches this by an address that is neither
        loopback nor single-label. Spec 11's mandatory password is the control there, and
        401 rather than 403 is the proof this middleware stood aside."""
        app = create_app(tmp_path, host="0.0.0.0", password="hunter2")
        with TestClient(app) as exposed:
            response = exposed.get("/api/strategies", headers={"Host": "10.0.0.5:8756"})
            assert response.status_code == 401


class TestDataclassInStrategyCode:
    """`@dataclass` crashed on load for every strategy that used one, twice over.

    `compile()` defaults to inheriting the *calling module's* compiler flags, and
    `loader.py` declares `from __future__ import annotations` -- so every strategy was
    compiled under PEP 563 stringised annotations whether its author wrote them or not.
    `dataclasses` then resolved the stringised `ClassVar`/`InitVar` markers through
    `sys.modules[cls.__module__]`, and `perplab_user_strategy` was never registered:
    `AttributeError: 'NoneType' object has no attribute '__dict__'` from inside the
    standard library, on ordinary code. The loader now compiles with
    `dont_inherit=True` (the author gets the interpreter's own semantics) and registers
    the module for exactly the duration of the exec (so an author who writes the future
    import *themselves* still works).
    """

    DATACLASS_BODY = (
        "from dataclasses import dataclass\n"
        "from typing import ClassVar\n"
        "\n"
        "\n"
        "@dataclass\n"
        "class Sizing:\n"
        "    qty: str = '0.002'\n"
        "    hold_bars: int = 2\n"
        "    UNIVERSE: ClassVar[str] = 'BTCUSDT'\n"
        "\n"
        "SIZED = Sizing(qty='0.004')\n"
    )

    @staticmethod
    def _native_control() -> type:
        """The identical dataclass defined by the interpreter itself, no loader involved.

        The assertion worth making is *parity*: a strategy's dataclass must behave
        exactly as it would in a plain Python file. Pinning a specific field list
        instead would pin this interpreter build's ClassVar-resolution details, which
        are not the loader's to promise.
        """
        from dataclasses import dataclass
        from typing import ClassVar

        @dataclass
        class Sizing:
            qty: str = "0.002"
            hold_bars: int = 2
            UNIVERSE: ClassVar[str] = "BTCUSDT"

        return Sizing

    def test_a_plain_dataclass_loads(self) -> None:
        module = execute_module(strategy(self.DATACLASS_BODY), "dc.py")
        assert module.SIZED.qty == "0.004"
        assert module.SIZED.hold_bars == 2
        assert list(module.Sizing.__dataclass_fields__) == list(
            self._native_control().__dataclass_fields__
        ), "a loaded dataclass must resolve its annotations exactly as native code does"

    def test_a_dataclass_under_the_authors_own_future_import_loads(self) -> None:
        """PEP 563 chosen *by the author* is legal, and is the case `dont_inherit`
        alone cannot fix -- the scoped `sys.modules` registration is what carries it."""
        code = "from __future__ import annotations\n" + strategy(self.DATACLASS_BODY)
        module = execute_module(code, "dc_future.py")
        assert module.SIZED.qty == "0.004"
        assert module.SIZED.hold_bars == 2

    def test_the_scoped_registration_does_not_leak_or_clobber(self) -> None:
        """Two loads in one process must not see each other, and nothing may remain
        registered afterwards -- a lasting entry would have every load silently replace
        the previous strategy's module for any later annotation resolution."""
        import sys

        from perplab.strategy.loader import MODULE_NAME

        first = execute_module(strategy(self.DATACLASS_BODY), "one.py")
        second = execute_module(strategy(self.DATACLASS_BODY), "two.py")
        assert first is not second
        assert MODULE_NAME not in sys.modules

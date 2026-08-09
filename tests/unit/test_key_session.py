"""Tests for the in-memory API key session (spec 11).

Spec 11 is unusually concrete about this object: keys live *only* in backend process
memory, are never written to disk, database, config file, log line, browser storage or
crash dump, and are wiped after 12 h of inactivity with positions deliberately left open.
Three of those clauses are testable as properties of the code rather than of anybody's
discipline, and that is what is pinned here:

- the signature is the exact HMAC-SHA256 the exchange will recompute, so a session that
  signs at all signs correctly;
- nothing about the credential appears in `repr`, `str` or `to_json`, which are the three
  ways an object accidentally becomes text;
- the module's own source contains no filesystem or logging name at all, which is a
  cruder check than a reviewer and a far more reliable one.

The last of those is worth stating plainly: it does not prove keys never reach disk, it
proves this module has no means to put them there. That is the strongest form the
guarantee takes without an OS-level sandbox, and it fails loudly on the one-line change
that would break it.
"""

from __future__ import annotations

import ast
import hashlib
import hmac
import time
from pathlib import Path
from typing import Any

import pytest

from perplab.core.money import money_to_str, parse_money
from perplab.exchange import keys as keys_module
from perplab.exchange.keys import KEY_SESSION_TTL_S, KeySession, KeySessionExpired

API_KEY = "vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A"
API_SECRET = "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1wi9UwyBGZQvcSCzz1WCU1KLUEMWQVfP"

QUERY = (
    "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1"
    "&recvWindow=5000&timestamp=1499827319559"
)

EXPECTED_SIGNATURE = "d724342ddcbb1998300aa1bf97c30448233d1172f1f883c55a75145e740251e9"
"""HMAC-SHA256(API_SECRET, QUERY), hex-encoded.

Derived from the two literals above and nothing else:

    hmac.new(API_SECRET.encode(), QUERY.encode(), hashlib.sha256).hexdigest()

Written out as a constant rather than computed at assert time so that the expected value
cannot move when `sign` does. `test_the_signature_is_hmac_sha256_of_the_exact_query_string`
checks both halves: that the constant really is that digest, and that `sign` produces it.
"""


def session(**kwargs: Any) -> KeySession:
    """A live session on the fixture credential."""
    return KeySession(API_KEY, API_SECRET, **kwargs)


class TestSigning:
    def test_the_signature_is_hmac_sha256_of_the_exact_query_string(self) -> None:
        """Spec 11: HMAC-SHA256 over the query string, with a timestamp and recvWindow.

        Two assertions because they fail for different reasons. The first pins the
        constant to its stated derivation, so a typo in the literal is caught rather than
        silently weakening the test. The second is the real check: `sign` must hash the
        bytes it was given with the secret it holds, and nothing else -- a signature over a
        re-encoded query comes back as a bare `-1022 Signature for this request is not
        valid` with nothing to say which parameter moved.
        """
        assert (
            hmac.new(API_SECRET.encode(), QUERY.encode(), hashlib.sha256).hexdigest()
            == EXPECTED_SIGNATURE
        )
        assert session().sign(QUERY) == EXPECTED_SIGNATURE

    def test_one_changed_character_changes_the_signature(self) -> None:
        """The query must be signed as bytes, not as a parsed structure.

        `quantity=1` and `quantity=1.0` are the same order and different strings, so an
        implementation that normalised before hashing would sign something the exchange
        never sees.
        """
        assert session().sign(QUERY.replace("quantity=1", "quantity=1.0")) != (
            EXPECTED_SIGNATURE
        )

    def test_a_different_secret_gives_a_different_signature(self) -> None:
        """Guards against a `sign` that hashed the key, or a constant, instead."""
        other = KeySession(API_KEY, API_SECRET[:-1] + "Q")
        assert other.sign(QUERY) != EXPECTED_SIGNATURE

    def test_signing_counts_as_use_and_restarts_the_idle_countdown(self) -> None:
        """The TTL is a timeout on activity, not a hard cap on a session's life.

        A live run reconciling every 60 s (spec 6.7) must therefore keep its own keys alive
        for as long as it runs -- expiring the credential out from under an open position
        would halt the run and leave the exposure standing.
        """
        live = session(ttl_s=100.0)
        live.touch(time.monotonic() - 99.0)
        assert live.expires_in_s < 2.0

        live.sign(QUERY)

        assert live.expires_in_s > 99.0

    def test_the_api_key_is_handed_out_as_bytes(self) -> None:
        """Every `str` built from the buffer is an object `wipe()` cannot reach.

        `httpx` takes a bytes header value, so the signed client never has to materialise
        one -- which is the only reason the buffer is scrubbable at all.
        """
        live = session()
        assert live.api_key_bytes == API_KEY.encode()
        assert isinstance(live.api_key_bytes, bytes)

    def test_handing_out_the_api_key_counts_as_use_and_restarts_the_countdown(
        self,
    ) -> None:
        """A keyed-but-unsigned request is activity too, and spec 11's "idle" must mean it.

        The listen-key create, keepalive and close send `X-MBX-APIKEY` with no signature
        (`signed.py` calls them with `signed=False`), so they read this property and never
        reach `sign`. A session whose only traffic is holding the user-data stream open is
        therefore invisible to a timer that only signing refreshes: its keys are wiped at
        twelve hours, the next keepalive fails, and the fill feed dies with the socket still
        healthy -- mid-position, on a session that was active the whole time. That the live
        path also signs every 60 s to reconcile (spec 6.7) hides this, but that is a
        coincidence of one caller rather than a property of the session.

        Same numbers as `test_signing_counts_as_use_and_restarts_the_idle_countdown`, so
        the two endpoints are pinned to the same rule: one second of life left before the
        read, a full hundred after it.
        """
        live = session(ttl_s=100.0)
        live.touch(time.monotonic() - 99.0)
        assert live.expires_in_s < 2.0

        _ = live.api_key_bytes

        assert live.expires_in_s > 99.0


class TestExpiry:
    def test_the_default_ttl_is_the_twelve_hours_spec_11_names(self) -> None:
        """12 h * 60 min * 60 s = 43200."""
        assert KEY_SESSION_TTL_S == 43_200

    def test_a_session_expires_once_it_has_idled_past_its_ttl(self) -> None:
        """Idle is measured from the last use, so the boundary is `touch + ttl`.

        With `touch(1000.0)` and `ttl_s=60.0`: at 1059.9 the session has idled 59.9 s and
        is live; at 1060.0 it has idled exactly 60.0 s and is gone. Expiry is reached
        rather than exceeded, which is the same convention `core.risk` uses for a limit
        that is a count or a loss.
        """
        live = session(ttl_s=60.0)
        live.touch(1000.0)

        assert not live.expired(1059.9)
        assert live.expired(1060.0)

    def test_touch_restarts_the_countdown(self) -> None:
        """Same numbers, one use in the middle: 1059.0 + 60.0 = 1119.0."""
        live = session(ttl_s=60.0)
        live.touch(1000.0)
        live.touch(1059.0)

        assert not live.expired(1118.9)
        assert live.expired(1119.0)

    def test_an_idled_session_refuses_to_sign_and_says_positions_are_still_open(
        self,
    ) -> None:
        """Spec 11 wipes keys and halts the run **with positions left open**.

        The message has to say so. An operator reading "session expired" and nothing else
        would reasonably assume the platform had flattened the account, and the one thing
        worse than an unattended position is an unattended position somebody believes was
        closed.
        """
        live = session(ttl_s=60.0)
        live.touch(time.monotonic() - 61.0)

        with pytest.raises(KeySessionExpired) as excinfo:
            live.sign(QUERY)

        message = str(excinfo.value)
        assert "idled past" in message
        assert "left" in message and "open" in message

    def test_the_countdown_is_floored_at_zero_rather_than_going_negative(self) -> None:
        """This figure is rendered in the UI.

        A countdown running past zero into negative numbers describes a session that is
        already gone as though it were still there.
        """
        live = session(ttl_s=60.0)
        live.touch(time.monotonic() - 600.0)

        assert live.expires_in_s == 0.0

    def test_the_constructor_refuses_a_non_positive_ttl(self) -> None:
        with pytest.raises(ValueError, match="ttl_s must be positive"):
            session(ttl_s=0.0)

    def test_the_constructor_refuses_an_empty_credential(self) -> None:
        """Half a credential is not a session that can be validated (spec 11)."""
        with pytest.raises(ValueError, match="required"):
            KeySession("", API_SECRET)
        with pytest.raises(ValueError, match="required"):
            KeySession(API_KEY, "")


class TestWipe:
    def test_wipe_zeroes_the_buffers_rather_than_dropping_them(self) -> None:
        """Rebinding alone leaves the bytes in the heap, and a core dump is one of the six
        places spec 11 names by hand.

        Reaching for the private buffers is the point of the test: the observable behaviour
        of a dropped reference and an overwritten one is identical, so only the buffer
        itself can distinguish them.
        """
        live = session()
        assert bytes(live._key) == API_KEY.encode()

        live.wipe()

        assert bytes(live._key) == b""
        assert bytes(live._secret) == b""

    def test_a_wiped_session_can_neither_sign_nor_hand_out_the_key(self) -> None:
        """This is what makes the kill switch's step 4 (spec 7) mean something."""
        live = session()
        live.wipe()

        with pytest.raises(KeySessionExpired, match="wiped"):
            live.sign(QUERY)
        with pytest.raises(KeySessionExpired, match="wiped"):
            _ = live.api_key_bytes

    def test_wipe_is_idempotent(self) -> None:
        """The kill switch may fire twice, and a disconnect may follow it."""
        live = session()
        live.wipe()
        live.wipe()

        assert live.wiped
        assert live.expired()
        assert live.expires_in_s == 0.0


class TestPresentation:
    """The three ways an object accidentally becomes text."""

    def test_repr_and_str_carry_neither_the_key_nor_the_secret(self) -> None:
        """A traceback frame renders `repr` of every local, and crash reporters ship it.

        The alias is shown because it is what spec 11 permits and what makes a repr useful
        at all.
        """
        live = session(alias="main-subaccount")

        for text in (repr(live), str(live)):
            assert API_KEY not in text
            assert API_SECRET not in text
            assert "main-subaccount" in text

    def test_repr_of_a_wiped_session_says_so_and_still_leaks_nothing(self) -> None:
        live = session(alias="main-subaccount")
        live.wipe()

        text = repr(live)
        assert "wiped" in text
        assert API_KEY not in text and API_SECRET not in text

    def test_to_json_carries_only_the_alias_the_balance_and_the_countdown(self) -> None:
        """Spec 11: *"Never echoed back to the UI -- only the account alias and balance."*

        Asserted as an exact key set rather than a set of absences, because the failure
        mode is a field somebody adds, and a test listing what must not be there cannot
        know about it.
        """
        live = session(alias="main", balance=parse_money("1234.56789012"))

        payload = live.to_json()

        assert set(payload) == {"alias", "balance", "connected_ms", "expires_in_s"}
        assert payload["alias"] == "main"
        assert payload["balance"] == money_to_str(parse_money("1234.56789012"))

    def test_record_validation_stores_what_the_signed_check_found(self) -> None:
        """The credential exists before it is known to be good.

        It has to be able to sign in order to be validated at all, which is why the balance
        arrives after construction rather than in it.
        """
        live = session()
        assert live.balance is None

        live.record_validation(alias="main", balance=parse_money("500.00"))

        assert live.alias == "main"
        assert live.balance == parse_money("500.00")

    def test_a_balance_that_was_never_read_renders_as_null_not_as_zero(self) -> None:
        """Zero is a balance somebody observed; null is the absence of an observation."""
        assert session().to_json()["balance"] is None


_KEYS_SOURCE_PATH = Path(keys_module.__file__)
_KEYS_SOURCE = _KEYS_SOURCE_PATH.read_text(encoding="utf-8")

_FORBIDDEN_IMPORTS = frozenset(
    {
        # Six places spec 11 names, and the modules that reach each of them.
        "logging",
        "os",
        "io",
        "pathlib",
        "shutil",
        "tempfile",
        "json",
        "pickle",
        "shelve",
        "sqlite3",
        "subprocess",
        "socket",
        "http",
        "urllib",
        "traceback",
        "faulthandler",
    }
)


class TestTheModuleCannotWriteACredentialAnywhere:
    """Structural guards, in the spirit of `test_decimal_is_confined_to_the_accounting_seam`.

    These do not prove a key never reaches disk. They prove this module has no means to put
    it there, which is the strongest form the guarantee takes without an OS sandbox -- and
    unlike a convention it fails on the one-line change that would break it.
    """

    def test_the_source_contains_no_filesystem_or_logging_name(self) -> None:
        """Spec 11: never a disk file, never a log line.

        Text rather than AST, deliberately. A `logging.getLogger` reached through an
        already-imported module, or an `open` shadowed by a local alias, is invisible to an
        import check and obvious to this one.
        """
        for token in ("open(", "Path", "logging"):
            assert token not in _KEYS_SOURCE, (
                f"{_KEYS_SOURCE_PATH.name} contains {token!r}. Spec 11 forbids this module "
                "the means to put a credential on disk or in a log line; if a genuinely "
                "new capability is needed here, it belongs in a module that never holds "
                "the key."
            )

    def test_the_module_imports_nothing_that_can_write_a_byte(self) -> None:
        offenders: list[str] = []
        for node in ast.walk(ast.parse(_KEYS_SOURCE, filename=str(_KEYS_SOURCE_PATH))):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or ""]
            else:
                continue
            for name in names:
                if name.split(".")[0] in _FORBIDDEN_IMPORTS:
                    offenders.append(f"line {node.lineno}: {name}")

        assert not offenders, (
            "keys.py imports a module that can write a credential somewhere spec 11 "
            "forbids: " + ", ".join(offenders)
        )

    def test_the_session_is_not_a_dataclass(self) -> None:
        """`@dataclass` would generate the exact leak this class exists to prevent.

        The generated `__repr__` renders every field, `asdict` copies the secret into a
        plain dict anything at all could serialise, and both arrive by adding one decorator
        line to a class that already looks like a record.
        """
        assert not hasattr(KeySession, "__dataclass_fields__")

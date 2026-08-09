"""The in-memory API key session (spec 11).

Spec 11 says where a credential may live in one sentence: *"Held only in backend process
memory, in a session object. Never written to disk, database, config file, log line,
browser storage, or crash dump."* This module exists to make that sentence enforceable
rather than aspirational.

Left to convention it would not survive a month, because every way of breaking it is one
line somebody adds in a hurry: a session object in a traceback frame that a crash reporter
serialises, an f-string in a debug record, `json.dumps(session)` in an API route that meant
to return the alias. So the credential never leaves this object as text -- `__repr__`,
`__str__` and `to_json` all render the alias and the balance and nothing else -- and this
module imports nothing that can write a byte anywhere. `tests/unit/test_key_session.py`
reads this file's own source and fails if a filesystem or log-writing name appears in it,
which is a cruder check than a human reviewer and a far more reliable one.

The other half of spec 11 is idle expiry: twelve hours without use and the keys are wiped
and live sessions halted, **with positions left open**. That is deliberate and it is stated
in the spec -- a forced market close on session expiry would be worse than the exposure --
so expiry here is a loss of the ability to trade, never an exit.
"""

from __future__ import annotations

import hashlib
import hmac
import time
from typing import Any

from perplab.core.money import Money, money_to_str

__all__ = ["KEY_SESSION_TTL_S", "KeySession", "KeySessionExpired"]

KEY_SESSION_TTL_S = 12 * 60 * 60
"""Spec 11's default idle timeout, in seconds.

Idle means *no signed request*, not "no human at the keyboard". A live session reconciling
against the exchange every 60 s (spec 6.7) therefore keeps its own keys alive for as long
as it runs, which is the intended reading: expiring the credential out from under an open
position would halt the run and leave the exposure standing.
"""


class KeySessionExpired(RuntimeError):
    """The session can no longer sign, because it was wiped or it idled past its TTL.

    One class for both states because they are the same state from the caller's side: there
    is no credential here any more and the only way forward is for a human to enter one
    again. The message says which of the two happened, so the Feed can say so too.
    """


class KeySession:
    """One set of exchange credentials, held in memory and nowhere else.

    Deliberately **not** a dataclass. `@dataclass` generates a `__repr__` that renders every
    field, which is precisely the leak this class exists to prevent, and the generated
    `__eq__` would compare secrets while `dataclasses.asdict` would copy them into a plain
    dict that anything at all could serialise.

    The key and the secret are held as `bytearray` rather than `str` so that `wipe()` can
    overwrite the bytes in place. Python strings are immutable and interned, so a `str`
    credential cannot be scrubbed at all -- setting the attribute to `""` only drops a
    reference and leaves the characters in the heap until the allocator happens to reuse
    them. This is honest about its limits: the `str` the *caller* built to construct this
    object is beyond our reach, so the entry path should hand its input straight here and
    keep no name of its own.
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        *,
        alias: str = "",
        balance: Money | None = None,
        ttl_s: float = KEY_SESSION_TTL_S,
    ) -> None:
        if not api_key or not api_secret:
            raise ValueError(
                "an API key and secret are both required; enter them in the Data & Feed "
                "tab rather than constructing an empty session"
            )
        if ttl_s <= 0:
            raise ValueError(f"ttl_s must be positive, got {ttl_s}")

        self._key = bytearray(api_key.encode("utf-8"))
        self._secret = bytearray(api_secret.encode("utf-8"))
        self._wiped = False

        self.alias = alias
        self.balance = balance
        """Wallet balance observed by the validating request, for display only.

        Spec 11 shows the alias and the balance and nothing else, and this is the balance
        as it stood the moment the key was accepted -- not a live figure. The reconciliation
        loop of spec 6.7 keeps its own, because a stale number presented as current is the
        kind of wrong-but-plausible value this codebase treats as worse than an error.
        """

        self.ttl_s = float(ttl_s)
        self.connected_ms = _now_ms()
        self.last_used_ms = self.connected_ms

        # Expiry is measured on the monotonic clock while `connected_ms` / `last_used_ms`
        # are wall clock. They are not interchangeable: an NTP correction of an hour --
        # ordinary on a machine that has just woken, and something spec 11 actively expects
        # since signing requires NTP sync -- would either expire a live session instantly or
        # extend a dead one, if the timeout were measured on wall time. Wall time is kept
        # anyway because "connected at 14:02" is what an operator needs to see.
        self._last_used_s = time.monotonic()

    # ---------------------------------------------------------------------- credential

    @property
    def api_key_bytes(self) -> bytes:
        """The key for the `X-MBX-APIKEY` header.

        Bytes rather than `str` because every `str` produced from the buffer is a new
        immutable object that `wipe()` cannot reach. `httpx` accepts a bytes header value,
        so the signed client never has to materialise one.

        **This counts as use, exactly as signing does.** Three endpoints are keyed but
        unsigned -- the listen-key create, keepalive and close -- and they are the ones a
        session holding only the user-data stream open uses. With the idle timer refreshed
        by `sign` alone, such a session's keys were wiped at twelve hours, the next keepalive
        failed, and the fill feed died an hour later with the socket still healthy. That the
        live path also signs every sixty seconds for reconciliation made it invisible, but
        that is a coincidence of one caller rather than a property of the session.
        """
        self._require_live()
        self.touch()
        return bytes(self._key)

    def export_for_child(self) -> dict[str, str]:
        """The credential as plain strings, for handing to a session worker over a pipe.

        **The one place this class produces a `str`, and it is deliberately hard to reach by
        accident.** Everything else -- `to_json`, `__repr__`, `__str__`, `api_key_bytes` --
        is either scrubbed or bytes, because a `str` is immutable and `wipe()` cannot reach
        one. But a paper session runs in its own process (spec 2.3, spec 11) and there is no
        way to give a child a credential without materialising it once.

        Named for what it is for, so a reader of a call site can see that a credential is
        crossing a process boundary. The caller must clear the returned mapping as soon as it
        has been written; `store.launch_session` does, and `live.worker` clears its own copy
        in a `finally`.
        """
        self._require_live()
        self.touch()
        return {
            "api_key": bytes(self._key).decode("utf-8"),
            "api_secret": bytes(self._secret).decode("utf-8"),
        }

    def sign(self, query: str) -> str:
        """HMAC-SHA256 of the query string, hex-encoded, as spec 11 and Appendix B require.

        Signing counts as use, so this refreshes the idle timer. That is what makes the TTL
        a timeout on *activity* rather than a hard 12 h cap on every session.

        The query must be the exact bytes that go on the wire. Re-encoding parameters after
        signing them -- letting an HTTP client rebuild the query from a dict, say -- yields
        a signature over a string the exchange never sees, and the failure is a blanket
        `-1022 Signature for this request is not valid` with nothing to say which parameter
        moved.
        """
        self._require_live()
        self.touch()
        return hmac.new(bytes(self._secret), query.encode("utf-8"), hashlib.sha256).hexdigest()

    # -------------------------------------------------------------------------- session

    def touch(self, now_s: float | None = None) -> None:
        """Mark the session as used, restarting the idle countdown."""
        self._last_used_s = time.monotonic() if now_s is None else now_s
        self.last_used_ms = _now_ms()

    def expired(self, now_s: float | None = None) -> bool:
        """Whether the idle timeout has elapsed. A wiped session is always expired."""
        if self._wiped:
            return True
        now = time.monotonic() if now_s is None else now_s
        return now - self._last_used_s >= self.ttl_s

    @property
    def expires_in_s(self) -> float:
        """Seconds of inactivity remaining, floored at zero.

        Floored rather than allowed to go negative because this figure is rendered in the
        UI, and a countdown that runs past zero into negative numbers describes a session
        that is already gone as though it were still there.
        """
        if self._wiped:
            return 0.0
        return max(0.0, self.ttl_s - (time.monotonic() - self._last_used_s))

    @property
    def wiped(self) -> bool:
        return self._wiped

    def record_validation(self, *, alias: str, balance: Money | None) -> None:
        """Record what the validating signed request found (spec 11).

        Kept separate from construction because the credential exists before it is known to
        be good: the session has to be able to sign in order to be validated at all, and a
        constructor that took an already-verified balance would need the caller to hold the
        key outside a session to obtain one.
        """
        self._require_live()
        self.alias = alias
        self.balance = balance
        self.touch()

    def wipe(self) -> None:
        """Destroy the credential. Idempotent, and safe to call from the kill switch.

        The buffers are zeroed in place rather than dropped. Rebinding the attributes alone
        would leave the bytes in the heap for an arbitrary length of time -- long enough to
        reach a core dump, which is one of the six places spec 11 names by hand.
        """
        for buffer in (self._key, self._secret):
            for i in range(len(buffer)):
                buffer[i] = 0
            buffer.clear()
        self._wiped = True

    def _require_live(self) -> None:
        if self._wiped:
            raise KeySessionExpired(
                "the key session was wiped (disconnect, or the kill switch fired); "
                "re-enter the API key in the Data & Feed tab before trading again"
            )
        if self.expired():
            raise KeySessionExpired(
                f"the key session idled past its {self.ttl_s:.0f} s timeout; "
                "re-enter the API key in the Data & Feed tab. Open positions were left "
                "open deliberately (spec 11) and still need attention."
            )

    # -------------------------------------------------------------------- presentation

    def to_json(self) -> dict[str, Any]:
        """What the UI is allowed to see: spec 11's alias and balance, and the countdown.

        There is no branch here that could ever include the credential, which is the point.
        A method that redacted a field would be one edit away from not redacting it.
        """
        return {
            "alias": self.alias,
            "balance": None if self.balance is None else money_to_str(self.balance),
            "connected_ms": self.connected_ms,
            "expires_in_s": self.expires_in_s,
        }

    def __repr__(self) -> str:
        state = "wiped" if self._wiped else f"expires_in_s={self.expires_in_s:.0f}"
        return f"KeySession(alias={self.alias!r}, {state})"

    __str__ = __repr__


def _now_ms() -> int:
    """Wall-clock epoch milliseconds, for the timestamps an operator reads."""
    return int(time.time() * 1000)

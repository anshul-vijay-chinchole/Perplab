"""Strategy library operations (spec 5.6).

New, save, list, archive, delete, import, export, version history, diff. The rules that
matter are spec's, not this module's, and each is enforced here rather than in the UI:

**Every save writes an immutable version row.** A run records the exact version hash it
used, so "which code produced this equity curve" is always answerable (spec 5.6). Version
rows are never updated and never deleted while the strategy exists.

**One deliberate deviation, stated plainly:** a save whose code is byte-identical to the
current head returns that head instead of writing a duplicate row. The purpose of a version
is to identify the code behind a result, and identical code identifies the same result --
so the duplicate row would differ only in its timestamp while making the history harder to
read. `save()` returns `created=False` in that case so the editor can say "no changes"
rather than claiming a version it did not make. A save that changes only the *message* does
write a row, because that is a change to the record.

**Delete is blocked by runs, not just discouraged.** Archive is the soft, recoverable
operation; delete is permanent and refuses while any non-archived run references any
version of the strategy. A deleted strategy would leave those runs unreproducible, which
spec 5.6 rules out.

**Import never writes what it reads.** A `.perplab` bundle is a zip, and extracting one to
disk is the classic path-traversal footgun. Two known member names are read straight out of
the archive and nothing is ever written, so a crafted member name has nowhere to go.
"""

from __future__ import annotations

import difflib
import hashlib
import io
import json
import re
import sqlite3
import threading
import time
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

from perplab import __version__
from perplab.store.db import connect
from perplab.strategy.template import NEW_STRATEGY_TEMPLATE
from perplab.strategy.validate import ValidationResult

__all__ = [
    "BUNDLE_SUFFIX",
    "BUNDLE_CODE_MEMBER",
    "BUNDLE_MANIFEST_MEMBER",
    "MAX_CODE_BYTES",
    "LibraryError",
    "NotFound",
    "NameInUse",
    "DeleteBlocked",
    "StrategySummary",
    "StrategyVersion",
    "SaveOutcome",
    "ImportedStrategy",
    "StrategyLibrary",
]

BUNDLE_SUFFIX = ".perplab"
BUNDLE_CODE_MEMBER = "strategy.py"
BUNDLE_MANIFEST_MEMBER = "manifest.json"
BUNDLE_FORMAT = 1

MAX_CODE_BYTES = 1_000_000
"""A megabyte of strategy source.

Not a security control -- see spec 2.3 -- but a bound on what the validator will be asked to
`ast.parse` and what a single SQLite row will hold. A strategy that reaches this has almost
certainly had a data file pasted into it.
"""

_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 _.()\-]{0,79}$")
"""Names a filesystem will accept, plus parentheses.

Parentheses are in the set for two reasons that turn out to be the same one:
`Mean Reversion (BTC)` is how people actually name things, and `_unique_name` appends
` (2)` on an import collision -- so excluding them made importing a bundle twice fail with
a message telling the author their own generated name was invalid.
"""
_TAG_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_\-]{0,31}$")


class LibraryError(RuntimeError):
    """Base for library failures that are the caller's fault, not the platform's."""


class NotFound(LibraryError):
    pass


class NameInUse(LibraryError):
    pass


class DeleteBlocked(LibraryError):
    """Delete refused because runs still reference the strategy (spec 5.6)."""


@dataclass(frozen=True, slots=True)
class StrategyVersion:
    id: int
    strategy_id: int
    version_no: int
    code: str
    code_sha256: str
    created_ms: int
    message: str
    class_name: str | None
    params: tuple[dict[str, Any], ...]
    requires: dict[str, Any] | None
    valid: bool
    diagnostics: tuple[dict[str, Any], ...]

    def to_json(self, *, include_code: bool = True) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "id": self.id,
            "strategy_id": self.strategy_id,
            "version_no": self.version_no,
            "code_sha256": self.code_sha256,
            "created_ms": self.created_ms,
            "message": self.message,
            "class_name": self.class_name,
            "params": list(self.params),
            "requires": self.requires,
            "valid": self.valid,
            "diagnostics": list(self.diagnostics),
        }
        if include_code:
            payload["code"] = self.code
        return payload


@dataclass(frozen=True, slots=True)
class StrategySummary:
    id: int
    name: str
    notes: str
    created_ms: int
    updated_ms: int
    archived_ms: int | None
    tags: tuple[str, ...]
    head: StrategyVersion | None
    version_count: int
    run_count: int

    @property
    def archived(self) -> bool:
        return self.archived_ms is not None

    def to_json(self, *, include_code: bool = False) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "notes": self.notes,
            "created_ms": self.created_ms,
            "updated_ms": self.updated_ms,
            "archived_ms": self.archived_ms,
            "archived": self.archived,
            "tags": list(self.tags),
            "head": None if self.head is None else self.head.to_json(include_code=include_code),
            "version_count": self.version_count,
            "run_count": self.run_count,
        }


@dataclass(frozen=True, slots=True)
class SaveOutcome:
    """Result of a save. `created` is False when the code was unchanged."""

    strategy: StrategySummary
    version: StrategyVersion
    created: bool


@dataclass(frozen=True, slots=True)
class ImportedStrategy:
    """A decoded bundle or `.py` file, before it is written to the library."""

    name: str
    code: str
    notes: str = ""
    tags: tuple[str, ...] = ()
    source_version_no: int | None = None
    source_sha256: str | None = None
    warnings: tuple[str, ...] = field(default=())


def _now_ms() -> int:
    return int(time.time() * 1000)


def _read_member(archive: zipfile.ZipFile, name: str, limit: int) -> str:
    """Read one zip member as UTF-8, refusing anything over `limit` *decompressed* bytes.

    Both halves matter. The declared size is checked first so a bomb is refused without
    being expanded, and the read is still capped at `limit + 1` because the declared size
    is attacker-controlled and a lying header would otherwise expand unbounded anyway.

    The router's size check is on the *compressed* upload, which is no protection at all:
    a 204 KB bundle decompressed to 200 MB and peaked at 459 MB of allocation inside the
    server process, and the 1 MB source limit only applied afterwards, to a string that had
    already been materialised.
    """
    info = archive.getinfo(name)
    if info.file_size > limit:
        raise LibraryError(
            f"the bundle's {name} expands to {info.file_size:,} bytes; the limit is "
            f"{limit:,}. A strategy this size almost always has a data file pasted into it."
        )
    with archive.open(name) as handle:
        raw = handle.read(limit + 1)
    if len(raw) > limit:
        raise LibraryError(
            f"the bundle's {name} expands past the {limit:,}-byte limit; its declared size "
            "was smaller, so the archive is malformed or deliberately misleading."
        )
    return raw.decode("utf-8")


def code_hash(code: str) -> str:
    """SHA-256 over the code's UTF-8 bytes.

    Over the code exactly as stored, with no normalisation. Stripping whitespace or
    normalising line endings before hashing would make two versions that *are* different
    files hash the same, and the hash is what a run records to identify what it executed
    (spec 12.1).
    """
    return hashlib.sha256(code.encode("utf-8")).hexdigest()


class StrategyLibrary:
    """CRUD, versioning and bundles over the SQLite store.

    A single connection guarded by a lock. The alternative -- a connection per thread --
    would need every caller to think about which one it holds, for a workload of a few
    writes per minute from one person.
    """

    def __init__(
        self,
        root: Path | str,
        *,
        clock: Callable[[], int] = _now_ms,
        connection: sqlite3.Connection | None = None,
    ) -> None:
        self.root = Path(root)
        self._clock = clock
        self._lock = threading.RLock()
        self._db = connection if connection is not None else connect(self.root)

    def close(self) -> None:
        with self._lock:
            self._db.close()

    def __enter__(self) -> StrategyLibrary:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    # -------------------------------------------------------------------- validation

    @staticmethod
    def _check_name(name: str) -> str:
        cleaned = (name or "").strip()
        if not _NAME_RE.match(cleaned):
            raise LibraryError(
                f"{name!r} is not a usable strategy name. Use 1-80 characters starting "
                "with a letter or digit; letters, digits, spaces, dot, dash and "
                "underscore are allowed. The name becomes an export filename, so the "
                "characters a filesystem rejects are rejected here."
            )
        return cleaned

    @staticmethod
    def _check_tags(tags: Iterable[str] | None) -> tuple[str, ...]:
        if not tags:
            return ()
        cleaned: list[str] = []
        for tag in tags:
            value = (tag or "").strip()
            if not _TAG_RE.match(value):
                raise LibraryError(
                    f"{tag!r} is not a usable tag. Use 1-32 characters: letters, digits, "
                    "dash, underscore."
                )
            if value.lower() not in {c.lower() for c in cleaned}:
                cleaned.append(value)
        return tuple(cleaned)

    @staticmethod
    def _check_code(code: str) -> str:
        if not isinstance(code, str):
            raise LibraryError(f"code must be text, got {type(code).__name__}")
        size = len(code.encode("utf-8"))
        if size > MAX_CODE_BYTES:
            raise LibraryError(
                f"strategy source is {size:,} bytes; the limit is {MAX_CODE_BYTES:,}. A "
                "strategy this size almost always has a data file pasted into it — load "
                "data through `requires['datasets']` instead."
            )
        if "\x00" in code:
            raise LibraryError(
                "strategy source contains a null byte, so it is not text. This usually "
                "means a binary file was opened as a strategy."
            )
        # Normalise line endings on the way in. Windows editors write CRLF, Monaco writes
        # LF, and a file that round-trips through both would otherwise produce a new
        # version on every save with no visible change and a different code hash.
        return code.replace("\r\n", "\n").replace("\r", "\n")

    # ------------------------------------------------------------------------ reads

    def _row_to_version(self, row: sqlite3.Row) -> StrategyVersion:
        return StrategyVersion(
            id=row["id"],
            strategy_id=row["strategy_id"],
            version_no=row["version_no"],
            code=row["code"],
            code_sha256=row["code_sha256"],
            created_ms=row["created_ms"],
            message=row["message"],
            class_name=row["class_name"],
            params=tuple(json.loads(row["params_json"])),
            requires=json.loads(row["requires_json"]) if row["requires_json"] else None,
            valid=bool(row["valid"]),
            diagnostics=tuple(json.loads(row["diagnostics_json"])),
        )

    def _summary(self, strategy_id: int) -> StrategySummary:
        row = self._db.execute(
            "SELECT * FROM strategies WHERE id = ?", (strategy_id,)
        ).fetchone()
        if row is None:
            raise NotFound(f"no strategy with id {strategy_id}")

        head = None
        if row["head_version_id"] is not None:
            head_row = self._db.execute(
                "SELECT * FROM strategy_versions WHERE id = ?", (row["head_version_id"],)
            ).fetchone()
            if head_row is not None:
                head = self._row_to_version(head_row)

        tags = tuple(
            r["tag"]
            for r in self._db.execute(
                "SELECT tag FROM strategy_tags WHERE strategy_id = ? ORDER BY tag",
                (strategy_id,),
            )
        )
        version_count = self._db.execute(
            "SELECT COUNT(*) AS n FROM strategy_versions WHERE strategy_id = ?",
            (strategy_id,),
        ).fetchone()["n"]
        run_count = self._db.execute(
            "SELECT COUNT(*) AS n FROM runs WHERE strategy_id = ?", (strategy_id,)
        ).fetchone()["n"]

        return StrategySummary(
            id=row["id"],
            name=row["name"],
            notes=row["notes"],
            created_ms=row["created_ms"],
            updated_ms=row["updated_ms"],
            archived_ms=row["archived_ms"],
            tags=tags,
            head=head,
            version_count=version_count,
            run_count=run_count,
        )

    def get(self, strategy_id: int) -> StrategySummary:
        with self._lock:
            return self._summary(strategy_id)

    def get_by_name(self, name: str) -> StrategySummary:
        with self._lock:
            row = self._db.execute(
                "SELECT id FROM strategies WHERE name = ? COLLATE NOCASE", (name,)
            ).fetchone()
            if row is None:
                raise NotFound(f"no strategy named {name!r}")
            return self._summary(row["id"])

    def list(
        self,
        *,
        include_archived: bool = False,
        search: str | None = None,
        tag: str | None = None,
    ) -> tuple[StrategySummary, ...]:
        """List strategies, newest-updated first.

        Archived strategies are excluded by default and also excluded from the trials
        counter (spec 5.6, 9.4) -- an archived experiment that is still counted as a trial
        makes the multiple-testing correction pessimistic in a way that punishes tidying up.
        """
        with self._lock:
            sql = "SELECT id FROM strategies"
            clauses: list[str] = []
            args: list[Any] = []
            if not include_archived:
                clauses.append("archived_ms IS NULL")
            if search:
                # `%` and `_` are LIKE wildcards, so an unescaped search for "%" matched
                # every strategy and "a_b" matched "axb". Escaped rather than stripped: a
                # name may legitimately contain an underscore, and searching for it should
                # find it rather than quietly matching anything.
                clauses.append(
                    "(name LIKE ? ESCAPE '\\' COLLATE NOCASE "
                    "OR notes LIKE ? ESCAPE '\\' COLLATE NOCASE)"
                )
                escaped = (
                    search.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
                )
                pattern = f"%{escaped}%"
                args.extend([pattern, pattern])
            if tag:
                clauses.append(
                    "id IN (SELECT strategy_id FROM strategy_tags "
                    "WHERE tag = ? COLLATE NOCASE)"
                )
                args.append(tag)
            if clauses:
                sql += " WHERE " + " AND ".join(clauses)
            sql += " ORDER BY updated_ms DESC, id DESC"
            ids = [r["id"] for r in self._db.execute(sql, args)]
            return tuple(self._summary(sid) for sid in ids)

    def versions(self, strategy_id: int) -> tuple[StrategyVersion, ...]:
        """Every version, newest first."""
        with self._lock:
            self._summary(strategy_id)
            rows = self._db.execute(
                "SELECT * FROM strategy_versions WHERE strategy_id = ? "
                "ORDER BY version_no DESC",
                (strategy_id,),
            ).fetchall()
            return tuple(self._row_to_version(row) for row in rows)

    def version(self, strategy_id: int, version_no: int) -> StrategyVersion:
        with self._lock:
            row = self._db.execute(
                "SELECT * FROM strategy_versions WHERE strategy_id = ? AND version_no = ?",
                (strategy_id, version_no),
            ).fetchone()
            if row is None:
                raise NotFound(f"strategy {strategy_id} has no version {version_no}")
            return self._row_to_version(row)

    def diff(self, strategy_id: int, left_no: int, right_no: int) -> str:
        """Unified diff between two versions of one strategy."""
        left = self.version(strategy_id, left_no)
        right = self.version(strategy_id, right_no)
        return "".join(
            difflib.unified_diff(
                left.code.splitlines(keepends=True),
                right.code.splitlines(keepends=True),
                fromfile=f"v{left.version_no}",
                tofile=f"v{right.version_no}",
                n=3,
            )
        )

    # ----------------------------------------------------------------------- writes

    def create(
        self,
        name: str,
        *,
        code: str | None = None,
        notes: str = "",
        tags: Iterable[str] | None = None,
        message: str = "created",
        validation: ValidationResult | None = None,
    ) -> SaveOutcome:
        """Create a strategy with its first version (spec 5.6, "New")."""
        clean_name = self._check_name(name)
        clean_tags = self._check_tags(tags)
        clean_code = self._check_code(
            NEW_STRATEGY_TEMPLATE if code is None else code
        )
        now = self._clock()

        with self._lock, self._db:
            existing = self._db.execute(
                "SELECT id FROM strategies WHERE name = ? COLLATE NOCASE", (clean_name,)
            ).fetchone()
            if existing is not None:
                raise NameInUse(f"a strategy named {clean_name!r} already exists")

            cursor = self._db.execute(
                "INSERT INTO strategies (name, notes, created_ms, updated_ms) "
                "VALUES (?, ?, ?, ?)",
                (clean_name, notes or "", now, now),
            )
            strategy_id = int(cursor.lastrowid)
            for tag in clean_tags:
                self._db.execute(
                    "INSERT INTO strategy_tags (strategy_id, tag) VALUES (?, ?)",
                    (strategy_id, tag),
                )
            version = self._insert_version(
                strategy_id, clean_code, message=message, validation=validation, now=now
            )
            summary = self._summary(strategy_id)

        return SaveOutcome(strategy=summary, version=version, created=True)

    def save(
        self,
        strategy_id: int,
        code: str,
        *,
        message: str = "",
        validation: ValidationResult | None = None,
    ) -> SaveOutcome:
        """Write a new immutable version, unless the code is byte-identical to the head.

        See the module docstring for why the identical-code case does not write a row.
        """
        clean_code = self._check_code(code)
        digest = code_hash(clean_code)
        now = self._clock()

        with self._lock, self._db:
            summary = self._summary(strategy_id)
            head = summary.head
            # No message, or the same message, means nothing about the record changed.
            # A *new* message against identical code is a real edit to the history and
            # does write a row -- otherwise annotating a version would silently do nothing.
            unchanged_message = not message or message == head.message if head else False
            if head is not None and head.code_sha256 == digest and unchanged_message:
                return SaveOutcome(strategy=summary, version=head, created=False)
            version = self._insert_version(
                strategy_id, clean_code, message=message, validation=validation, now=now
            )
            # Built inside the lock. Outside it, a concurrent save could land between the
            # insert and the read, and the response would carry one version's metadata
            # beside another version's head -- so the editor would render head's code
            # against the wrong version number. Storage was always serialised correctly;
            # it was the returned pair that could disagree with itself.
            summary = self._summary(strategy_id)
        return SaveOutcome(strategy=summary, version=version, created=True)

    def _insert_version(
        self,
        strategy_id: int,
        code: str,
        *,
        message: str,
        validation: ValidationResult | None,
        now: int,
    ) -> StrategyVersion:
        """Insert a version row and move the head. Caller holds the lock and transaction."""
        next_no = (
            self._db.execute(
                "SELECT COALESCE(MAX(version_no), 0) AS n FROM strategy_versions "
                "WHERE strategy_id = ?",
                (strategy_id,),
            ).fetchone()["n"]
            + 1
        )
        cursor = self._db.execute(
            "INSERT INTO strategy_versions "
            "(strategy_id, version_no, code, code_sha256, created_ms, message, "
            " class_name, params_json, requires_json, valid, diagnostics_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                strategy_id,
                next_no,
                code,
                code_hash(code),
                now,
                message or "",
                None if validation is None else validation.class_name,
                json.dumps(list(validation.params) if validation else []),
                json.dumps(validation.requires) if validation and validation.requires else None,
                1 if (validation is not None and validation.ok) else 0,
                json.dumps(
                    [d.to_json() for d in validation.diagnostics] if validation else []
                ),
            ),
        )
        version_id = int(cursor.lastrowid)
        self._db.execute(
            "UPDATE strategies SET head_version_id = ?, updated_ms = ? WHERE id = ?",
            (version_id, now, strategy_id),
        )
        row = self._db.execute(
            "SELECT * FROM strategy_versions WHERE id = ?", (version_id,)
        ).fetchone()
        return self._row_to_version(row)

    def update_metadata(
        self,
        strategy_id: int,
        *,
        name: str | None = None,
        notes: str | None = None,
        tags: Iterable[str] | None = None,
    ) -> StrategySummary:
        """Rename, re-note or re-tag. Does not create a version — no code changed."""
        now = self._clock()
        with self._lock, self._db:
            self._summary(strategy_id)
            if name is not None:
                clean = self._check_name(name)
                clash = self._db.execute(
                    "SELECT id FROM strategies WHERE name = ? COLLATE NOCASE AND id != ?",
                    (clean, strategy_id),
                ).fetchone()
                if clash is not None:
                    raise NameInUse(f"a strategy named {clean!r} already exists")
                self._db.execute(
                    "UPDATE strategies SET name = ? WHERE id = ?", (clean, strategy_id)
                )
            if notes is not None:
                self._db.execute(
                    "UPDATE strategies SET notes = ? WHERE id = ?", (notes, strategy_id)
                )
            if tags is not None:
                clean_tags = self._check_tags(tags)
                self._db.execute(
                    "DELETE FROM strategy_tags WHERE strategy_id = ?", (strategy_id,)
                )
                for tag in clean_tags:
                    self._db.execute(
                        "INSERT INTO strategy_tags (strategy_id, tag) VALUES (?, ?)",
                        (strategy_id, tag),
                    )
            self._db.execute(
                "UPDATE strategies SET updated_ms = ? WHERE id = ?", (now, strategy_id)
            )
        return self.get(strategy_id)

    def archive(self, strategy_id: int, *, archived: bool = True) -> StrategySummary:
        """Soft-hide, fully recoverable (spec 5.6)."""
        now = self._clock()
        with self._lock, self._db:
            self._summary(strategy_id)
            self._db.execute(
                "UPDATE strategies SET archived_ms = ?, updated_ms = ? WHERE id = ?",
                (now if archived else None, now, strategy_id),
            )
        return self.get(strategy_id)

    def blocking_runs(self, strategy_id: int) -> int:
        """Non-archived runs referencing this strategy — what blocks a delete."""
        with self._lock:
            return int(
                self._db.execute(
                    "SELECT COUNT(*) AS n FROM runs "
                    "WHERE strategy_id = ? AND archived_ms IS NULL",
                    (strategy_id,),
                ).fetchone()["n"]
            )

    def delete(self, strategy_id: int, *, confirm_name: str) -> dict[str, int]:
        """Hard delete. Requires the typed name and refuses while live runs reference it.

        `confirm_name` is checked here rather than only in the UI. The dialog is what stops
        an accidental click; this is what stops a mistaken API call, and spec 10.4 asks for
        typed confirmation on destructive actions because the two failure modes are
        different.

        **Archived runs are deleted along with the strategy**, and the count comes back so
        the caller can say so. Spec 5.6 makes only *non-archived* runs blocking, which
        settles whether the delete proceeds but not what happens to the archived ones.
        Keeping them would leave rows describing runs whose code no longer exists --
        unreproducible records that look like records -- so they go. Reporting the number
        is what keeps that from being a silent side effect of a different confirmation.
        """
        with self._lock:
            summary = self._summary(strategy_id)
            if confirm_name.strip() != summary.name:
                raise LibraryError(
                    f"delete requires the strategy's exact name; got {confirm_name!r}, "
                    f"expected {summary.name!r}"
                )
            blocking = self.blocking_runs(strategy_id)
            if blocking:
                raise DeleteBlocked(
                    f"{summary.name!r} is referenced by {blocking} run(s) that are not "
                    "archived. Deleting it would make those runs unreproducible (spec "
                    "5.6). Archive the strategy instead, or archive the runs first."
                )
            with self._db:
                # Order matters, and every step of it is a foreign key doing its job.
                # Archived runs reference versions, versions reference the strategy, and
                # the strategy references its head version. Removing them in any other
                # order trips the constraint that exists to prevent exactly this.
                runs = self._db.execute(
                    "DELETE FROM runs WHERE strategy_id = ?", (strategy_id,)
                ).rowcount
                self._db.execute(
                    "UPDATE strategies SET head_version_id = NULL WHERE id = ?",
                    (strategy_id,),
                )
                versions = self._db.execute(
                    "DELETE FROM strategy_versions WHERE strategy_id = ?", (strategy_id,)
                ).rowcount
                self._db.execute("DELETE FROM strategies WHERE id = ?", (strategy_id,))
            return {"runs": max(runs, 0), "versions": max(versions, 0)}

    # ---------------------------------------------------------------------- bundles

    def export_bundle(self, strategy_id: int, *, version_no: int | None = None) -> bytes:
        """Build a `.perplab` bundle: code plus a manifest (spec 5.6, "Export")."""
        with self._lock:
            summary = self._summary(strategy_id)
            version = (
                summary.head if version_no is None else self.version(strategy_id, version_no)
            )
            if version is None:
                raise NotFound(f"{summary.name!r} has no versions to export")

        manifest = {
            "format": BUNDLE_FORMAT,
            "perplab_version": __version__,
            "name": summary.name,
            "notes": summary.notes,
            "tags": list(summary.tags),
            "version_no": version.version_no,
            "code_sha256": version.code_sha256,
            "created_ms": version.created_ms,
            # No `exported_ms`. It was the one field that changed between two exports of the
            # same version, which made the byte-identical claim below false while the
            # comment asserting it stayed. The export time is not a property of the strategy
            # and `created_ms` already records when the version was written; keeping it
            # would trade a checksummable artefact for a timestamp nobody reads.
            "class_name": version.class_name,
            "params": list(version.params),
            "requires": version.requires,
        }

        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            # Fixed member timestamps so exporting the same version twice produces
            # byte-identical bundles. A bundle whose bytes change every time it is written
            # cannot be checksummed, and "is this the same strategy I sent you" becomes a
            # question with no cheap answer.
            for member, payload in (
                (BUNDLE_MANIFEST_MEMBER, json.dumps(manifest, indent=2, sort_keys=True)),
                (BUNDLE_CODE_MEMBER, version.code),
            ):
                info = zipfile.ZipInfo(member, date_time=(1980, 1, 1, 0, 0, 0))
                info.compress_type = zipfile.ZIP_DEFLATED
                info.external_attr = 0o644 << 16
                archive.writestr(info, payload)
        return buffer.getvalue()

    @staticmethod
    def read_bundle(data: bytes, *, filename: str = "strategy") -> ImportedStrategy:
        """Decode a `.perplab` bundle or a raw `.py` file (spec 5.6, "Import").

        Nothing is extracted to disk. Two known member names are read out of the archive
        and everything else in it is ignored, so a member called `../../autoexec` has
        nowhere to be written to.
        """
        stem = Path(filename).stem or "imported"
        if not data:
            raise LibraryError("the uploaded file is empty")

        if not zipfile.is_zipfile(io.BytesIO(data)):
            try:
                code = data.decode("utf-8")
            except UnicodeDecodeError:
                raise LibraryError(
                    "the file is neither a .perplab bundle nor UTF-8 Python source"
                ) from None
            return ImportedStrategy(name=stem, code=code)

        warnings: list[str] = []
        try:
            with zipfile.ZipFile(io.BytesIO(data)) as archive:
                names = set(archive.namelist())
                if BUNDLE_CODE_MEMBER not in names:
                    raise LibraryError(
                        f"the bundle has no {BUNDLE_CODE_MEMBER}. A .perplab bundle is a "
                        f"zip holding {BUNDLE_CODE_MEMBER} and {BUNDLE_MANIFEST_MEMBER}."
                    )
                code = _read_member(archive, BUNDLE_CODE_MEMBER, MAX_CODE_BYTES)
                manifest: dict[str, Any] = {}
                if BUNDLE_MANIFEST_MEMBER in names:
                    try:
                        manifest = json.loads(
                            _read_member(archive, BUNDLE_MANIFEST_MEMBER, MAX_CODE_BYTES)
                        )
                    except (json.JSONDecodeError, LibraryError):
                        warnings.append(
                            "the bundle's manifest.json is unreadable; the code was "
                            "imported and its name, notes and tags were not"
                        )
                else:
                    warnings.append(
                        "the bundle has no manifest.json; the code was imported without "
                        "its name, notes or tags"
                    )
        except LibraryError:
            raise
        except (zipfile.BadZipFile, UnicodeDecodeError, EOFError) as exc:
            # A corrupted archive still passes `is_zipfile` -- that only checks the end-of
            # -central-directory signature -- and a member that is not UTF-8 raises on
            # decode. Both used to escape as an unhandled exception and reach the client as
            # a 500, which reads as a platform fault for a file the user chose.
            raise LibraryError(
                f"the bundle could not be read: {type(exc).__name__}: {exc}"
            ) from None

        declared = manifest.get("code_sha256")
        if declared and declared != code_hash(code):
            # A mismatch is worth surfacing and not worth refusing over: the code is right
            # there and readable, and an author who hand-edited a bundle should not be
            # locked out of their own strategy. It does mean the manifest's params and
            # requires may describe different code, so they are not trusted.
            warnings.append(
                "the bundle's code does not match the hash in its manifest, so the "
                "manifest may describe a different version. The code was imported as-is "
                "and will be re-validated."
            )

        name = manifest.get("name") if isinstance(manifest.get("name"), str) else stem
        tags = manifest.get("tags")
        return ImportedStrategy(
            name=name or stem,
            code=code,
            notes=manifest.get("notes") if isinstance(manifest.get("notes"), str) else "",
            tags=tuple(t for t in tags if isinstance(t, str)) if isinstance(tags, list) else (),
            source_version_no=manifest.get("version_no")
            if isinstance(manifest.get("version_no"), int)
            else None,
            source_sha256=declared if isinstance(declared, str) else None,
            warnings=tuple(warnings),
        )

    def import_bundle(
        self,
        data: bytes,
        *,
        filename: str = "strategy",
        name: str | None = None,
        validation: ValidationResult | None = None,
    ) -> tuple[SaveOutcome, tuple[str, ...]]:
        """Decode and store a bundle, auto-suffixing the name if it is taken."""
        return self.store_imported(
            self.read_bundle(data, filename=filename),
            filename=filename,
            name=name,
            validation=validation,
        )

    def store_imported(
        self,
        imported: ImportedStrategy,
        *,
        filename: str = "strategy",
        name: str | None = None,
        validation: ValidationResult | None = None,
    ) -> tuple[SaveOutcome, tuple[str, ...]]:
        """Store an already-decoded bundle.

        Split out from `import_bundle` so a caller that has to inspect the code first --
        the API validates it before storing -- does not have to decode the archive twice.
        Decompression is the expensive half of an import.
        """
        target = name or imported.name
        outcome = self.create(
            self._unique_name(target),
            code=imported.code,
            notes=imported.notes,
            tags=imported.tags,
            message=(
                f"imported from {filename}"
                + (
                    f" (v{imported.source_version_no})"
                    if imported.source_version_no is not None
                    else ""
                )
            ),
            validation=validation,
        )
        return outcome, imported.warnings

    def _unique_name(self, name: str) -> str:
        """`EMACross`, then `EMACross (2)`, `EMACross (3)`, ...

        Auto-suffixing rather than refusing, because import is usually "give me a copy of
        this to work from" and failing the whole upload over a name collision means the
        author has to rename a file and try again.
        """
        base = self._check_name(name)
        with self._lock:
            taken = {
                row["name"].lower()
                for row in self._db.execute("SELECT name FROM strategies")
            }
        if base.lower() not in taken:
            return base
        for suffix in range(2, 1000):
            marker = f" ({suffix})"
            # Trim the base to make room rather than giving up. Refusing once the name
            # reached 77 characters was the exact failure this method's docstring rules
            # out -- and it hit hardest on imports, where the name came from a file the
            # author did not choose and the only remedy was to rename it and try again.
            candidate = base[: 80 - len(marker)].rstrip() + marker
            if candidate.lower() not in taken:
                return candidate
        raise NameInUse(f"cannot find an unused name based on {base!r}")

    def export_filename(self, strategy_id: int, version_no: int | None = None) -> str:
        summary = self.get(strategy_id)
        version = summary.head if version_no is None else self.version(strategy_id, version_no)
        stem = re.sub(r"[^A-Za-z0-9._-]+", "-", summary.name).strip("-") or "strategy"
        suffix = f"-v{version.version_no}" if version is not None else ""
        return f"{stem}{suffix}{BUNDLE_SUFFIX}"


def strategy_ids(summaries: Sequence[StrategySummary]) -> tuple[int, ...]:
    return tuple(s.id for s in summaries)

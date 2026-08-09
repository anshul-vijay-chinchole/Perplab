"""Strategy library operations (spec 5.6).

Every rule spec 5.6 states has a test here, and the two that are easy to get subtly wrong
have several: version immutability, and the delete rule that protects run reproducibility.

`validation=None` throughout — these tests are about storage, not about the validator, and
running the real one would put two subprocess spawns behind every save for no added
coverage.
"""

from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

import pytest

from perplab.store.db import SCHEMA_VERSION, SchemaTooNew, connect
from perplab.strategy.library import (
    BUNDLE_CODE_MEMBER,
    BUNDLE_MANIFEST_MEMBER,
    MAX_CODE_BYTES,
    DeleteBlocked,
    LibraryError,
    NameInUse,
    NotFound,
    StrategyLibrary,
    code_hash,
)

CODE_A = "from perplab import Strategy\n\n\nclass A(Strategy):\n    pass\n"
CODE_B = "from perplab import Strategy\n\n\nclass B(Strategy):\n    x = 1\n"


@pytest.fixture
def library(tmp_path: Path) -> StrategyLibrary:
    ticks = iter(range(1_700_000_000_000, 1_700_000_999_000, 1000))
    with StrategyLibrary(tmp_path, clock=lambda: next(ticks)) as lib:
        yield lib


class TestCreateAndSave:
    def test_create_writes_a_first_version(self, library: StrategyLibrary) -> None:
        outcome = library.create("EMACross", code=CODE_A, notes="trend", tags=["btc"])
        assert outcome.created
        assert outcome.version.version_no == 1
        assert outcome.version.code_sha256 == code_hash(CODE_A)
        assert outcome.strategy.tags == ("btc",)

    def test_create_defaults_to_the_template(self, library: StrategyLibrary) -> None:
        outcome = library.create("Fresh")
        assert "class MyStrategy(Strategy)" in outcome.version.code

    def test_each_save_writes_an_immutable_version(self, library: StrategyLibrary) -> None:
        strategy = library.create("S", code=CODE_A).strategy
        library.save(strategy.id, CODE_B, message="second")
        library.save(strategy.id, CODE_A, message="back to first")
        versions = library.versions(strategy.id)
        assert [v.version_no for v in versions] == [3, 2, 1]
        assert versions[-1].code == CODE_A
        assert versions[1].code == CODE_B

    def test_an_identical_save_does_not_create_a_version(self, library: StrategyLibrary) -> None:
        """The documented deviation from "every save writes a row": identical code
        identifies the same result, so the duplicate row would differ only in timestamp
        while making the history harder to read. Reported as `created=False` rather than
        claimed as a save that happened."""
        strategy = library.create("S", code=CODE_A).strategy
        outcome = library.save(strategy.id, CODE_A)
        assert not outcome.created
        assert outcome.version.version_no == 1
        assert library.get(strategy.id).version_count == 1

    def test_a_new_message_on_identical_code_does_write_a_version(
        self, library: StrategyLibrary
    ) -> None:
        """Annotating a version is a real edit to the record; silently dropping it would
        make the message field unreliable."""
        strategy = library.create("S", code=CODE_A).strategy
        outcome = library.save(strategy.id, CODE_A, message="checked against TradingView")
        assert outcome.created
        assert outcome.version.version_no == 2

    def test_crlf_is_normalised_so_editors_do_not_mint_versions(
        self, library: StrategyLibrary
    ) -> None:
        """Windows editors write CRLF and Monaco writes LF. Without normalisation a file
        round-tripping through both produces a new version on every save with no visible
        change and a different code hash."""
        strategy = library.create("S", code=CODE_A).strategy
        outcome = library.save(strategy.id, CODE_A.replace("\n", "\r\n"))
        assert not outcome.created

    def test_head_follows_the_latest_version(self, library: StrategyLibrary) -> None:
        strategy = library.create("S", code=CODE_A).strategy
        library.save(strategy.id, CODE_B, message="b")
        assert library.get(strategy.id).head.code == CODE_B

    def test_a_duplicate_name_is_refused_case_insensitively(
        self, library: StrategyLibrary
    ) -> None:
        """Two strategies differing only in case is a filing mistake, and their export
        bundles collide on any case-insensitive filesystem — which is all of Windows."""
        library.create("EMACross", code=CODE_A)
        with pytest.raises(NameInUse):
            library.create("emacross", code=CODE_B)

    @pytest.mark.parametrize("name", ["", "   ", "-leading", "a" * 81, "bad/slash", "no\ttab"])
    def test_unusable_names_are_refused(self, library: StrategyLibrary, name: str) -> None:
        with pytest.raises(LibraryError, match="not a usable strategy name"):
            library.create(name, code=CODE_A)

    def test_parentheses_are_allowed_in_names(self, library: StrategyLibrary) -> None:
        """`_unique_name` generates ` (2)` on an import collision, so excluding them made
        importing a bundle twice fail with a message rejecting its own generated name."""
        outcome = library.create("Mean Reversion (BTC)", code=CODE_A)
        assert outcome.strategy.name == "Mean Reversion (BTC)"

    def test_oversized_source_is_refused(self, library: StrategyLibrary) -> None:
        with pytest.raises(LibraryError, match="limit is"):
            library.create("Big", code="x = 1\n" + "# pad\n" * MAX_CODE_BYTES)

    def test_a_null_byte_is_refused_as_not_text(self, library: StrategyLibrary) -> None:
        with pytest.raises(LibraryError, match="null byte"):
            library.create("Binary", code="x = 1\x00\n")


class TestMetadata:
    def test_rename_retag_and_renote_without_a_new_version(
        self, library: StrategyLibrary
    ) -> None:
        strategy = library.create("Old", code=CODE_A).strategy
        updated = library.update_metadata(
            strategy.id, name="New", notes="why", tags=["a", "b"]
        )
        assert updated.name == "New"
        assert updated.tags == ("a", "b")
        assert updated.version_count == 1

    def test_renaming_onto_an_existing_name_is_refused(
        self, library: StrategyLibrary
    ) -> None:
        library.create("Taken", code=CODE_A)
        other = library.create("Free", code=CODE_B).strategy
        with pytest.raises(NameInUse):
            library.update_metadata(other.id, name="taken")

    def test_duplicate_tags_collapse(self, library: StrategyLibrary) -> None:
        strategy = library.create("S", code=CODE_A, tags=["btc", "BTC", "trend"]).strategy
        assert library.get(strategy.id).tags == ("btc", "trend")

    @pytest.mark.parametrize("tag", ["", "has space", "a" * 33, "bad/char"])
    def test_unusable_tags_are_refused(self, library: StrategyLibrary, tag: str) -> None:
        with pytest.raises(LibraryError, match="not a usable tag"):
            library.create("S", code=CODE_A, tags=[tag])


class TestListing:
    def test_archived_are_hidden_by_default_and_recoverable(
        self, library: StrategyLibrary
    ) -> None:
        strategy = library.create("Hidden", code=CODE_A).strategy
        library.create("Visible", code=CODE_B)
        library.archive(strategy.id)

        assert [s.name for s in library.list()] == ["Visible"]
        assert {s.name for s in library.list(include_archived=True)} == {"Hidden", "Visible"}

        library.archive(strategy.id, archived=False)
        assert {s.name for s in library.list()} == {"Hidden", "Visible"}

    def test_search_matches_name_and_notes(self, library: StrategyLibrary) -> None:
        library.create("Alpha", code=CODE_A, notes="momentum on funding")
        library.create("Beta", code=CODE_B, notes="mean reversion")
        assert [s.name for s in library.list(search="funding")] == ["Alpha"]
        assert [s.name for s in library.list(search="bet")] == ["Beta"]

    def test_tag_filter(self, library: StrategyLibrary) -> None:
        library.create("Alpha", code=CODE_A, tags=["trend"])
        library.create("Beta", code=CODE_B, tags=["meanrev"])
        assert [s.name for s in library.list(tag="TREND")] == ["Alpha"]

    def test_newest_updated_first(self, library: StrategyLibrary) -> None:
        first = library.create("First", code=CODE_A).strategy
        library.create("Second", code=CODE_B)
        library.save(first.id, CODE_B, message="touch")
        assert [s.name for s in library.list()] == ["First", "Second"]


class TestVersionsAndDiff:
    def test_diff_between_two_versions(self, library: StrategyLibrary) -> None:
        strategy = library.create("S", code=CODE_A).strategy
        library.save(strategy.id, CODE_B, message="b")
        diff = library.diff(strategy.id, 1, 2)
        assert "-class A(Strategy):" in diff
        assert "+class B(Strategy):" in diff

    def test_diff_of_a_version_against_itself_is_empty(
        self, library: StrategyLibrary
    ) -> None:
        strategy = library.create("S", code=CODE_A).strategy
        assert library.diff(strategy.id, 1, 1) == ""

    def test_an_unknown_version_is_a_not_found(self, library: StrategyLibrary) -> None:
        strategy = library.create("S", code=CODE_A).strategy
        with pytest.raises(NotFound):
            library.version(strategy.id, 99)

    def test_an_unknown_strategy_is_a_not_found(self, library: StrategyLibrary) -> None:
        with pytest.raises(NotFound):
            library.get(4242)


class TestDelete:
    def test_delete_requires_the_exact_name(self, library: StrategyLibrary) -> None:
        strategy = library.create("EMACross", code=CODE_A).strategy
        with pytest.raises(LibraryError, match="exact name"):
            library.delete(strategy.id, confirm_name="emacross")
        library.delete(strategy.id, confirm_name="EMACross")
        with pytest.raises(NotFound):
            library.get(strategy.id)

    def test_delete_removes_every_version(self, library: StrategyLibrary) -> None:
        strategy = library.create("S", code=CODE_A).strategy
        library.save(strategy.id, CODE_B, message="b")
        library.delete(strategy.id, confirm_name="S")
        remaining = library._db.execute("SELECT COUNT(*) AS n FROM strategy_versions").fetchone()
        assert remaining["n"] == 0

    def test_delete_is_blocked_by_a_live_run(self, library: StrategyLibrary) -> None:
        """Spec 5.6: runs must remain reproducible. Deleting the code behind a run leaves
        the run unreproducible, so the rule is enforced rather than advised."""
        strategy = library.create("S", code=CODE_A).strategy
        _add_run(library, strategy.id, archived=False)
        with pytest.raises(DeleteBlocked, match="not archived"):
            library.delete(strategy.id, confirm_name="S")

    def test_an_archived_run_does_not_block_but_is_removed_and_reported(
        self, library: StrategyLibrary
    ) -> None:
        """Spec 5.6 makes only non-archived runs blocking. Keeping the archived ones would
        leave rows describing runs whose code no longer exists, so they go with it — and
        the count comes back, because the name the user typed was the strategy's."""
        strategy = library.create("S", code=CODE_A).strategy
        _add_run(library, strategy.id, archived=True)
        removed = library.delete(strategy.id, confirm_name="S")
        assert removed == {"runs": 1, "versions": 1}
        assert library.list(include_archived=True) == ()
        assert library._db.execute("SELECT COUNT(*) AS n FROM runs").fetchone()["n"] == 0

    def test_archive_is_the_recoverable_alternative(self, library: StrategyLibrary) -> None:
        strategy = library.create("S", code=CODE_A).strategy
        _add_run(library, strategy.id, archived=False)
        archived = library.archive(strategy.id)
        assert archived.archived
        assert library.get(strategy.id).version_count == 1


class TestBundles:
    def test_export_round_trips_through_import(self, library: StrategyLibrary) -> None:
        strategy = library.create(
            "EMACross", code=CODE_A, notes="trend following", tags=["btc", "trend"]
        ).strategy
        data = library.export_bundle(strategy.id)
        imported = library.read_bundle(data, filename="EMACross-v1.perplab")
        assert imported.code == CODE_A
        assert imported.name == "EMACross"
        assert imported.notes == "trend following"
        assert imported.tags == ("btc", "trend")
        assert imported.warnings == ()

    def test_the_same_version_exports_byte_identically(
        self, library: StrategyLibrary
    ) -> None:
        """Fixed member timestamps. A bundle whose bytes change on every write cannot be
        checksummed, so "is this the same strategy I sent you" stops having a cheap answer.
        The manifest's `exported_ms` is the one field that moves, so this compares the
        code member rather than the whole archive."""
        strategy = library.create("S", code=CODE_A).strategy
        first = _member(library.export_bundle(strategy.id), BUNDLE_CODE_MEMBER)
        second = _member(library.export_bundle(strategy.id), BUNDLE_CODE_MEMBER)
        assert first == second

    def test_importing_a_taken_name_suffixes_rather_than_failing(
        self, library: StrategyLibrary
    ) -> None:
        strategy = library.create("EMACross", code=CODE_A).strategy
        data = library.export_bundle(strategy.id)
        outcome, warnings = library.import_bundle(data, filename="EMACross.perplab")
        assert outcome.strategy.name == "EMACross (2)"
        assert warnings == ()
        outcome2, _ = library.import_bundle(data, filename="EMACross.perplab")
        assert outcome2.strategy.name == "EMACross (3)"

    def test_a_plain_py_file_imports_with_the_filename_as_its_name(
        self, library: StrategyLibrary
    ) -> None:
        outcome, warnings = library.import_bundle(
            CODE_A.encode("utf-8"), filename="my_idea.py"
        )
        assert outcome.strategy.name == "my_idea"
        assert warnings == ()

    def test_a_bundle_with_no_manifest_imports_the_code_and_says_so(
        self, library: StrategyLibrary
    ) -> None:
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr(BUNDLE_CODE_MEMBER, CODE_A)
        imported = StrategyLibrary.read_bundle(buffer.getvalue(), filename="mystery.perplab")
        assert imported.code == CODE_A
        assert imported.name == "mystery"
        assert any("no manifest" in w for w in imported.warnings)

    def test_a_tampered_bundle_warns_rather_than_refusing(
        self, library: StrategyLibrary
    ) -> None:
        """The code is right there and readable; locking the author out of their own
        strategy over a stale hash would be worse than saying the manifest may not describe
        it."""
        strategy = library.create("S", code=CODE_A).strategy
        original = library.export_bundle(strategy.id)
        tampered = _replace_member(original, BUNDLE_CODE_MEMBER, CODE_B)
        imported = StrategyLibrary.read_bundle(tampered, filename="s.perplab")
        assert imported.code == CODE_B
        assert any("does not match the hash" in w for w in imported.warnings)

    def test_a_bundle_with_no_code_member_is_refused(self) -> None:
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr(BUNDLE_MANIFEST_MEMBER, json.dumps({"name": "x"}))
        with pytest.raises(LibraryError, match=f"no {BUNDLE_CODE_MEMBER}"):
            StrategyLibrary.read_bundle(buffer.getvalue())

    def test_a_traversal_member_name_is_ignored_not_written(self, tmp_path: Path) -> None:
        """Nothing is extracted to disk, so a member called `../../autoexec` has nowhere
        to go. Asserted rather than assumed, because "we only read two names" is exactly
        the sort of invariant a later refactor breaks."""
        target = tmp_path.parent / "escaped.txt"
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr(BUNDLE_CODE_MEMBER, CODE_A)
            archive.writestr("../../escaped.txt", "pwned")
        imported = StrategyLibrary.read_bundle(buffer.getvalue())
        assert imported.code == CODE_A
        assert not target.exists()

    def test_a_non_utf8_non_zip_file_is_refused(self) -> None:
        with pytest.raises(LibraryError, match="neither a .perplab bundle nor UTF-8"):
            StrategyLibrary.read_bundle(b"\xff\xfe\x00\x01binary")

    def test_an_empty_upload_is_refused(self) -> None:
        with pytest.raises(LibraryError, match="empty"):
            StrategyLibrary.read_bundle(b"")

    def test_the_export_filename_is_filesystem_safe(
        self, library: StrategyLibrary
    ) -> None:
        strategy = library.create("Mean Reversion (BTC)", code=CODE_A).strategy
        name = library.export_filename(strategy.id)
        assert name == "Mean-Reversion-BTC-v1.perplab"


class TestSchema:
    def test_foreign_keys_are_enforced(self, tmp_path: Path) -> None:
        """SQLite ships with `PRAGMA foreign_keys = OFF`, so a schema full of REFERENCES
        clauses does nothing unless every connection turns them on. The reference that
        matters is the one protecting run reproducibility."""
        import sqlite3

        connection = connect(tmp_path)
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO runs (strategy_id, version_id, mode, status, created_ms) "
                "VALUES (999, 999, 'BACKTEST', 'DONE', 1)"
            )
        connection.close()

    def test_a_newer_schema_is_refused_rather_than_partially_read(
        self, tmp_path: Path
    ) -> None:
        connection = connect(tmp_path)
        with connection:
            connection.execute("UPDATE schema_version SET version = ?", (SCHEMA_VERSION + 1,))
        connection.close()
        with pytest.raises(SchemaTooNew, match="Upgrade PerpLab"):
            connect(tmp_path)

    def test_reopening_finds_the_same_data(self, tmp_path: Path) -> None:
        with StrategyLibrary(tmp_path) as first:
            first.create("Persisted", code=CODE_A)
        with StrategyLibrary(tmp_path) as second:
            assert [s.name for s in second.list()] == ["Persisted"]


def _add_run(library: StrategyLibrary, strategy_id: int, *, archived: bool) -> None:
    version = library.get(strategy_id).head
    with library._db:
        library._db.execute(
            "INSERT INTO runs (strategy_id, version_id, mode, status, created_ms, "
            "archived_ms) VALUES (?, ?, 'BACKTEST', 'DONE', 1, ?)",
            (strategy_id, version.id, 1 if archived else None),
        )


def _member(data: bytes, name: str) -> bytes:
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        return archive.read(name)


def _replace_member(data: bytes, name: str, payload: str) -> bytes:
    out = io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(data)) as source, zipfile.ZipFile(out, "w") as target:
        for info in source.infolist():
            target.writestr(info, payload if info.filename == name else source.read(info))
    return out.getvalue()

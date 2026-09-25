"""
Check version-policy decisions on a fixed generated schema and optional external Calibre fixtures.

Changing PRAGMA versions simulates policy inputs, not historical schema layouts.
External fixtures are discovered at import; an empty parametrization retains
pytest’s skip behavior.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_b3_compat_matrix.py
"""
from __future__ import annotations

import sqlite3
import zipfile
from pathlib import Path

import pytest

from LiuXin_alpha.utils.calibre_compat.calibre_database_emulation import (
    CalibreDB,
    CalibreReader,
    CalibreUnsupportedVersionError,
    CalibreVersionPolicy,
)
from LiuXin_alpha.databases.database_driver_plugins.SQL.calibre_database_generator import (
    CalibreLibraryBuilder,
)
from LiuXin_alpha.databases.database_driver_plugins.SQL.calibre_database_generator.database_generator import (
    calibre_metadata_application_id,
    calibre_metadata_user_version,
)


def _set_pragmas(*, metadata_db: Path, user_version: int | None = None, application_id: int | None = None) -> None:
    """
    Set supplied SQLite user_version/application_id values after int conversion, commit, and close in finally.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_b3_compat_matrix.py


    :param metadata_db: Database file to open with sqlite3; a missing file may be
        created.
    :param user_version: Optional version value converted to int and interpolated into
        PRAGMA.
    :param application_id: Optional application ID converted to int and interpolated
        into PRAGMA.
    :return: None; omitted values are left unchanged and SQL/conversion errors
        propagate.
    """
    conn = sqlite3.connect(str(metadata_db))
    try:
        if user_version is not None:
            conn.execute(f"PRAGMA user_version = {int(user_version)}")
        if application_id is not None:
            conn.execute(f"PRAGMA application_id = {int(application_id)}")
        conn.commit()
    finally:
        conn.close()


def _add_one_book_with_custom_column(lib_root: Path) -> None:
    """
    Add a compatibility-canary book and a single text custom column with the value laconic.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_b3_compat_matrix.py


    :param lib_root: Existing generated library root to mutate.
    :return: None; writes schema, book metadata, and an EPUB file under the supplied
        library.
    """
    b = CalibreLibraryBuilder(lib_root)
    b.create_custom_column(label="mood", name="Mood", datatype="text", is_multiple=False)
    added = b.add_book(
        title="Compat Canary",
        authors=["Ada Lovelace"],
        formats={"EPUB": b"epub-bytes"},
        series=("Matrix", 1),
        tags=["compat"],
        languages=["en"],
    )
    b.set_custom_value(book_id=added.book_id, label="mood", value="laconic")


@pytest.mark.parametrize(
    "sim_user_version, expect_status, expect_action, expect_warning_substr",
    [
        # Current snapshot: clean.
        (calibre_metadata_user_version(), "ok", "continue", None),
        # Slightly older: still OK.
        (max(0, calibre_metadata_user_version() - 1), "older_than_latest", "continue", None),
        # Newer: should warn but continue (default policy).
        (calibre_metadata_user_version() + 1, "newer_than_supported", "continue_with_warnings", "schema_newer_than_supported:"),
    ],
)
def test_b3_compat_matrix_smoke_on_generated_library(
    provision_calibre_library,
    sim_user_version: int,
    expect_status: str,
    expect_action: str,
    expect_warning_substr: str | None,
) -> None:
    """
    Set a simulated schema version and check policy status/action/warnings, custom-column discovery, and a readable canary title.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_b3_compat_matrix.py::test_b3_compat_matrix_smoke_on_generated_library


    :param provision_calibre_library: Fixture factory creating an isolated blank
        library; skips when SQLite lacks the required FTS5 support.
    :param sim_user_version: Parametrized PRAGMA version written to the unchanged
        generated schema.
    :param expect_status: Expected version-plan status string.
    :param expect_action: Expected version-plan action string.
    :param expect_warning_substr: Warning substring to require, or None to require no
        warnings.
    :return: None; failed expectations raise AssertionError.
    """
    lib = provision_calibre_library(name=f"lib_b3_{sim_user_version}")
    _add_one_book_with_custom_column(lib.root)

    # Simulate older/newer versions via PRAGMA; schema remains the same, but the
    # version-policy logic must behave deterministically.
    _set_pragmas(metadata_db=lib.metadata_db, user_version=int(sim_user_version))

    expected_app = calibre_metadata_application_id()
    latest = calibre_metadata_user_version()

    db = CalibreDB.from_root(lib.root)
    info = db.schema_info(
        version_policy=CalibreVersionPolicy(
            expected_application_id=int(expected_app),
            latest_supported_user_version=int(latest),
            known_user_version_max=int(latest),
        )
    )
    assert info.user_version == int(sim_user_version)
    assert info.version_plan is not None
    assert info.version_plan.status == expect_status
    assert info.version_plan.action == expect_action

    if expect_warning_substr is None:
        assert not info.version_plan.warnings
    else:
        assert any(expect_warning_substr in w for w in info.version_plan.warnings)

    # Custom columns should be discoverable.
    labels = {c.label for c in info.custom_columns}
    assert "mood" in labels

    # And the reader should still iterate at least one payload without exceptions.
    r = CalibreReader.from_root(lib.root)
    items = list(r.iter_book_payloads(include_files=False, include_covers=False))
    assert len(items) >= 1
    assert items[0].title == "Compat Canary"


def test_b3_strict_policy_refuses_newer_user_version_but_best_effort_can_still_report(provision_calibre_library) -> None:
    """
    Check a strict newer-version policy raises normally while best-effort inspection reports a refusal plan and the actual version.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_b3_compat_matrix.py::test_b3_strict_policy_refuses_newer_user_version_but_best_effort_can_still_report


    :param provision_calibre_library: Fixture factory creating an isolated blank
        library; skips when SQLite lacks the required FTS5 support.
    :return: None; failed expectations raise AssertionError.
    """
    lib = provision_calibre_library(name="lib_b3_strict_refuse")
    _add_one_book_with_custom_column(lib.root)

    latest = int(calibre_metadata_user_version())
    _set_pragmas(metadata_db=lib.metadata_db, user_version=latest + 123)

    db = CalibreDB.from_root(lib.root)

    strict = CalibreVersionPolicy(
        expected_application_id=int(calibre_metadata_application_id()),
        latest_supported_user_version=latest,
        known_user_version_max=latest,
        allow_newer_user_version=False,
    )

    with pytest.raises(CalibreUnsupportedVersionError):
        _ = db.schema_info(version_policy=strict, best_effort=False)

    info = db.schema_info(version_policy=strict, best_effort=True)
    assert info.version_plan is not None
    assert info.version_plan.action == "refuse"
    assert info.user_version == latest + 123


def _discover_external_compat_fixtures() -> list[Path]:
    """
    List sorted non-hidden library directories and ZIP files under the module-relative compatibility fixture directory.

    Resolve the root as __file__.parents[1]/fixtures/calibre_libraries/compat, which is
    under tests/databases in this layout. Accept directories with metadata.db and ZIP
    suffixes without inspecting archive contents.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_b3_compat_matrix.py


    :return: List of Paths, or an empty list when the fixture root does not exist.
    """
    here = Path(__file__).resolve()
    fixtures_root = here.parents[1] / "fixtures" / "calibre_libraries" / "compat"
    if not fixtures_root.exists():
        return []
    out: list[Path] = []
    for p in sorted(fixtures_root.iterdir()):
        if p.name.startswith("."):
            continue
        if p.is_dir() and (p / "metadata.db").exists():
            out.append(p)
        elif p.is_file() and p.suffix.lower() == ".zip":
            out.append(p)
    return out


@pytest.mark.parametrize("fixture_path", _discover_external_compat_fixtures())
def test_b3_external_fixture_opens_and_iterates_best_effort(tmp_path: Path, fixture_path: Path) -> None:
    """
    Inspect an external library directory or extract a ZIP, then require nonnegative PRAGMAs and a first payload with a string title.

    For ZIPs, use the first discovered directory containing metadata.db; directories are
    read in place. No cases run when fixture discovery produces an empty
    parametrization.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_b3_compat_matrix.py::test_b3_external_fixture_opens_and_iterates_best_effort


    :param tmp_path: Pytest-provided temporary directory for isolated database and
        fixture files.
    :param fixture_path: Trusted optional directory or ZIP selected by
        compatibility-fixture discovery.
    :return: None; failed expectations raise AssertionError.
    """
    if fixture_path.is_dir():
        root = fixture_path
    else:
        # zip -> extract
        dst = tmp_path / fixture_path.stem
        dst.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(str(fixture_path), "r") as z:
            z.extractall(dst)
        # Find the first directory in the zip that looks like a Calibre library root.
        candidates = [dst] + [p for p in dst.rglob("*") if p.is_dir()]
        root = None
        for c in candidates:
            if (c / "metadata.db").exists():
                root = c
                break
        assert root is not None, f"Could not find metadata.db inside {fixture_path}"

    db = CalibreDB.from_root(root)
    info = db.schema_info(best_effort=True)
    # Must at least record pragmas; plan may be None if snapshot not importable in isolation.
    assert info.application_id >= 0
    assert info.user_version >= 0

    r = CalibreReader.from_root(root)
    # Best-effort iteration should not explode even on partially-mangled DBs.
    it = r.iter_book_payloads(best_effort=True, include_files=False, include_covers=False)
    first = next(it, None)
    assert first is not None
    assert isinstance(first.title, str)

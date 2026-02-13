from __future__ import annotations

import sqlite3
import zipfile
from pathlib import Path

import pytest

from LiuXin_alpha.databases.calibre_emulation import (
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
    b = CalibreLibraryBuilder(lib_root)
    b.create_custom_column(label="mood", name="Mood", datatype="text", is_multiple=False)
    book_id = b.add_book(
        title="Compat Canary",
        authors=["Ada Lovelace"],
        formats={"EPUB": b"epub-bytes"},
        series=("Matrix", 1),
        tags=["compat"],
        languages=["en"],
    )
    b.set_custom_value(book_id=book_id, label="mood", value="laconic")


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
    """Return a list of external compat fixtures (dirs or zips).

    Drop fixtures into:
        tests/fixtures/calibre_libraries/compat/

    Supported shapes:
        - directory that contains metadata.db at its root (and book folders)
        - zip file containing a directory with metadata.db at its root
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
    """Compat harness for real-world fixture libraries (older Calibre versions etc).

    These are optional: if you haven't added any fixtures yet, no cases are
    collected and the test is effectively skipped.
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

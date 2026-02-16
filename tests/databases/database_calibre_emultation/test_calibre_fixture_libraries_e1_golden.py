from __future__ import annotations

from typing import Dict, List

import pytest

from databases.calibre_fixture_libraries import (
    CalibreFixtureSpec,
    discover_calibre_fixtures,
    find_data_repo_root,
    load_expected_snapshot,
)


_DATA_ROOT = find_data_repo_root()
_SPECS: List[CalibreFixtureSpec] = []
if _DATA_ROOT is not None:
    _SPECS = discover_calibre_fixtures(_DATA_ROOT)

pytestmark = pytest.mark.skipif(
    not _SPECS,
    reason=(
        "No Calibre fixture libraries found. "
        "Check out LiuXin_alpha_data or set LIUXIN_ALPHA_DATA_ROOT."
    ),
)


def _specs_by_name(specs: List[CalibreFixtureSpec]) -> Dict[str, CalibreFixtureSpec]:
    out: Dict[str, CalibreFixtureSpec] = {}
    for s in specs:
        out[s.name] = s
    return out


def test_e1_required_fixtures_present() -> None:
    by = _specs_by_name(_SPECS)
    required = {
        "01_minimal",
        "02_customs_stress",
        "03_filesystem_drift",
        "04_unicode_chaos",
    }
    missing = sorted(required - set(by.keys()))
    assert not missing, f"Missing E1 fixture(s): {missing}. Did you run scripts/generate_calibre_fixture_libraries.py?"


@pytest.mark.parametrize(
    "fixture_name",
    [
        "01_minimal",
        "02_customs_stress",
        "03_filesystem_drift",
        "04_unicode_chaos",
        "05_mangled_optional_tables",
        "06_schema_drift_pragmas",
    ],
)
def test_e1_expected_snapshot_shape_and_counts(fixture_name: str) -> None:
    by = _specs_by_name(_SPECS)
    if fixture_name not in by:
        pytest.skip(f"Fixture not present: {fixture_name}")

    snap = load_expected_snapshot(by[fixture_name])
    assert isinstance(snap, dict)

    schema = snap.get("schema")
    assert isinstance(schema, dict)
    assert "application_id" in schema
    assert "user_version" in schema
    assert "custom_columns" in schema
    assert "issues" in schema

    counts = snap.get("counts")
    assert isinstance(counts, dict)
    for k in (
        "books",
        "formats_total",
        "authors_unique",
        "tags_unique",
        "custom_columns",
        "drift_events_total",
    ):
        assert k in counts, f"expected counts.{k}"

    books = snap.get("books")
    assert isinstance(books, list)
    assert len(books) == int(counts["books"])

    # Basic per-book shape.
    for b in books:
        assert isinstance(b, dict)
        assert "calibre_book_id" in b
        assert "title" in b
        assert "authors" in b
        assert "tags" in b
        assert "languages" in b
        assert "identifiers" in b
        assert "series" in b
        assert "formats" in b
        assert "custom" in b
        assert "warnings" in b
        assert "drift" in b
        assert "comments_html" in b
        assert "cover_path" in b


def test_e1_minimal_fixture_expectations() -> None:
    by = _specs_by_name(_SPECS)
    spec = by.get("01_minimal")
    if spec is None:
        pytest.skip("Fixture not present: 01_minimal")

    snap = load_expected_snapshot(spec)
    assert snap["counts"]["books"] == 1
    assert snap["counts"]["formats_total"] >= 1

    book = snap["books"][0]
    assert book["title"] == "Minimal Fixture"
    assert "Ada Example" in book["authors"]
    assert any(f.get("fmt") == "EPUB" for f in book["formats"])


def test_e1_customs_stress_fixture_expectations() -> None:
    by = _specs_by_name(_SPECS)
    spec = by.get("02_customs_stress")
    if spec is None:
        pytest.skip("Fixture not present: 02_customs_stress")

    snap = load_expected_snapshot(spec)
    assert snap["counts"]["books"] == 1
    assert snap["counts"]["custom_columns"] >= 8

    book = snap["books"][0]
    custom = book.get("custom")
    assert isinstance(custom, dict)
    # A few representative fields.
    assert "txt1" in custom
    assert "txtm" in custom
    assert "serx" in custom
    assert "when" in custom


def test_e1_filesystem_drift_fixture_expectations() -> None:
    by = _specs_by_name(_SPECS)
    spec = by.get("03_filesystem_drift")
    if spec is None:
        pytest.skip("Fixture not present: 03_filesystem_drift")

    snap = load_expected_snapshot(spec)
    assert snap["counts"]["books"] == 1
    assert snap["counts"]["drift_events_total"] >= 2

    book = snap["books"][0]
    drift = book.get("drift")
    assert isinstance(drift, list)
    codes = {d.get("code") for d in drift if isinstance(d, dict)}

    # This fixture is designed to trigger multiple drift conditions.
    assert "book_folder_case_mismatch" in codes or "missing_book_folder" in codes
    assert "missing_format_file" in codes
    assert "missing_cover_file" in codes
    assert "orphan_file" in codes


def test_e1_unicode_chaos_fixture_expectations() -> None:
    by = _specs_by_name(_SPECS)
    spec = by.get("04_unicode_chaos")
    if spec is None:
        pytest.skip("Fixture not present: 04_unicode_chaos")

    snap = load_expected_snapshot(spec)
    assert snap["counts"]["books"] == 1
    book = snap["books"][0]
    # Just make sure we preserved non-ascii content end-to-end.
    assert any(ord(ch) > 127 for ch in book["title"])
    assert any(any(ord(ch) > 127 for ch in a) for a in book["authors"])

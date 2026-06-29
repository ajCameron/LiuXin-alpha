from __future__ import annotations

from pathlib import Path

import pytest

from tests.databases.calibre_fixture_libraries import (
    discover_calibre_fixtures,
    extract_library_zip,
    find_data_repo_root,
    load_expected_snapshot,
    normalize_snapshot,
)


def _sum_counter(d: dict) -> int:
    total = 0
    for v in d.values():
        try:
            total += int(v)
        except Exception:
            pass
    return int(total)


@pytest.mark.parametrize("spec", ["_ALL"], ids=["fixtures"])
def test_scan_report_matches_expected_counts(tmp_path: Path, spec: str) -> None:
    data_root = find_data_repo_root()
    if data_root is None:
        pytest.skip("LiuXin_alpha_data not present; set LIUXIN_ALPHA_DATA_ROOT to run fixture tests")

    specs = discover_calibre_fixtures(data_root)
    if not specs:
        pytest.skip("No calibre fixtures discovered under LiuXin_alpha_data/calibre_libraries")

    # Keep this test reasonably fast: the detailed golden comparisons live in E1.
    # Here we focus on scan-report aggregation correctness.
    from LiuXin_alpha.utils.calibre_compat.calibre_database_emulation import scan_calibre_library

    for s in specs:
        lib_root = extract_library_zip(s, tmp_path / s.schema_key / s.name)
        expected = normalize_snapshot(load_expected_snapshot(s))

        rep = scan_calibre_library(lib_root, best_effort=True, filesystem_reconcile=True)

        assert rep.mode == "db"
        assert rep.schema is not None

        exp_counts = expected.get("counts", {})
        assert rep.counts.books == int(exp_counts.get("books", 0))
        assert rep.counts.formats_total == int(exp_counts.get("formats_total", 0))
        assert rep.counts.authors_unique == int(exp_counts.get("authors_unique", 0))
        assert rep.counts.tags_unique == int(exp_counts.get("tags_unique", 0))
        assert rep.counts.custom_columns == int(exp_counts.get("custom_columns", 0))
        assert rep.counts.drift_events_total == int(exp_counts.get("drift_events_total", 0))

        # Drift summary totals should be internally consistent.
        assert _sum_counter(dict(rep.drift.by_code)) == rep.counts.drift_events_total


def test_scan_report_opf_fallback(tmp_path: Path) -> None:
    data_root = find_data_repo_root()
    if data_root is None:
        pytest.skip("LiuXin_alpha_data not present; set LIUXIN_ALPHA_DATA_ROOT to run fixture tests")

    specs = discover_calibre_fixtures(data_root)
    if not specs:
        pytest.skip("No calibre fixtures discovered under LiuXin_alpha_data/calibre_libraries")

    # Use the first fixture, delete metadata.db, and ensure sidecar mode still yields results.
    s = specs[0]
    lib_root = extract_library_zip(s, tmp_path / "opf_fallback" / s.schema_key / s.name)
    md = lib_root / "metadata.db"
    if md.exists():
        md.unlink()

    from LiuXin_alpha.utils.calibre_compat.calibre_database_emulation import scan_calibre_library

    rep = scan_calibre_library(lib_root, best_effort=True, max_books=50)
    assert rep.mode == "opf"
    assert rep.schema is None
    assert rep.counts.books >= 1

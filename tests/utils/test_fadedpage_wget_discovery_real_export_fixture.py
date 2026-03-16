from __future__ import annotations

import importlib.util
import json
import sys
from collections import Counter
from pathlib import Path


def _load_script():
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "fadedpage_wget_discovery.py"
    spec = importlib.util.spec_from_file_location("fadedpage_wget_discovery", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _load_fixture() -> dict:
    fixture_path = Path(__file__).resolve().parents[1] / "fixtures" / "fadedpage_wget_discovery" / "real_export_snapshot.json"
    return json.loads(fixture_path.read_text(encoding="utf-8"))


def test_real_export_snapshot_stats_and_profiles_are_stable() -> None:
    payload = _load_fixture()

    assert payload["stats"] == {
        "accepted_count": 906,
        "book_count": 148,
        "candidate_count": 906,
        "group_count": 148,
        "observed_urls": 14501,
        "reason_counts": {
            "accepted": 906,
            "filtered_after_classification": 1,
            "not_ebook_shaped": 13594,
        },
        "rejected_count": 13595,
        "rejection_reason_counts": {
            "filtered_after_classification": 1,
            "not_ebook_shaped": 13594,
        },
    }

    books = payload["books"]
    assert not [book["stem"] for book in books if book.get("suspicious")]
    assert "robots" not in {book["stem"] for book in books}

    format_profiles = Counter(tuple(book.get("extensions") or []) for book in books)
    assert format_profiles == {
        ("epub", "html", "mobi", "pdf", "txt", "zip"): 145,
        ("epub", "html", "mobi", "pdf", "txt"): 1,
        ("epub", "html", "mobi", "txt", "zip"): 1,
        ("html", "txt"): 1,
    }


def test_real_export_snapshot_collapses_known_fadedpage_suffix_families() -> None:
    payload = _load_fixture()
    books = {book["stem"]: book for book in payload["books"]}

    target = books["201410M0"]
    assert target["variant_count"] == 7
    assert target["extensions"] == ["epub", "html", "mobi", "pdf", "txt", "zip"]
    assert target["source_stems"] == ["201410M0", "201410M0-a5", "201410M0-h", "201410M0-k"]
    assert target["variant_suffixes"] == ["-a5", "-h", "-k"]
    assert target["warnings"] == []
    assert target["reader_page_count"] == 1
    assert target["download_format_count"] == 6


def test_real_export_snapshot_text_report_stays_clean_and_informative() -> None:
    script = _load_script()
    payload = _load_fixture()

    report = script.render_text_report(payload, report_limit=5)

    assert "Faded Page Discovery" in report
    assert "Observed URLs: 14501" in report
    assert "Reasons: accepted=906, filtered_after_classification=1, not_ebook_shaped=13594" in report
    assert "Suspicious / Incomplete Books" in report
    assert "  - none" in report
    assert "145 books | epub, html, mobi, pdf, txt, zip" in report
    assert "201410M0 | 7 variants | epub, html, mobi, pdf, txt, zip | source_stems=201410M0, 201410M0-a5, 201410M0-h, 201410M0-k" in report

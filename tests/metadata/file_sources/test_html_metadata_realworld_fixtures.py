from __future__ import annotations

from collections.abc import Mapping

import pytest

from tests.support.html_ingest_fixture_expectations import EXPECTED_HTML_INGEST_RESULTS


def _values(raw):
    if raw is None:
        return []
    if isinstance(raw, Mapping):
        return list(raw.keys())
    if isinstance(raw, str):
        return [raw]
    try:
        return list(raw)
    except TypeError:
        return [raw]


def _non_empty_identifiers(md) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for key, raw in getattr(md, "identifiers", {}).items():
        vals = _values(raw)
        if vals:
            out[key] = vals
    return out


def _summary(md) -> dict[str, object]:
    return {
        "title": getattr(md, "title", None),
        "authors": _values(getattr(md, "authors", None)),
        "languages": _values(getattr(md, "languages", None)),
        "tags": _values(getattr(md, "tags", None)),
        "identifiers": _non_empty_identifiers(md),
    }


@pytest.mark.parametrize(
    "fixture_name,expected_summary",
    sorted(EXPECTED_HTML_INGEST_RESULTS.items()),
)
def test_html_broken_fixture_corpus_path_and_stream_parity(
    fixture_name: str,
    expected_summary: dict[str, object],
    html_ingest_fixture,
) -> None:
    from LiuXin_alpha.metadata.file_sources.html import get_metadata

    path = html_ingest_fixture(filename=fixture_name, verify_hash=True)

    md_from_path = get_metadata(path)
    with path.open("rb") as stream:
        md_from_stream = get_metadata(stream)
        assert stream.tell() == 0

    summary_path = _summary(md_from_path)
    summary_stream = _summary(md_from_stream)
    assert summary_path == summary_stream

    assert summary_path == expected_summary


@pytest.mark.parametrize("fixture_name", sorted(EXPECTED_HTML_INGEST_RESULTS))
def test_html_broken_fixture_corpus_is_deterministic(
    fixture_name: str,
    html_ingest_fixture,
) -> None:
    from LiuXin_alpha.metadata.file_sources.html import get_metadata

    path = html_ingest_fixture(filename=fixture_name, verify_hash=True)
    first = _summary(get_metadata(path))
    for _ in range(5):
        assert _summary(get_metadata(path)) == first


def test_html_known_md_fixtures_remain_stable_and_equivalent(html_expected_title, md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources.html import get_metadata

    html_path = md_test_fixture(file_ext="html", file_num=1, verify_hash=True)
    htm_path = md_test_fixture(file_ext="htm", file_num=1, verify_hash=False)

    html_md = get_metadata(html_path)
    htm_md = get_metadata(htm_path)

    html_summary = _summary(html_md)
    htm_summary = _summary(htm_md)

    assert html_summary == htm_summary
    assert html_summary["title"] == html_expected_title
    assert html_summary["authors"] == ["Unknown"]
    assert html_summary["tags"] == []
    assert html_summary["identifiers"] == {}


@pytest.fixture
def html_expected_title() -> str:
    return "The Project Gutenberg eBook of Twenty Thousand Leagues Under the Sea, by Jules Verne"

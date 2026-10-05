"""
Verify HTML metadata parsing against representative real-world fixtures.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test html metadata realworld fixtures through its owning regression module::

        python -m pytest -q tests/metadata/file_sources/test_html_metadata_realworld_fixtures.py
"""
from __future__ import annotations

from collections.abc import Mapping

import pytest

from tests.support.html_ingest_fixture_expectations import EXPECTED_HTML_INGEST_RESULTS


def _values(raw):
    """
    Perform the values test-helper operation with deterministic inputs.

    Example:
        Exercise values through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_html_metadata_realworld_fixtures.py


    :param raw: Value supplied for raw in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
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
    """
    Perform the non empty identifiers test-helper operation with deterministic inputs.

    Example:
        Exercise non empty identifiers through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_html_metadata_realworld_fixtures.py


    :param md: Value supplied for md in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    out: dict[str, list[str]] = {}
    for key, raw in getattr(md, "identifiers", {}).items():
        vals = _values(raw)
        if vals:
            out[key] = vals
    return out


def _summary(md) -> dict[str, object]:
    """
    Perform the summary test-helper operation with deterministic inputs.

    Example:
        Exercise summary through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_html_metadata_realworld_fixtures.py


    :param md: Value supplied for md in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
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
    """
    Verify html broken fixture corpus path and stream parity.

    Example:
        Exercise test html broken fixture corpus path and stream parity through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_html_metadata_realworld_fixtures.py


    :param fixture_name: Value supplied for fixture name in the focused test operation.
    :param expected_summary: Value supplied for expected summary in the focused test
        operation.
    :param html_ingest_fixture: Value supplied for html ingest fixture in the focused
        test operation.
    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify html broken fixture corpus remains deterministic.

    Example:
        Exercise test html broken fixture corpus is deterministic through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_html_metadata_realworld_fixtures.py


    :param fixture_name: Value supplied for fixture name in the focused test operation.
    :param html_ingest_fixture: Value supplied for html ingest fixture in the focused
        test operation.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.html import get_metadata

    path = html_ingest_fixture(filename=fixture_name, verify_hash=True)
    first = _summary(get_metadata(path))
    for _ in range(5):
        assert _summary(get_metadata(path)) == first


def test_html_known_md_fixtures_remain_stable_and_equivalent(html_expected_title, md_test_fixture) -> None:
    """
    Verify html known md fixtures remain stable and equivalent.

    Example:
        Exercise test html known md fixtures remain stable and equivalent through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_html_metadata_realworld_fixtures.py


    :param html_expected_title: Value supplied for html expected title in the focused
        test operation.
    :param md_test_fixture: Value supplied for md test fixture in the focused test
        operation.
    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Perform the html expected title test-helper operation with deterministic inputs.

    Example:
        Exercise html expected title through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_html_metadata_realworld_fixtures.py


    :return: The deterministic value, row, identity or collection described above.
    """
    return "The Project Gutenberg eBook of Twenty Thousand Leagues Under the Sea, by Jules Verne"

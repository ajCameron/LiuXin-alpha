"""
Verify metadata inference from filenames and free-form strings.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test from string metadata source through its owning regression module::

        python -m pytest -q tests/metadata/file_sources/test_from_string_metadata_source.py
"""
from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path

import pytest


def _values(raw):
    """
    Perform the values test-helper operation with deterministic inputs.

    Example:
        Exercise values through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_from_string_metadata_source.py


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


def _first(raw):
    """
    Perform the first test-helper operation with deterministic inputs.

    Example:
        Exercise first through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_from_string_metadata_source.py


    :param raw: Value supplied for raw in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    vals = _values(raw)
    return vals[0] if vals else None


def _series_index_for(md, series_name: str):
    """
    Perform the series index for test-helper operation with deterministic inputs.

    Example:
        Exercise series index for through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_from_string_metadata_source.py


    :param md: Value supplied for md in the focused test operation.
    :param series_name: Value supplied for series name in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    raw = getattr(md, "series_index", None)
    if isinstance(raw, Mapping):
        return raw.get(series_name)
    return None


def test_from_string_module_import_smoke() -> None:
    """
    Verify from string module import smoke.

    Example:
        Exercise test from string module import smoke through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_from_string_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.file_sources.from_string as m

    assert m is not None


def test_from_string_title_author_basic_hyphen() -> None:
    """
    Verify from string title author basic hyphen.

    Example:
        Exercise test from string title author basic hyphen through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_from_string_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.from_string import get_metadata

    md = get_metadata("The Left Hand of Darkness - Ursula K. Le Guin.epub")

    assert md.title == "The Left Hand of Darkness"
    assert _values(md.authors) == ["Ursula K. Le Guin"]


def test_from_string_author_title_basic_hyphen() -> None:
    """
    Verify from string author title basic hyphen.

    Example:
        Exercise test from string author title basic hyphen through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_from_string_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.from_string import get_metadata

    md = get_metadata("Isaac Asimov - Foundation.azw3")

    assert md.title == "Foundation"
    assert _values(md.authors) == ["Isaac Asimov"]


def test_from_string_by_pattern_multiple_authors_unicode() -> None:
    """
    Verify from string by pattern multiple authors unicode.

    Example:
        Exercise test from string by pattern multiple authors unicode through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_from_string_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.from_string import get_metadata

    md = get_metadata("世界の終りとハードボイルド・ワンダーランド by 村上 春樹 & Γιάννης")

    assert md.title == "世界の終りとハードボイルド・ワンダーランド"
    assert _values(md.authors) == ["村上 春樹", "Γιάννης"]


def test_from_string_extracts_isbn_and_drops_it() -> None:
    """
    Verify from string extracts isbn and drops it.

    Example:
        Exercise test from string extracts isbn and drops it through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_from_string_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.from_string import drop_isbn_from_string, get_isbn_from_string

    raw = "Book Title (ISBN 978-1-4028-9462-6) - Jane Doe"
    isbns = get_isbn_from_string(raw)
    dropped = drop_isbn_from_string(raw)

    assert isbns == ["9781402894626"]
    assert "978-1-4028-9462-6" not in dropped
    assert "ISBN" not in dropped.upper()


def test_from_string_pop_date_extracts_bracketed_date() -> None:
    """
    Verify from string pop date extracts bracketed date.

    Example:
        Exercise test from string pop date extracts bracketed date through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_from_string_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.from_string import pop_date

    pubdate, remainder = pop_date("A Book (2020-12-31) - Jane Doe")

    assert pubdate is not None
    assert getattr(pubdate, "year", None) == 2020
    assert "2020-12-31" not in remainder


def test_from_string_pop_date_does_not_strip_unbracketed_year_title_prefix() -> None:
    """
    Verify from string pop date does not strip unbracketed year title prefix.

    Example:
        Exercise test from string pop date does not strip unbracketed year title prefix through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_from_string_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.from_string import pop_date

    pubdate, remainder = pop_date("2001 A Space Odyssey")

    assert pubdate is None
    assert remainder == "2001 A Space Odyssey"


def test_from_string_parses_series_tags_comments_and_date() -> None:
    """
    Verify from string parses series tags comments and date.

    Example:
        Exercise test from string parses series tags comments and date through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_from_string_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.from_string import get_metadata

    md = get_metadata("The Name of the Wind - Patrick Rothfuss (Kingkiller Chronicle #1) [tags: fantasy, epic] (2007)")

    assert md.title == "The Name of the Wind"
    assert _values(md.authors) == ["Patrick Rothfuss"]
    assert _first(md.series) == "Kingkiller Chronicle"
    assert float(_series_index_for(md, "Kingkiller Chronicle")) == 1.0
    assert set(_values(md.tags)) == {"fantasy", "epic"}
    assert getattr(md.pubdate, "year", None) == 2007


def test_from_string_custom_regex_override_on_full_path() -> None:
    """
    Verify from string custom regex override on full path.

    Example:
        Exercise test from string custom regex override on full path through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_from_string_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.from_string import get_metadata

    pattern = re.compile(
        r".*/(?P<authors>[^/]+) - (?P<title>[^\[]+) \[(?P<series>[^\]]+) (?P<series_index>\d+)\] \((?P<published>\d{4})\)$"
    )
    source = "/srv/books/scifi/Arthur C. Clarke - Childhood's End [Space Masters 2] (1953).epub"

    md = get_metadata(source, force_regex=pattern, full_path_regex=True)

    assert md.title == "Childhood's End"
    assert _values(md.authors) == ["Arthur C. Clarke"]
    assert _first(md.series) == "Space Masters"
    assert float(_series_index_for(md, "Space Masters")) == 2.0
    assert getattr(md.pubdate, "year", None) == 1953


def test_from_string_tokenize_preserves_parenthesized_tokens() -> None:
    """
    Verify from string tokenize preserves parenthesized tokens.

    Example:
        Exercise test from string tokenize preserves parenthesized tokens through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_from_string_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.from_string import tokenize

    tokens = tokenize("Title_(Part 1)-Author")

    assert "Title" in tokens
    assert "(Part 1)" in tokens
    assert "Author" in tokens


def test_from_string_separator_count_returns_ordered_counts() -> None:
    """
    Verify from string separator count returns ordered counts.

    Example:
        Exercise test from string separator count returns ordered counts through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_from_string_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.from_string import get_separator_count

    counts = get_separator_count("a-b-c_d")

    # '-' should be at least as common as '_' for this string.
    assert counts["-"] >= counts["_"]


def test_from_string_handles_pathlike_input(tmp_path: Path) -> None:
    """
    Verify from string handles pathlike input.

    Example:
        Exercise test from string handles pathlike input through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_from_string_metadata_source.py


    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.from_string import get_metadata

    fake = tmp_path / "A Fire Upon the Deep - Vernor Vinge.mobi"
    md = get_metadata(fake)

    assert md.title == "A Fire Upon the Deep"
    assert _values(md.authors) == ["Vernor Vinge"]


def test_from_string_returns_unknown_author_when_not_detectable() -> None:
    """
    Verify from string returns unknown author when not detectable.

    Example:
        Exercise test from string returns unknown author when not detectable through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_from_string_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.from_string import get_metadata

    md = get_metadata("totally_weird_filename_without_author_information")

    assert md.title == "totally weird filename without author information"
    assert _first(md.authors) == "Unknown"


def test_from_string_tolerates_non_matching_force_regex() -> None:
    """
    Verify from string tolerates non matching force regex.

    Example:
        Exercise test from string tolerates non matching force regex through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_from_string_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.from_string import get_metadata

    md = get_metadata("Dune - Frank Herbert", force_regex=r"^DOES_NOT_MATCH$")

    assert md.title == "Dune"
    assert _values(md.authors) == ["Frank Herbert"]

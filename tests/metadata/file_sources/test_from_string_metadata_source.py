from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path

import pytest


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


def _first(raw):
    vals = _values(raw)
    return vals[0] if vals else None


def _series_index_for(md, series_name: str):
    raw = getattr(md, "series_index", None)
    if isinstance(raw, Mapping):
        return raw.get(series_name)
    return None


def test_from_string_module_import_smoke() -> None:
    import LiuXin_alpha.metadata.file_sources.from_string as m

    assert m is not None


def test_from_string_title_author_basic_hyphen() -> None:
    from LiuXin_alpha.metadata.file_sources.from_string import get_metadata

    md = get_metadata("The Left Hand of Darkness - Ursula K. Le Guin.epub")

    assert md.title == "The Left Hand of Darkness"
    assert _values(md.authors) == ["Ursula K. Le Guin"]


def test_from_string_author_title_basic_hyphen() -> None:
    from LiuXin_alpha.metadata.file_sources.from_string import get_metadata

    md = get_metadata("Isaac Asimov - Foundation.azw3")

    assert md.title == "Foundation"
    assert _values(md.authors) == ["Isaac Asimov"]


def test_from_string_by_pattern_multiple_authors_unicode() -> None:
    from LiuXin_alpha.metadata.file_sources.from_string import get_metadata

    md = get_metadata("世界の終りとハードボイルド・ワンダーランド by 村上 春樹 & Γιάννης")

    assert md.title == "世界の終りとハードボイルド・ワンダーランド"
    assert _values(md.authors) == ["村上 春樹", "Γιάννης"]


def test_from_string_extracts_isbn_and_drops_it() -> None:
    from LiuXin_alpha.metadata.file_sources.from_string import drop_isbn_from_string, get_isbn_from_string

    raw = "Book Title (ISBN 978-1-4028-9462-6) - Jane Doe"
    isbns = get_isbn_from_string(raw)
    dropped = drop_isbn_from_string(raw)

    assert isbns == ["9781402894626"]
    assert "978-1-4028-9462-6" not in dropped
    assert "ISBN" not in dropped.upper()


def test_from_string_pop_date_extracts_bracketed_date() -> None:
    from LiuXin_alpha.metadata.file_sources.from_string import pop_date

    pubdate, remainder = pop_date("A Book (2020-12-31) - Jane Doe")

    assert pubdate is not None
    assert getattr(pubdate, "year", None) == 2020
    assert "2020-12-31" not in remainder


def test_from_string_pop_date_does_not_strip_unbracketed_year_title_prefix() -> None:
    from LiuXin_alpha.metadata.file_sources.from_string import pop_date

    pubdate, remainder = pop_date("2001 A Space Odyssey")

    assert pubdate is None
    assert remainder == "2001 A Space Odyssey"


def test_from_string_parses_series_tags_comments_and_date() -> None:
    from LiuXin_alpha.metadata.file_sources.from_string import get_metadata

    md = get_metadata("The Name of the Wind - Patrick Rothfuss (Kingkiller Chronicle #1) [tags: fantasy, epic] (2007)")

    assert md.title == "The Name of the Wind"
    assert _values(md.authors) == ["Patrick Rothfuss"]
    assert _first(md.series) == "Kingkiller Chronicle"
    assert float(_series_index_for(md, "Kingkiller Chronicle")) == 1.0
    assert set(_values(md.tags)) == {"fantasy", "epic"}
    assert getattr(md.pubdate, "year", None) == 2007


def test_from_string_custom_regex_override_on_full_path() -> None:
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
    from LiuXin_alpha.metadata.file_sources.from_string import tokenize

    tokens = tokenize("Title_(Part 1)-Author")

    assert "Title" in tokens
    assert "(Part 1)" in tokens
    assert "Author" in tokens


def test_from_string_separator_count_returns_ordered_counts() -> None:
    from LiuXin_alpha.metadata.file_sources.from_string import get_separator_count

    counts = get_separator_count("a-b-c_d")

    # '-' should be at least as common as '_' for this string.
    assert counts["-"] >= counts["_"]


def test_from_string_handles_pathlike_input(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.from_string import get_metadata

    fake = tmp_path / "A Fire Upon the Deep - Vernor Vinge.mobi"
    md = get_metadata(fake)

    assert md.title == "A Fire Upon the Deep"
    assert _values(md.authors) == ["Vernor Vinge"]


def test_from_string_returns_unknown_author_when_not_detectable() -> None:
    from LiuXin_alpha.metadata.file_sources.from_string import get_metadata

    md = get_metadata("totally_weird_filename_without_author_information")

    assert md.title == "totally weird filename without author information"
    assert _first(md.authors) == "Unknown"


def test_from_string_tolerates_non_matching_force_regex() -> None:
    from LiuXin_alpha.metadata.file_sources.from_string import get_metadata

    md = get_metadata("Dune - Frank Herbert", force_regex=r"^DOES_NOT_MATCH$")

    assert md.title == "Dune"
    assert _values(md.authors) == ["Frank Herbert"]

from __future__ import annotations

import re
from collections.abc import Iterator

from LiuXin_alpha.metadata.metadata import MetaData


def _values(raw):
    if raw is None:
        return []
    if isinstance(raw, dict):
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


class _BadIterable:
    def __iter__(self) -> Iterator[str]:
        raise RuntimeError("cannot iterate")


class _SeriesCarrier:
    def __init__(self, series) -> None:
        self.series = series


def test_from_string_unicode_torture_filename_with_regex_group_aliases() -> None:
    from LiuXin_alpha.metadata.file_sources.from_string import get_metadata

    pattern = re.compile(
        r"^(?P<title>.+?) -- (?P<author>.+?) -- "
        r"(?P<series>.+?) #(?P<series_index>\d+,\d+) -- "
        r"(?P<tag>.+?) -- (?P<lang>.+?) -- "
        r"(?P<publisher>.+?) -- (?P<year>\d{4}) -- "
        r"(?P<isbn>978-0-306-40615-7) -- (?P<comment>.+)$"
    )
    source = (
        "Café\u0301 Σειρά — 世界 😀 -- Renée Faßbinder / 李白 | Александр Пушкин -- "
        "Unicode シリーズ #7,5 -- naïve; δοκιμή | テスト -- ar -- "
        "دار النشر -- 2024 -- 978-0-306-40615-7 -- הערה مرحبا"
    )

    md = get_metadata(source, force_regex=[r"(", b"", pattern], full_path_regex=True)

    assert md.title == "Café\u0301 Σειρά — 世界 😀"
    assert _values(md.authors) == ["Renée Faßbinder", "李白", "Александр Пушкин"]
    assert _first(md.series) == "Unicode シリーズ"
    assert float(getattr(md, "series_index")["Unicode シリーズ"]) == 7.5
    assert set(_values(md.tags)) == {"naïve", "δοκιμή", "テスト"}
    assert md.language == "ar"
    assert _first(md.publisher) == "دار النشر"
    assert getattr(md.pubdate, "year", None) == 2024
    assert _first(md.isbn) == "9780306406157"
    assert "مرحبا" in _first(md.comments)


def test_from_string_bytes_invalid_utf8_and_fullwidth_separators_fail_soft() -> None:
    from LiuXin_alpha.metadata.file_sources.from_string import get_metadata

    raw = "タイトル＿副題 by 作者\xff名 (tags: ＳＦ;歴史) (published:2020)".encode("utf-8", "surrogatepass")
    md = get_metadata(raw)

    assert "タイトル" in md.title
    assert _first(md.authors)
    assert getattr(md.pubdate, "year", None) == 2020


def test_from_string_helper_edges_cover_scores_and_null_paths(monkeypatch) -> None:
    import LiuXin_alpha.metadata.file_sources.from_string as fs

    assert fs._coerce_text(None) == ""
    assert "\ufffd" in fs._coerce_text(b"\xff")
    assert fs._split_authors("") == []
    assert fs._split_authors("by Alice Example; Alice Example | Bob Writer") == [
        "Alice Example",
        "Bob Writer",
    ]
    assert fs._author_score("") == 0
    assert fs._author_score("Alice & Bob") > fs._author_score("Volume 2")
    assert fs._title_score("") == 0
    assert fs._title_score("The Long Road: Home?") > 0

    monkeypatch.setattr(fs, "check_name", lambda _text: (_ for _ in ()).throw(RuntimeError("boom")))
    assert fs._author_score("Exploding Name") >= 0
    assert fs._title_score("Exploding Title") >= 0

    md = MetaData()
    fs._set_if_non_empty(md, "title", None)
    fs._set_if_non_empty(md, "title", "   ")
    assert md.is_null("title")
    fs._set_if_non_empty(md, "title", "Set Title")
    assert md.title == "Set Title"

    fs._append_comment(md, "")
    fs._append_comment(md, "  comment text  ")
    assert _first(md.comments) == "comment text"

    assert fs._known_series_names(MetaData()) == []
    assert fs._known_series_names(_SeriesCarrier(None)) == []
    assert fs._known_series_names(_SeriesCarrier({"Series A": 1})) == ["Series A"]
    assert fs._known_series_names(_SeriesCarrier("  Series B  ")) == ["Series B"]
    assert fs._known_series_names(_SeriesCarrier(_BadIterable())) == []


def test_from_string_regex_application_and_series_index_edges() -> None:
    import LiuXin_alpha.metadata.file_sources.from_string as fs

    md = MetaData()
    md.series = "Existing Series"
    fs._apply_regex_groups_to_metadata(
        md,
        {
            "series_index": "3.25",
            "isbn": "not-an-isbn",
            "publisher": "  Publisher  ",
            "tags": "one,,two/three",
            "language": "el",
            "published": "not-a-date",
            "comments": "  hello  ",
        },
    )

    assert float(md.series_index["Existing Series"]) == 3.25
    assert md.is_null("isbn")
    assert _first(md.publisher) == "Publisher"
    assert set(_values(md.tags)) == {"one", "two", "three"}
    assert md.language == "el"
    assert _first(md.comments) == "hello"
    assert md.is_null("pubdate")

    fs._apply_regex_groups_to_metadata(md, {"series_index": "not-a-number"})
    assert float(md.series_index["Existing Series"]) == 3.25


def test_from_string_private_parser_helpers_torture_tokens() -> None:
    import LiuXin_alpha.metadata.file_sources.from_string as fs

    assert fs._parse_date_value(None) is None
    assert fs._parse_date_value("not-a-date") is None
    assert getattr(fs._parse_date_value("1999"), "year", None) == 1999
    assert getattr(fs._parse_date_value("2020_12_31"), "year", None) == 2020

    compiled = fs._compile_patterns(True)
    assert compiled
    assert fs._compile_patterns(False) == []
    assert fs._compile_patterns(42) == []
    assert len(fs._compile_patterns([re.compile("x"), b"(?P<title>bytes)", "(", ""])) == 2

    parenthesized, plain = fs._consume_parenthesized_tokens(["Title", "(Series #2)", "[tags: a;b]"])
    assert parenthesized == ["(Series #2)", "[tags: a;b]"]
    assert plain == ["Title"]

    series, kept = fs._extract_series_from_parenthesized_tokens(["(comment)", "(Saga [4])", "(tail)"])
    assert series == ("Saga", 4.0)
    assert kept == ["(comment)", "(tail)"]
    assert fs._extract_series_from_parenthesized_tokens(["(comment)"]) == (None, ["(comment)"])

    tags, comments = fs._extract_tags_and_comments_from_parenthesized(
        ["(tags: alpha; beta|gamma)", "(#delta #epsilon)", "(plain comment)"]
    )
    assert tags == ["alpha", "beta", "gamma", "delta", "epsilon"]
    assert comments == ["plain comment"]

    assert fs._parse_title_and_authors_heuristic("") == (None, [], [])
    assert fs._parse_title_and_authors_heuristic("No Separator Title") == ("No Separator Title", [], [])
    assert fs._parse_title_and_authors_heuristic("Book by ") == ("Book by", [], [])
    assert fs._parse_title_and_authors_heuristic("Alice Example / OneWord")[0] == "OneWord"
    assert fs._parse_title_and_authors_heuristic("OneWord / Alice Example")[0] == "OneWord"
    assert fs._parse_title_and_authors_heuristic("Alice Example - The Long Road Home")[0] == "The Long Road Home"
    assert fs._parse_title_and_authors_heuristic("The Long Road Home - Alice Example")[0] == "The Long Road Home"

    assert fs.tokenize("") == []
    assert fs.tokenize("Title (SPLIT) (SPLIT) Author").count("(SPLIT)") == 1
    assert fs.split_out_parenthesized_text(None) == []
    assert fs.extract_by_parenthesis_regex("abc", r"\(([^)]*)\)") == ["abc"]
    assert fs.test_for_parenthesis("plain") is False
    assert fs.test_for_parenthesis("plain [tag]") is True


def test_from_string_public_isbn_and_date_edges() -> None:
    from LiuXin_alpha.metadata.file_sources.from_string import (
        drop_isbn_from_string,
        get_isbn_from_string,
        pop_date,
    )

    assert get_isbn_from_string("bad 1234567890 value") == []
    assert get_isbn_from_string("ISBN-13: 978 0 306 40615 7") == ["9780306406157"]
    assert "978" not in drop_isbn_from_string("Title ISBN-13: 978 0 306 40615 7 ISBN")
    assert "0306406152" not in drop_isbn_from_string("Title 0-306-40615-2 Author")

    date, rest = pop_date("Title published:2021-04-05 Author")
    assert getattr(date, "year", None) == 2021
    assert "published" not in rest

    date, rest = pop_date("Title (not-a-date)")
    assert date is None
    assert rest == "Title (not-a-date)"

    date, rest = pop_date("Title (2020-99-99)")
    assert date is None
    assert rest == "Title (2020-99-99)"

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from LiuXin_alpha.metadata.book import base as book_base
from LiuXin_alpha.metadata.book.base import calibreMetadata, field_from_string, human_readable


def _text_multiple_meta(name: str = "Custom Tags") -> dict[str, object]:
    return {
        "name": name,
        "datatype": "text",
        "is_multiple": {
            "cache_to_list": "|",
            "ui_to_list": ",",
            "list_to_ui": ", ",
        },
        "display": {},
    }


def _series_meta(name: str = "Custom Series") -> dict[str, object]:
    return {
        "name": name,
        "datatype": "series",
        "is_multiple": {},
        "display": {},
    }


def test_calibre_metadata_core_accessors_identifiers_and_copies() -> None:
    metadata = calibreMetadata("  Example Book  ", ["Author One"])

    assert metadata.title == "Example Book"
    assert metadata.authors == ["Author One"]
    assert metadata.author == ["Author One"]
    assert metadata.language == "und"
    assert metadata.is_null("tags")
    assert metadata.has_key("tags")
    assert "tags" in set(metadata)

    metadata.set_attr("publisher", "  Test Publisher  ")
    metadata.language = "eng"
    metadata.isbn = " 978,0000000001 "
    metadata.set_identifier(" DOI: ", " 10.1234/demo,value ")

    assert metadata.publisher == "Test Publisher"
    assert metadata.languages == ["eng"]
    assert metadata.language == "eng"
    assert metadata.isbn == "978|0000000001"
    assert metadata.get_identifiers() == {
        "isbn": "978|0000000001",
        "doi": "10.1234/demo|value",
    }

    copied_identifiers = metadata.get_identifiers()
    copied_identifiers["isbn"] = "changed"
    assert metadata.isbn == "978|0000000001"

    metadata.set_identifier("", "ignored")
    metadata.set_identifier("doi", "")
    assert not metadata.has_identifier("doi")


def test_custom_metadata_values_extras_and_non_none_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(book_base, "sort_key", lambda value: str(value).casefold())

    metadata = calibreMetadata("Custom Book", ["Writer"])
    metadata.tags = ["Zulu", "alpha"]
    metadata.set_user_metadata("#labels", _text_multiple_meta("Labels"))
    metadata.set_user_metadata("#sequence", _series_meta("Sequence"))

    assert metadata.get("#labels") == []
    metadata.set("#labels", ["beta", "Alpha"])
    metadata.set("#sequence", "Arc", extra=2.0)

    assert metadata.get_extra("#sequence") == 2.0
    assert metadata.get("#missing", "fallback") == "fallback"
    assert metadata.format_tags() == "alpha, Zulu"
    assert metadata.format_field("#labels") == ("Labels", "beta, Alpha")
    assert metadata.format_field("#sequence") == ("Sequence", "Arc [2]")
    assert metadata.format_field("#sequence_index") == ("Sequence_index", "2")

    fields = metadata.all_non_none_fields()
    assert fields["title"] == "Custom Book"
    assert fields["#labels"] == ["beta", "Alpha"]
    assert fields["#sequence_index"] == 2.0

    user_metadata = metadata.get_all_user_metadata(make_copy=True)
    user_metadata["#labels"]["#value#"].append("mutated")
    assert metadata.get("#labels") == ["beta", "Alpha"]


def test_deepcopy_metadata_and_smart_update_merge_semantics() -> None:
    target = calibreMetadata("Original", ["Unknown"])
    target.tags = ["Existing", "Keep"]
    target.comments = "short"
    target.cover_data = ("jpeg", b"tiny")
    target.set_identifier("isbn", "old")
    target.set_user_metadata("#labels", _text_multiple_meta("Labels"))
    target.set("#labels", ["alpha"])

    source = calibreMetadata("Replacement", ["Real Author"])
    source.title_sort = "Replacement Sort"
    source.author_sort = "Author, Real"
    source.author_sort_map = {"Real Author": "Author, Real"}
    source.tags = ["existing", "New"]
    source.comments = "a much longer comment"
    source.cover_data = ("jpeg", b"longer-cover")
    source.set_identifier("isbn", "new")
    source.set_identifier("doi", "10.1234/demo")
    source.set_user_metadata("#labels", _text_multiple_meta("Labels"))
    source.set("#labels", ["ALPHA", "Beta"])

    target.smart_update(source)

    assert target.title == "Replacement"
    assert target.title_sort == "Replacement Sort"
    assert target.authors == ["Real Author"]
    assert target.author_sort == "Author, Real"
    assert target.tags == ["existing", "Keep", "New"]
    assert target.comments == "a much longer comment"
    assert target.cover_data == ("jpeg", b"longer-cover")
    assert target.get_identifiers() == {"isbn": "new", "doi": "10.1234/demo"}
    assert target.get("#labels") == ["ALPHA", "Beta"]

    clone = target.deepcopy_metadata()
    clone.title = "Clone"
    clone.set("#labels", ["clone"])

    assert target.title == "Replacement"
    assert target.get("#labels") == ["ALPHA", "Beta"]
    assert target.deepcopy(class_generator=object) is None


def test_formatting_helpers_and_string_representation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(book_base, "sort_key", lambda value: str(value).casefold())

    metadata = calibreMetadata("Formatted", ["Ada Lovelace", "Grace Hopper"])
    metadata.publisher = "Publisher"
    metadata.book_producer = "Producer"
    metadata.tags = ["beta", "Alpha"]
    metadata.series = "Series"
    metadata.series_index = 2.5
    metadata.languages = ["eng", "fra"]
    metadata.rating = 8
    metadata.pubdate = datetime(2024, 1, 2, tzinfo=timezone.utc)
    metadata.rights = "Public Domain"
    metadata.comments = "Comment text"
    metadata.set_identifier("isbn", "9780000000001")

    assert human_readable(2 * 1024 * 1024) == "2.00MB"
    assert metadata.format_series_index("bad") == "1"
    assert metadata.format_authors() == "Ada Lovelace & Grace Hopper"
    assert metadata.format_rating() == "8.0"
    assert metadata.format_rating(4, divide_by=2) == "2.0"
    assert metadata.format_field("authors") == ("Authors", "Ada Lovelace & Grace Hopper")
    assert metadata.format_field("series") == ("Series", "Series [2.50]")
    assert metadata.format_field("rating") == ("Rating", "4")
    assert metadata.format_field("size") == ("Size", None)
    assert metadata.format_field("missing") == (None, None)

    rendered = str(metadata)
    assert "Formatted" in rendered
    assert "Ada Lovelace & Grace Hopper" in rendered
    assert "Series #2.50" in rendered
    assert "isbn:9780000000001" in rendered


def test_unicode_torture_metadata_fields_formatting_and_copies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(book_base, "sort_key", lambda value: str(value).casefold())

    title = "Résumé 東京 普通话 简体中文 日本語 こんにちは العربية עברית Звёзды 한국어 हिन्दी e\u0301 🚀"
    authors = [
        "李 白",
        "王小明",
        "山田太郎",
        "أحمد بن خالد",
        "Мария Иванова",
        "Renée O'Connor",
        "अनामिका",
    ]
    metadata = calibreMetadata("  " + title + "  ", authors)
    metadata.publisher = "出版社 / 简体中文出版社 / 日本の出版社 / دار النشر / Издательство"
    metadata.comments = (
        "<p>naïve café 東京 普通话 简体中文 日本語 こんにちは العربية עברית "
        "Звёзды हिन्दी e\u0301 🚀</p>"
    )
    metadata.tags = [
        "café",
        "東京",
        "普通话",
        "简体中文",
        "日本語",
        "こんにちは",
        "العربية",
        "עברית",
        "Звёзды",
        "हिन्दी",
        "e\u0301",
        "🚀",
    ]
    metadata.languages = ["jpn", "zho", "cmn", "ara", "heb", "rus", "hin", "kor"]
    metadata.set_identifier("DOI", "10.1234/東京-مرحبا")
    metadata.set_user_metadata("#labels", _text_multiple_meta("Etiquetas / 标签 / وسوم"))
    metadata.set("#labels", ["niño", "漢字", "العربية", "हिन्दी", "e\u0301"])

    assert metadata.title == title
    assert metadata.format_authors() == " & ".join(authors)
    assert metadata.format_field("authors") == ("Authors", " & ".join(authors))
    assert metadata.format_field("#labels") == (
        "Etiquetas / 标签 / وسوم",
        "niño, 漢字, العربية, हिन्दी, e\u0301",
    )
    assert (
        metadata.format_tags()
        == "café, e\u0301, Звёзды, עברית, العربية, हिन्दी, こんにちは, 日本語, 普通话, 東京, 简体中文, 🚀"
    )
    assert metadata.get_identifiers() == {"doi": "10.1234/東京-مرحبا"}

    data = metadata.get_data()
    data["tags"].append("mutated")
    assert "mutated" not in metadata.tags

    rendered = str(metadata)
    for expected in ["東京", "普通话", "简体中文", "日本語", "こんにちは", "العربية", "עברית", "Звёзды", "हिन्दी", "🚀"]:
        assert expected in rendered


@pytest.mark.parametrize(
    ("field", "raw", "field_metadata", "expected"),
    [
        ("count", "42", {"datatype": "int"}, 42),
        ("ratio", "4.5", {"datatype": "float"}, 4.5),
        ("rating", "3.5", {"datatype": "rating"}, 7.0),
        ("flag", "yes", {"datatype": "bool"}, True),
        ("flag", "No", {"datatype": "bool"}, False),
        (
            "identifiers",
            "isbn:9780000000001,doi:10.1234/demo",
            {"datatype": "text", "is_multiple": {"ui_to_list": ","}},
            {"isbn": "9780000000001", "doi": "10.1234/demo"},
        ),
        (
            "languages",
            "eng, fra",
            {"datatype": "text", "is_multiple": {"ui_to_list": ","}},
            ["eng", "fra"],
        ),
        ("plain", "raw value", {"datatype": "text", "is_multiple": {}}, "raw value"),
    ],
)
def test_field_from_string_parses_supported_datatypes(
    field: str,
    raw: str,
    field_metadata: dict[str, object],
    expected: object,
) -> None:
    assert field_from_string(field, raw, field_metadata) == expected


def test_field_from_string_rejects_unknown_bool_values() -> None:
    with pytest.raises(ValueError, match="Unknown value"):
        field_from_string("flag", "sometimes", {"datatype": "bool"})


def test_field_from_string_preserves_foreign_language_iso_codes() -> None:
    assert field_from_string(
        "languages",
        "jpn, zho, cmn, ara, heb, rus, hin, kor",
        {"datatype": "text", "is_multiple": {"ui_to_list": ","}},
    ) == ["jpn", "zho", "cmn", "ara", "heb", "rus", "hin", "kor"]

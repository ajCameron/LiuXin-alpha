"""
Cover metadata utility parsing, normalization, formatting and identifier edge cases.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test utils coverage through its owning regression module::

        python -m pytest -q tests/metadata/test_utils_coverage.py
"""
from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from LiuXin_alpha.metadata import ebook_metadata_tools, utils
from LiuXin_alpha.metadata import identifiers as identifier_policy
from LiuXin_alpha.metadata.identifiers import (
    validate_legacy_identifier as validate_identifier,
)


def test_numeric_author_and_title_helpers_cover_edge_shapes() -> None:
    """
    Verify numeric author and title helpers cover edge shapes.

    Example:
        Exercise test numeric author and title helpers cover edge shapes through its owning regression module::

            python -m pytest -q tests/metadata/test_utils_coverage.py


    :return: None; the function records state or raises through its assertions.
    """
    assert utils.soft_float_to_int(3.0) == 3
    assert utils.soft_float_to_int("3.5") == 3.5

    assert utils.string_to_authors("Ada && Bob and Grace") == ["Ada & Bob", "Grace"]
    assert utils.string_to_authors("") == []
    assert utils.authors_to_string(["Ada & Bob", "Grace"]) == "Ada && Bob & Grace"
    assert utils.authors_to_string(["Ada & Bob", "Grace"], xml_safe=True) == (
        "Ada ,0420,,0420, Bob ,0420, Grace"
    )
    assert utils.authors_to_string(None) == ""

def test_roman_series_identifier_and_doi_helpers() -> None:
    """
    Verify roman series identifier and doi helpers.

    Example:
        Exercise test roman series identifier and doi helpers through its owning regression module::

            python -m pytest -q tests/metadata/test_utils_coverage.py


    :return: None; the function records state or raises through its assertions.
    """
    assert utils.roman(4) == "IV"
    assert utils.roman(0) == "0"
    assert utils.roman(4000) == "4000"
    assert utils.fmt_sidx(None) == "1"
    assert utils.fmt_sidx("2.50") == "2.50"
    assert utils.fmt_sidx(4, use_roman=True) == "IV"
    assert utils.fmt_sidx(object()).startswith("<object object")

    assert utils.check_isbn10 is identifier_policy.check_isbn10
    assert utils.check_isbn13 is identifier_policy.check_isbn13
    assert utils.validate_identifier is identifier_policy.validate_legacy_identifier
    assert validate_identifier is identifier_policy.validate_legacy_identifier
    assert utils.check_isbn("0-261-10357-1") == "0261103571"
    assert utils.check_isbn(None) is None
    assert utils.check_isbn("0000000000") is None
    assert utils.check_isbn("123") is None
    assert utils.format_isbn("0261103571") == "02-6110-357-1"
    assert utils.format_isbn("9780261103573") == "978-02-6110-357-3"
    assert utils.format_isbn("not an isbn") == "not an isbn"
    assert utils.check_issn(None) is None
    assert utils.check_issn("2049-3630") == "20493630"
    assert utils.check_issn("0000-0000") == "00000000"
    assert utils.check_issn("bad") is None
    assert utils.check_doi("doi:10.1234/example.abc") == "10.1234/example.abc"
    assert utils.check_doi("") is None
    assert utils.check_doi("not a doi") is None

def test_resource_and_collection_helpers(tmp_path) -> None:
    """
    Verify calibre metadata resource and collection helpers.

    Example:
        Exercise test calibre metadata resource and collection helpers through its owning regression module::

            python -m pytest -q tests/metadata/test_utils_coverage.py


    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    book_dir = tmp_path / "book root"
    book_dir.mkdir()
    chapter = book_dir / "chapter é.xhtml"
    chapter.write_text("<p>Hello</p>", encoding="utf-8")

    resource = utils.Resource(str(chapter), basedir=str(book_dir))
    assert resource.mime_type == "application/xhtml+xml"
    assert resource.href() == "chapter%20%C3%A9.xhtml"
    assert "chapter" in repr(resource)

    bytes_resource = utils.Resource(bytes(chapter), basedir=bytes(book_dir))
    assert bytes_resource.path == str(chapter)
    assert bytes_resource.href() == "chapter%20%C3%A9.xhtml"

    remote = utils.Resource("https://example.invalid/a book.xhtml", is_path=False)
    assert remote.path is None
    assert remote.href() == "https://example.invalid/a book.xhtml"

    root_resource = utils.Resource(str(book_dir), basedir=str(book_dir))
    assert root_resource.href() == ""
    no_base_resource = utils.Resource(str(chapter), basedir="")
    assert no_base_resource.href()

    file_url = utils.Resource(chapter.as_uri() + "#frag%20one", basedir=str(book_dir), is_path=False)
    assert file_url.href(str(book_dir)) == "chapter%20%C3%A9.xhtml#frag%20one"

    collection = utils.ResourceCollection()
    assert not collection
    collection.append(resource)
    assert collection
    assert len(collection) == 1
    assert collection[0] is resource
    replacement = utils.Resource(str(chapter), basedir=str(book_dir))
    collection.replace(0, 1, [replacement])
    assert collection[0] is replacement
    collection.set_basedir(str(tmp_path))
    assert replacement.basedir() == str(tmp_path)
    assert str(collection).startswith("[Resource(")
    assert repr(collection).startswith("[Resource(")
    collection.remove(replacement)
    assert len(collection) == 0
    with pytest.raises(ValueError):
        collection.append(object())

    nested = book_dir / "text"
    nested.mkdir()
    nested_file = nested / "chapter2.xhtml"
    nested_file.write_text("<p>Two</p>", encoding="utf-8")
    from_disk = utils.ResourceCollection.from_directory_contents(str(book_dir))
    assert sorted(res.href(str(book_dir)) for res in from_disk) == [
        "chapter%20%C3%A9.xhtml",
        "text/chapter2.xhtml",
    ]


def test_ebook_metadata_tools_timestamp_and_identifier_helpers() -> None:
    """
    Verify ebook metadata tools timestamp and identifier helpers.

    Example:
        Exercise test ebook metadata tools timestamp and identifier helpers through its owning regression module::

            python -m pytest -q tests/metadata/test_utils_coverage.py


    :return: None; the function records state or raises through its assertions.
    """
    now = datetime(2020, 1, 1, tzinfo=timezone.utc)

    with pytest.raises(TypeError):
        ebook_metadata_tools.to_epoch_ms(None)
    with pytest.raises(TypeError):
        ebook_metadata_tools.to_epoch_ms(True)
    with pytest.raises(ValueError):
        ebook_metadata_tools.to_epoch_ms(float("nan"))
    with pytest.raises(ValueError):
        ebook_metadata_tools.to_epoch_ms("")
    with pytest.raises(ValueError):
        ebook_metadata_tools.to_epoch_ms("not a timestamp")
    with pytest.raises(TypeError):
        ebook_metadata_tools.to_epoch_ms(object())

    assert ebook_metadata_tools.to_epoch_ms(datetime(1970, 1, 1, tzinfo=timezone.utc), now=now) == 0
    assert ebook_metadata_tools.to_epoch_ms(date(1970, 1, 2), now=now) == 86_400_000
    assert ebook_metadata_tools.to_epoch_ms(1, now=now) == 1000
    assert ebook_metadata_tools.to_epoch_ms(1_000_000_000_000, now=now) == 1_000_000_000_000
    assert ebook_metadata_tools.to_epoch_ms(1_000_000_000_000_000, now=now) == 1_000_000_000_000
    assert ebook_metadata_tools.to_epoch_ms(1_000_000_000_000_000_000, now=now) == 1_000_000_000_000
    assert ebook_metadata_tools.to_epoch_ms("1.5", now=now) == 1500
    assert ebook_metadata_tools.to_epoch_ms("1970-01-01T00:00:01Z", now=now) == 1000
    assert ebook_metadata_tools.to_epoch_ms("01/02/1970 00:00:00", now=now) == 2_678_400_000
    assert ebook_metadata_tools.to_epoch_ms(b"1970-01-01 00:00:01", now=now) == 1000
    assert ebook_metadata_tools.to_epoch_ms(b"\xffDate(1000000000)", now=now, clamp_range=True) == (
        1_000_000_000_000
    )
    assert ebook_metadata_tools.to_epoch_ms("/Date(1000000000)/", now=now) == 1_000_000_000_000

    assert ebook_metadata_tools.check_isbn10("not-isbn") is None
    assert ebook_metadata_tools.check_isbn13("9780000000040") == "9780000000040"
    assert ebook_metadata_tools.check_isbn13("bad") is None
    assert ebook_metadata_tools.check_isbn(None) is None
    assert ebook_metadata_tools.check_isbn("0000000000") is None
    assert ebook_metadata_tools.check_isbn("123") is None
    assert ebook_metadata_tools.check_isbn13("9780261103573") == "9780261103573"
    assert ebook_metadata_tools.check_isbn("978-0-261-10357-3") == "9780261103573"
    assert ebook_metadata_tools.format_isbn("0261103571") == "02-6110-357-1"
    assert ebook_metadata_tools.format_isbn("9780261103573") == "978-02-6110-357-3"
    assert ebook_metadata_tools.format_isbn("not an isbn") == "not an isbn"
    assert ebook_metadata_tools.check_issn(None) is None
    assert ebook_metadata_tools.check_issn("0000-0000") == "00000000"
    assert ebook_metadata_tools.check_issn("bad") is None
    assert ebook_metadata_tools.check_doi("See 10.1234/example") == "10.1234/example"
    assert ebook_metadata_tools.check_doi(None) is None
    assert ebook_metadata_tools.check_doi("not a doi") is None


def test_ebook_metadata_tools_author_title_and_name_helpers(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Verify ebook metadata tools author title and name helpers.

    Example:
        Exercise test ebook metadata tools author title and name helpers through its owning regression module::

            python -m pytest -q tests/metadata/test_utils_coverage.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    monkeypatch.setattr(
        ebook_metadata_tools,
        "load_names",
        lambda lower_case=True: ({"ada", "arthur"}, {"lovelace", "clarke"}),
    )

    assert ebook_metadata_tools.authors_to_string(["Ada & Bob", "Grace"]) == "Ada && Bob & Grace"
    assert ebook_metadata_tools.authors_to_string(None) == ""
    assert ebook_metadata_tools.check_name("Ada Lovelace") is True
    assert ebook_metadata_tools.check_name("Dr Ada Lovelace") is True
    assert ebook_metadata_tools.check_name("Unknown Token") is False
    assert ebook_metadata_tools.check_name("Dr Jr") is False
    assert ebook_metadata_tools.score_title("Some Title") == 0


def test_unicode_and_foreign_language_torture_for_metadata_helpers(tmp_path) -> None:
    """
    Verify unicode and foreign language torture for metadata helpers.

    Example:
        Exercise test unicode and foreign language torture for metadata helpers through its owning regression module::

            python -m pytest -q tests/metadata/test_utils_coverage.py


    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    authors = ["李 白", "山田 太郎", "أحمد & سارة", "שרה"]

    assert utils.authors_to_string(authors) == "李 白 & 山田 太郎 & أحمد && سارة & שרה"
    assert utils.string_to_authors("李 白 and 山田 太郎 with أحمد && سارة") == [
        "李 白",
        "山田 太郎",
        "أحمد & سارة",
    ]
    assert ebook_metadata_tools.authors_to_string(authors) == "李 白 & 山田 太郎 & أحمد && سارة & שרה"

    assert utils.check_doi("前缀 10.5555/測試-δοκιμή😀 trailing") == "10.5555/測試-δοκιμή😀"
    assert ebook_metadata_tools.check_doi("مقدمة 10.5555/اختبار/世界") == "10.5555/اختبار/世界"

    now = datetime(2020, 1, 1, tzinfo=timezone.utc)
    assert ebook_metadata_tools.to_epoch_ms("発売日 Date(1609459200000) 測試", now=now) == 1_609_459_200_000
    assert ebook_metadata_tools.to_epoch_ms("\ufeff1970-01-01T00:00:01Z\u200f", now=now) == 1000
    assert ebook_metadata_tools.to_epoch_ms(b"\xef\xbb\xbf1970-01-01T00:00:01Z", now=now) == 1000

    book_dir = tmp_path / "圖書館 مكتبة"
    book_dir.mkdir()
    chapter = book_dir / "第1章 café 😀.xhtml"
    chapter.write_text("<p>こんにちは مرحبا שלום</p>", encoding="utf-8")

    resource = utils.Resource(str(chapter), basedir=str(book_dir))
    assert resource.href() == "%E7%AC%AC1%E7%AB%A0%20caf%C3%A9%20%F0%9F%98%80.xhtml"

    file_url = utils.Resource(chapter.as_uri() + "#章 😀", basedir=str(book_dir), is_path=False)
    assert file_url.href(str(book_dir)) == (
        "%E7%AC%AC1%E7%AB%A0%20caf%C3%A9%20%F0%9F%98%80.xhtml"
        "#%E7%AB%A0%20%F0%9F%98%80"
    )

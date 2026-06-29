from __future__ import annotations

import io
from datetime import date, datetime, timezone
from xml.etree import ElementTree

import pytest

from LiuXin_alpha.errors import InputIntegrityError
from LiuXin_alpha.file_formats.oeb.base import OPF
from LiuXin_alpha.metadata import ebook_metadata_tools
from LiuXin_alpha.metadata import utils
from LiuXin_alpha.utils.libraries.liuxin_etree import etree


def test_numeric_author_and_title_helpers_cover_edge_shapes() -> None:
    assert utils.soft_float_to_int(3.0) == 3
    assert utils.soft_float_to_int("3.5") == 3.5

    assert utils.string_to_authors("Ada && Bob and Grace") == ["Ada & Bob", "Grace"]
    assert utils.string_to_authors("") == []
    assert utils.authors_to_string(["Ada & Bob", "Grace"]) == "Ada && Bob & Grace"
    assert utils.authors_to_string(["Ada & Bob", "Grace"], xml_safe=True) == (
        "Ada ,0420,,0420, Bob ,0420, Grace"
    )
    assert utils.authors_to_string(None) == ""

    assert utils.author_to_author_sort("", method="nocomma") == ""
    assert utils.author_to_author_sort("Plato", method="nocomma") == "Plato"
    assert utils.author_to_author_sort("Arthur C Clarke", method="copy") == "Arthur C Clarke"
    assert utils.author_to_author_sort("Dr Arthur C Clarke Jr", method="nocomma") == "Clarke Arthur C Jr"
    assert utils.author_to_author_sort("Dr Prof", method="nocomma") == "Dr Prof"
    assert utils.author_to_author_sort("Jr Sr", method="nocomma") == "Jr Sr"
    assert utils.author_to_author_sort("Clarke, Arthur", method="comma") == "Clarke, Arthur"
    assert utils.author_to_author_sort("Ada Lovelace", method=None) == "Lovelace, Ada"
    assert utils.author_to_author_sort("ACME Corporation", method="nocomma") == "ACME Corporation"
    assert utils.authors_to_sort_string(["Arthur C Clarke", "Ada Lovelace"]) == (
        "Clarke, Arthur C & Lovelace, Ada"
    )

    assert utils.get_title_sort_pat("spa") is utils.get_title_sort_pat("spa")
    assert utils.title_sort("The Hobbit") == "Hobbit, The"
    assert utils.title_sort('"The Hobbit') == "Hobbit, The"
    assert utils.title_sort("The Hobbit", order="strictly_alphabetic") == "The Hobbit"
    assert utils.title_sort("El Aleph", lang="spa") == "Aleph, El"


def test_roman_series_identifier_and_doi_helpers() -> None:
    assert utils.roman(4) == "IV"
    assert utils.roman(0) == "0"
    assert utils.roman(4000) == "4000"
    assert utils.fmt_sidx(None) == "1"
    assert utils.fmt_sidx("2.50") == "2.50"
    assert utils.fmt_sidx(4, use_roman=True) == "IV"
    assert utils.fmt_sidx(object()).startswith("<object object")

    assert utils.check_isbn10("0261103571") == "0261103571"
    assert utils.check_isbn10("not-isbn") is None
    assert utils.check_isbn13("9780261103573") == "9780261103573"
    assert utils.check_isbn13("9780000000040") == "9780000000040"
    assert utils.check_isbn13("bad") is None
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

    utils.validate_identifier("isbn", "0261103571")
    utils.validate_identifier("isbn", "9780261103573")
    with pytest.raises(InputIntegrityError):
        utils.validate_identifier("isbn", "not-an-isbn")
    with pytest.raises(NotImplementedError):
        utils.validate_identifier("doi", "10.1234/example")


def test_calibre_metadata_resource_and_collection_helpers(tmp_path) -> None:
    source = utils.calibreMetaInformation("Title", ["Author"])
    source.publisher = "Publisher"
    copied = utils.calibreMetaInformation(source)

    assert copied.title == "Title"
    assert copied.authors == ["Author"]
    assert copied.publisher == "Publisher"

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


def test_opf_version_parse_manifest_and_language_helpers(tmp_path) -> None:
    assert utils.parse_opf_version("3.0") == utils.OPFVersion(3, 0, 0)
    assert utils.parse_opf_version("bad") == utils.OPFVersion(2, 0, 0)
    assert utils.parse_opf_version("4.bad") == utils.OPFVersion(4, 0, 0)

    raw = b"""<?xml version='1.0' encoding='utf-8'?>
<package xmlns="http://www.idpf.org/2007/opf" version="3.0">
  <manifest><item id="chap" href="chapter.xhtml"/></manifest>
</package>
"""
    path = tmp_path / "metadata.opf"
    path.write_bytes(raw)

    assert utils.parse_opf(path).tag == OPF("package")
    assert utils.parse_opf(io.BytesIO(raw)).tag == OPF("package")
    assert utils.parse_opf(raw.decode("utf-8")).tag == OPF("package")
    with pytest.raises(ValueError, match="Empty file"):
        utils.parse_opf(b"")

    assert utils.normalize_languages(["en-US", "zho-TW"], ["eng", "zho", "qaa-x-private"]) == [
        "en-US",
        "zh-TW",
        "qaa-X",
    ]
    assert utils.normalize_languages(["zh-Hant-TW"], ["zho"]) == ["zh-TW"]
    assert utils.normalize_languages(["", "-US"], ["", None]) == []

    assert utils.ensure_unique("cover.jpg", {"cover.jpg", "cover-1.jpg"}) == "cover-2.jpg"

    root = etree.Element(OPF("package"))
    etree.SubElement(root, OPF("manifest"))
    item = utils.create_manifest_item(root, "cover.jpg", "cover", media_type=None)
    assert item.get("href") == "cover.jpg"
    assert item.get("id") == "cover"
    assert item.get("media-type") == "image/jpeg"

    stdlib_root = ElementTree.Element(OPF("package"))
    ElementTree.SubElement(stdlib_root, OPF("manifest"))
    existing = ElementTree.SubElement(stdlib_root.find(OPF("manifest")), OPF("item"))
    existing.set("id", "cover")
    existing.set("href", "cover.jpg")

    item = utils.create_manifest_item(stdlib_root, "cover.jpg", "cover")
    assert item.get("href") == "cover-1.jpg"
    assert item.get("id") == "cover-1"
    assert item.get("media-type") == "image/jpeg"

    no_manifest = etree.Element(OPF("package"))
    assert utils.create_manifest_item(no_manifest, "cover.bin", "cover") is None
    etree.SubElement(no_manifest, OPF("manifest"))
    item = utils.create_manifest_item(no_manifest, "payload.unknownext", "payload", media_type=None)
    assert item.get("media-type") == "application/octet-stream"


def test_ebook_metadata_tools_timestamp_and_identifier_helpers() -> None:
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
    monkeypatch.setattr(
        ebook_metadata_tools,
        "load_names",
        lambda lower_case=True: ({"ada", "arthur"}, {"lovelace", "clarke"}),
    )

    assert ebook_metadata_tools.authors_to_string(["Ada & Bob", "Grace"]) == "Ada && Bob & Grace"
    assert ebook_metadata_tools.authors_to_string(None) == ""
    assert ebook_metadata_tools.author_to_author_sort("", method="nocomma") == ""
    assert ebook_metadata_tools.author_to_author_sort("Plato", method="nocomma") == "Plato"
    assert ebook_metadata_tools.author_to_author_sort("Arthur C Clarke", method="copy") == "Arthur C Clarke"
    assert ebook_metadata_tools.author_to_author_sort("Dr Arthur C Clarke Jr", method="nocomma") == (
        "Clarke Arthur C Jr"
    )
    assert ebook_metadata_tools.author_to_author_sort("Dr Prof", method="nocomma") == "Dr Prof"
    assert ebook_metadata_tools.author_to_author_sort("Jr Sr", method="nocomma") == "Jr Sr"
    assert ebook_metadata_tools.author_to_author_sort("Clarke, Arthur", method="comma") == "Clarke, Arthur"
    assert ebook_metadata_tools.author_to_author_sort("ACME Corporation", method="nocomma") == "ACME Corporation"
    assert ebook_metadata_tools.authors_to_sort_string(["Arthur C Clarke"]) == "Clarke, Arthur C"
    assert ebook_metadata_tools.get_title_sort_pat("spa") is ebook_metadata_tools.get_title_sort_pat("spa")
    assert ebook_metadata_tools.title_sort("The Hobbit") == "Hobbit, The"
    assert ebook_metadata_tools.title_sort('"The Hobbit') == "Hobbit, The"
    assert ebook_metadata_tools.title_sort("The Hobbit", order="strictly_alphabetic") == "The Hobbit"

    assert ebook_metadata_tools.check_name("Ada Lovelace") is True
    assert ebook_metadata_tools.check_name("Dr Ada Lovelace") is True
    assert ebook_metadata_tools.check_name("Unknown Token") is False
    assert ebook_metadata_tools.check_name("Dr Jr") is False
    assert ebook_metadata_tools.score_title("Some Title") == 0


def test_unicode_and_foreign_language_torture_for_metadata_helpers(tmp_path) -> None:
    authors = ["李 白", "山田 太郎", "أحمد & سارة", "שרה"]

    assert utils.authors_to_string(authors) == "李 白 & 山田 太郎 & أحمد && سارة & שרה"
    assert utils.string_to_authors("李 白 and 山田 太郎 with أحمد && سارة") == [
        "李 白",
        "山田 太郎",
        "أحمد & سارة",
    ]
    assert ebook_metadata_tools.authors_to_string(authors) == "李 白 & 山田 太郎 & أحمد && سارة & שרה"

    # The legacy sort helper is Roman-name shaped, but it must not lose
    # non-Latin tokens while applying that shape.
    assert utils.author_to_author_sort("李 白", method="nocomma") == "白 李"
    assert utils.author_to_author_sort("山田 太郎", method="nocomma") == "太郎 山田"
    assert ebook_metadata_tools.author_to_author_sort("李 白", method="nocomma") == "白 李"

    assert utils.title_sort("L’Étranger", lang="fra") == "Étranger, L’"
    assert utils.title_sort("Die Verwandlung", lang="deu") == "Verwandlung, Die"
    assert utils.title_sort("Las ciudades invisibles", lang="spa") == "ciudades invisibles, Las"
    assert ebook_metadata_tools.title_sort("L’Étranger", lang="fra") == "Étranger, L’"
    assert ebook_metadata_tools.title_sort("Die Verwandlung", lang="deu") == "Verwandlung, Die"

    assert utils.check_doi("前缀 10.5555/測試-δοκιμή😀 trailing") == "10.5555/測試-δοκιμή😀"
    assert ebook_metadata_tools.check_doi("مقدمة 10.5555/اختبار/世界") == "10.5555/اختبار/世界"

    assert utils.normalize_languages(
        ["ja-JP", "ara-EG", "he-IL", "zh-Hans-CN"],
        ["jpn", "ara", "heb", "zho", "kor"],
    ) == ["ja-JP", "ar-EG", "he-IL", "zh-CN", "ko"]

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

    raw = """<?xml version='1.0' encoding='utf-8'?>
<package xmlns="http://www.idpf.org/2007/opf" version="3.0">
  <metadata>
    <dc:title xmlns:dc="http://purl.org/dc/elements/1.1/">雪国 — 😀</dc:title>
    <dc:language xmlns:dc="http://purl.org/dc/elements/1.1/">ja-JP</dc:language>
  </metadata>
  <manifest><item id="章" href="text/第1章.xhtml"/></manifest>
</package>
"""
    assert utils.parse_opf(raw).tag == OPF("package")

from __future__ import annotations

import io
import shutil
import zipfile
import xml.etree.ElementTree as ET
from collections.abc import Mapping
from pathlib import Path

import pytest

from LiuXin_alpha.metadata.utils import calibreMetaInformation


def _values(raw):
    if raw is None:
        return []
    if isinstance(raw, Mapping):
        return list(raw.keys())
    if isinstance(raw, str):
        return [raw]
    return list(raw)


def _container_xml(*, opf_path: str = "content.opf") -> str:
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">'
        "<rootfiles>"
        f'<rootfile full-path="{opf_path}" media-type="application/oebps-package+xml"/>'
        "</rootfiles>"
        "</container>"
    )


def _build_epub(path: Path, *, opf_bytes: bytes, container_xml: str | None = None) -> None:
    container_xml = container_xml or _container_xml(opf_path="content.opf")
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("mimetype", "application/epub+zip")
        zf.writestr("META-INF/container.xml", container_xml)
        zf.writestr("content.opf", opf_bytes)


def _opf_text_from_epub(epub_path: Path) -> str:
    with zipfile.ZipFile(epub_path, "r") as zf:
        root = ET.fromstring(zf.read("META-INF/container.xml"))
        opf_path = None
        for node in root.iter():
            if node.tag.rsplit("}", 1)[-1] != "rootfile":
                continue
            media_type = node.attrib.get("media-type", "")
            full_path = node.attrib.get("full-path")
            if full_path and media_type == "application/oebps-package+xml":
                opf_path = full_path
                break
        assert opf_path is not None
        return zf.read(opf_path).decode("utf-8", "replace")


def _contains_forbidden_xml_char(text: str) -> bool:
    for ch in text:
        cp = ord(ch)
        if cp == 0x7F:
            return True
        if cp in (0x9, 0xA, 0xD):
            continue
        if 0x20 <= cp <= 0xD7FF:
            continue
        if 0xE000 <= cp <= 0xFFFD:
            continue
        if 0x10000 <= cp <= 0x10FFFF:
            continue
        return True
    return False


def _opf_with_cover() -> bytes:
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<package xmlns="http://www.idpf.org/2007/opf" version="2.0" unique-identifier="bookid">'
        '<metadata xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:opf="http://www.idpf.org/2007/opf">'
        "<dc:title>Original</dc:title>"
        "<dc:creator>Original Author</dc:creator>"
        '<meta name="cover" content="cover-image"/>'
        "</metadata>"
        "<manifest>"
        '<item id="cover-image" href="images/cover.jpg" media-type="image/jpeg"/>'
        '<item id="chapter" href="text/chapter.xhtml" media-type="application/xhtml+xml"/>'
        "</manifest>"
        '<spine><itemref idref="chapter"/></spine>'
        "</package>"
    ).encode("utf-8")


def test_epub_metadata_module_import_smoke() -> None:
    import LiuXin_alpha.metadata.file_sources.epub as epub_md

    assert epub_md is not None


def test_epub_all_hashed_fixtures_reader_smoke(md_test_fixtures_for_ext) -> None:
    from LiuXin_alpha.metadata.file_sources.epub import get_metadata, get_metadata_inplace, get_quick_metadata

    fixtures = md_test_fixtures_for_ext(file_ext="epub", verify_hash=True)
    assert fixtures

    for fixture in fixtures:
        md_from_path = get_metadata(fixture, extract_cover=False, calibre_metadata=True)
        assert md_from_path.title
        assert _values(md_from_path.authors)

        with fixture.open("rb") as stream:
            md_from_stream = get_metadata(stream, extract_cover=False, calibre_metadata=True)
            md_quick = get_quick_metadata(stream)
            assert stream.tell() == 0

        md_inplace = get_metadata_inplace(fixture)
        assert md_from_stream.title == md_from_path.title
        assert md_quick.title == md_from_path.title
        assert md_inplace.title == md_from_path.title


def test_epub_legacy_fixture1_expectations(md_test_fixture) -> None:
    fixture = md_test_fixture(file_ext="epub", file_num=1, verify_hash=True)
    opf_text = _opf_text_from_epub(fixture)

    assert "Twenty Thousand Leagues Under the Seas: An Underwater Tour of the World" in opf_text
    assert "Jules Verne" in opf_text
    assert "F. P. Walter" in opf_text
    assert "Submarines (Ships) -- Fiction" in opf_text
    assert "Science fiction" in opf_text
    assert "Sea stories" in opf_text
    assert "Underwater exploration -- Fiction" in opf_text


def test_epub_metadata_reads_known_fixture_path(md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources.epub import get_metadata

    fixture = md_test_fixture(file_ext="epub", file_num=1, verify_hash=True)
    metadata = get_metadata(fixture, extract_cover=False, calibre_metadata=True)

    assert metadata.title == "Twenty Thousand Leagues Under the Seas: An Underwater Tour of the World"
    assert _values(metadata.authors) == ["Jules Verne"]


def test_epub_metadata_reads_stream_and_rewinds(md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources.epub import get_metadata

    fixture = md_test_fixture(file_ext="epub", file_num=2, verify_hash=True)
    with fixture.open("rb") as stream:
        metadata = get_metadata(stream, extract_cover=False, calibre_metadata=True)
        assert stream.tell() == 0

    assert metadata.title == "20,000 Leagues Under the Sea"
    assert _values(metadata.authors) == ["Jules Verne"]


def test_epub_quick_metadata_matches_regular_without_cover(md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources.epub import get_metadata, get_quick_metadata

    fixture = md_test_fixture(file_ext="epub", file_num=3, verify_hash=True)
    with fixture.open("rb") as stream:
        quick = get_quick_metadata(stream)
    full = get_metadata(fixture, extract_cover=False, calibre_metadata=True)

    assert quick.title == full.title
    assert _values(quick.authors) == _values(full.authors)


def test_epub_metadata_inplace_returns_liuxin_container(md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources.epub import get_metadata_inplace

    fixture = md_test_fixture(file_ext="epub", file_num=1, verify_hash=True)
    metadata = get_metadata_inplace(fixture)

    assert metadata.title == "Twenty Thousand Leagues Under the Seas: An Underwater Tour of the World"
    assert _values(metadata.authors) == ["Jules Verne"]
    assert hasattr(metadata, "to_calibre")


def test_epub_metadata_reader_plugin_is_available(md_test_fixture) -> None:
    from LiuXin_alpha.customize.builtins.metadata_readers import get_metadata_reader_plugins

    fixture = md_test_fixture(file_ext="epub", file_num=1, verify_hash=True)

    plugins = get_metadata_reader_plugins()
    epub_cls = next((p for p in plugins if p.__name__ == "EPUBMetadataReader"), None)
    assert epub_cls is not None

    reader = epub_cls(None)
    with fixture.open("rb") as stream:
        metadata = reader.get_metadata(stream=stream, ftype="epub")
    inplace_metadata = reader.get_metadata_inplace(file_path=str(fixture), ftype="epub")

    assert metadata.title == "Twenty Thousand Leagues Under the Seas: An Underwater Tour of the World"
    assert _values(metadata.authors) == ["Jules Verne"]
    assert inplace_metadata.title == "Twenty Thousand Leagues Under the Seas: An Underwater Tour of the World"


def test_epub_cover_extracts_raster_cover_when_present(md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources.epub import get_metadata

    fixture = md_test_fixture(file_ext="epub", file_num=2, verify_hash=True)
    metadata = get_metadata(fixture, extract_cover=True, calibre_metadata=True)

    assert metadata.cover_data is not None
    fmt, raw = metadata.cover_data
    assert fmt in {"jpeg", "jpg", "png", "webp", "gif"}
    assert isinstance(raw, (bytes, bytearray))
    assert len(raw) > 64


def test_epub_set_metadata_roundtrip_path(tmp_path: Path, md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources.epub import get_metadata, set_metadata

    source = md_test_fixture(file_ext="epub", file_num=2, verify_hash=True)
    target = tmp_path / "epub_roundtrip.epub"
    shutil.copy2(source, target)

    updated = calibreMetaInformation("Updated EPUB Title", ["Alice Author", "Bob Author"])
    updated.tags = ["unicode tag", "adventure"]
    updated.comments = "Updated comments for roundtrip."
    updated.publisher = "LiuXin Press"

    set_metadata(target, updated)
    metadata = get_metadata(target, extract_cover=False, calibre_metadata=True)

    assert metadata.title == "Updated EPUB Title"
    assert _values(metadata.authors) == ["Alice Author", "Bob Author"]

    # The OPF facade currently writes more metadata fields than it reads back
    # via `get_metadata()`. Validate those fields against the stored OPF payload.
    opf_text = _opf_text_from_epub(target)

    assert "unicode tag" in opf_text
    assert "adventure" in opf_text
    assert "Updated comments for roundtrip." in opf_text
    assert "LiuXin Press" in opf_text


def test_epub_set_metadata_accepts_stream(tmp_path: Path, md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources.epub import get_metadata, set_metadata

    source = md_test_fixture(file_ext="epub", file_num=3, verify_hash=True)
    target = tmp_path / "epub_stream_roundtrip.epub"
    shutil.copy2(source, target)

    updated = calibreMetaInformation("Stream EPUB Title", ["Stream Writer"])

    with target.open("r+b") as stream:
        set_metadata(stream, updated)
        assert stream.tell() == 0

    metadata = get_metadata(target, extract_cover=False, calibre_metadata=True)
    assert metadata.title == "Stream EPUB Title"
    assert _values(metadata.authors) == ["Stream Writer"]


def test_epub_update_metadata_mutates_opf_object(md_test_fixture) -> None:
    from LiuXin_alpha.file_formats.opf.opf2 import OPF
    from LiuXin_alpha.metadata.file_sources.epub import update_metadata

    fixture = md_test_fixture(file_ext="epub", file_num=1, verify_hash=True)

    with zipfile.ZipFile(fixture, "r") as zf:
        root = ET.fromstring(zf.read("META-INF/container.xml"))
        opf_path = None
        for node in root.iter():
            if node.tag.rsplit("}", 1)[-1] != "rootfile":
                continue
            media_type = node.attrib.get("media-type", "")
            full_path = node.attrib.get("full-path")
            if full_path and media_type == "application/oebps-package+xml":
                opf_path = full_path
                break
        assert opf_path is not None
        opf_raw = zf.read(opf_path)

    opf = OPF(io.BytesIO(opf_raw), populate_spine=False, read_toc=False)
    updated = calibreMetaInformation("Updated OPF Title", ["Metadata Bot"])
    update_metadata(opf, updated, apply_null=True)

    assert opf.title == "Updated OPF Title"
    assert _values(opf.authors) == ["Metadata Bot"]


def test_epub_writer_legacy_fields_present_in_opf_payload(tmp_path: Path, md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources.epub import set_metadata

    source = md_test_fixture(file_ext="epub", file_num=1, verify_hash=True)
    target = tmp_path / "epub_legacy_fields.epub"
    shutil.copy2(source, target)

    updated = calibreMetaInformation("Legacy Field Title", ["Legacy Author"])
    updated.tags = ["tag-alpha", "tag-beta"]
    updated.comments = "Legacy comments text"
    updated.publisher = "Legacy Publisher"
    updated.series = "Legacy Series"
    updated.series_index = "7.25"
    updated.isbn = "9780306406157"
    updated.lccn = "LC-123456"
    updated.amazon = "B00TEST123"

    set_metadata(target, updated)
    opf_text = _opf_text_from_epub(target)

    for expected in (
        "Legacy Field Title",
        "Legacy Author",
        "tag-alpha",
        "tag-beta",
        "Legacy comments text",
        "Legacy Publisher",
        "Legacy Series",
        "7.25",
        "9780306406157",
        "LC-123456",
        "B00TEST123",
    ):
        assert expected in opf_text


def test_epub_unicode_torture_roundtrip_title_and_authors(tmp_path: Path, md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources.epub import get_metadata, set_metadata

    source = md_test_fixture(file_ext="epub", file_num=2, verify_hash=True)
    target = tmp_path / "epub_unicode_torture.epub"
    shutil.copy2(source, target)

    title = (
        "Ångström — Καλημέρα — Привет — 你好 — नमस्ते — العربية — עברית — 日本語 — 한글 — 😀 — e\u0301"
    )
    authors = [
        "Łukasz Żółć",
        "Мария Иванова",
        "山田 太郎",
        "علي بن أحمد",
        "नागार्जुन लेखक",
        "Emoji 😀 Author",
    ]
    updated = calibreMetaInformation(title, authors)
    updated.tags = ["タグ", "тег", "وسم", "टैग", "tag😀", "combining-e\u0301"]
    updated.comments = (
        "Line 1\nLine 2\r\nZero-width:\u200b; RTL:\u202eabc\u202c; Combining: a\u0300e\u0301i\u0302o\u0308u\u0323"
    )
    updated.publisher = "出版社 / Издатель / الناشر / प्रकाशक"
    updated.series = "シリーズ №١ — Série №2 — Series №3"
    updated.series_index = "12.75"

    set_metadata(target, updated)
    metadata = get_metadata(target, extract_cover=False, calibre_metadata=True)
    opf_text = _opf_text_from_epub(target)

    assert metadata.title == title
    assert _values(metadata.authors) == authors
    assert "combining-e" in opf_text
    assert "出版社" in opf_text
    assert "Series" in opf_text
    assert "12.75" in opf_text


def test_epub_set_metadata_preserves_zip_members_replaces_cover_and_sanitizes_xml(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.epub import get_metadata, set_metadata

    target = tmp_path / "container_contract.epub"
    with zipfile.ZipFile(target, "w") as zf:
        zf.writestr("mimetype", "application/epub+zip")
        zf.writestr("META-INF/container.xml", _container_xml(opf_path="OEBPS/content.opf"))
        zf.writestr("OEBPS/content.opf", _opf_with_cover())
        zf.writestr("OEBPS/text/chapter.xhtml", b"<html><body>chapter bytes</body></html>")
        zf.writestr("OEBPS/images/cover.jpg", b"old-cover-bytes")
        zf.writestr("OEBPS/styles/main.css", b"body { color: black; }")

    with zipfile.ZipFile(target, "r") as zf:
        preserved = {
            "mimetype": zf.read("mimetype"),
            "META-INF/container.xml": zf.read("META-INF/container.xml"),
            "OEBPS/text/chapter.xhtml": zf.read("OEBPS/text/chapter.xhtml"),
            "OEBPS/styles/main.css": zf.read("OEBPS/styles/main.css"),
        }

    title = "EPUB\x00Title\ud800 😀"
    authors = ["Alice\x01 One", "Bob\udfff Two"]
    updated = calibreMetaInformation(title, authors)
    updated.tags = ["tag\x02one", "emoji 😀"]
    updated.comments = "Comment\x03 with <xml> & emoji 😀"
    updated.publisher = "Pub\x04lisher"
    updated.cover_data = ("jpeg", b"new-cover-bytes")

    set_metadata(target, updated)

    with zipfile.ZipFile(target, "r") as zf:
        assert zf.testzip() is None
        for name, payload in preserved.items():
            assert zf.read(name) == payload
        assert zf.read("OEBPS/images/cover.jpg") == b"new-cover-bytes"
        opf_raw = zf.read("OEBPS/content.opf")

    opf_text = opf_raw.decode("utf-8")
    assert not _contains_forbidden_xml_char(opf_text)
    ET.fromstring(opf_raw)
    assert "EPUBTitle" in opf_text
    assert "Publisher" in opf_text

    metadata = get_metadata(target, extract_cover=False, calibre_metadata=True)
    assert metadata.title == "EPUBTitle 😀"
    assert _values(metadata.authors) == ["Alice One", "Bob Two"]
    assert updated.title == title
    assert updated.authors == authors


def test_epub_broken_encoding_in_opf_is_tolerated(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.epub import get_metadata

    opf = (
        b'<?xml version="1.0" encoding="utf-8"?>'
        b'<package xmlns="http://www.idpf.org/2007/opf" version="2.0">'
        b'<metadata xmlns:dc="http://purl.org/dc/elements/1.1/">'
        b"<dc:title>Bad\x96Title</dc:title>"
        b"<dc:creator>Writer\x96Name</dc:creator>"
        b"</metadata><manifest></manifest><spine></spine></package>"
    )
    target = tmp_path / "broken_encoding.epub"
    _build_epub(target, opf_bytes=opf)

    metadata = get_metadata(target, extract_cover=False, calibre_metadata=True)
    assert "Bad" in metadata.title
    assert _values(metadata.authors)


def test_epub_with_unicode_opf_path_in_container_is_supported(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.epub import get_metadata

    unicode_opf_path = "OEBPS/δοκιμή/書名.opf"
    opf = (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<package xmlns="http://www.idpf.org/2007/opf" version="2.0">'
        '<metadata xmlns:dc="http://purl.org/dc/elements/1.1/">'
        "<dc:title>Unicode Path Title</dc:title>"
        "<dc:creator>Unicode Path Author</dc:creator>"
        "</metadata><manifest></manifest><spine></spine></package>"
    ).encode("utf-8")
    target = tmp_path / "unicode_path.epub"

    with zipfile.ZipFile(target, "w") as zf:
        zf.writestr("mimetype", "application/epub+zip")
        zf.writestr("META-INF/container.xml", _container_xml(opf_path=unicode_opf_path))
        zf.writestr(unicode_opf_path, opf)

    metadata = get_metadata(target, extract_cover=False, calibre_metadata=True)
    assert metadata.title == "Unicode Path Title"
    assert _values(metadata.authors) == ["Unicode Path Author"]


def test_epub_invalid_zip_logs_and_raises(tmp_path: Path, monkeypatch) -> None:
    from LiuXin_alpha.metadata.file_sources.epub import EPubException, get_metadata

    bad = tmp_path / "not_a_zip.epub"
    bad.write_bytes(b"not zip data at all")

    events: list[tuple[str, str]] = []

    def _log_exception(base, exc, level, *pairs, **kwargs):
        events.append((str(base), str(exc)))
        return str(base)

    monkeypatch.setattr("LiuXin_alpha.metadata.file_sources.epub.default_log.log_exception", _log_exception)

    with pytest.raises(EPubException):
        get_metadata(bad, extract_cover=False)

    assert events
    assert any("Failed to open EPUB as ZIP container." in base for base, _ in events)
    assert any("Failed to parse EPUB metadata." in base for base, _ in events)


def test_epub_missing_container_logs_and_raises(tmp_path: Path, monkeypatch) -> None:
    from LiuXin_alpha.metadata.file_sources.epub import EPubException, get_metadata

    bad = tmp_path / "missing_container.epub"
    with zipfile.ZipFile(bad, "w") as zf:
        zf.writestr("mimetype", "application/epub+zip")

    events: list[tuple[str, str]] = []

    def _log_exception(base, exc, level, *pairs, **kwargs):
        events.append((str(base), str(exc)))
        return str(base)

    monkeypatch.setattr("LiuXin_alpha.metadata.file_sources.epub.default_log.log_exception", _log_exception)

    with pytest.raises(EPubException):
        get_metadata(bad, extract_cover=False)

    assert events
    assert any("Missing OCF container.xml file" in exc for _, exc in events)


def test_epub_missing_opf_logs_and_raises(tmp_path: Path, monkeypatch) -> None:
    from LiuXin_alpha.metadata.file_sources.epub import EPubException, get_metadata

    target = tmp_path / "missing_opf.epub"
    with zipfile.ZipFile(target, "w") as zf:
        zf.writestr("mimetype", "application/epub+zip")
        zf.writestr("META-INF/container.xml", _container_xml(opf_path="missing-content.opf"))

    events: list[tuple[str, str]] = []

    def _log_exception(base, exc, level, *pairs, **kwargs):
        events.append((str(base), str(exc)))
        return str(base)

    monkeypatch.setattr("LiuXin_alpha.metadata.file_sources.epub.default_log.log_exception", _log_exception)

    with pytest.raises(EPubException):
        get_metadata(target, extract_cover=False)

    assert events
    assert any("Failed to parse EPUB metadata." in base for base, _ in events)


def test_epub_malformed_container_xml_logs_and_raises(tmp_path: Path, monkeypatch) -> None:
    from LiuXin_alpha.metadata.file_sources.epub import EPubException, get_metadata

    target = tmp_path / "broken_container.epub"
    with zipfile.ZipFile(target, "w") as zf:
        zf.writestr("mimetype", "application/epub+zip")
        zf.writestr("META-INF/container.xml", b"<container><rootfiles>")  # intentionally malformed XML

    events: list[tuple[str, str]] = []

    def _log_exception(base, exc, level, *pairs, **kwargs):
        events.append((str(base), str(exc)))
        return str(base)

    monkeypatch.setattr("LiuXin_alpha.metadata.file_sources.epub.default_log.log_exception", _log_exception)

    with pytest.raises(EPubException):
        get_metadata(target, extract_cover=False)

    assert events
    assert any("Failed to parse EPUB metadata." in base for base, _ in events)


def test_epub_set_metadata_invalid_zip_logs_and_raises(monkeypatch) -> None:
    from LiuXin_alpha.metadata.file_sources.epub import EPubException, set_metadata

    events: list[tuple[str, str]] = []

    def _log_exception(base, exc, level, *pairs, **kwargs):
        events.append((str(base), str(exc)))
        return str(base)

    monkeypatch.setattr("LiuXin_alpha.metadata.file_sources.epub.default_log.log_exception", _log_exception)

    bad_stream = io.BytesIO(b"not a zip")
    updated = calibreMetaInformation("bad stream test", ["writer"])
    with pytest.raises(EPubException):
        set_metadata(bad_stream, updated)

    assert events
    assert any("Failed to write EPUB metadata." in base for base, _ in events)

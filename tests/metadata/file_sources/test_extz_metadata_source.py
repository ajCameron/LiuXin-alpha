from __future__ import annotations

import io
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


def _cover_tuple(raw):
    if isinstance(raw, tuple) and len(raw) == 2:
        return raw
    if isinstance(raw, Mapping):
        if not raw:
            return None
        first = next(iter(raw.keys()))
        if isinstance(first, tuple) and len(first) == 2:
            return first
    return None


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


def _container_xml(opf_path: str) -> str:
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">'
        "<rootfiles>"
        f'<rootfile full-path="{opf_path}" media-type="application/oebps-package+xml"/>'
        "</rootfiles>"
        "</container>"
    )


def _opf_template(
    *,
    title: str = "Sample Title",
    authors: tuple[str, ...] = ("Sample Author",),
    include_cover: bool = False,
    cover_href: str = "cover.jpg",
) -> bytes:
    creator_xml = "".join(f"<dc:creator>{author}</dc:creator>" for author in authors)
    cover_meta = '<meta name="cover" content="cover-item"/>' if include_cover else ""
    cover_manifest = (
        f'<item id="cover-item" href="{cover_href}" media-type="image/jpeg"/>'
        if include_cover
        else ""
    )
    xml = (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<package xmlns="http://www.idpf.org/2007/opf" version="2.0" unique-identifier="bookid">'
        '<metadata xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:opf="http://www.idpf.org/2007/opf">'
        f"<dc:title>{title}</dc:title>"
        f"{creator_xml}"
        f"{cover_meta}"
        "</metadata>"
        "<manifest>"
        f"{cover_manifest}"
        "</manifest>"
        "<spine/>"
        "</package>"
    )
    return xml.encode("utf-8")


def _opf_template_with_cover_id(*, title: str = "ID Cover", author: str = "Author", cover_id: str = "coverid", cover_href: str = "images/cover.jpg") -> bytes:
    xml = (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<package xmlns="http://www.idpf.org/2007/opf" version="2.0" unique-identifier="bookid">'
        '<metadata xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:opf="http://www.idpf.org/2007/opf">'
        f"<dc:title>{title}</dc:title>"
        f"<dc:creator>{author}</dc:creator>"
        f'<meta name="cover" content="{cover_id}"/>'
        "</metadata>"
        "<manifest>"
        f'<item id="{cover_id}" href="{cover_href}" media-type="image/jpeg"/>'
        "</manifest>"
        "<spine/>"
        "</package>"
    )
    return xml.encode("utf-8")


def _opf_template_with_guide_cover(*, title: str = "Guide Cover", author: str = "Author", cover_href: str = "images/guide-cover.jpg") -> bytes:
    xml = (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<package xmlns="http://www.idpf.org/2007/opf" version="2.0" unique-identifier="bookid">'
        '<metadata xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:opf="http://www.idpf.org/2007/opf">'
        f"<dc:title>{title}</dc:title>"
        f"<dc:creator>{author}</dc:creator>"
        "</metadata>"
        "<manifest>"
        '<item id="text" href="index.html" media-type="application/xhtml+xml"/>'
        "</manifest>"
        '<guide><reference type="cover" title="Cover" href="' + cover_href + '"/></guide>'
        "<spine/>"
        "</package>"
    )
    return xml.encode("utf-8")


def _opf_template_with_txtz_cover_relpath(
    *,
    title: str = "TXTZ Cover",
    author: str = "Author",
    cover_href: str = "images/txtz-cover.jpg",
) -> bytes:
    xml = (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<package xmlns="http://www.idpf.org/2007/opf" version="2.0" unique-identifier="bookid">'
        '<metadata xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:opf="http://www.idpf.org/2007/opf">'
        f"<dc:title>{title}</dc:title>"
        f"<dc:creator>{author}</dc:creator>"
        "</metadata>"
        "<manifest/>"
        f"<cover-relpath-from-base>{cover_href}</cover-relpath-from-base>"
        "<spine/>"
        "</package>"
    )
    return xml.encode("utf-8")


def _build_extz_archive(path: Path, members: dict[str, bytes]) -> None:
    with zipfile.ZipFile(path, "w") as zf:
        for name, payload in members.items():
            zf.writestr(name, payload)


def _opf_text(path: Path) -> str:
    with zipfile.ZipFile(path, "r") as zf:
        opf_names = [name for name in zf.namelist() if name.lower().endswith(".opf")]
        assert opf_names, "Archive missing OPF"
        opf_name = sorted(opf_names)[0]
        return zf.read(opf_name).decode("utf-8", "replace")


def test_extz_module_import_smoke() -> None:
    import LiuXin_alpha.metadata.file_sources.extz as extz

    assert extz is not None


def test_extz_all_hashed_fixtures_reader_smoke(md_test_fixtures_for_ext) -> None:
    from LiuXin_alpha.metadata.file_sources.extz import get_metadata

    htmlz_fixtures = md_test_fixtures_for_ext(file_ext="htmlz", verify_hash=True)
    txtz_fixtures = md_test_fixtures_for_ext(file_ext="txtz", verify_hash=True)
    fixtures = [*htmlz_fixtures, *txtz_fixtures]
    assert fixtures

    for fixture in fixtures:
        metadata = get_metadata(fixture)
        assert metadata.title is not None
        assert metadata.authors is not None


def test_extz_legacy_htmlz_expectations_via_plugin(md_test_fixture) -> None:
    from LiuXin_alpha.customize.builtins.metadata_readers import get_metadata_reader_plugins

    fixture = md_test_fixture(file_ext="htmlz", file_num=1, verify_hash=True)
    plugins = get_metadata_reader_plugins()
    htmlz_cls = next((p for p in plugins if p.__name__ == "HTMLZMetadataReader"), None)
    assert htmlz_cls is not None

    with fixture.open("rb") as stream:
        metadata = htmlz_cls(None).get_metadata(stream, "htmlz")

    assert metadata.title == "Unknown"
    assert _values(metadata.authors) == []


def test_extz_legacy_txtz_expectations(md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources.extz import get_metadata

    fixture = md_test_fixture(file_ext="txtz", file_num=1, verify_hash=True)
    metadata = get_metadata(fixture)

    assert metadata.title == "20,000 Leagues Under the Sea"
    assert _values(metadata.authors) == ["Jules Verne"]
    assert _values(metadata.tags) == []


def test_extz_reader_plugins_available(md_test_fixture) -> None:
    from LiuXin_alpha.customize.builtins.metadata_readers import get_metadata_reader_plugins

    txtz_fixture = md_test_fixture(file_ext="txtz", file_num=1, verify_hash=True)
    classes = {p.__name__: p for p in get_metadata_reader_plugins()}
    assert "HTMLZMetadataReader" in classes
    assert "TXTZMetadataReader" in classes

    with txtz_fixture.open("rb") as stream:
        txtz_md = classes["TXTZMetadataReader"](None).get_metadata(stream, "txtz")
    assert txtz_md.title == "20,000 Leagues Under the Sea"


def test_extz_get_first_opf_name_prefers_container_target(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.extz import get_first_opf_name
    from LiuXin_alpha.utils.libraries.calibre_zipfile import ZipFile

    archive = tmp_path / "container_pref.extz"
    _build_extz_archive(
        archive,
        {
            "metadata.opf": _opf_template(title="Root OPF"),
            "META-INF/container.xml": _container_xml("OEBPS/content.opf").encode("utf-8"),
            "OEBPS/content.opf": _opf_template(title="Nested OPF"),
        },
    )
    with ZipFile(archive, "r") as zf:
        assert get_first_opf_name(zf) == "OEBPS/content.opf"


def test_extz_get_first_opf_name_falls_back_to_top_level_sorted(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.extz import get_first_opf_name
    from LiuXin_alpha.utils.libraries.calibre_zipfile import ZipFile

    archive = tmp_path / "top_level_sorted.extz"
    _build_extz_archive(
        archive,
        {
            "zeta.opf": _opf_template(title="z"),
            "alpha.opf": _opf_template(title="a"),
        },
    )
    with ZipFile(archive, "r") as zf:
        assert get_first_opf_name(zf) == "alpha.opf"


def test_extz_cover_extraction_from_synthetic_archive(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.extz import get_metadata

    archive = tmp_path / "with_cover.txtz"
    cover_payload = b"\xff\xd8\xff\xdb\x00Cfake-jpeg-data"
    _build_extz_archive(
        archive,
        {
            "metadata.opf": _opf_template(title="Cover Book", authors=("Cover Author",), include_cover=True),
            "cover.jpg": cover_payload,
            "index.txt": b"hello",
        },
    )

    metadata = get_metadata(archive, extract_cover=True)
    assert metadata.title == "Cover Book"
    cover = _cover_tuple(metadata.cover_data)
    assert cover is not None
    fmt, raw = cover
    assert fmt in {"jpg", "jpeg"}
    assert raw == cover_payload


def test_extz_cover_extraction_resolves_cover_meta_id_to_manifest_href(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.extz import get_metadata

    archive = tmp_path / "cover_id.txtz"
    cover_payload = b"\xff\xd8\xff\xdb\x00Cid-cover-jpeg"
    _build_extz_archive(
        archive,
        {
            "metadata.opf": _opf_template_with_cover_id(),
            "images/cover.jpg": cover_payload,
            "index.txt": b"hello",
        },
    )

    metadata = get_metadata(archive, extract_cover=True)
    cover = _cover_tuple(metadata.cover_data)
    assert cover is not None
    fmt, raw = cover
    assert fmt in {"jpg", "jpeg"}
    assert raw == cover_payload


def test_extz_cover_extraction_falls_back_to_guide_cover_reference(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.extz import get_metadata

    archive = tmp_path / "guide_cover.htmlz"
    cover_payload = b"\xff\xd8\xff\xdb\x00Cguide-cover-jpeg"
    _build_extz_archive(
        archive,
        {
            "metadata.opf": _opf_template_with_guide_cover(),
            "images/guide-cover.jpg": cover_payload,
            "index.html": b"<html/>",
        },
    )

    metadata = get_metadata(archive, extract_cover=True)
    cover = _cover_tuple(metadata.cover_data)
    assert cover is not None
    fmt, raw = cover
    assert fmt in {"jpg", "jpeg"}
    assert raw == cover_payload


def test_extz_cover_extraction_falls_back_to_txtz_cover_relpath(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.extz import get_metadata

    archive = tmp_path / "txtz_cover_relpath.txtz"
    cover_payload = b"\xff\xd8\xff\xdb\x00Ctxtz-cover-jpeg"
    _build_extz_archive(
        archive,
        {
            "metadata.opf": _opf_template_with_txtz_cover_relpath(),
            "images/txtz-cover.jpg": cover_payload,
            "index.txt": b"text",
        },
    )

    metadata = get_metadata(archive, extract_cover=True)
    cover = _cover_tuple(metadata.cover_data)
    assert cover is not None
    fmt, raw = cover
    assert fmt in {"jpg", "jpeg"}
    assert raw == cover_payload


def test_extz_set_metadata_roundtrip_path(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.extz import get_metadata, set_metadata

    archive = tmp_path / "rw_roundtrip.txtz"
    _build_extz_archive(
        archive,
        {
            "metadata.opf": _opf_template(title="Original", authors=("One",)),
            "index.txt": b"original",
        },
    )

    updated = calibreMetaInformation("Updated Title", ["Alice", "Bob"])
    updated.tags = ["tag-a", "tag-b"]
    updated.comments = "Updated comment"
    updated.publisher = "Updated Publisher"
    set_metadata(archive, updated)

    metadata = get_metadata(archive, extract_cover=False)
    assert metadata.title == "Updated Title"
    assert _values(metadata.authors) == ["Alice", "Bob"]

    opf_text = _opf_text(archive)
    assert "tag-a" in opf_text
    assert "tag-b" in opf_text
    assert "Updated comment" in opf_text
    assert "Updated Publisher" in opf_text


def test_extz_set_metadata_accepts_stream(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.extz import get_metadata, set_metadata

    archive = tmp_path / "rw_stream.txtz"
    _build_extz_archive(
        archive,
        {
            "metadata.opf": _opf_template(title="Original Stream", authors=("Stream",)),
            "index.txt": b"stream",
        },
    )

    updated = calibreMetaInformation("Stream Updated", ["Writer"])
    with archive.open("r+b") as stream:
        set_metadata(stream, updated)
        assert stream.tell() == 0

    metadata = get_metadata(archive, extract_cover=False)
    assert metadata.title == "Stream Updated"
    assert _values(metadata.authors) == ["Writer"]


def test_extz_unicode_torture_roundtrip(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.extz import get_metadata, set_metadata

    archive = tmp_path / "unicode_torture.txtz"
    _build_extz_archive(
        archive,
        {
            "metadata.opf": _opf_template(title="Start", authors=("Start Author",)),
            "index.txt": b"unicode",
        },
    )

    title = "Ångström — Καλημέρα — Привет — 你好 — नमस्ते — العربية — עברית — 日本語 — 😀 — e\u0301"
    authors = ["Łukasz Żółć", "Мария Иванова", "山田 太郎", "Emoji 😀 Author"]
    updated = calibreMetaInformation(title, authors)
    updated.tags = ["タグ", "тег", "وسم", "टैग", "tag😀", "combining-e\u0301"]
    updated.comments = "RTL:\u202eabc\u202c; combining: a\u0300e\u0301i\u0302o\u0308u\u0323"
    updated.publisher = "出版社 / Издатель / الناشر"
    set_metadata(archive, updated)

    metadata = get_metadata(archive, extract_cover=False)
    assert metadata.title == title
    assert _values(metadata.authors) == authors

    opf_text = _opf_text(archive)
    assert "出版社" in opf_text
    assert "tag😀" in opf_text
    assert "combining-e" in opf_text


def test_extz_set_metadata_preserves_members_replaces_cover_and_sanitizes_xml(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.extz import get_metadata, set_metadata

    archive = tmp_path / "container_contract.txtz"
    _build_extz_archive(
        archive,
        {
            "metadata.opf": _opf_template(title="Original", authors=("Original Author",), include_cover=True),
            "index.txt": b"original body bytes",
            "notes/readme.txt": b"readme bytes",
            "cover.jpg": b"old-cover-bytes",
        },
    )

    with zipfile.ZipFile(archive, "r") as zf:
        preserved = {
            "index.txt": zf.read("index.txt"),
            "notes/readme.txt": zf.read("notes/readme.txt"),
        }

    title = "EXTZ\x00Title\ud800 😀"
    authors = ["Alice\x01 One", "Bob\udfff Two"]
    updated = calibreMetaInformation(title, authors)
    updated.tags = ["tag\x02one", "emoji 😀"]
    updated.comments = "Comment\x03 with <xml> & emoji 😀"
    updated.publisher = "Pub\x04lisher"
    updated.cover_data = ("jpeg", b"new-cover-bytes")

    set_metadata(archive, updated)

    with zipfile.ZipFile(archive, "r") as zf:
        assert zf.testzip() is None
        for name, payload in preserved.items():
            assert zf.read(name) == payload
        assert zf.read("cover.jpg") == b"new-cover-bytes"
        opf_raw = zf.read("metadata.opf")

    opf_text = opf_raw.decode("utf-8")
    assert not _contains_forbidden_xml_char(opf_text)
    ET.fromstring(opf_raw)
    assert "EXTZTitle" in opf_text
    assert "Publisher" in opf_text

    metadata = get_metadata(archive, extract_cover=False)
    assert metadata.title == "EXTZTitle 😀"
    assert _values(metadata.authors) == ["Alice One", "Bob Two"]
    assert updated.title == title
    assert updated.authors == authors


def test_extz_broken_encoding_in_opf_is_tolerated(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.extz import get_metadata

    opf = (
        b'<?xml version="1.0" encoding="utf-8"?>'
        b'<package xmlns="http://www.idpf.org/2007/opf" version="2.0">'
        b'<metadata xmlns:dc="http://purl.org/dc/elements/1.1/">'
        b"<dc:title>Bad\x96Title</dc:title>"
        b"<dc:creator>Writer\x96Name</dc:creator>"
        b"</metadata><manifest></manifest><spine/></package>"
    )
    archive = tmp_path / "broken_encoding.txtz"
    _build_extz_archive(archive, {"metadata.opf": opf, "index.txt": b"text"})

    metadata = get_metadata(archive)
    assert "Bad" in metadata.title
    assert _values(metadata.authors)


def test_extz_get_metadata_invalid_zip_raises_by_default_and_can_opt_into_fallback(monkeypatch) -> None:
    from LiuXin_alpha.metadata.file_sources.extz import ExtzFormatError, get_metadata

    events: list[tuple[str, str]] = []

    def _log_exception(base, exc, level, *pairs, **kwargs):
        events.append((str(base), str(exc)))
        return str(base)

    monkeypatch.setattr("LiuXin_alpha.metadata.file_sources.extz.default_log.log_exception", _log_exception)

    with pytest.raises(ExtzFormatError):
        get_metadata(io.BytesIO(b"not a zip stream"))

    metadata = get_metadata(io.BytesIO(b"not a zip stream"), fallback_on_parse_error=True)
    assert metadata.title == "Unknown"
    assert events
    assert any("Problem extracting metadata from an EXTZ archive." in base for base, _ in events)


def test_extz_get_metadata_missing_opf_with_credible_htmlz_content_returns_fallback(
    tmp_path: Path,
) -> None:
    from LiuXin_alpha.metadata.file_sources.extz import get_metadata

    archive = tmp_path / "no_opf.htmlz"
    _build_extz_archive(archive, {"index.html": b"<html/>"})

    metadata = get_metadata(archive)
    assert metadata.title == "Unknown"


def test_extz_set_metadata_invalid_zip_logs_and_raises(monkeypatch) -> None:
    from LiuXin_alpha.metadata.file_sources.extz import set_metadata

    events: list[tuple[str, str]] = []

    def _log_exception(base, exc, level, *pairs, **kwargs):
        events.append((str(base), str(exc)))
        return str(base)

    monkeypatch.setattr("LiuXin_alpha.metadata.file_sources.extz.default_log.log_exception", _log_exception)

    with pytest.raises(Exception):
        set_metadata(io.BytesIO(b"not zip"), calibreMetaInformation("x", ["y"]))
    assert events
    assert any("Failed to write metadata to EXTZ archive." in base for base, _ in events)


def test_extz_set_metadata_missing_opf_logs_and_raises(tmp_path: Path, monkeypatch) -> None:
    from LiuXin_alpha.metadata.file_sources.extz import set_metadata

    archive = tmp_path / "set_missing_opf.htmlz"
    _build_extz_archive(archive, {"index.html": b"<html/>"})

    events: list[tuple[str, str]] = []

    def _log_exception(base, exc, level, *pairs, **kwargs):
        events.append((str(base), str(exc)))
        return str(base)

    monkeypatch.setattr("LiuXin_alpha.metadata.file_sources.extz.default_log.log_exception", _log_exception)

    with pytest.raises(Exception):
        set_metadata(archive, calibreMetaInformation("x", ["y"]))
    assert events
    assert any("Failed to write metadata to EXTZ archive." in base for base, _ in events)

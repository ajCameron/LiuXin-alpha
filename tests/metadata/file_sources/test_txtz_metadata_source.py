from __future__ import annotations

import io
import zipfile
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
    try:
        return list(raw)
    except TypeError:
        return [raw]


def _first(raw):
    vals = _values(raw)
    return vals[0] if vals else None


def _snapshot(md) -> dict:
    return {
        "title": _first(getattr(md, "title", None)),
        "authors": sorted(_values(getattr(md, "authors", None))),
        "tags": sorted(_values(getattr(md, "tags", None))),
    }


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


def _build_txtz(path: Path, members: dict[str, bytes]) -> None:
    with zipfile.ZipFile(path, "w") as zf:
        for name, payload in members.items():
            zf.writestr(name, payload)


def test_txtz_module_import_smoke() -> None:
    import LiuXin_alpha.metadata.file_sources.txtz as txtz_md

    assert txtz_md is not None


def test_txtz_reader_plugin_is_available_and_preserves_stream_position(md_test_fixture) -> None:
    from LiuXin_alpha.customize.builtins.metadata_readers import get_metadata_reader_plugins

    fixture = md_test_fixture(file_ext="txtz", file_num=1, verify_hash=True)
    stream = fixture.open("rb")
    stream.seek(13)

    try:
        plugins = get_metadata_reader_plugins()
        txtz_cls = next((p for p in plugins if p.__name__ == "TXTZMetadataReader"), None)
        assert txtz_cls is not None

        reader = txtz_cls(None)
        md = reader.get_metadata(stream=stream, ftype="txtz")
        assert stream.tell() == 13
        assert md.title == "20,000 Leagues Under the Sea"
        assert _values(md.authors) == ["Jules Verne"]
    finally:
        stream.close()


def test_txtz_get_metadata_fixture_legacy_expectations(md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources.txtz import get_metadata

    fixture = md_test_fixture(file_ext="txtz", file_num=1, verify_hash=True)
    md = get_metadata(fixture)
    assert md.title == "20,000 Leagues Under the Sea"
    assert _values(md.authors) == ["Jules Verne"]
    assert _values(md.tags) == []


def test_txtz_falls_back_to_embedded_txt_when_opf_missing(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.txtz import get_metadata

    txt_payload = "Fallback Title\n\n\nby Fallback Author\nBody line.\n".encode("utf-8")
    archive = tmp_path / "fallback.txtz"
    _build_txtz(
        archive,
        {
            "index.txt": txt_payload,
        },
    )

    md = get_metadata(archive)
    assert md.title == "Fallback Title"
    assert _values(md.authors) == ["Fallback Author"]


def test_txtz_falls_back_to_gutenberg_header_when_opf_missing(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.txtz import get_metadata

    txt_payload = (
        b"\xef\xbb\xbfThe Project Gutenberg Etext of 20000 Leagues Under the Seas by Jules\r\n"
        b"Verne\r\n\r\nCopyright line.\r\n"
    )
    archive = tmp_path / "fallback_gutenberg.txtz"
    _build_txtz(archive, {"book.txt": txt_payload})

    md = get_metadata(archive)
    assert md.title == "20000 Leagues Under the Seas"
    assert _values(md.authors) == ["Jules Verne"]


def test_txtz_cover_fallback_when_opf_missing(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.txtz import get_metadata

    cover = b"\xff\xd8\xff\xe0fake-jpeg"
    archive = tmp_path / "cover_fallback.txtz"
    _build_txtz(
        archive,
        {
            "index.txt": b"Title only\n",
            "images/cover.jpeg": cover,
        },
    )

    md = get_metadata(archive, extract_cover=True)
    cover_tuple = _cover_tuple(md.cover_data)
    assert cover_tuple is not None
    fmt, payload = cover_tuple
    assert fmt == "jpg"
    assert payload == cover


def test_txtz_set_metadata_roundtrip_uses_extz_writer(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.txtz import get_metadata, set_metadata

    opf = (
        b'<?xml version="1.0" encoding="utf-8"?>'
        b'<package xmlns="http://www.idpf.org/2007/opf" version="2.0" unique-identifier="bookid">'
        b'<metadata xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:opf="http://www.idpf.org/2007/opf">'
        b"<dc:title>Original</dc:title>"
        b"<dc:creator>One</dc:creator>"
        b"</metadata><manifest/><spine/></package>"
    )
    archive = tmp_path / "write_roundtrip.txtz"
    _build_txtz(archive, {"metadata.opf": opf, "index.txt": b"body"})

    mi = calibreMetaInformation("Updated Title", ["Alice", "Bob"])
    set_metadata(archive, mi)
    md = get_metadata(archive)

    assert md.title == "Updated Title"
    assert _values(md.authors) == ["Alice", "Bob"]


def test_txtz_set_metadata_roundtrip_unicode_torture(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.txtz import get_metadata, set_metadata

    opf = (
        b'<?xml version="1.0" encoding="utf-8"?>'
        b'<package xmlns="http://www.idpf.org/2007/opf" version="2.0" unique-identifier="bookid">'
        b'<metadata xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:opf="http://www.idpf.org/2007/opf">'
        b"<dc:title>Original</dc:title>"
        b"<dc:creator>One</dc:creator>"
        b"</metadata><manifest/><spine/></package>"
    )
    archive = tmp_path / "write_roundtrip_unicode.txtz"
    _build_txtz(archive, {"metadata.opf": opf, "index.txt": b"body"})

    mi = calibreMetaInformation(
        "Unicode Torture — café Καλημέρα 日本語 😀",
        ["Alice Δ", "李白", "Боб"],
    )
    mi.tags = ["txtz-roundtrip", "δοκιμή", "漢字", "emoji😀"]
    mi.comments = "Comments e\u0301 / é / 👩🏽\u200d💻"

    set_metadata(archive, mi)
    md = get_metadata(archive)

    assert "Unicode Torture" in str(md.title)
    assert any("Alice" in str(author) for author in _values(md.authors))
    assert md is not None


def test_txtz_set_metadata_invalid_zip_raises() -> None:
    from LiuXin_alpha.metadata.file_sources.txtz import set_metadata

    with pytest.raises(Exception):
        set_metadata(io.BytesIO(b"not-a-zip"), calibreMetaInformation("x", ["y"]))


def test_txtz_invalid_payload_raises_by_default_and_can_opt_into_fallback() -> None:
    from LiuXin_alpha.metadata.file_sources.extz import ExtzFormatError
    from LiuXin_alpha.metadata.file_sources.txtz import get_metadata

    with pytest.raises(ExtzFormatError):
        get_metadata(io.BytesIO(b"not-a-zip"))

    md = get_metadata(io.BytesIO(b"not-a-zip"), fallback_on_parse_error=True)
    assert _first(md.title) == "Unknown"
    assert _values(md.authors) == ["Unknown"]


def test_txtz_md_fixture_smoke_and_deterministic(md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources.txtz import get_metadata

    fixture = md_test_fixture(file_ext="txtz", file_num=1, verify_hash=True)
    md_1 = get_metadata(fixture)
    md_2 = get_metadata(fixture)

    assert _snapshot(md_1) == _snapshot(md_2)
    assert md_1.title == "20,000 Leagues Under the Sea"

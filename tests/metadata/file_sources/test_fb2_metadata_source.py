from __future__ import annotations

import io
import shutil
import zipfile
from collections.abc import Mapping
from pathlib import Path

import pytest

from LiuXin_alpha.metadata.utils import calibreMetaInformation
from tests.support.file_format_zip import write_zip_archive


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


def _first_value(raw) -> str:
    values = _values(raw)
    return str(values[0]) if values else ""


def _cover_pair(raw):
    if isinstance(raw, tuple) and len(raw) == 2:
        return raw
    if isinstance(raw, Mapping):
        for key in raw.keys():
            if isinstance(key, tuple) and len(key) == 2:
                return key
    return None, b""


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


def _build_fb2_xml(*, title: str, first_name: str, last_name: str, encoding: str = "UTF-8") -> str:
    return (
        f'<?xml version="1.0" encoding="{encoding}"?>'
        '<FictionBook xmlns="http://www.gribuser.ru/xml/fictionbook/2.0">'
        "<description><title-info>"
        "<genre>sample</genre>"
        f"<author><first-name>{first_name}</first-name><last-name>{last_name}</last-name></author>"
        f"<book-title>{title}</book-title>"
        "<lang>en</lang>"
        "</title-info></description>"
        "<body><section><p>body</p></section></body>"
        "</FictionBook>"
    )


def test_fb2_metadata_module_import_smoke() -> None:
    import LiuXin_alpha.metadata.file_sources.fb2 as fb2_md

    assert fb2_md is not None


def test_fb2_all_hashed_fixtures_reader_smoke(md_test_fixtures_for_ext) -> None:
    from LiuXin_alpha.metadata.file_sources.fb2 import get_metadata, get_metadata_inplace

    fixtures = md_test_fixtures_for_ext(file_ext="fb2", verify_hash=True)
    assert fixtures

    for fixture in fixtures:
        md_from_path = get_metadata(fixture)
        assert md_from_path is not None

        with fixture.open("rb") as stream:
            md_from_stream = get_metadata(stream)
            assert stream.tell() == 0

        md_inplace = get_metadata_inplace(fixture)
        assert md_inplace is not None

        assert md_from_stream.title == md_from_path.title
        assert md_inplace.title == md_from_path.title


def test_fb2_legacy_fixture1_expectations(md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources.fb2 import get_metadata

    fixture = md_test_fixture(file_ext="fb2", file_num=1, verify_hash=True)
    metadata = get_metadata(fixture)

    assert metadata.title == "20,000 Leagues Under the Sea"
    assert _values(metadata.authors) == ["Jules Verne"]
    assert "antique" in _values(metadata.tags)
    assert _first_value(metadata.publisher) == "http://english-e-books.net/"


def test_fb2_reader_plugin_is_available(md_test_fixture) -> None:
    from LiuXin_alpha.customize.builtins.metadata_readers import get_metadata_reader_plugins

    fixture = md_test_fixture(file_ext="fb2", file_num=1, verify_hash=True)

    plugins = get_metadata_reader_plugins()
    fb2_cls = next((p for p in plugins if p.__name__ == "FB2MetadataReader"), None)
    assert fb2_cls is not None
    assert {"fb2", "fbz"}.issubset(set(fb2_cls.file_types))

    reader = fb2_cls(None)
    with fixture.open("rb") as stream:
        metadata = reader.get_metadata(stream=stream, ftype="fb2")
    inplace_metadata = reader.get_metadata_inplace(file_path=str(fixture), ftype="fb2")

    assert metadata.title == "20,000 Leagues Under the Sea"
    assert _values(metadata.authors) == ["Jules Verne"]
    assert inplace_metadata.title == "20,000 Leagues Under the Sea"


def test_fb2_cover_extracts_binary_from_fixture(md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources.fb2 import get_metadata

    fixture = md_test_fixture(file_ext="fb2", file_num=3, verify_hash=True)
    metadata = get_metadata(fixture)

    assert metadata.cover_data is not None
    fmt, raw = _cover_pair(metadata.cover_data)
    assert fmt in {"jpeg", "jpg", "png", "gif", "webp"}
    assert isinstance(raw, (bytes, bytearray))
    assert len(raw) > 64


def test_fb2_set_metadata_roundtrip_path(tmp_path: Path, md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources.fb2 import get_metadata, set_metadata

    source = md_test_fixture(file_ext="fb2", file_num=1, verify_hash=True)
    target = tmp_path / "fb2_roundtrip.fb2"
    shutil.copy2(source, target)

    updated = calibreMetaInformation("Updated FB2 Title", ["Alice Example", "Bob Writer"])
    updated.tags = ["unicode-tag", "adventure"]
    updated.comments = "<p>Paragraph 1</p><p>Paragraph 2 &amp; 三</p>"
    updated.publisher = "Alpha Press"
    updated.series = "Series Name"
    updated.series_index = 3.5

    set_metadata(target, updated)
    metadata = get_metadata(target)

    assert metadata.title == "Updated FB2 Title"
    assert _values(metadata.authors) == ["Alice Example", "Bob Writer"]
    assert set(_values(metadata.tags)) == {"unicode-tag", "adventure"}
    comments = _first_value(metadata.comments)
    assert "Paragraph 1" in comments
    assert "Paragraph 2" in comments
    assert "三" in comments
    assert _first_value(metadata.publisher) == "Alpha Press"
    assert _first_value(metadata.series) == "Series Name"
    payload = target.read_text(encoding="utf-8", errors="replace")
    assert "sequence" in payload
    assert 'name="Series Name"' in payload
    assert 'number="3.5"' in payload


def test_fb2_set_metadata_roundtrip_stream(tmp_path: Path, md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources.fb2 import get_metadata, set_metadata

    source = md_test_fixture(file_ext="fb2", file_num=3, verify_hash=True)
    target = tmp_path / "fb2_stream_roundtrip.fb2"
    shutil.copy2(source, target)

    updated = calibreMetaInformation("Stream FB2 Title", ["Stream Author"])

    with target.open("r+b") as stream:
        set_metadata(stream, updated)
        assert stream.tell() == 0

    metadata = get_metadata(target)
    assert metadata.title == "Stream FB2 Title"
    assert _values(metadata.authors) == ["Stream Author"]


def test_fb2_writer_plugin_is_available(tmp_path: Path, md_test_fixture) -> None:
    from LiuXin_alpha.customize.builtins.metadata_writers import get_metadata_set_plugins
    from LiuXin_alpha.metadata.file_sources.fb2 import get_metadata

    source = md_test_fixture(file_ext="fb2", file_num=1, verify_hash=True)
    target = tmp_path / "fb2_writer_plugin.fb2"
    shutil.copy2(source, target)

    plugins = get_metadata_set_plugins()
    fb2_cls = next((p for p in plugins if p.__name__ == "FB2MetadataWriter"), None)
    assert fb2_cls is not None
    assert {"fb2", "fbz"}.issubset(set(fb2_cls.file_types))

    writer = fb2_cls(None)
    updated = calibreMetaInformation("Plugin FB2 Title", ["Plugin Author"])

    with target.open("r+b") as stream:
        writer.set_metadata(stream=stream, mi=updated, type="fb2")

    metadata = get_metadata(target)
    assert metadata.title == "Plugin FB2 Title"
    assert _values(metadata.authors) == ["Plugin Author"]


def test_fb2_unicode_torture_roundtrip(tmp_path: Path, md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources.fb2 import get_metadata, set_metadata

    source = md_test_fixture(file_ext="fb2", file_num=1, verify_hash=True)
    target = tmp_path / "fb2_unicode_torture.fb2"
    shutil.copy2(source, target)

    title = "Unicode — Καλημέρα — こんにちは — Привет — مرحبا — नमस्ते — 😀"
    authors = [
        "Renée Faßbinder",
        "李白",
        "Александр Пушкин",
        "محمد بن موسى",
    ]
    tags = ["naïve", "δοκιμή", "テスト", "испытание", "اختبار", "परीक्षण"]

    updated = calibreMetaInformation(title, authors)
    updated.tags = tags
    updated.comments = "<p>Αλφα</p><p>бета</p><p>三</p><p>😀</p>"

    set_metadata(target, updated)
    metadata = get_metadata(target)

    assert metadata.title == title
    assert _values(metadata.authors) == authors
    assert set(_values(metadata.tags)) == set(tags)
    comments = _first_value(metadata.comments)
    for expected in ("Αλφα", "бета", "三", "😀"):
        assert expected in comments


def test_fb2_set_metadata_sanitizes_hostile_xml_without_mutating_input(
    tmp_path: Path,
    md_test_fixture,
) -> None:
    from LiuXin_alpha.metadata.file_sources.fb2 import get_metadata, set_metadata
    from LiuXin_alpha.utils.libraries.liuxin_etree import etree

    source = md_test_fixture(file_ext="fb2", file_num=1, verify_hash=True)
    target = tmp_path / "fb2_hostile_xml.fb2"
    shutil.copy2(source, target)

    title = "Bad\x00Title\ud800 😀"
    authors = ["Author\x01 One", "Second\udfff Author"]
    tags = ["Tag\x02One", "Emoji 😀"]

    updated = calibreMetaInformation(title, authors)
    updated.tags = tags
    updated.comments = "First\x03 paragraph\nSecond\ud800 paragraph 😀"
    updated.publisher = "Pub\x04lisher"
    updated.series = "Series\x05Name"

    set_metadata(target, updated)

    payload = target.read_text(encoding="utf-8")
    assert not _contains_forbidden_xml_char(payload)
    etree.fromstring(payload.encode("utf-8"))

    metadata = get_metadata(target)
    assert "BadTitle" in metadata.title
    assert "😀" in metadata.title
    assert _values(metadata.authors) == ["Author One", "Second Author"]
    assert set(_values(metadata.tags)) == {"TagOne", "Emoji 😀"}
    assert "First paragraph" in _first_value(metadata.comments)
    assert "Second paragraph 😀" in _first_value(metadata.comments)
    assert _first_value(metadata.publisher) == "Publisher"
    assert _first_value(metadata.series) == "SeriesName"

    assert updated.title == title
    assert updated.authors == authors
    assert updated.tags == tags


def test_fb2_malformed_payload_raises_by_default_and_can_opt_into_fallback(monkeypatch) -> None:
    import LiuXin_alpha.metadata.file_sources.fb2 as fb2

    calls: list[tuple[str, str]] = []

    def _record(message, _err, level, *args):
        calls.append((message, level))

    monkeypatch.setattr(fb2.default_log, "log_exception", _record)

    stream = io.BytesIO(b"this is not xml")
    stream.name = "broken_file.fb2"

    with pytest.raises(fb2.FB2ParseError):
        fb2.get_metadata(stream)

    metadata = fb2.get_metadata(stream, fallback_on_parse_error=True)

    assert metadata.title == "broken_file"
    assert _first_value(metadata.authors).lower() == "unknown"
    assert calls


def test_fb2_set_metadata_on_malformed_payload_logs_and_raises(monkeypatch) -> None:
    import LiuXin_alpha.metadata.file_sources.fb2 as fb2

    events: list[tuple[str, str]] = []

    def _record(message, err, level, *pairs):
        events.append((str(message), str(err)))

    monkeypatch.setattr(fb2.default_log, "log_exception", _record)

    stream = io.BytesIO(b"this is not xml")
    with pytest.raises(Exception):
        fb2.set_metadata(stream, calibreMetaInformation("Updated", ["Author"]))

    assert events
    assert any("Failed to write metadata to FB2 source." in msg for msg, _ in events)


def test_fb2_reads_windows1251_encoded_payload() -> None:
    from LiuXin_alpha.metadata.file_sources.fb2 import get_metadata

    xml_text = _build_fb2_xml(
        title="Привет мир",
        first_name="Иван",
        last_name="Петров",
        encoding="Windows-1251",
    )
    stream = io.BytesIO(xml_text.encode("cp1251"))
    stream.name = "cp1251.fb2"

    metadata = get_metadata(stream)
    assert metadata.title == "Привет мир"
    assert _values(metadata.authors) == ["Иван Петров"]


def test_fb2_handles_declared_encoding_mismatch_without_crashing() -> None:
    from LiuXin_alpha.metadata.file_sources.fb2 import get_metadata

    xml_text = _build_fb2_xml(
        title="Тест mismatch",
        first_name="Иван",
        last_name="Петров",
        encoding="UTF-8",
    )

    # Deliberately encode in cp1251 while declaration says UTF-8.
    stream = io.BytesIO(xml_text.encode("cp1251", "replace"))
    stream.name = "encoding_mismatch.fb2"

    metadata = get_metadata(stream)
    assert metadata is not None
    assert bool(metadata.title)


def test_fb2_reads_and_writes_fb2_inside_zip_container_preserving_other_members(
    tmp_path: Path,
    md_test_fixture,
) -> None:
    from LiuXin_alpha.metadata.file_sources.fb2 import get_metadata, set_metadata

    source = md_test_fixture(file_ext="fb2", file_num=1, verify_hash=True)
    zip_target = tmp_path / "book_bundle.fbz"

    with zipfile.ZipFile(zip_target, "w") as zf:
        zf.writestr("book/book.fb2", source.read_bytes())
        zf.writestr("book/extra.txt", "keep me")

    before = get_metadata(zip_target)
    assert before.title == "20,000 Leagues Under the Sea"

    updated = calibreMetaInformation("Zipped FB2 Title", ["Zip Author"])
    set_metadata(zip_target, updated)

    after = get_metadata(zip_target)
    assert after.title == "Zipped FB2 Title"
    assert _values(after.authors) == ["Zip Author"]

    with zipfile.ZipFile(zip_target, "r") as zf:
        names = set(zf.namelist())
        assert "book/extra.txt" in names
        payload = zf.read("book/book.fb2")
        assert b"Zipped FB2 Title" in payload


@pytest.mark.parametrize(
    ("case_id", "members", "match"),
    (
        ("no_fb2_member", {"readme.txt": b"not a book"}, "no FB2 member"),
        (
            "multiple_fb2_members",
            {
                "a/book.fb2": _build_fb2_xml(title="A", first_name="A", last_name="One").encode("utf-8"),
                "b/book.fb2": _build_fb2_xml(title="B", first_name="B", last_name="Two").encode("utf-8"),
            },
            "multiple FB2 members",
        ),
        (
            "unsafe_member_path",
            {
                "book/book.fb2": _build_fb2_xml(title="Safe", first_name="A", last_name="One").encode("utf-8"),
                "book/../escape.txt": b"unsafe",
            },
            "unsafe path",
        ),
    ),
)
def test_fb2_metadata_rejects_hostile_zip_payloads(
    tmp_path: Path,
    case_id: str,
    members: dict[str, bytes],
    match: str,
) -> None:
    import LiuXin_alpha.metadata.file_sources.fb2 as fb2

    archive = tmp_path / f"{case_id}.fbz"
    write_zip_archive(archive, members)

    with pytest.raises(fb2.FB2ParseError, match=match):
        fb2.get_metadata(archive)

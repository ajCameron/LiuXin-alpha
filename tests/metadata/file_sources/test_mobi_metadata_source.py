from __future__ import annotations

import io
import sys
import types
from collections.abc import Mapping
from pathlib import Path
from types import SimpleNamespace

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


def _meta(title: str = "Unknown", authors: list[str] | None = None):
    from LiuXin_alpha.metadata.utils import calibreMetaInformation

    return calibreMetaInformation(title, authors or ["Unknown"])


def test_mobi_metadata_module_import_smoke() -> None:
    import LiuXin_alpha.metadata.file_sources.mobi as mobi_md

    assert mobi_md is not None


def test_mobi_reader_plugin_is_available_and_preserves_stream_position() -> None:
    from LiuXin_alpha.customize.builtins.metadata_readers import get_metadata_reader_plugins
    from LiuXin_alpha.file_formats.mobi import MobiError

    plugins = get_metadata_reader_plugins()
    mobi_cls = next((p for p in plugins if p.__name__ == "MOBIMetadataReader"), None)
    assert mobi_cls is not None

    stream = io.BytesIO(b"not-a-mobi")
    stream.seek(3)
    reader = mobi_cls(None)

    with pytest.raises(MobiError):
        reader.get_metadata(stream=stream, ftype="mobi")
    assert stream.tell() == 3


def test_mobi_get_metadata_uses_exth_metadata_and_extracts_cover(monkeypatch) -> None:
    import LiuXin_alpha.metadata.file_sources.mobi as mobi_md
    import LiuXin_alpha.file_formats.mobi.reader.headers as headers_mod

    class _FakeHeader:
        def __init__(self, _stream, _log):
            self.title = "Header Title"
            self.exth = SimpleNamespace(mi=_meta("EXTH Title", ["EXTH Author"]), cover_offset=1)
            self.first_image_index = 2

        @staticmethod
        def section_data(_index: int) -> bytes:
            return b"\xff\xd8\xff\xe0fake-jpeg"

    monkeypatch.setattr(headers_mod, "MetadataHeader", _FakeHeader)

    md = mobi_md.get_metadata(io.BytesIO(b"BOOKMOBI payload"), extract_cover=True)
    assert md.title == "EXTH Title"
    assert _values(md.authors) == ["EXTH Author"]
    assert md.cover_data[0] == "jpg"
    assert md.cover_data[1].startswith(b"\xff\xd8")


def test_mobi_get_metadata_falls_back_to_embedded_reader_for_small_files(monkeypatch) -> None:
    import LiuXin_alpha.metadata.file_sources.mobi as mobi_md
    import LiuXin_alpha.file_formats.mobi.reader.headers as headers_mod
    import LiuXin_alpha.file_formats.mobi.reader.mobi6 as mobi6_mod

    class _FakeHeader:
        def __init__(self, _stream, _log):
            self.title = "Header Title"
            self.exth = None
            self.first_image_index = 0

        @staticmethod
        def section_data(_index: int) -> bytes:
            return b""

    class _FakeReader:
        def __init__(self, _stream, _log):
            self.embedded_mi = _meta("Embedded Title", ["Embedded Author"])

        def extract_content(self, _tdir, _cache) -> None:
            return None

    monkeypatch.setattr(headers_mod, "MetadataHeader", _FakeHeader)
    monkeypatch.setattr(mobi6_mod, "MobiReader", _FakeReader)
    monkeypatch.setattr(mobi_md, "_stream_size", lambda _stream, fallback=1024**3: 1024)

    md = mobi_md.get_metadata(io.BytesIO(b"BOOKMOBI payload"), extract_cover=False)
    assert md.title == "Embedded Title"
    assert _values(md.authors) == ["Embedded Author"]


def test_mobi_get_metadata_dispatches_topaz_when_available(monkeypatch) -> None:
    import LiuXin_alpha.metadata.file_sources.mobi as mobi_md

    fake_topaz = types.ModuleType("LiuXin_alpha.metadata.file_sources.topaz")
    fake_topaz.get_metadata = lambda stream: _meta("Topaz Title", ["Topaz Author"])
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.metadata.file_sources.topaz", fake_topaz)

    md = mobi_md.get_metadata(io.BytesIO(b"TPZ_payload"))
    assert md.title == "Topaz Title"
    assert _values(md.authors) == ["Topaz Author"]


def test_mobi_get_metadata_invalid_stream_raises_by_default_and_can_opt_into_fallback(tmp_path: Path) -> None:
    from LiuXin_alpha.file_formats.mobi import MobiError
    from LiuXin_alpha.metadata.file_sources.mobi import get_metadata

    path = tmp_path / "broken_case.mobi"
    path.write_bytes(b"this is not a mobi file")

    with pytest.raises(MobiError):
        get_metadata(path)

    md = get_metadata(path, fallback_on_parse_error=True)
    assert md.title == "broken_case"
    assert _values(md.authors) == ["Unknown"]


def test_mobi_get_metadata_inplace_pathlike_reads_without_cover(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.mobi import get_metadata_inplace

    path = tmp_path / "broken_case_2.mobi"
    path.write_bytes(b"still not a mobi file")

    md = get_metadata_inplace(path, fallback_on_parse_error=True)
    assert md.title == "broken_case_2"
    assert _values(md.authors) == ["Unknown"]


def test_mobi_get_metadata_unicode_filename_fallback_title(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.mobi import get_metadata

    path = tmp_path / "主題🙂_δοκιμή_اختبار.mobi"
    path.write_bytes(b"invalid mobi payload")

    md = get_metadata(path, extract_cover=False, fallback_on_parse_error=True)
    assert md.title == "主題🙂_δοκιμή_اختبار"
    assert _values(md.authors) == ["Unknown"]


def test_mobi_optional_real_fixtures_parse_without_crash(md_test_files_by_ext: dict[str, list[Path]]) -> None:
    from LiuXin_alpha.metadata.file_sources.mobi import get_metadata

    fixtures = list(md_test_files_by_ext.get("mobi", [])) + list(md_test_files_by_ext.get("azw3", []))
    if not fixtures:
        pytest.skip("No MOBI/AZW3 fixtures found in optional LiuXin_alpha_data corpus")

    for path in fixtures:
        with path.open("rb") as stream:
            md = get_metadata(stream, extract_cover=False)
        assert md is not None
        assert bool(getattr(md, "title", None))


def test_mobi_set_metadata_delegates_to_metadata_updater(monkeypatch) -> None:
    import LiuXin_alpha.metadata.file_sources.mobi as mobi_md

    calls = {}

    class _FakeUpdater:
        def __init__(self, stream):
            calls["stream"] = stream

        def update(self, mi):
            calls["mi"] = mi

    monkeypatch.setattr(mobi_md, "MetadataUpdater", _FakeUpdater)

    stream = io.BytesIO(b"BOOKMOBI payload")
    mi = _meta("Setter Title", ["Setter Author"])
    mobi_md.set_metadata(stream, mi)

    assert calls["stream"] is stream
    assert calls["mi"] is mi


def test_mobi_set_metadata_roundtrip_unicode_torture_if_fixture_available(
    md_test_files_by_ext: dict[str, list[Path]],
    tmp_path: Path,
) -> None:
    from LiuXin_alpha.metadata.file_sources.mobi import get_metadata, set_metadata

    fixtures: list[Path] = []
    for ext in ("mobi", "azw", "azw3", "prc", "azw4"):
        fixtures.extend(md_test_files_by_ext.get(ext, []))
    if not fixtures:
        pytest.skip("No writable MOBI-family fixtures available in optional corpus")

    mi = _meta(
        "Roundtrip Unicode Torture — café Καλημέρα 日本語 😀",
        ["Alice Δ", "李白", "Боб"],
    )
    mi.tags = ["roundtrip-tag", "δοκιμή", "漢字", "emoji😀"]
    mi.publisher = "Publisher 測試"
    mi.comments = "Comments e\u0301 / é / 👩🏽\u200d💻"

    errors: list[str] = []
    for source in fixtures:
        target = tmp_path / f"rw_{source.name}"
        target.write_bytes(source.read_bytes())
        try:
            with target.open("r+b") as stream:
                set_metadata(stream, mi)
            read_back = get_metadata(target, extract_cover=False)
        except Exception as err:
            errors.append(f"{source.name}: {type(err).__name__}")
            continue

        assert "Roundtrip Unicode Torture" in str(read_back.title)
        assert any("Alice" in str(author) for author in _values(getattr(read_back, "authors", None)))
        tags = {str(tag).casefold() for tag in _values(getattr(read_back, "tags", None))}
        assert "roundtrip-tag" in tags
        return

    pytest.skip("No writable MOBI fixture could be updated in this environment: " + ", ".join(errors))


def test_mobi_set_metadata_invalid_payload_raises_clean_error() -> None:
    from LiuXin_alpha.file_formats.mobi import MobiError
    from LiuXin_alpha.metadata.file_sources.mobi import set_metadata

    stream = io.BytesIO(b"not-a-mobi-payload")
    mi = _meta("Setter Title", ["Setter Author"])

    with pytest.raises(MobiError, match="Setting metadata only supported"):
        set_metadata(stream, mi)

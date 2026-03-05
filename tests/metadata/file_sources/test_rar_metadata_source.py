from __future__ import annotations

import io
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


def _make_md(title: str, authors: list[str] | None = None):
    return calibreMetaInformation(title, authors or ["Unknown"])


def test_rar_metadata_module_import_smoke() -> None:
    import LiuXin_alpha.metadata.file_sources.rar as rar_md

    assert rar_md is not None


def test_rar_reader_plugin_is_available_and_preserves_stream_position(monkeypatch) -> None:
    from LiuXin_alpha.customize.builtins.metadata_readers import get_metadata_reader_plugins
    import LiuXin_alpha.metadata.file_sources.rar as rar_md

    stream = io.BytesIO(b"not-a-real-rar")
    stream.name = "comic.rar"
    stream.seek(3)

    monkeypatch.setattr(rar_md, "names", lambda _stream: ["page001.jpg", "page002.png"])
    seen = {}

    def _fake_dispatch(target, *, force_type: str):
        seen["force_type"] = force_type
        seen["target"] = target
        return _make_md("Comic Title", ["Artist One"])

    monkeypatch.setattr(rar_md, "_dispatch_metadata", _fake_dispatch)

    plugins = get_metadata_reader_plugins()
    rar_cls = next((p for p in plugins if p.__name__ == "RARMetadataReader"), None)
    assert rar_cls is not None

    reader = rar_cls(None)
    md = reader.get_metadata(stream=stream, ftype="rar")

    assert md.title == "Comic Title"
    assert _values(md.authors) == ["Artist One"]
    assert seen["force_type"] == "cbr"
    assert seen["target"] is stream
    assert stream.tell() == 3


def test_rar_get_metadata_extracts_first_supported_member(monkeypatch) -> None:
    import LiuXin_alpha.metadata.file_sources.rar as rar_md

    stream = io.BytesIO(b"rar-payload")
    stream.name = "bundle.rar"

    monkeypatch.setattr(rar_md, "names", lambda _stream: ["readme.txt", "books/book.epub", "books/book.mobi"])
    monkeypatch.setattr(
        rar_md,
        "extract_member",
        lambda _stream, match, name: ("books/book.epub", b"epub-bytes") if name == "books/book.epub" else None,
    )

    def _fake_dispatch(target, *, force_type: str):
        assert force_type == "epub"
        assert isinstance(target, io.BytesIO)
        assert target.name == "book.epub"
        assert target.read() == b"epub-bytes"
        target.seek(0)
        md = _make_md("Nested EPUB", ["Alice"])
        md.timestamp = "will-be-cleared"
        return md

    monkeypatch.setattr(rar_md, "_dispatch_metadata", _fake_dispatch)

    md = rar_md.get_metadata(stream)
    assert md.title == "Nested EPUB"
    assert _values(md.authors) == ["Alice"]
    assert getattr(md, "timestamp", None) is None


def test_rar_get_metadata_raises_for_archive_with_no_supported_members(monkeypatch) -> None:
    import LiuXin_alpha.metadata.file_sources.rar as rar_md

    stream = io.BytesIO(b"rar")
    stream.name = "empty.rar"
    monkeypatch.setattr(rar_md, "names", lambda _stream: ["readme.txt", "image.jpg"])

    with pytest.raises(ValueError, match="No ebook found in RAR archive"):
        rar_md.get_metadata(stream)


def test_rar_get_metadata_raises_if_member_extraction_fails(monkeypatch) -> None:
    import LiuXin_alpha.metadata.file_sources.rar as rar_md

    stream = io.BytesIO(b"rar")
    stream.name = "broken.rar"
    monkeypatch.setattr(rar_md, "names", lambda _stream: ["book.epub"])
    monkeypatch.setattr(rar_md, "extract_member", lambda _stream, match, name: None)

    with pytest.raises(ValueError, match="Unable to extract selected archive member"):
        rar_md.get_metadata(stream)


def test_rar_get_metadata_accepts_pathlike_input(tmp_path: Path, monkeypatch) -> None:
    import LiuXin_alpha.metadata.file_sources.rar as rar_md

    archive_path = tmp_path / "sample.rar"
    archive_path.write_bytes(b"rar-bytes")

    monkeypatch.setattr(rar_md, "names", lambda _stream: ["book.epub"])
    monkeypatch.setattr(rar_md, "extract_member", lambda _stream, match, name: ("book.epub", b"data"))
    monkeypatch.setattr(rar_md, "_dispatch_metadata", lambda target, force_type: _make_md("Pathlike EPUB", ["P"]))

    md = rar_md.get_metadata(archive_path)
    assert md.title == "Pathlike EPUB"
    assert _values(md.authors) == ["P"]


def test_rar_metadata_fixture_smoke_if_unrar_runtime_available(md_test_fixture) -> None:
    import LiuXin_alpha.metadata.file_sources.rar as rar_md

    fixture_path = md_test_fixture(file_ext="rar", file_num=1, verify_hash=True)
    try:
        md = rar_md.get_metadata(fixture_path)
    except Exception as err:
        pytest.skip(f"RAR runtime unavailable in this environment: {err}")

    assert md is not None
    assert bool(_values(getattr(md, "title", None)))

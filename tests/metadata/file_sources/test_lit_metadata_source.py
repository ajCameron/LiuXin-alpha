from __future__ import annotations

import io
from collections.abc import Mapping
from pathlib import Path

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


def _build_basic_opf(*, guide: str) -> str:
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<package xmlns="http://www.idpf.org/2007/opf" '
        'xmlns:dc="http://purl.org/dc/elements/1.1/" '
        'xmlns:opf="http://www.idpf.org/2007/opf" version="2.0">'
        "<metadata>"
        "<dc:title>Lit Metadata Title</dc:title>"
        '<dc:creator opf:role="aut">Ada Lovelace</dc:creator>'
        "</metadata>"
        "<manifest>"
        '<item id="c1" href="cover.jpg" media-type="image/jpeg"/>'
        '<item id="c2" href="cover-standard.jpg" media-type="image/jpeg"/>'
        "</manifest>"
        "<spine/>"
        f"<guide>{guide}</guide>"
        "</package>"
    )


class _ManifestItem:
    def __init__(self, path: str, internal: str) -> None:
        self.path = path
        self.internal = internal


def test_lit_metadata_module_import_smoke() -> None:
    import LiuXin_alpha.metadata.file_sources.lit as lit_md

    assert lit_md is not None


def test_lit_reader_plugin_is_available() -> None:
    from LiuXin_alpha.customize.builtins.metadata_readers import get_metadata_reader_plugins

    plugins = get_metadata_reader_plugins()
    lit_cls = next((p for p in plugins if p.__name__ == "LITMetadataReader"), None)
    assert lit_cls is not None


def test_lit_get_metadata_extracts_title_author_and_cover(monkeypatch) -> None:
    from LiuXin_alpha.metadata.file_sources import lit as lit_md

    class _InnerLit:
        manifest = {"cover": _ManifestItem("cover.jpg", "cover-internal")}

        @staticmethod
        def get_file(path: str) -> bytes:
            assert path == "/data/cover-internal"
            return b"\x11\x22\x33\x44\x55"

    class _Container:
        def __init__(self, _stream, _log) -> None:
            self._litfile = _InnerLit()

        @staticmethod
        def get_metadata() -> str:
            return _build_basic_opf(guide='<reference type="cover" href="cover.jpg"/>')

    monkeypatch.setattr(lit_md, "_load_lit_container_class", lambda: _Container)

    md = lit_md.get_metadata(io.BytesIO(b"not-a-real-lit"))

    assert md.title == "Lit Metadata Title"
    assert _values(md.authors) == ["Ada Lovelace"]
    assert isinstance(md.cover_data, tuple) and len(md.cover_data) == 2
    assert md.cover_data[0] == "jpg"
    assert md.cover_data[1] == b"\x11\x22\x33\x44\x55"


def test_lit_cover_standard_reference_preferred_when_present(monkeypatch) -> None:
    from LiuXin_alpha.metadata.file_sources import lit as lit_md

    class _InnerLit:
        manifest = {
            "cover": _ManifestItem("cover.jpg", "cover-internal"),
            "cover-standard": _ManifestItem("cover-standard.jpg", "cover-standard-internal"),
        }

        @staticmethod
        def get_file(path: str) -> bytes:
            if path.endswith("cover-internal"):
                return b"A" * 16
            if path.endswith("cover-standard-internal"):
                return b"B" * 8
            raise KeyError(path)

    class _Container:
        def __init__(self, _stream, _log) -> None:
            self._litfile = _InnerLit()

        @staticmethod
        def get_metadata() -> str:
            return _build_basic_opf(
                guide=(
                    '<reference type="cover" href="cover.jpg"/>'
                    '<reference type="cover-standard" href="cover-standard.jpg"/>'
                )
            )

    monkeypatch.setattr(lit_md, "_load_lit_container_class", lambda: _Container)

    md = lit_md.get_metadata(io.BytesIO(b"not-a-real-lit"))
    assert md.cover_data[1] == b"B" * 8


def test_lit_get_metadata_reader_error_raises_by_default_and_can_opt_into_fallback(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from LiuXin_alpha.metadata.file_sources import lit as lit_md

    class _BrokenContainer:
        def __init__(self, _stream, _log) -> None:
            raise ValueError("broken lit fixture")

    monkeypatch.setattr(lit_md, "_load_lit_container_class", lambda: _BrokenContainer)

    lit_path = tmp_path / "fallback_title.lit"
    lit_path.write_bytes(b"broken")

    with pytest.raises(lit_md.LitFormatError):
        lit_md.get_metadata(lit_path)

    md = lit_md.get_metadata(lit_path, fallback_on_parse_error=True)
    assert md.title == "fallback_title"
    assert _values(md.authors) == ["Unknown"]


def test_lit_stream_position_restored_after_read(monkeypatch) -> None:
    from LiuXin_alpha.metadata.file_sources import lit as lit_md

    class _InnerLit:
        manifest = {}

        @staticmethod
        def get_file(_path: str) -> bytes:
            raise KeyError("no cover")

    class _Container:
        def __init__(self, _stream, _log) -> None:
            self._litfile = _InnerLit()

        @staticmethod
        def get_metadata() -> str:
            return _build_basic_opf(guide="")

    monkeypatch.setattr(lit_md, "_load_lit_container_class", lambda: _Container)

    stream = io.BytesIO(b"prefix-and-suffix")
    stream.seek(6)
    md = lit_md.get_metadata(stream)

    assert md.title == "Lit Metadata Title"
    assert stream.tell() == 6

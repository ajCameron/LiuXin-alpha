from __future__ import annotations

import io
import struct
import zlib
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


def _build_lrx_payload(
    *,
    title: str = "LRX Title",
    author: str = "Ada Lovelace",
    publisher: str = "Example Press",
    categories: tuple[str, ...] = ("Speculative", "Archive"),
    language: str = "en",
    title_sort: str = "Title Sort",
    author_sort: str = "Lovelace, Ada",
    lrf_version: int = 700,
) -> bytes:
    category_nodes = "".join(f"<Category>{x}</Category>" for x in categories)
    xml = (
        "<Root>"
        "<BookInfo>"
        f'<Title reading="{title_sort}">{title}</Title>'
        f'<Author reading="{author_sort}">{author}</Author>'
        f"<Publisher>{publisher}</Publisher>"
        f"{category_nodes}"
        "</BookInfo>"
        "<DocInfo>"
        f"<Language>{language}</Language>"
        "</DocInfo>"
        "</Root>"
    ).encode("utf-8")

    compressed = zlib.compress(xml)
    size_with_footer = len(compressed) + 4
    if size_with_footer > 0xFFFF:
        raise AssertionError("Test LRX payload compression result exceeded 16-bit size field")

    marker = "LRF\x00".encode("utf-16-le")
    payload = bytearray()
    payload.extend(struct.pack(">L", 12))
    payload.extend(b"ftypLRX2")
    payload.extend(struct.pack(">L", 8))
    payload.extend(b"bbeb")
    payload.extend(marker)
    payload.extend(struct.pack("<L", lrf_version))
    payload.extend(b"\x00\x00\x00\x00")

    compressed_size_offset = 20 + 0x4C
    if len(payload) < compressed_size_offset + 2:
        payload.extend(b"\x00" * (compressed_size_offset + 2 - len(payload)))
    payload[compressed_size_offset : compressed_size_offset + 2] = struct.pack("<H", size_with_footer)

    uncompressed_size_offset = compressed_size_offset + 2 + (6 if lrf_version >= 800 else 0)
    payload_size_needed = uncompressed_size_offset + 4 + len(compressed)
    if len(payload) < payload_size_needed:
        payload.extend(b"\x00" * (payload_size_needed - len(payload)))
    payload[uncompressed_size_offset : uncompressed_size_offset + 4] = struct.pack("<L", len(xml))
    payload[uncompressed_size_offset + 4 : uncompressed_size_offset + 4 + len(compressed)] = compressed
    return bytes(payload)


def test_lrx_metadata_module_import_smoke() -> None:
    import LiuXin_alpha.metadata.file_sources.lrx as lrx_md

    assert lrx_md is not None


def test_lrx_reader_plugin_is_available_and_keeps_stream_position() -> None:
    from LiuXin_alpha.customize.builtins.metadata_readers import get_metadata_reader_plugins

    plugins = get_metadata_reader_plugins()
    lrx_cls = next((p for p in plugins if p.__name__ == "LRXMetadataReader"), None)
    assert lrx_cls is not None

    payload = _build_lrx_payload(title="Plugin Title", author="Plugin Author")
    stream = io.BytesIO(payload)
    stream.seek(4)

    reader = lrx_cls(None)
    md = reader.get_metadata(stream=stream, ftype="lrx")

    assert md.title == "Plugin Title"
    assert _values(md.authors) == ["Plugin Author"]
    assert stream.tell() == 4


def test_lrx_get_metadata_extracts_fields_and_unicode() -> None:
    from LiuXin_alpha.metadata.file_sources.lrx import get_metadata

    payload = _build_lrx_payload(
        title="Gödel 漢字 🙂",
        author="Renée Faßbinder",
        publisher="Éditions Δ",
        categories=("Тег", "タグ", "tag"),
        language="ja",
        title_sort="Godel",
        author_sort="Fassbinder, Renee",
    )
    md = get_metadata(io.BytesIO(payload))

    assert md.title == "Gödel 漢字 🙂"
    assert _values(md.authors) == ["Renée Faßbinder"]
    assert getattr(md, "publisher", None) == "Éditions Δ"
    assert _values(getattr(md, "tags", None)) == ["Тег", "タグ", "tag"]
    assert getattr(md, "language", None) == "ja"
    assert getattr(md, "title_sort", None) == "Godel"
    assert getattr(md, "author_sort", None) == "Fassbinder, Renee"


def test_lrx_get_metadata_supports_version_800_layout() -> None:
    from LiuXin_alpha.metadata.file_sources.lrx import get_metadata

    payload = _build_lrx_payload(lrf_version=800, title="Version 800")
    md = get_metadata(io.BytesIO(payload))

    assert md.title == "Version 800"
    assert _values(md.authors) == ["Ada Lovelace"]


def test_lrx_get_metadata_pathlike_input(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.lrx import get_metadata

    path = tmp_path / "path_like_test.lrx"
    path.write_bytes(_build_lrx_payload(title="From Path"))

    md = get_metadata(path)
    assert md.title == "From Path"
    assert _values(md.authors) == ["Ada Lovelace"]


def test_lrx_invalid_header_returns_safe_default() -> None:
    from LiuXin_alpha.metadata.file_sources.lrx import get_metadata

    md = get_metadata(io.BytesIO(b"not-a-valid-lrx"))
    assert md.title == "Unknown"
    assert _values(md.authors) == ["Unknown"]


def test_lrx_unsupported_librie_header_returns_safe_default() -> None:
    from LiuXin_alpha.metadata.file_sources.lrx import get_metadata

    payload = b"\x00\x00\x00\x00LRX2" + b"\x00" * 8
    md = get_metadata(io.BytesIO(payload))
    assert md.title == "Unknown"
    assert _values(md.authors) == ["Unknown"]


def test_lrx_truncated_payload_falls_back_to_filename_title(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.lrx import get_metadata

    path = tmp_path / "broken_sample.lrx"
    payload = bytearray(_build_lrx_payload(title="Ignored"))
    del payload[-5:]
    path.write_bytes(bytes(payload))

    md = get_metadata(path)
    assert md.title == "broken_sample"
    assert _values(md.authors) == ["Unknown"]


def test_lrx_optional_real_fixtures_parse_without_crash(md_test_files_by_ext: dict[str, list[Path]]) -> None:
    from LiuXin_alpha.metadata.file_sources.lrx import get_metadata

    fixtures = list(md_test_files_by_ext.get("lrx", []))
    if not fixtures:
        pytest.skip("No .lrx fixtures found in optional LiuXin_alpha_data corpus")

    for path in fixtures:
        with path.open("rb") as stream:
            md = get_metadata(stream)
        assert md is not None
        assert bool(getattr(md, "title", None))

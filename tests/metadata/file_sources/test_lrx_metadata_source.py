"""
Verify LRX metadata handling and optional corpus behavior.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test lrx metadata source through its owning regression module::

        python -m pytest -q tests/metadata/file_sources/test_lrx_metadata_source.py
"""
from __future__ import annotations

import io
import struct
import zlib
from collections.abc import Mapping
from pathlib import Path

import pytest


def _values(raw):
    """
    Perform the values test-helper operation with deterministic inputs.

    Example:
        Exercise values through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_lrx_metadata_source.py


    :param raw: Value supplied for raw in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
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
    """
    Perform the build lrx payload test-helper operation with deterministic inputs.

    Example:
        Exercise build lrx payload through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_lrx_metadata_source.py


    :param title: Value supplied for title in the focused test operation.
    :param author: Value supplied for author in the focused test operation.
    :param publisher: Value supplied for publisher in the focused test operation.
    :param categories: Value supplied for categories in the focused test operation.
    :param language: Value supplied for language in the focused test operation.
    :param title_sort: Value supplied for title sort in the focused test operation.
    :param author_sort: Value supplied for author sort in the focused test operation.
    :param lrf_version: Value supplied for lrf version in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
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
    """
    Verify lrx metadata module import smoke.

    Example:
        Exercise test lrx metadata module import smoke through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_lrx_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.file_sources.lrx as lrx_md

    assert lrx_md is not None


def test_lrx_reader_plugin_is_available_and_keeps_stream_position() -> None:
    """
    Verify lrx reader plugin remains available and keeps stream position.

    Example:
        Exercise test lrx reader plugin is available and keeps stream position through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_lrx_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify lrx get metadata extracts fields and unicode.

    Example:
        Exercise test lrx get metadata extracts fields and unicode through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_lrx_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify lrx get metadata supports version 800 layout.

    Example:
        Exercise test lrx get metadata supports version 800 layout through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_lrx_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.lrx import get_metadata

    payload = _build_lrx_payload(lrf_version=800, title="Version 800")
    md = get_metadata(io.BytesIO(payload))

    assert md.title == "Version 800"
    assert _values(md.authors) == ["Ada Lovelace"]


def test_lrx_get_metadata_pathlike_input(tmp_path: Path) -> None:
    """
    Verify lrx get metadata pathlike input.

    Example:
        Exercise test lrx get metadata pathlike input through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_lrx_metadata_source.py


    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.lrx import get_metadata

    path = tmp_path / "path_like_test.lrx"
    path.write_bytes(_build_lrx_payload(title="From Path"))

    md = get_metadata(path)
    assert md.title == "From Path"
    assert _values(md.authors) == ["Ada Lovelace"]


def test_lrx_invalid_header_raises_by_default_and_can_opt_into_fallback() -> None:
    """
    Verify lrx invalid header raises by default and can opt into fallback.

    Example:
        Exercise test lrx invalid header raises by default and can opt into fallback through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_lrx_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.lrx import LrxFormatError, get_metadata

    with pytest.raises(LrxFormatError):
        get_metadata(io.BytesIO(b"not-a-valid-lrx"))

    md = get_metadata(io.BytesIO(b"not-a-valid-lrx"), fallback_on_parse_error=True)
    assert md.title == "Unknown"
    assert _values(md.authors) == ["Unknown"]


def test_lrx_unsupported_librie_header_returns_safe_default() -> None:
    """
    Verify lrx unsupported librie header returns safe default.

    Example:
        Exercise test lrx unsupported librie header returns safe default through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_lrx_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.lrx import get_metadata

    payload = b"\x00\x00\x00\x00LRX2" + b"\x00" * 8
    md = get_metadata(io.BytesIO(payload))
    assert md.title == "Unknown"
    assert _values(md.authors) == ["Unknown"]


def test_lrx_truncated_payload_raises_by_default_and_can_opt_into_fallback(tmp_path: Path) -> None:
    """
    Verify lrx truncated payload raises by default and can opt into fallback.

    Example:
        Exercise test lrx truncated payload raises by default and can opt into fallback through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_lrx_metadata_source.py


    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.lrx import LrxFormatError, get_metadata

    path = tmp_path / "broken_sample.lrx"
    payload = bytearray(_build_lrx_payload(title="Ignored"))
    del payload[-5:]
    path.write_bytes(bytes(payload))

    with pytest.raises(LrxFormatError):
        get_metadata(path)

    md = get_metadata(path, fallback_on_parse_error=True)
    assert md.title == "broken_sample"
    assert _values(md.authors) == ["Unknown"]


def test_lrx_optional_real_fixtures_parse_without_crash(md_test_files_by_ext: dict[str, list[Path]]) -> None:
    """
    Verify lrx optional real fixtures parse without crash.

    Example:
        Exercise test lrx optional real fixtures parse without crash through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_lrx_metadata_source.py


    :param md_test_files_by_ext: Value supplied for md test files by ext in the focused
        test operation.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.lrx import get_metadata

    fixtures = list(md_test_files_by_ext.get("lrx", []))
    if not fixtures:
        pytest.skip("No .lrx fixtures found in optional LiuXin_alpha_data corpus")

    for path in fixtures:
        with path.open("rb") as stream:
            md = get_metadata(stream)
        assert md is not None
        assert bool(getattr(md, "title", None))

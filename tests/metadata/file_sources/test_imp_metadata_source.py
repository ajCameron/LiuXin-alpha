"""
Verify IMP binary metadata parsing and malformed-input fallbacks.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test imp metadata source through its owning regression module::

        python -m pytest -q tests/metadata/file_sources/test_imp_metadata_source.py
"""
from __future__ import annotations

import io
from collections.abc import Mapping
from pathlib import Path

import pytest


def _values(raw):
    """
    Perform the values test-helper operation with deterministic inputs.

    Example:
        Exercise values through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_imp_metadata_source.py


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


def _first(raw):
    """
    Perform the first test-helper operation with deterministic inputs.

    Example:
        Exercise first through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_imp_metadata_source.py


    :param raw: Value supplied for raw in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    vals = _values(raw)
    return vals[0] if vals else None


def _build_imp_payload(
    *,
    category: str = "Fiction",
    title: str = "A Sample Title",
    author: str = "Alice and Bob",
    magic: bytes = b"\x00\x01BOOKDOUG",
    encoding: str = "utf-8",
) -> bytes:
    """
    Perform the build imp payload test-helper operation with deterministic inputs.

    Example:
        Exercise build imp payload through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_imp_metadata_source.py


    :param category: Value supplied for category in the focused test operation.
    :param title: Value supplied for title in the focused test operation.
    :param author: Value supplied for author in the focused test operation.
    :param magic: Value supplied for magic in the focused test operation.
    :param encoding: Value supplied for encoding in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    return b"".join(
        [
            magic,
            b"\x00" * 38,
            "ignored".encode(encoding) + b"\x00",
            category.encode(encoding) + b"\x00",
            b"skip-title\x00" + title.encode(encoding) + b"\x00",
            b"skip-author-1\x00skip-author-2\x00" + author.encode(encoding) + b"\x00",
        ]
    )


def test_imp_metadata_module_import_smoke() -> None:
    """
    Verify imp metadata module import smoke.

    Example:
        Exercise test imp metadata module import smoke through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_imp_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.file_sources.imp as imp_md

    assert imp_md is not None


def test_imp_reader_plugin_is_available_and_uses_stream_without_cursor_drift() -> None:
    """
    Verify imp reader plugin remains available and uses stream without cursor drift.

    Example:
        Exercise test imp reader plugin is available and uses stream without cursor drift through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_imp_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.customize.builtins.metadata_readers import get_metadata_reader_plugins

    payload = _build_imp_payload(title="Plugin Title", author="Plugin Author")
    stream = io.BytesIO(payload)

    plugins = get_metadata_reader_plugins()
    imp_cls = next((p for p in plugins if p.__name__ == "IMPMetadataReader"), None)
    assert imp_cls is not None

    reader = imp_cls(None)
    metadata = reader.get_metadata(stream=stream, ftype="imp")
    assert stream.tell() == 0
    assert metadata.title == "Plugin Title"
    assert _values(metadata.authors) == ["Plugin Author"]


def test_imp_get_metadata_reads_title_authors_and_category() -> None:
    """
    Verify imp get metadata reads title authors and category.

    Example:
        Exercise test imp get metadata reads title authors and category through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_imp_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.imp import get_metadata

    payload = _build_imp_payload(
        category="Science Fiction",
        title="Across The Stars",
        author="Renée Faßbinder and 李白",
    )
    md = get_metadata(io.BytesIO(payload))

    assert md.title == "Across The Stars"
    assert _values(md.authors) == ["Renée Faßbinder", "李白"]
    assert getattr(md, "category", None) == "Science Fiction"


def test_imp_get_metadata_pathlike_input(tmp_path: Path) -> None:
    """
    Verify imp get metadata pathlike input.

    Example:
        Exercise test imp get metadata pathlike input through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_imp_metadata_source.py


    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.imp import get_metadata

    path = tmp_path / "sample.imp"
    path.write_bytes(_build_imp_payload(title="Path Title", author="Path Author"))

    md = get_metadata(path)
    assert md.title == "Path Title"
    assert _first(md.authors) == "Path Author"


def test_imp_invalid_magic_raises_by_default_and_can_opt_into_fallback() -> None:
    """
    Verify imp invalid magic raises by default and can opt into fallback.

    Example:
        Exercise test imp invalid magic raises by default and can opt into fallback through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_imp_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.imp import ImpFormatError, get_metadata

    with pytest.raises(ImpFormatError):
        get_metadata(io.BytesIO(b"not-an-imp-file"))

    md = get_metadata(io.BytesIO(b"not-an-imp-file"), fallback_on_parse_error=True)
    assert md.title == "Unknown"
    assert _first(md.authors) == "Unknown"


def test_imp_truncated_payload_fails_gracefully() -> None:
    """
    Verify imp truncated payload fails gracefully.

    Example:
        Exercise test imp truncated payload fails gracefully through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_imp_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.imp import get_metadata

    payload = (
        b"\x00\x01BOOKDOUG"
        + b"\x00" * 38
        + b"ignored\x00"
        + b"category-without-null-termination-and-no-more-bytes"
    )
    md = get_metadata(io.BytesIO(payload))

    assert md is not None
    assert _first(md.title) in {"Unknown", "category-without-null-termination-and-no-more-bytes"}


def test_imp_cp1252_payload_decodes_accented_text() -> None:
    """
    Verify imp cp1252 payload decodes accented text.

    Example:
        Exercise test imp cp1252 payload decodes accented text through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_imp_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.imp import get_metadata

    payload = _build_imp_payload(
        category="Roman",
        title="Café naïve",
        author="José and Anaïs",
        encoding="cp1252",
    )
    md = get_metadata(io.BytesIO(payload))

    assert md.title == "Café naïve"
    assert _values(md.authors) == ["José", "Anaïs"]

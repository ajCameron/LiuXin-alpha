"""
Verify LRF metadata and cover parsing with bounded binary reads.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test lrf metadata source through its owning regression module::

        python -m pytest -q tests/metadata/file_sources/test_lrf_metadata_source.py
"""
from __future__ import annotations

import io
import importlib
from pathlib import Path

import pytest


def test_lrf_metadata_module_import_smoke() -> None:
    """
    Verify lrf metadata module import smoke.

    Example:
        Exercise test lrf metadata module import smoke through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_lrf_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    importlib.import_module("LiuXin_alpha.metadata.file_sources.lrf")


def test_lrf_metadata_rejects_malformed_stream_and_preserves_cursor() -> None:
    """
    Verify lrf metadata rejects malformed stream and preserves cursor.

    Example:
        Exercise test lrf metadata rejects malformed stream and preserves cursor through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_lrf_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.lrf import LrfFormatError, get_metadata

    stream = io.BytesIO(b"not an lrf")
    stream.name = "bad.lrf"
    stream.seek(3)

    with pytest.raises(LrfFormatError, match="Failed to read metadata from LRF file") as exc_info:
        get_metadata(stream)

    assert exc_info.value.__cause__ is not None
    assert stream.tell() == 3


def test_lrf_metadata_fallback_is_explicit_opt_in() -> None:
    """
    Verify lrf metadata fallback remains explicit opt in.

    Example:
        Exercise test lrf metadata fallback is explicit opt in through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_lrf_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.lrf import get_metadata

    stream = io.BytesIO(b"not an lrf")
    stream.name = "Fallback LRF.lrf"
    stream.seek(4)

    mi = get_metadata(stream, fallback_on_parse_error=True)

    assert mi.title == "Fallback LRF"
    assert mi.authors == ["Unknown"]
    assert stream.tell() == 4


def test_lrf_metadata_fallback_accepts_pathlike(tmp_path: Path) -> None:
    """
    Verify lrf metadata fallback accepts pathlike.

    Example:
        Exercise test lrf metadata fallback accepts pathlike through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_lrf_metadata_source.py


    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.lrf import get_metadata_inplace

    path = tmp_path / "Pathlike Fallback.lrf"
    path.write_bytes(b"not an lrf")

    mi = get_metadata_inplace(path, fallback_on_parse_error=True)

    assert mi.title == "Pathlike Fallback"
    assert mi.authors == ["Unknown"]


def test_lrf_metadata_reader_plugin_is_available() -> None:
    """
    Verify lrf metadata reader plugin remains available.

    Example:
        Exercise test lrf metadata reader plugin is available through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_lrf_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.customize.builtins.metadata_readers import get_metadata_reader_plugins

    plugins = get_metadata_reader_plugins()
    lrf_cls = next((p for p in plugins if p.__name__ == "LRFMetadataReader"), None)

    assert lrf_cls is not None

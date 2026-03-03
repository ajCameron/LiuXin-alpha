from __future__ import annotations

import shutil
from collections.abc import Mapping
from pathlib import Path

import pytest

from LiuXin_alpha.metadata.utils import calibreMetaInformation

pytest.importorskip("lxml")


def _field_values(raw):
    if raw is None:
        return []
    if isinstance(raw, Mapping):
        return list(raw.keys())
    if isinstance(raw, str):
        return [raw]
    return list(raw)


def test_docx_metadata_module_import_smoke() -> None:
    import LiuXin_alpha.metadata.file_sources.docx as docx_md

    assert docx_md is not None


def test_docx_metadata_reads_known_fixture(md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources.docx import get_metadata

    fixture = md_test_fixture(file_ext="docx", file_num=1, verify_hash=True)
    metadata = get_metadata(fixture)

    assert metadata.title == "DOCX Demo"
    assert _field_values(metadata.authors) == ["Kovid Goyal"]
    assert "Demonstration of DOCX support in calibre" in _field_values(metadata.comments)


def test_docx_metadata_reads_stream_and_rewinds(md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources.docx import get_metadata

    fixture = md_test_fixture(file_ext="docx", file_num=1, verify_hash=True)
    with fixture.open("rb") as stream:
        metadata = get_metadata(stream)
        assert stream.tell() == 0

    assert metadata.title == "DOCX Demo"


def test_docx_reader_plugin_is_available(md_test_fixture) -> None:
    from LiuXin_alpha.customize.builtins.metadata_readers import get_metadata_reader_plugins

    fixture = md_test_fixture(file_ext="docx", file_num=1, verify_hash=True)

    plugins = get_metadata_reader_plugins()
    docx_cls = next((p for p in plugins if p.__name__ == "DocXMetadataReader"), None)
    assert docx_cls is not None

    reader = docx_cls(None)
    with fixture.open("rb") as stream:
        metadata = reader.get_metadata(stream=stream, ftype="docx")
    inplace_metadata = reader.get_metadata_inplace(file_path=str(fixture), ftype="docx")

    assert metadata.title == "DOCX Demo"
    assert inplace_metadata.title == "DOCX Demo"


def test_docx_set_metadata_roundtrip_path(tmp_path: Path, md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources.docx import get_metadata, set_metadata

    source = md_test_fixture(file_ext="docx", file_num=1, verify_hash=True)
    target = tmp_path / "roundtrip.docx"
    shutil.copy2(source, target)

    updated = calibreMetaInformation("Unicode title — 測試", ["Alice Example", "Bob Writer"])
    updated.tags = ["fantasy", "unicode test"]
    updated.comments = "Docx metadata write smoke"
    updated.publisher = "Alpha Press"

    set_metadata(target, updated)
    metadata = get_metadata(target)

    assert metadata.title == "Unicode title — 測試"
    assert _field_values(metadata.authors) == ["Alice Example", "Bob Writer"]
    assert _field_values(metadata.tags) == ["fantasy", "unicode test"]
    assert _field_values(metadata.comments) == ["Docx metadata write smoke"]
    assert _field_values(metadata.publisher) == ["Alpha Press"]


def test_docx_set_metadata_roundtrip_stream(tmp_path: Path, md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources.docx import get_metadata, set_metadata

    source = md_test_fixture(file_ext="docx", file_num=1, verify_hash=True)
    target = tmp_path / "roundtrip_stream.docx"
    shutil.copy2(source, target)

    updated = calibreMetaInformation("Stream title", ["Stream Author"])
    updated.publisher = "Stream Publisher"

    with target.open("r+b") as stream:
        set_metadata(stream, updated)
        assert stream.tell() == 0

    metadata = get_metadata(target)
    assert metadata.title == "Stream title"
    assert _field_values(metadata.authors) == ["Stream Author"]
    assert _field_values(metadata.publisher) == ["Stream Publisher"]

from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from tests.support.file_format_docx import NullLog, build_unicode_docx, rewrite_docx_zip


def _assert_docx_convert_rejects_without_partial_output(
    archive: Path,
    out_dir: Path,
    match: str,
    monkeypatch=None,
    docx_cls=None,
) -> None:
    from LiuXin_alpha.file_formats.docx import InvalidDOCX
    import LiuXin_alpha.file_formats.docx.to_html as to_html_mod

    if docx_cls is not None:
        assert monkeypatch is not None
        monkeypatch.setattr(to_html_mod, "DOCX", docx_cls)

    out_dir.mkdir()
    with pytest.raises(InvalidDOCX, match=match):
        to_html_mod.Convert(str(archive), dest_dir=str(out_dir), log=NullLog())()

    assert not (out_dir / "index.html").exists()
    assert not (out_dir / "metadata.opf").exists()
    assert not (out_dir / "docx.css").exists()
    assert not (out_dir / "images").exists()


@pytest.mark.parametrize(
    ("case_id", "remove", "replace", "match"),
    (
        ("missing_content_types", ("[Content_Types].xml",), {}, r"no \[Content_Types\]\.xml"),
        ("empty_content_types", (), {"[Content_Types].xml": b""}, r"malformed \[Content_Types\]\.xml"),
        ("wrong_content_types_root", (), {"[Content_Types].xml": b"<not-types/>"}, r"malformed \[Content_Types\]\.xml"),
        ("missing_package_relationships", ("_rels/.rels",), {}, r"no _rels/\.rels"),
        ("empty_package_relationships", (), {"_rels/.rels": b""}, r"malformed _rels/\.rels"),
        ("wrong_package_relationships_root", (), {"_rels/.rels": b"<not-rels/>"}, r"malformed _rels/\.rels"),
        ("missing_main_document", ("word/document.xml",), {}, "no main document"),
        ("empty_main_document", (), {"word/document.xml": b""}, r"malformed word/document\.xml"),
        ("wrong_main_document_root", (), {"word/document.xml": b"<not-document/>"}, r"malformed word/document\.xml"),
        ("empty_core_properties", (), {"docProps/core.xml": b""}, r"malformed docProps/core\.xml"),
    ),
)
def test_docx_convert_rejects_malformed_container_members_without_partial_output(
    tmp_path: Path,
    case_id: str,
    remove: tuple[str, ...],
    replace: dict[str, bytes],
    match: str,
) -> None:
    base = build_unicode_docx(tmp_path / "base.docx")
    hostile = tmp_path / f"{case_id}.docx"
    rewrite_docx_zip(base.path, hostile, remove=remove, replace=replace)

    _assert_docx_convert_rejects_without_partial_output(
        hostile,
        tmp_path / f"{case_id}_out",
        match,
    )


@pytest.mark.parametrize(
    ("case_id", "member_name"),
    (
        ("parent_escape", "../escape.txt"),
        ("nested_parent_escape", "word/../../escape.txt"),
        ("internal_parent_component", "word/media/../escape.bin"),
        ("absolute_path", "/absolute.txt"),
        ("drive_path", "C:/absolute.txt"),
    ),
)
def test_docx_convert_rejects_unsafe_archive_member_paths_before_extraction(
    tmp_path: Path,
    case_id: str,
    member_name: str,
) -> None:
    base = build_unicode_docx(tmp_path / "base.docx")
    hostile = tmp_path / f"{case_id}.docx"
    rewrite_docx_zip(base.path, hostile, add={member_name: b"unsafe"})

    _assert_docx_convert_rejects_without_partial_output(
        hostile,
        tmp_path / f"{case_id}_out",
        "unsafe path",
    )


def test_docx_convert_rejects_too_many_archive_members_without_partial_output(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from LiuXin_alpha.file_formats.docx.container import DOCX

    class StrictDOCX(DOCX):
        max_archive_members = 8

    base = build_unicode_docx(tmp_path / "small.docx", lines=("small",))
    hostile = tmp_path / "too_many_members.docx"
    rewrite_docx_zip(
        base.path,
        hostile,
        add={f"word/media/many/{i}.bin": b"x" for i in range(12)},
    )

    _assert_docx_convert_rejects_without_partial_output(
        hostile,
        tmp_path / "too_many_out",
        "too many archive members",
        monkeypatch=monkeypatch,
        docx_cls=StrictDOCX,
    )


def test_docx_convert_rejects_oversized_archive_member_without_partial_output(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from LiuXin_alpha.file_formats.docx.container import DOCX

    class StrictDOCX(DOCX):
        max_member_uncompressed_size = 10 * 1024

    base = build_unicode_docx(tmp_path / "small.docx", lines=("small",))
    hostile = tmp_path / "oversized_member.docx"
    rewrite_docx_zip(base.path, hostile, add={"word/media/big.bin": b"x" * (20 * 1024)})

    _assert_docx_convert_rejects_without_partial_output(
        hostile,
        tmp_path / "oversized_out",
        "member is too large",
        monkeypatch=monkeypatch,
        docx_cls=StrictDOCX,
    )


def test_docx_convert_rejects_excessive_total_expansion_without_partial_output(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from LiuXin_alpha.file_formats.docx.container import DOCX

    class StrictDOCX(DOCX):
        max_member_uncompressed_size = 100 * 1024
        max_total_uncompressed_size = 30 * 1024

    base = build_unicode_docx(tmp_path / "small.docx", lines=("small",))
    hostile = tmp_path / "large_total.docx"
    rewrite_docx_zip(
        base.path,
        hostile,
        add={f"word/media/chunk-{i}.bin": b"x" * (8 * 1024) for i in range(6)},
    )

    _assert_docx_convert_rejects_without_partial_output(
        hostile,
        tmp_path / "large_total_out",
        "expands to too much data",
        monkeypatch=monkeypatch,
        docx_cls=StrictDOCX,
    )


def test_docx_convert_rejects_suspicious_compression_ratio_without_partial_output(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from LiuXin_alpha.file_formats.docx.container import DOCX

    class StrictDOCX(DOCX):
        max_compression_ratio = 20
        min_compression_ratio_check_size = 32 * 1024

    base = build_unicode_docx(tmp_path / "small.docx", lines=("small",))
    hostile = tmp_path / "ratio_bomb_shape.docx"
    rewrite_docx_zip(
        base.path,
        hostile,
        add={"word/media/repeated.bin": b"0" * (128 * 1024)},
        add_compression=zipfile.ZIP_DEFLATED,
    )

    _assert_docx_convert_rejects_without_partial_output(
        hostile,
        tmp_path / "ratio_out",
        "suspicious compression ratio",
        monkeypatch=monkeypatch,
        docx_cls=StrictDOCX,
    )

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from xml.etree import ElementTree as ET

from tests.support.file_format_odt import (
    ODT_DESCRIPTION,
    ODT_IMAGE_BYTES,
    ODT_TITLE,
    NullLog,
    build_unicode_odt,
    rewrite_odt_zip,
)
from tests.support.file_format_unicode import assert_fragments_present, assert_no_replacement_chars


def _opf_text(path: Path) -> str:
    return path.read_text("utf-8", "replace")


def test_odt_extract_preserves_unicode_body_metadata_and_embedded_assets(tmp_path: Path) -> None:
    from LiuXin_alpha.file_formats.odt.input import Extract

    fixture = build_unicode_odt(tmp_path / "container_Καλημέρα_世界.odt", include_image=True)
    out_dir = tmp_path / "extract_out"

    with fixture.path.open("rb") as stream:
        opf_path = Path(Extract()(stream, str(out_dir), NullLog()))

    assert opf_path == out_dir / "metadata.opf"
    assert (out_dir / "index.xhtml").exists()
    assert (out_dir / "odfpy.css").exists()
    ET.parse(opf_path)

    html = (out_dir / "index.xhtml").read_text("utf-8", "replace")
    assert_fragments_present(html, fixture.text_fragments, context="ODT index.xhtml")
    assert_no_replacement_chars(html, context="ODT index.xhtml")

    opf = _opf_text(opf_path)
    assert ODT_TITLE in opf
    assert "José" in opf
    assert ODT_DESCRIPTION in opf

    copied_pictures = [out_dir / member for member in fixture.picture_members]
    assert copied_pictures
    for picture in copied_pictures:
        assert picture.exists()
        assert picture.read_bytes().startswith(b"\x89PNG")


def test_odt_input_plugin_uses_container_workdir_and_preserves_unicode(tmp_path: Path, monkeypatch) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.odt_input import ODTInput

    fixture = build_unicode_odt(tmp_path / "plugin_container.odt", include_image=True)
    workdir = tmp_path / "plugin_work"
    workdir.mkdir()
    monkeypatch.chdir(workdir)

    with fixture.path.open("rb") as stream:
        opf_path = Path(ODTInput(None).convert(stream, SimpleNamespace(), "odt", NullLog(), {}))

    assert opf_path.is_absolute()
    assert opf_path.name == "metadata.opf"
    assert opf_path.parent == workdir
    html = (opf_path.parent / "index.xhtml").read_text("utf-8", "replace")
    assert_fragments_present(html, fixture.text_fragments, context="ODTInput index.xhtml")
    assert_no_replacement_chars(html, context="ODTInput index.xhtml")


def test_odt_extract_copies_nested_picture_members_without_path_escape(tmp_path: Path) -> None:
    from LiuXin_alpha.file_formats.odt.input import Extract

    base = build_unicode_odt(tmp_path / "base.odt")
    hostile = tmp_path / "hostile_pictures.odt"
    rewrite_odt_zip(
        base.path,
        hostile,
        add={
            "Pictures/深/图像.png": ODT_IMAGE_BYTES,
            "Pictures/../../escape.txt": b"outside",
            "Pictures/../inside_escape.txt": b"inside",
            "/Pictures/absolute.png": b"absolute",
        },
    )
    out_dir = tmp_path / "extract_out"

    with hostile.open("rb") as stream:
        Path(Extract()(stream, str(out_dir), NullLog()))

    assert (out_dir / "Pictures" / "深" / "图像.png").read_bytes() == ODT_IMAGE_BYTES
    assert not (tmp_path / "escape.txt").exists()
    assert not (out_dir / "inside_escape.txt").exists()
    assert not (out_dir / "Pictures" / "absolute.png").exists()

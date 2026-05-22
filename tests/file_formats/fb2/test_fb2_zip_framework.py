from __future__ import annotations

import io
import zipfile
from pathlib import Path
from types import SimpleNamespace
from xml.etree import ElementTree as ET

from tests.support.file_format_fb2 import (
    FB2_COVER_ID,
    FB2_DESCRIPTION,
    FB2_NS,
    FB2_PUBLISHER,
    FB2_TITLE,
    FB2_ZIP_MEMBER,
    NullLog,
    build_zipped_fb2,
    fb2_body_text,
    fb2_bytes,
    fb2_zip_bytes,
    parse_zipped_fb2,
    png_bytes,
    read_fb2_binary,
    read_zipped_fb2_member,
    rewrite_zipped_fb2,
    zipped_fb2_members,
)
from tests.support.file_format_unicode import assert_fragments_present, assert_no_replacement_chars


def _write_member_to_path(tmp_path: Path, member_payload: bytes, suffix: str = ".fb2") -> Path:
    path = tmp_path / f"payload{suffix}"
    path.write_bytes(member_payload)
    return path


def test_fb2_input_plugin_declares_fbz_support() -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.fb2_input import FB2Input

    assert {"fb2", "fbz"}.issubset(FB2Input.file_types)


def test_zipped_fb2_fixture_builds_valid_container_shape_and_unicode_payload(tmp_path: Path) -> None:
    fixture = build_zipped_fb2(tmp_path / "fixture_Καλημέρα_世界.fbz")

    with zipfile.ZipFile(fixture.path, "r") as zf:
        infos = zf.infolist()
        members = {info.filename for info in infos}
        assert all(info.compress_type == zipfile.ZIP_DEFLATED for info in infos)

    assert fixture.fb2_member == FB2_ZIP_MEMBER
    assert fixture.fb2_member in members
    assert zipped_fb2_members(fixture.path) == (fixture.fb2_member,)

    root = parse_zipped_fb2(fixture.path, fixture.fb2_member)
    assert root.tag == f"{{{FB2_NS}}}FictionBook"
    text = "\n".join(root.itertext())
    assert FB2_TITLE in text
    assert_fragments_present(text, fixture.text_fragments, context="zipped FB2 XML fixture")
    assert_no_replacement_chars(text, context="zipped FB2 XML fixture")

    extracted_member = _write_member_to_path(
        tmp_path,
        read_zipped_fb2_member(fixture.path, fixture.fb2_member),
    )
    assert fb2_body_text(extracted_member)
    assert read_fb2_binary(extracted_member, FB2_COVER_ID).startswith(b"\x89PNG")


def test_zipped_fb2_fixture_supports_utf16_payload_extra_binaries_and_extra_members(
    tmp_path: Path,
) -> None:
    extra_binary = b"plain embedded note"
    extra_members = {
        "notes/readme_שלום.txt": "שלום zip note".encode("utf-8"),
        "metadata/credits_世界.json": b'{"source": "fixture"}',
    }
    fixture = build_zipped_fb2(
        tmp_path / "utf16_extra.fbz",
        member_name="nested/世界/book_utf16.fb2",
        encoding="utf-16",
        extra_binaries={"notes_مرحبا": ("text/plain", extra_binary)},
        extra_members=extra_members,
    )

    assert set(fixture.extra_members) == set(extra_members)
    assert "notes_مرحبا" in fixture.binary_ids
    assert set(zipped_fb2_members(fixture.path)) == {fixture.fb2_member, *extra_members}

    payload = read_zipped_fb2_member(fixture.path, fixture.fb2_member)
    assert payload.startswith((b"\xff\xfe", b"\xfe\xff"))
    root = parse_zipped_fb2(fixture.path, fixture.fb2_member)
    text = "\n".join(root.itertext())
    assert_fragments_present(text, fixture.text_fragments, context="UTF-16 zipped FB2 XML")
    assert_no_replacement_chars(text, context="UTF-16 zipped FB2 XML")

    extracted_member = _write_member_to_path(tmp_path, payload)
    assert read_fb2_binary(extracted_member, "notes_مرحبا") == extra_binary
    for member_name, expected in extra_members.items():
        assert read_zipped_fb2_member(fixture.path, member_name) == expected


def test_fb2_zip_bytes_can_build_stream_payload_for_router_tests() -> None:
    payload = fb2_zip_bytes(member_name="book.fb2", extra_members={"z-last.txt": b"extra"})

    with zipfile.ZipFile(io.BytesIO(payload), "r") as zf:
        assert zf.read("book.fb2").startswith(b"<?xml")
        assert zf.read("z-last.txt") == b"extra"


def test_zipped_fb2_rewrite_helper_removes_replaces_and_adds_members(tmp_path: Path) -> None:
    fixture = build_zipped_fb2(
        tmp_path / "base.fbz",
        extra_members={"notes/original.txt": b"remove me"},
    )
    rewritten = tmp_path / "rewritten.fbz"
    replacement = fb2_bytes(title="Replacement Καλημέρα 世界")

    rewrite_zipped_fb2(
        fixture.path,
        rewritten,
        remove=("notes/original.txt",),
        replace={fixture.fb2_member: replacement},
        add={"notes/extra_世界.txt": b"extra"},
        add_compression=zipfile.ZIP_DEFLATED,
    )

    with zipfile.ZipFile(rewritten, "r") as zf:
        members = {info.filename for info in zf.infolist()}
        extra_info = zf.getinfo("notes/extra_世界.txt")
        assert "notes/original.txt" not in members
        assert "notes/extra_世界.txt" in members
        assert extra_info.compress_type == zipfile.ZIP_DEFLATED
        assert zf.read(fixture.fb2_member) == replacement

    text = "\n".join(parse_zipped_fb2(rewritten, fixture.fb2_member).itertext())
    assert "Replacement Καλημέρα 世界" in text


def test_fb2_input_convert_accepts_fbz_and_preserves_unicode_outputs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.fb2_input import FB2Input

    cover_id = "zip_cover_世界.png"
    extra_id = "zip_illustration_cafe\u0301.png"
    fixture = build_zipped_fb2(
        tmp_path / "convert_Καλημέρα_世界.fbz",
        member_name="nested/世界/book.fb2",
        cover_id=cover_id,
        extra_binaries={extra_id: ("image/png", png_bytes(width=12, height=10))},
        extra_members={"notes/ignored_שלום.txt": "not converted".encode("utf-8")},
    )
    workdir = tmp_path / "fbz-convert-work"
    workdir.mkdir()
    monkeypatch.chdir(workdir)

    with fixture.path.open("rb") as stream:
        opf_path = Path(
            FB2Input(None).convert(
                stream,
                SimpleNamespace(no_inline_fb2_toc=False),
                "fbz",
                NullLog(),
                {},
            )
        )

    assert opf_path == workdir / "metadata.opf"
    assert (workdir / "index.xhtml").exists()
    assert (workdir / "inline-styles.css").exists()
    assert not (workdir / "notes").exists()
    ET.parse(opf_path)

    html = (workdir / "index.xhtml").read_text("utf-8", "replace")
    assert FB2_TITLE in html
    assert f'<img src="{cover_id}"' in html
    assert_fragments_present(html, fixture.text_fragments, context="FBZInput index.xhtml")
    assert_no_replacement_chars(html, context="FBZInput index.xhtml")

    css = (workdir / "inline-styles.css").read_text("utf-8", "replace")
    assert "font-family" in css
    assert_no_replacement_chars(css, context="FBZInput inline-styles.css")

    opf = opf_path.read_text("utf-8", "replace")
    assert FB2_TITLE in opf
    assert "José" in opf
    assert "Niño" in opf
    assert "Иван" in opf
    assert "Петров" in opf
    assert FB2_DESCRIPTION in opf
    assert FB2_PUBLISHER in opf
    assert "index.xhtml" in opf
    assert "inline-styles.css" in opf
    assert cover_id in opf
    assert extra_id in opf
    assert "ignored_שלום" not in opf
    assert_no_replacement_chars(opf, context="FBZInput metadata.opf")

    assert (workdir / cover_id).read_bytes().startswith(b"\x89PNG")
    assert (workdir / extra_id).read_bytes().startswith(b"\x89PNG")


def test_fb2_input_convert_accepts_utf16_fbz_without_replacement_chars(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.fb2_input import FB2Input

    fixture = build_zipped_fb2(
        tmp_path / "convert_utf16.fbz",
        member_name="nested/世界/book_utf16.fb2",
        encoding="utf-16",
        cover_id="zip_cover_utf16.png",
    )
    workdir = tmp_path / "fbz-utf16-work"
    workdir.mkdir()
    monkeypatch.chdir(workdir)

    with fixture.path.open("rb") as stream:
        opf_path = Path(
            FB2Input(None).convert(
                stream,
                SimpleNamespace(no_inline_fb2_toc=False),
                "fbz",
                NullLog(),
                {},
            )
        )

    html = (opf_path.parent / "index.xhtml").read_text("utf-8", "replace")
    assert_fragments_present(html, fixture.text_fragments, context="UTF-16 FBZInput index.xhtml")
    assert_no_replacement_chars(html, context="UTF-16 FBZInput index.xhtml")

    opf = opf_path.read_text("utf-8", "replace")
    assert FB2_TITLE in opf
    assert_no_replacement_chars(opf, context="UTF-16 FBZInput metadata.opf")

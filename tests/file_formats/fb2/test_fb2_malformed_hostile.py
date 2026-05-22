from __future__ import annotations

import base64
import io
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.support.file_format_fb2 import (
    FB2_COVER_ID,
    FB2_NS,
    FB2_TITLE,
    NullLog,
    build_unicode_fb2,
    build_zipped_fb2,
    fb2_bytes,
    fb2_zip_bytes,
    png_bytes,
    rewrite_fb2_text,
    rewrite_zipped_fb2,
)
from tests.support.file_format_unicode import assert_fragments_present, assert_no_replacement_chars
from tests.support.file_format_zip import write_zip_archive


def _convert_payload(payload: bytes, workdir: Path, monkeypatch, log: NullLog | None = None) -> Path:
    from LiuXin_alpha.file_formats.conversion.plugins.fb2_input import FB2Input

    workdir.mkdir()
    monkeypatch.chdir(workdir)
    plugin = FB2Input(None)
    with io.BytesIO(payload) as stream:
        return Path(
            plugin.convert(
                stream,
                SimpleNamespace(no_inline_fb2_toc=False),
                "fb2",
                log or NullLog(),
                {},
            )
        )


def _convert_path(path: Path, workdir: Path, monkeypatch, log: NullLog | None = None) -> Path:
    from LiuXin_alpha.file_formats.conversion.plugins.fb2_input import FB2Input

    workdir.mkdir()
    monkeypatch.chdir(workdir)
    with path.open("rb") as stream:
        return Path(
            FB2Input(None).convert(
                stream,
                SimpleNamespace(no_inline_fb2_toc=False),
                "fb2",
                log or NullLog(),
                {},
            )
        )


def _assert_fb2_archive_rejects_without_partial_output(
    archive: Path,
    workdir: Path,
    monkeypatch,
    match: str,
    input_cls=None,
) -> NullLog:
    from LiuXin_alpha.file_formats.conversion.plugins.fb2_input import FB2Input

    workdir.mkdir()
    monkeypatch.chdir(workdir)
    plugin_cls = input_cls or FB2Input
    log = NullLog()

    with archive.open("rb") as stream:
        with pytest.raises(ValueError, match=match):
            plugin_cls(None).convert(
                stream,
                SimpleNamespace(no_inline_fb2_toc=False),
                "fbz",
                log,
                {},
            )

    assert list(workdir.iterdir()) == []
    preflight_messages = [message for message in log.messages if "FB2 preflight rejected" in message]
    assert preflight_messages
    assert match in preflight_messages[-1]
    return log


def test_fb2_input_rejects_non_xml_payload_without_partial_output(tmp_path: Path, monkeypatch) -> None:
    workdir = tmp_path / "wrong-format-work"

    with pytest.raises(ValueError, match="not valid XML"):
        _convert_payload(b"\0not an fb2 document", workdir, monkeypatch)

    assert list(workdir.iterdir()) == []


def test_fb2_input_rejects_non_zip_fbz_payload_without_partial_output(
    tmp_path: Path,
    monkeypatch,
) -> None:
    hostile = tmp_path / "not_zip.fbz"
    hostile.write_bytes("not a zipped FB2: Καλημέρα".encode("utf-8"))

    _assert_fb2_archive_rejects_without_partial_output(
        hostile,
        tmp_path / "not_zip_fbz_work",
        monkeypatch,
        "invalid ZIP",
    )


def test_fb2_input_rejects_corrupt_zip_payload_without_partial_output(
    tmp_path: Path,
    monkeypatch,
) -> None:
    hostile = tmp_path / "corrupt.fbz"
    hostile.write_bytes(b"PKnot-really-a-zip")

    _assert_fb2_archive_rejects_without_partial_output(
        hostile,
        tmp_path / "corrupt_zip_work",
        monkeypatch,
        "invalid ZIP",
    )


def test_fb2_input_rejects_zip_without_fb2_member_without_partial_output(
    tmp_path: Path,
    monkeypatch,
) -> None:
    hostile = tmp_path / "no_fb2.fbz"
    write_zip_archive(hostile, {"readme_世界.txt": b"not a book"})

    _assert_fb2_archive_rejects_without_partial_output(
        hostile,
        tmp_path / "no_fb2_work",
        monkeypatch,
        "no FB2 member",
    )


def test_fb2_input_rejects_ambiguous_multiple_fb2_members_without_partial_output(
    tmp_path: Path,
    monkeypatch,
) -> None:
    hostile = tmp_path / "multiple.fbz"
    write_zip_archive(
        hostile,
        {
            "a/book.fb2": fb2_bytes(title="First"),
            "b/book.fb2": fb2_bytes(title="Second"),
        },
    )

    _assert_fb2_archive_rejects_without_partial_output(
        hostile,
        tmp_path / "multiple_fb2_work",
        monkeypatch,
        "multiple FB2 members",
    )


@pytest.mark.parametrize(
    ("case_id", "member_name"),
    (
        ("parent_escape", "../book.fb2"),
        ("nested_parent_escape", "books/../../book.fb2"),
        ("internal_parent_component", "books/text/../book.fb2"),
        ("absolute_path", "/book.fb2"),
        ("drive_path", "C:/book.fb2"),
    ),
)
def test_fb2_input_rejects_unsafe_archive_member_paths_without_partial_output(
    tmp_path: Path,
    monkeypatch,
    case_id: str,
    member_name: str,
) -> None:
    base = build_zipped_fb2(tmp_path / "base.fbz")
    hostile = tmp_path / f"{case_id}.fbz"
    rewrite_zipped_fb2(base.path, hostile, add={member_name: fb2_bytes()})

    _assert_fb2_archive_rejects_without_partial_output(
        hostile,
        tmp_path / f"{case_id}_work",
        monkeypatch,
        "unsafe path",
    )


def test_fb2_input_rejects_too_many_archive_members_without_partial_output(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.fb2_input import FB2Input

    class StrictFB2Input(FB2Input):
        max_archive_members = 8

    base = build_zipped_fb2(tmp_path / "base.fbz")
    hostile = tmp_path / "too_many.fbz"
    rewrite_zipped_fb2(
        base.path,
        hostile,
        add={f"notes/many-{i}.txt": b"x" for i in range(12)},
    )

    _assert_fb2_archive_rejects_without_partial_output(
        hostile,
        tmp_path / "too_many_work",
        monkeypatch,
        "too many archive members",
        input_cls=StrictFB2Input,
    )


def test_fb2_input_rejects_oversized_archive_member_without_partial_output(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.fb2_input import FB2Input

    class StrictFB2Input(FB2Input):
        max_member_uncompressed_size = 10 * 1024

    base = build_zipped_fb2(tmp_path / "base.fbz")
    hostile = tmp_path / "oversized.fbz"
    rewrite_zipped_fb2(base.path, hostile, add={"notes/big.bin": b"x" * (20 * 1024)})

    _assert_fb2_archive_rejects_without_partial_output(
        hostile,
        tmp_path / "oversized_work",
        monkeypatch,
        "member is too large",
        input_cls=StrictFB2Input,
    )


def test_fb2_input_rejects_excessive_total_expansion_without_partial_output(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.fb2_input import FB2Input

    class StrictFB2Input(FB2Input):
        max_member_uncompressed_size = 100 * 1024
        max_total_uncompressed_size = 30 * 1024

    base = build_zipped_fb2(tmp_path / "base.fbz")
    hostile = tmp_path / "large_total.fbz"
    rewrite_zipped_fb2(
        base.path,
        hostile,
        add={f"notes/chunk-{i}.bin": b"x" * (8 * 1024) for i in range(6)},
    )

    _assert_fb2_archive_rejects_without_partial_output(
        hostile,
        tmp_path / "large_total_work",
        monkeypatch,
        "expands to too much data",
        input_cls=StrictFB2Input,
    )


def test_fb2_input_rejects_suspicious_compression_ratio_without_partial_output(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.fb2_input import FB2Input

    class StrictFB2Input(FB2Input):
        max_compression_ratio = 20
        min_compression_ratio_check_size = 32 * 1024

    hostile = tmp_path / "ratio.fbz"
    hostile.write_bytes(
        fb2_zip_bytes(
            extra_members={"notes/repeated.bin": b"0" * (128 * 1024)},
            compression=zipfile.ZIP_DEFLATED,
        )
    )

    _assert_fb2_archive_rejects_without_partial_output(
        hostile,
        tmp_path / "ratio_work",
        monkeypatch,
        "suspicious compression ratio",
        input_cls=StrictFB2Input,
    )


def test_fb2_input_rejects_bad_declared_encoding_without_partial_output(tmp_path: Path, monkeypatch) -> None:
    payload = fb2_bytes().replace(b'encoding="utf-8"', b'encoding="utf-16"')
    workdir = tmp_path / "bad-encoding-work"

    with pytest.raises(Exception):
        _convert_payload(payload, workdir, monkeypatch)

    assert list(workdir.iterdir()) == []


def test_fb2_input_recovers_malformed_xml_without_losing_multilingual_text(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fixture = build_unicode_fb2(tmp_path / "base.fb2")
    malformed = tmp_path / "malformed_ampersand.fb2"
    rewrite_fb2_text(
        fixture.path,
        malformed,
        replace={"Fixture tail text keeps nested sections visible.": "Broken ampersand & Καλημέρα tail"},
    )
    log = NullLog()

    opf_path = _convert_path(malformed, tmp_path / "recover-work", monkeypatch, log)

    assert opf_path.exists()
    html = (opf_path.parent / "index.xhtml").read_text("utf-8", "replace")
    assert "Broken ampersand" in html
    assert_fragments_present(html, fixture.text_fragments, context="recoverable malformed FB2")
    assert_no_replacement_chars(html, context="recoverable malformed FB2")


@pytest.mark.parametrize(
    "binary_id",
    (
        "../escape.png",
        "images/../../escape.png",
        "images/../escape.png",
        "/absolute.png",
        "C:/absolute.png",
        "nested\\escape.png",
    ),
)
def test_fb2_extract_embedded_content_sanitizes_unsafe_binary_ids(
    tmp_path: Path,
    monkeypatch,
    binary_id: str,
) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.fb2_input import FB2Input
    from LiuXin_alpha.utils.libraries.liuxin_etree import etree

    fixture = build_unicode_fb2(tmp_path / "unsafe.fb2", cover_id=binary_id)
    workdir = tmp_path / "extract-work"
    workdir.mkdir()
    monkeypatch.chdir(workdir)
    log = NullLog()
    plugin = FB2Input(None)
    plugin.log = log

    plugin.extract_embedded_content(etree.fromstring(fixture.path.read_bytes()))

    mapped = plugin.binary_map[binary_id]
    assert mapped.startswith("fb2_binary_")
    assert mapped.endswith(".png")
    assert "/" not in mapped
    assert "\\" not in mapped
    assert (workdir / mapped).read_bytes().startswith(b"\x89PNG")
    assert not (tmp_path / "escape.png").exists()
    assert any("unsafe filename" in message for message in log.messages)


def test_fb2_input_convert_sanitizes_unsafe_cover_id_in_xhtml_and_opf(
    tmp_path: Path,
    monkeypatch,
) -> None:
    unsafe_id = "../escape.png"
    fixture = build_unicode_fb2(tmp_path / "unsafe_cover.fb2", cover_id=unsafe_id)
    log = NullLog()

    opf_path = _convert_path(fixture.path, tmp_path / "unsafe-cover-work", monkeypatch, log)

    html = (opf_path.parent / "index.xhtml").read_text("utf-8", "replace")
    opf = opf_path.read_text("utf-8", "replace")
    assert "../escape" not in html
    assert "../escape" not in opf
    assert "fb2_binary_0001.png" in html
    assert "fb2_binary_0001.png" in opf
    assert (opf_path.parent / "fb2_binary_0001.png").read_bytes().startswith(b"\x89PNG")
    assert not (tmp_path / "escape.png").exists()
    assert any("unsafe filename" in message for message in log.messages)


def test_fb2_input_skips_corrupted_binary_payload_and_reports_warning(
    tmp_path: Path,
    monkeypatch,
) -> None:
    cover_payload = b"cover-payload"
    fixture = build_unicode_fb2(tmp_path / "broken_binary.fb2", cover_id=FB2_COVER_ID, cover_data=cover_payload)
    broken = tmp_path / "broken_binary_payload.fb2"
    rewrite_fb2_text(
        fixture.path,
        broken,
        replace={base64.b64encode(cover_payload).decode("ascii"): "not-valid-@@@-base64"},
    )
    log = NullLog()

    opf_path = _convert_path(broken, tmp_path / "broken-binary-work", monkeypatch, log)

    assert opf_path.exists()
    assert (opf_path.parent / "index.xhtml").exists()
    assert not (opf_path.parent / f"{FB2_COVER_ID}.png").exists()
    opf = opf_path.read_text("utf-8", "replace")
    assert f"{FB2_COVER_ID}.png" not in opf
    assert any("corrupted" in message for message in log.messages)


def test_fb2_input_keeps_odd_unicode_binary_ids_local_and_reportable(
    tmp_path: Path,
    monkeypatch,
) -> None:
    odd_id = "画像_cafe\u0301_مرحبا.png"
    fixture = build_unicode_fb2(tmp_path / "odd_unicode_binary.fb2", cover_id=odd_id)

    opf_path = _convert_path(fixture.path, tmp_path / "odd-unicode-work", monkeypatch)

    html = (opf_path.parent / "index.xhtml").read_text("utf-8", "replace")
    opf = opf_path.read_text("utf-8", "replace")
    assert odd_id in html
    assert odd_id in opf
    assert FB2_TITLE in opf
    assert (opf_path.parent / odd_id).read_bytes().startswith(b"\x89PNG")
    assert_no_replacement_chars(html, context="odd unicode FB2 binary id XHTML")
    assert_no_replacement_chars(opf, context="odd unicode FB2 binary id OPF")


def test_fb2_malformed_fixture_still_has_expected_namespace_after_rewrite(tmp_path: Path) -> None:
    fixture = build_unicode_fb2(tmp_path / "base_namespace.fb2")
    rewritten = tmp_path / "namespace_rewrite.fb2"
    rewrite_fb2_text(
        fixture.path,
        rewritten,
        replace={FB2_TITLE: "Namespace rewrite Καλημέρα"},
    )

    from tests.support.file_format_fb2 import parse_fb2

    assert parse_fb2(rewritten).tag == f"{{{FB2_NS}}}FictionBook"

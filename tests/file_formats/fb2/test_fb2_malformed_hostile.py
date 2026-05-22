from __future__ import annotations

import base64
import io
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.support.file_format_fb2 import (
    FB2_COVER_ID,
    FB2_NS,
    FB2_TITLE,
    NullLog,
    build_unicode_fb2,
    fb2_bytes,
    png_bytes,
    rewrite_fb2_text,
)
from tests.support.file_format_unicode import assert_fragments_present, assert_no_replacement_chars


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


def test_fb2_input_rejects_non_xml_payload_without_partial_output(tmp_path: Path, monkeypatch) -> None:
    workdir = tmp_path / "wrong-format-work"

    with pytest.raises(ValueError, match="not valid XML"):
        _convert_payload(b"\0not an fb2 document", workdir, monkeypatch)

    assert list(workdir.iterdir()) == []


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

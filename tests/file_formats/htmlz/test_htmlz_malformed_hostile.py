from __future__ import annotations

import re
import zipfile
from pathlib import Path
from types import SimpleNamespace
from xml.sax.saxutils import escape

import pytest

from tests.support.file_format_htmlz import (
    HTMLZ_HTML_MEMBER,
    HTMLZ_IMAGE_MEMBER,
    HTMLZ_OPF_MEMBER,
    HTMLZ_TITLE,
    NullLog,
    build_unicode_htmlz,
    install_htmlz_input_pipeline_stubs,
    rewrite_htmlz_zip,
)


def _opf_with_cover(cover_href: str) -> bytes:
    cover_href = escape(cover_href, {'"': "&quot;"})
    return f"""<?xml version="1.0" encoding="utf-8"?>
<package xmlns="http://www.idpf.org/2007/opf" version="2.0" unique-identifier="BookId">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:title>{HTMLZ_TITLE}</dc:title>
    <meta name="cover" content="cover-image"/>
  </metadata>
  <manifest>
    <item id="html" href="{HTMLZ_HTML_MEMBER}" media-type="text/html"/>
    <item id="cover-image" href="{cover_href}" media-type="image/png"/>
  </manifest>
  <spine>
    <itemref idref="html"/>
  </spine>
</package>
""".encode("utf-8")


def _convert_htmlz(archive: Path, workdir: Path, monkeypatch, log: NullLog):
    from LiuXin_alpha.file_formats.conversion.plugins.htmlz_input import HTMLZInput

    workdir.mkdir()
    monkeypatch.chdir(workdir)
    recorder = install_htmlz_input_pipeline_stubs(monkeypatch)
    options = SimpleNamespace(input_encoding=None, debug_pipeline="keep")

    with archive.open("rb") as stream:
        out = HTMLZInput(None).convert(stream, options, "htmlz", log, {})

    return out, recorder, options


def _assert_htmlz_rejects_before_html_handoff(
    archive: Path,
    workdir: Path,
    monkeypatch,
    match: str,
) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.htmlz_input import HTMLZInput

    workdir.mkdir()
    monkeypatch.chdir(workdir)
    recorder = install_htmlz_input_pipeline_stubs(monkeypatch)
    options = SimpleNamespace(input_encoding=None, debug_pipeline="keep")

    with archive.open("rb") as stream:
        with pytest.raises(Exception, match=match):
            HTMLZInput(None).convert(stream, options, "htmlz", NullLog(), {})

    assert recorder.html_input.calls == []
    assert recorder.metadata_calls == []
    assert recorder.metadata_transform_calls == []
    assert recorder.oeb.manifest.added == []
    assert options.debug_pipeline == "keep"


def _assert_optional_enrichment_loss(options, code: str, **details) -> None:
    report = options.conversion_report
    events = [event for event in report.loss_events if event.code == code]
    assert len(events) == 1
    event = events[0]
    assert event.phase == "htmlz-input"
    assert event.source_format == "htmlz"
    assert event.target_format == "oeb"
    assert event.edge_name == "htmlz-to-oeb"
    assert event.recoverable is True
    assert event.count == 1
    for key, value in details.items():
        assert event.details[key] == value
    assert event.message in report.warnings


def _assert_htmlz_preflight_rejects_without_partial_output(
    archive: Path,
    workdir: Path,
    monkeypatch,
    match: str,
    input_cls=None,
) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.htmlz_input import HTMLZInput

    workdir.mkdir()
    monkeypatch.chdir(workdir)
    plugin_cls = input_cls or HTMLZInput
    log = NullLog()
    options = SimpleNamespace(input_encoding=None, debug_pipeline="keep")

    with archive.open("rb") as stream:
        with pytest.raises(ValueError, match=match):
            plugin_cls(None).convert(stream, options, "htmlz", log, {})

    assert not list(workdir.rglob("*"))
    assert options.debug_pipeline == "keep"

    preflight_messages = [
        message for message in log.messages if "HTMLZ preflight rejected" in message
    ]
    assert preflight_messages
    assert re.search(match, preflight_messages[-1])


def test_htmlz_input_rejects_archive_without_top_level_html_before_html_handoff(
    tmp_path: Path,
    monkeypatch,
) -> None:
    base = build_unicode_htmlz(tmp_path / "base.htmlz")
    hostile = tmp_path / "missing_top_level_html.htmlz"
    rewrite_htmlz_zip(
        base.path,
        hostile,
        remove=(base.html_member,),
        add={"text/index.html": b"<html><body>Nested only</body></html>"},
        add_compression=zipfile.ZIP_DEFLATED,
    )

    _assert_htmlz_rejects_before_html_handoff(
        hostile,
        tmp_path / "missing_html_work",
        monkeypatch,
        "No top level HTML file found",
    )


def test_htmlz_input_rejects_empty_top_level_html_before_html_handoff(
    tmp_path: Path,
    monkeypatch,
) -> None:
    base = build_unicode_htmlz(tmp_path / "base.htmlz")
    hostile = tmp_path / "empty_index.htmlz"
    rewrite_htmlz_zip(base.path, hostile, replace={base.html_member: b""})

    _assert_htmlz_rejects_before_html_handoff(
        hostile,
        tmp_path / "empty_html_work",
        monkeypatch,
        "Top level HTML file index.html is empty",
    )


def test_htmlz_input_warns_and_ignores_malformed_optional_opf(
    tmp_path: Path,
    monkeypatch,
) -> None:
    base = build_unicode_htmlz(tmp_path / "base.htmlz")
    hostile = tmp_path / "malformed_opf.htmlz"
    rewrite_htmlz_zip(
        base.path,
        hostile,
        replace={HTMLZ_OPF_MEMBER: b""},
    )
    log = NullLog()

    out, recorder, options = _convert_htmlz(
        hostile,
        tmp_path / "malformed_opf_work",
        monkeypatch,
        log,
    )

    assert out is recorder.oeb
    assert len(recorder.html_input.calls) == 1
    assert options.debug_pipeline == "keep"
    assert recorder.oeb.manifest.added == []
    assert recorder.oeb.guide.added == []
    assert any("Could not read HTMLZ metadata file metadata.opf" in msg for msg in log.messages)
    _assert_optional_enrichment_loss(
        options,
        "optional-opf-enrichment-failed",
        opf_member=HTMLZ_OPF_MEMBER,
    )
    assert "reason" in options.conversion_report.loss_events[0].details


def test_htmlz_input_warns_and_ignores_missing_optional_cover(
    tmp_path: Path,
    monkeypatch,
) -> None:
    base = build_unicode_htmlz(tmp_path / "base.htmlz")
    hostile = tmp_path / "missing_cover.htmlz"
    rewrite_htmlz_zip(base.path, hostile, remove=(HTMLZ_IMAGE_MEMBER,))
    log = NullLog()

    out, recorder, options = _convert_htmlz(
        hostile,
        tmp_path / "missing_cover_work",
        monkeypatch,
        log,
    )

    assert out is recorder.oeb
    assert len(recorder.html_input.calls) == 1
    assert recorder.oeb.manifest.added == []
    assert recorder.oeb.guide.added == []
    assert any("HTMLZ cover file images/深/cover_世界.png was not found" in msg for msg in log.messages)
    _assert_optional_enrichment_loss(
        options,
        "optional-cover-missing",
        cover_path=HTMLZ_IMAGE_MEMBER,
    )


@pytest.mark.parametrize(
    "cover_href",
    (
        "../escape.png",
        "images/../../escape.png",
        "/absolute/cover.png",
        "C:/absolute/cover.png",
    ),
)
def test_htmlz_input_warns_and_ignores_unsafe_optional_cover_reference(
    tmp_path: Path,
    monkeypatch,
    cover_href: str,
) -> None:
    base = build_unicode_htmlz(tmp_path / "base.htmlz")
    hostile = tmp_path / "unsafe_cover_ref.htmlz"
    rewrite_htmlz_zip(
        base.path,
        hostile,
        replace={HTMLZ_OPF_MEMBER: _opf_with_cover(cover_href)},
    )
    log = NullLog()

    out, recorder, options = _convert_htmlz(
        hostile,
        tmp_path / "unsafe_cover_work",
        monkeypatch,
        log,
    )

    assert out is recorder.oeb
    assert len(recorder.html_input.calls) == 1
    assert recorder.oeb.manifest.added == []
    assert recorder.oeb.guide.added == []
    assert any(f"Ignoring unsafe HTMLZ cover path: {cover_href}" in msg for msg in log.messages)
    _assert_optional_enrichment_loss(
        options,
        "optional-cover-unsafe-path",
        cover_path=cover_href,
    )


def test_htmlz_input_rejects_non_zip_payload_before_extraction(
    tmp_path: Path,
    monkeypatch,
) -> None:
    hostile = tmp_path / "not_an_htmlz.htmlz"
    hostile.write_bytes("not an HTMLZ zip: Καλημέρα".encode("utf-8"))

    _assert_htmlz_preflight_rejects_without_partial_output(
        hostile,
        tmp_path / "not_zip_work",
        monkeypatch,
        "invalid ZIP",
    )


@pytest.mark.parametrize(
    ("case_id", "member_name"),
    (
        ("parent_escape", "../escape.txt"),
        ("nested_parent_escape", "images/../../escape.txt"),
        ("internal_parent_component", "images/../escape.png"),
        ("absolute_path", "/absolute.txt"),
        ("drive_path", "C:/absolute.txt"),
    ),
)
def test_htmlz_input_rejects_unsafe_archive_member_paths_before_extraction(
    tmp_path: Path,
    monkeypatch,
    case_id: str,
    member_name: str,
) -> None:
    base = build_unicode_htmlz(tmp_path / "base.htmlz")
    hostile = tmp_path / f"{case_id}.htmlz"
    rewrite_htmlz_zip(base.path, hostile, add={member_name: b"unsafe"})

    _assert_htmlz_preflight_rejects_without_partial_output(
        hostile,
        tmp_path / f"{case_id}_work",
        monkeypatch,
        "unsafe path",
    )


def test_htmlz_input_rejects_too_many_archive_members_without_partial_output(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.htmlz_input import HTMLZInput

    class StrictHTMLZInput(HTMLZInput):
        max_archive_members = 8

    base = build_unicode_htmlz(tmp_path / "small.htmlz", lines=("small",))
    hostile = tmp_path / "too_many_members.htmlz"
    rewrite_htmlz_zip(
        base.path,
        hostile,
        add={f"assets/many/{i}.bin": b"x" for i in range(12)},
    )

    _assert_htmlz_preflight_rejects_without_partial_output(
        hostile,
        tmp_path / "too_many_work",
        monkeypatch,
        "too many archive members",
        input_cls=StrictHTMLZInput,
    )


def test_htmlz_input_rejects_oversized_archive_member_without_partial_output(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.htmlz_input import HTMLZInput

    class StrictHTMLZInput(HTMLZInput):
        max_member_uncompressed_size = 10 * 1024

    base = build_unicode_htmlz(tmp_path / "small.htmlz", lines=("small",))
    hostile = tmp_path / "oversized_member.htmlz"
    rewrite_htmlz_zip(base.path, hostile, add={"assets/big.bin": b"x" * (20 * 1024)})

    _assert_htmlz_preflight_rejects_without_partial_output(
        hostile,
        tmp_path / "oversized_work",
        monkeypatch,
        "member is too large",
        input_cls=StrictHTMLZInput,
    )


def test_htmlz_input_rejects_excessive_total_expansion_without_partial_output(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.htmlz_input import HTMLZInput

    class StrictHTMLZInput(HTMLZInput):
        max_member_uncompressed_size = 100 * 1024
        max_total_uncompressed_size = 30 * 1024

    base = build_unicode_htmlz(tmp_path / "small.htmlz", lines=("small",))
    hostile = tmp_path / "large_total.htmlz"
    rewrite_htmlz_zip(
        base.path,
        hostile,
        add={f"assets/chunk-{i}.bin": b"x" * (8 * 1024) for i in range(6)},
    )

    _assert_htmlz_preflight_rejects_without_partial_output(
        hostile,
        tmp_path / "large_total_work",
        monkeypatch,
        "expands to too much data",
        input_cls=StrictHTMLZInput,
    )


def test_htmlz_input_rejects_suspicious_compression_ratio_without_partial_output(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.htmlz_input import HTMLZInput

    class StrictHTMLZInput(HTMLZInput):
        max_compression_ratio = 20
        min_compression_ratio_check_size = 32 * 1024

    base = build_unicode_htmlz(tmp_path / "small.htmlz", lines=("small",))
    hostile = tmp_path / "ratio_bomb_shape.htmlz"
    rewrite_htmlz_zip(
        base.path,
        hostile,
        add={"assets/repeated.bin": b"0" * (128 * 1024)},
        add_compression=zipfile.ZIP_DEFLATED,
    )

    _assert_htmlz_preflight_rejects_without_partial_output(
        hostile,
        tmp_path / "ratio_work",
        monkeypatch,
        "suspicious compression ratio",
        input_cls=StrictHTMLZInput,
    )

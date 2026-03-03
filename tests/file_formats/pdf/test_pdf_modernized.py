from __future__ import annotations

import importlib
import io
from pathlib import Path

import pytest


def test_pdf_modules_import_smoke() -> None:
    modules = (
        "LiuXin_alpha.file_formats.pdf",
        "LiuXin_alpha.file_formats.pdf.pageoptions",
        "LiuXin_alpha.file_formats.pdf.pdftohtml",
        "LiuXin_alpha.file_formats.pdf.reflow",
        "LiuXin_alpha.file_formats.pdf.outline_writer",
        "LiuXin_alpha.file_formats.pdf.writer",
        "LiuXin_alpha.file_formats.pdf.render.common",
        "LiuXin_alpha.file_formats.pdf.render.fonts",
        "LiuXin_alpha.file_formats.pdf.render.gradients",
        "LiuXin_alpha.file_formats.pdf.render.graphics",
        "LiuXin_alpha.file_formats.pdf.render.links",
        "LiuXin_alpha.file_formats.pdf.render.serialize",
        "LiuXin_alpha.file_formats.pdf.render.toc",
        "LiuXin_alpha.file_formats.conversion.plugins.pdf_input",
        "LiuXin_alpha.file_formats.conversion.plugins.pdf_output",
    )
    for module_name in modules:
        importlib.import_module(module_name)


def test_pageoptions_fallback_lookups() -> None:
    from LiuXin_alpha.file_formats.pdf.pageoptions import orientation, paper_size, size, unit

    assert isinstance(unit("inch"), int)
    assert isinstance(paper_size("a4"), int)
    assert isinstance(orientation("portrait"), int)
    assert unit("not-real") == unit("inch")
    assert paper_size("not-real") == paper_size("letter")
    assert size("42") == 42
    assert size("NaN") == 1


def test_pdftohtml_flip_images_strips_style_and_is_binary_safe(tmp_path: Path) -> None:
    from LiuXin_alpha.file_formats.pdf.pdftohtml import flip_images

    raw = (
        b"<html><head><STYLE>.x{color:red}</STYLE></head>"
        b'<body><IMG class="xflip" src="missing.png"/></body></html>'
    )
    out = flip_images(raw)
    assert isinstance(out, bytes)
    assert b"<STYLE>" not in out
    assert b"<IMG" in out


def test_pdf_render_common_name_and_string_serialization() -> None:
    from LiuXin_alpha.file_formats.pdf.render.common import Name, Stream, String

    stream = Stream()
    Name("A Name/With Spaces").pdf_serialize(stream)
    stream.write(b" ")
    String("hello (pdf)").pdf_serialize(stream)
    raw = stream.getvalue()
    assert raw.startswith(b"/")
    assert b"(hello (pdf))" in raw


def test_pdf_writer_raises_clear_error_without_qt() -> None:
    writer = importlib.import_module("LiuXin_alpha.file_formats.pdf.writer")
    if getattr(writer, "_HAS_QT", True):
        pytest.skip("PyQt5 is available in this environment; fallback path not active.")

    with pytest.raises(RuntimeError, match="PyQt5"):
        writer.get_pdf_printer(object())

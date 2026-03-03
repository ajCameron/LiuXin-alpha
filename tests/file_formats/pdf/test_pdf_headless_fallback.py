from __future__ import annotations

import io
import types
from pathlib import Path

import pytest


class _DummyLog:
    def debug(self, *_args, **_kwargs):
        return None

    def info(self, *_args, **_kwargs):
        return None

    def warning(self, *_args, **_kwargs):
        return None

    def warn(self, *_args, **_kwargs):
        return None

    def error(self, *_args, **_kwargs):
        return None

    def __call__(self, *_args, **_kwargs):
        return None


def _default_opts(**overrides):
    opts = types.SimpleNamespace(
        paper_size="letter",
        custom_size=None,
        unit="inch",
        orientation="portrait",
        margin_left=36,
        margin_right=36,
        margin_top=36,
        margin_bottom=36,
        pdf_default_font_size=12,
        uncompressed_pdf=False,
        pdf_mark_links=False,
        pdf_page_numbers=True,
        pdf_engine_mode="auto",
        old_pdf_engine=True,
    )
    for key, value in overrides.items():
        setattr(opts, key, value)
    return opts


def test_headless_pdf_writer_generates_pdf(tmp_path: Path) -> None:
    from LiuXin_alpha.file_formats.pdf.headless_writer import HeadlessPDFWriter

    html_path = tmp_path / "chapter.xhtml"
    html_path.write_text(
        """
        <html xmlns="http://www.w3.org/1999/xhtml">
          <body>
            <h1>Title — नमस्ते — こんにちは</h1>
            <p>First paragraph with unicode: café naïve 北京.</p>
            <p>Second paragraph.</p>
          </body>
        </html>
        """,
        encoding="utf-8",
    )

    writer = HeadlessPDFWriter(_default_opts(), _DummyLog())
    out = io.BytesIO()
    meta = types.SimpleNamespace(title="Headless PDF", author="Test Author", tags="fallback")
    writer.dump([str(html_path)], out, meta)

    raw = out.getvalue()
    assert raw.startswith(b"%PDF-1.4")
    assert b"/Type /Page" in raw
    assert b"Headless PDF" in raw


def test_pdf_output_auto_selects_headless_when_qt_missing(monkeypatch) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.pdf_output import PDFOutput

    plugin = PDFOutput(None)
    plugin.opts = _default_opts(pdf_engine_mode="auto")
    monkeypatch.setattr(plugin, "_qt_pdf_available", lambda: False)

    writer_cls = plugin._select_text_writer()
    assert writer_cls.__name__ == "HeadlessPDFWriter"


def test_pdf_output_headless_mode_rejects_image_collection() -> None:
    from LiuXin_alpha.file_formats import ConversionError
    from LiuXin_alpha.file_formats.conversion.plugins.pdf_output import PDFOutput

    plugin = PDFOutput(None)
    plugin.opts = _default_opts(pdf_engine_mode="headless")

    with pytest.raises(ConversionError, match="Headless PDF engine"):
        plugin._select_image_writer()

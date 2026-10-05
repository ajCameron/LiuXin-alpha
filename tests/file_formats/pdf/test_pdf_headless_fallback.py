"""
Provide test pdf headless fallback utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test pdf headless fallback through a consuming regression::

        python -m pytest -q tests/file_formats/pdf/test_pdf_headless_fallback.py
"""
from __future__ import annotations

import io
import types
from pathlib import Path

import pytest


class _DummyLog:
    """
    Provide the dummylog contract for validated ebook processing.

    Example:
        Exercise  DummyLog through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_headless_fallback.py
    """
    def debug(self, *_args, **_kwargs):
        """
        Perform the debug operation under explicit file-format and conversion rules.

        Example:
            Exercise  DummyLog.debug through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_headless_fallback.py


        :param _args: Value supplied for args under the utility contract.
        :param _kwargs: Value supplied for kwargs under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return None

    def info(self, *_args, **_kwargs):
        """
        Perform the info operation under explicit file-format and conversion rules.

        Example:
            Exercise  DummyLog.info through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_headless_fallback.py


        :param _args: Value supplied for args under the utility contract.
        :param _kwargs: Value supplied for kwargs under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return None

    def warning(self, *_args, **_kwargs):
        """
        Perform the warning operation under explicit file-format and conversion rules.

        Example:
            Exercise  DummyLog.warning through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_headless_fallback.py


        :param _args: Value supplied for args under the utility contract.
        :param _kwargs: Value supplied for kwargs under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return None

    def warn(self, *_args, **_kwargs):
        """
        Perform the warn operation under explicit file-format and conversion rules.

        Example:
            Exercise  DummyLog.warn through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_headless_fallback.py


        :param _args: Value supplied for args under the utility contract.
        :param _kwargs: Value supplied for kwargs under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return None

    def error(self, *_args, **_kwargs):
        """
        Perform the error operation under explicit file-format and conversion rules.

        Example:
            Exercise  DummyLog.error through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_headless_fallback.py


        :param _args: Value supplied for args under the utility contract.
        :param _kwargs: Value supplied for kwargs under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return None

    def __call__(self, *_args, **_kwargs):
        """
        Perform the call operation under explicit file-format and conversion rules.

        Example:
            Exercise  DummyLog.  call   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_headless_fallback.py


        :param _args: Value supplied for args under the utility contract.
        :param _kwargs: Value supplied for kwargs under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return None


def _default_opts(**overrides):
    """
    Perform the default opts operation under explicit file-format and conversion rules.

    Example:
        Exercise  default opts through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_headless_fallback.py


    :param overrides: Value supplied for overrides under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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
    """
    Perform the test headless pdf writer generates pdf operation under explicit file-format and conversion rules.

    Example:
        Exercise test headless pdf writer generates pdf through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_headless_fallback.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    """
    Perform the test pdf output auto selects headless when qt missing operation under explicit file-format and conversion rules.

    Example:
        Exercise test pdf output auto selects headless when qt missing through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_headless_fallback.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.conversion.plugins.pdf_output import PDFOutput

    plugin = PDFOutput(None)
    plugin.opts = _default_opts(pdf_engine_mode="auto")
    monkeypatch.setattr(plugin, "_qt_pdf_available", lambda: False)

    writer_cls = plugin._select_text_writer()
    assert writer_cls.__name__ == "HeadlessPDFWriter"


def test_pdf_output_headless_mode_rejects_image_collection() -> None:
    """
    Perform the test pdf output headless mode rejects image collection operation under explicit file-format and conversion rules.

    Example:
        Exercise test pdf output headless mode rejects image collection through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_headless_fallback.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats import ConversionError
    from LiuXin_alpha.file_formats.conversion.plugins.pdf_output import PDFOutput

    plugin = PDFOutput(None)
    plugin.opts = _default_opts(pdf_engine_mode="headless")

    with pytest.raises(ConversionError, match="Headless PDF engine"):
        plugin._select_image_writer()

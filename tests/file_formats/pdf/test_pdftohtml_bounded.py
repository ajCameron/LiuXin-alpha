"""
Check that LiuXin's ``pdftohtml`` runs poppler's tool within limits, in its own folder.

``pdftohtml()`` changed the process's folder while it ran and waited on the tool
with no limit; it also named ``isbsd`` without importing it, so it never ran at
all. It now runs the tool through ``utils.bounded_process``.

Example:
    Run the checks::

        python -m pytest -q tests/file_formats/pdf/test_pdftohtml_bounded.py
"""

from __future__ import annotations

import os
import shutil
import stat
from pathlib import Path

import pytest

from LiuXin_alpha.file_formats import ConversionError
from LiuXin_alpha.file_formats.pdf import pdftohtml as module
from LiuXin_alpha.file_formats.pdf.objects import minimal_pdf
from LiuXin_alpha.utils.bounded_process import ToolLimits


def text_pdf() -> bytes:
    """
    Write a one-page PDF with a line of text.

    Example:
        >>> text_pdf().startswith(b"%PDF")
        True


    :return: The PDF's bytes.
    """
    content = b"BT /F1 24 Tf 72 700 Td (Hello from pdftohtml) Tj ET"
    return minimal_pdf(
        b"<< /Title (Text) >>",
        catalog=b"<< /Type /Catalog /Pages 3 0 R >>",
        extra={
            3: b"<< /Type /Pages /Kids [4 0 R] /Count 1 >>",
            4: b"<< /Type /Page /Parent 3 0 R /MediaBox [0 0 612 792] "
            b"/Resources << /Font << /F1 5 0 R >> >> /Contents 6 0 R >>",
            5: b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
            6: b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
        },
    )


def fake_tool(folder: Path, script: str) -> str:
    """
    Write an executable shell script to stand in for pdftohtml.

    Example:
        Make a tool that hangs::

            tool = fake_tool(tmp_path, "sleep 30")


    :param folder: Where to write it.
    :param script: The shell commands.
    :return: Its path.
    """
    tool = folder / "pdftohtml"
    tool.write_text("#!/bin/sh\n" + script + "\n", encoding="utf-8")
    tool.chmod(tool.stat().st_mode | stat.S_IXUSR)
    return str(tool)


@pytest.mark.skipif(shutil.which("pdftohtml") is None, reason="poppler's pdftohtml is not installed")
def test_pdftohtml_writes_the_html_without_changing_folder(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Convert a PDF with poppler's tool, leaving this process's folder as it was.

    Example:
        >>> test_pdftohtml_writes_the_html_without_changing_folder(Path("/tmp"), None)  # doctest: +SKIP


    :param tmp_path: Pytest-managed temporary directory.
    :param monkeypatch: Pytest fixture, to use the tool on the PATH.
    :return: None; the assertions are the check.
    """
    monkeypatch.setattr(module, "PDFTOHTML", shutil.which("pdftohtml"))
    source = tmp_path / "book.pdf"
    source.write_bytes(text_pdf())
    output = tmp_path / "out"
    output.mkdir()
    before = os.getcwd()
    module.pdftohtml(str(output), str(source), True)
    assert os.getcwd() == before
    html = (output / "index.html").read_bytes()
    assert html.startswith(b"<!-- created by calibre's pdftohtml -->")
    assert b"Hello from pdftohtml" in html
    assert not (output / "src.pdf").exists()


@pytest.mark.skipif(os.name != "posix", reason="the stand-in tool is a shell script")
def test_a_tool_that_hangs_is_stopped(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Stop a pdftohtml that runs past its time limit, and say so.

    Example:
        >>> test_a_tool_that_hangs_is_stopped(Path("/tmp"), None)  # doctest: +SKIP


    :param tmp_path: Pytest-managed temporary directory.
    :param monkeypatch: Pytest fixture, for the stand-in tool and limits.
    :return: None; the assertions are the check.
    """
    monkeypatch.setattr(module, "PDFTOHTML", fake_tool(tmp_path, "sleep 30"))
    monkeypatch.setattr(module, "PDFTOHTML_LIMITS", ToolLimits(timeout=0.5))
    source = tmp_path / "book.pdf"
    source.write_bytes(text_pdf())
    output = tmp_path / "out"
    output.mkdir()
    with pytest.raises(ConversionError, match="pdftohtml was stopped"):
        module.pdftohtml(str(output), str(source), True)
    assert not (output / "src.pdf").exists()


def test_a_missing_tool_is_reported(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Say that pdftohtml could not be found.

    Example:
        >>> test_a_missing_tool_is_reported(Path("/tmp"), None)  # doctest: +SKIP


    :param tmp_path: Pytest-managed temporary directory.
    :param monkeypatch: Pytest fixture, for the missing tool.
    :return: None; the assertions are the check.
    """
    monkeypatch.setattr(module, "PDFTOHTML", str(tmp_path / "no-such-pdftohtml"))
    source = tmp_path / "book.pdf"
    source.write_bytes(text_pdf())
    with pytest.raises(ConversionError, match="Could not find pdftohtml"):
        module.pdftohtml(str(tmp_path), str(source), True)

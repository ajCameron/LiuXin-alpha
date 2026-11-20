"""
Read a PDF's Info fields and XMP packet with poppler's ``pdfinfo``, within limits.

LiuXin's own object reader hands a file here when it is encrypted with AES and the
``aes`` utils plugin's poppler layer serves: ``cryptography`` is missing and
``pdfinfo`` is on the PATH. ``pdfinfo`` decrypts the file itself. It runs through
:func:`LiuXin_alpha.utils.bounded_process.run_tool`, on a copy of the file in a
temporary folder, as calibre's reader runs it. This module also keeps the limits
and the tool lookup that the ported calibre reader's ``_poppler`` helpers share.

``pdfinfo`` prints one field to a line, and a value with a line break runs on to
the next line. Lines that do not start with a label ``pdfinfo`` prints are read as
the previous value's continuation, so a multi-line title stays whole.

Example:
    >>> parse_pdfinfo("Title:          Two\\nlines\\nAuthor:         Ann\\nPages:          3\\n")
    ({'Title': 'Two lines', 'Author': 'Ann'}, None)
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
from typing import IO

from LiuXin_alpha.file_formats.pdf.errors import PdfEncryptedError, PdfReadError
from LiuXin_alpha.utils.bounded_process import ToolLimits, run_tool

__all__ = [
    "INFO_FIELDS",
    "PDFINFO_LIMITS",
    "PDFTOPPM_LIMITS",
    "parse_pdfinfo",
    "read_with_poppler",
    "tool_directory",
]

#: Limits for ``pdfinfo``, whose output is the Info fields or the XMP packet.
PDFINFO_LIMITS = ToolLimits(timeout=30.0, cpu_seconds=30, memory_bytes=1 << 30, output_bytes=8 << 20)
#: Limits for ``pdftoppm``, which draws a page.
PDFTOPPM_LIMITS = ToolLimits(timeout=60.0, cpu_seconds=60, memory_bytes=1 << 30, output_bytes=64 << 20)
#: The Info fields ``pdfinfo`` prints.
INFO_FIELDS = ("Title", "Subject", "Keywords", "Author", "Creator", "Producer", "CreationDate", "ModDate")
#: Every label ``pdfinfo`` prints at the start of a line, so other lines are continuations.
_LABELS = frozenset(
    {
        *INFO_FIELDS,
        "Custom Metadata",
        "Metadata Stream",
        "Tagged",
        "UserProperties",
        "Suspects",
        "Form",
        "JavaScript",
        "Pages",
        "Encrypted",
        "Page size",
        "Page rot",
        "File size",
        "Optimized",
        "PDF version",
        "PDF subtype",
    }
)
_LABEL = re.compile(r"([A-Za-z][A-Za-z ]{0,30}):(?: +|$)")


def tool_directory() -> str | None:
    """
    Find the directory of the ``pdfinfo`` on the PATH, where poppler's other tools are too.

    Example:
        >>> tool_directory() is None or os.path.isdir(tool_directory())
        True


    :return: The directory, or ``None`` when poppler is not installed.
    """
    found = shutil.which("pdfinfo")
    return os.path.dirname(found) if found else None


def parse_pdfinfo(output: str) -> tuple[dict[str, str], str | None]:
    """
    Read the Info fields and the PDF version from ``pdfinfo -rawdates`` output.

    Example:
        >>> parse_pdfinfo("Title:   T\\nPDF version:    1.7\\n")
        ({'Title': 'T'}, '1.7')


    :param output: What ``pdfinfo`` printed, decoded.
    :return: The Info fields that have values, and the version.
    """
    fields: dict[str, list[str]] = {}
    version: str | None = None
    current: str | None = None
    for line in output.splitlines():
        match = _LABEL.match(line)
        if match is not None and match.group(1) in _LABELS:
            label, value = match.group(1), line[match.end() :]
            current = label if label in INFO_FIELDS else None
            if current is not None:
                fields[current] = [value.strip()]
            if label == "PDF version":
                version = value.strip() or None
        elif current is not None:
            fields[current].append(line.strip())
    info = {name: " ".join(part for part in parts if part) for name, parts in fields.items()}
    return {name: value for name, value in info.items() if value}, version


def _run_pdfinfo(folder: str, *options: str) -> bytes:
    """
    Run ``pdfinfo`` on ``src.pdf`` in a folder, within its limits.

    Example:
        >>> _run_pdfinfo("/tmp", "-v")  # doctest: +SKIP


    :param folder: The folder holding ``src.pdf``.
    :param options: Options before the file name.
    :return: What it printed.
    :raises PdfEncryptedError: The file needs a password.
    :raises PdfReadError: ``pdfinfo`` failed, was stopped, or is missing.
    """
    directory = tool_directory()
    if directory is None:
        raise PdfReadError("poppler's pdfinfo is not on the PATH")
    try:
        return run_tool([os.path.join(directory, "pdfinfo"), *options, "src.pdf"], cwd=folder, limits=PDFINFO_LIMITS)
    except subprocess.CalledProcessError as error:
        message = (error.stderr or b"").decode("utf-8", "replace").strip()
        if "password" in message.lower():
            raise PdfEncryptedError("this PDF needs a password to open") from None
        raise PdfReadError(f"poppler's pdfinfo could not read the file ({error.returncode}): {message[:200]}") from None


def read_with_poppler(target: bytes | bytearray | memoryview | IO[bytes]) -> object:
    """
    Read a PDF's Info fields and XMP packet with ``pdfinfo``, as LiuXin's object reader would.

    Example:
        >>> read_with_poppler(b"%PDF-1.4 not really")  # doctest: +SKIP


    :param target: The PDF's bytes, or a seekable binary file, read from its start.
    :return: A :class:`~LiuXin_alpha.file_formats.pdf.objects.PdfMetadata`.
    :raises PdfEncryptedError: The file needs a password.
    :raises PdfReadError: ``pdfinfo`` failed or is missing.
    """
    from LiuXin_alpha.file_formats.pdf.objects import PdfMetadata

    with tempfile.TemporaryDirectory(prefix="liuxin-pdf-") as folder:
        with open(os.path.join(folder, "src.pdf"), "wb") as copy:
            if isinstance(target, (bytes, bytearray, memoryview)):
                copy.write(target)
            else:
                target.seek(0)
                shutil.copyfileobj(target, copy)
        info, version = parse_pdfinfo(_run_pdfinfo(folder, "-enc", "UTF-8", "-rawdates").decode("utf-8", "replace"))
        xmp = _run_pdfinfo(folder, "-meta").strip() or None
    return PdfMetadata(dict(info), xmp, True, version, read_by="poppler's pdfinfo")

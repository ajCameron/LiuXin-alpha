# -*- coding: utf-8 -*-

"""
Read content from AZW4 files.

AZW4 is essentially a PDF wrapped in a MOBI/PDB-like container.
"""

from __future__ import annotations

import typing as _typing

import os
from pathlib import Path
from tempfile import NamedTemporaryFile

from collections.abc import Iterable, Mapping
from typing import BinaryIO, Protocol, Union, cast

from LiuXin_alpha.file_formats.pdb.formatreader import FormatReader

__license__ = "GPL v3"
__copyright__ = "2011, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"

_PDF_START_MARKER = b"%PDF"
_PDF_END_MARKER = b"%%EOF"


class _Logger(Protocol):
    def info(self: _typing.Self, *args: object) -> object: ...


class _InputPlugin(Protocol):
    options: Iterable[object]

    def convert(
        self: _typing.Self,
        stream: BinaryIO,
        options: object,
        file_ext: str,
        log: _Logger,
        accelerators: Mapping[str, object],
    ) -> object: ...


def extract_embedded_pdf_bytes(
    raw_data: bytes | bytearray | memoryview,
) -> bytes:
    """
    Extract the embedded PDF payload from raw AZW4 bytes.

    :param raw_data:
    :return:
    """
    if not isinstance(raw_data, (bytes, bytearray, memoryview)):
        raise TypeError("raw_data must be bytes-like")

    data = bytes(raw_data)
    start = data.find(_PDF_START_MARKER)
    if start < 0:
        raise ValueError("No embedded PDF found in AZW4 file")

    # Use the last EOF marker after the first PDF marker to capture full payload.
    eof = data.rfind(_PDF_END_MARKER, start)
    if eof < 0:
        raise ValueError("Embedded PDF appears truncated (missing %%EOF marker)")

    end = eof + len(_PDF_END_MARKER)
    while end < len(data) and data[end] in b"\x00\t\r\n ":
        end += 1

    return data[start:end]


# Todo: Add hash backing
def unwrap(stream: BinaryIO, output_path: Union[str, Path]) -> None:
    """
    Write the embedded PDF in ``stream`` to ``output_path``.

    :param stream:
    :param output_path:
    :return:
    """
    stream.seek(0)
    pdf_data = extract_embedded_pdf_bytes(stream.read())
    with open(output_path, "wb") as f:
        f.write(pdf_data)


def _plugin_for_input_format(file_ext: str) -> _InputPlugin | None:
    """
    Return the reader plugin for the given file extension.

    :param file_ext:
    :return:
    """
    from LiuXin_alpha.customize.ui import plugin_for_input_format

    return cast(_InputPlugin | None, plugin_for_input_format(file_ext))


def _apply_recommended_options(
    options: object | None,
    plugin: _InputPlugin,
) -> None:
    if options is None:
        return
    for opt in getattr(plugin, "options", ()):
        option_obj = getattr(opt, "option", None)
        option_name = getattr(option_obj, "name", None)
        if not option_name:
            continue
        if not hasattr(options, option_name):
            setattr(options, option_name, getattr(opt, "recommended_value", None))


class Reader(FormatReader):
    def __init__(
        self: _typing.Self,
        header: object,
        stream: BinaryIO,
        log: object,
        options: object,
    ) -> None:
        self.header = header
        self.stream = stream
        self.log = cast(_Logger, log)
        self.options = options

    def extract_content(
        self: _typing.Self,
        output_dir: str | os.PathLike[str] | None,
    ) -> object:
        self.log.info("Extracting PDF from AZW4 Container...")

        self.stream.seek(0)
        pdf_data = extract_embedded_pdf_bytes(self.stream.read())

        work_dir = Path(output_dir or os.getcwd())
        work_dir.mkdir(parents=True, exist_ok=True)

        pdf_path: Path | None = None
        try:
            with NamedTemporaryFile(
                mode="wb",
                suffix=".pdf",
                prefix="azw4_",
                dir=str(work_dir),
                delete=False,
            ) as tmp_pdf:
                tmp_pdf.write(pdf_data)
                pdf_path = Path(tmp_pdf.name)

            pdf_plugin = _plugin_for_input_format("pdf")
            if pdf_plugin is None:
                raise RuntimeError("No input plugin registered for 'pdf'")

            _apply_recommended_options(self.options, pdf_plugin)
            with pdf_path.open("rb") as pdf_temp_file:
                return pdf_plugin.convert(pdf_temp_file, self.options, "pdf", self.log, {})
        finally:
            if pdf_path is not None:
                try:
                    pdf_path.unlink()
                except FileNotFoundError:
                    pass
                except Exception:
                    # Temp cleanup should not mask conversion outcomes.
                    pass

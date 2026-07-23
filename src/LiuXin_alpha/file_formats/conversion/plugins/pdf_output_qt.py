# -*- coding: utf-8 -*-

"""
Dedicated Qt-backed PDF output plugin.
"""
from __future__ import annotations

import typing as _typing

from LiuXin_alpha.file_formats.conversion.plugins.pdf_output import PDFOutput

__license__ = "GPL 3"
__copyright__ = "2026, LiuXin contributors"
__docformat__ = "restructuredtext en"


class PDFQtOutput(PDFOutput):
    name = "PDF Output (Qt)"
    file_type = "pdfqt"

    def convert(self: _typing.Self, oeb_book: _typing.Any, output_path: _typing.Any, input_plugin: _typing.Any, opts: _typing.Any, log: _typing.Any) -> _typing.Any:
        setattr(opts, "pdf_engine_mode", "qt")
        return super().convert(oeb_book, output_path, input_plugin, opts, log)

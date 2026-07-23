# -*- coding: utf-8 -*-

"""
Read content from palmdoc pdb file.
"""
from __future__ import annotations

import typing as _typing

from LiuXin_alpha.file_formats.pdb.formatreader import FormatReader

from LiuXin_alpha.utils.libraries.liuxin_six import memory_range
from LiuXin_alpha.utils.ptempfiles import PersistentTemporaryFile

__license__ = "GPL v3"
__copyright__ = "2010, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


class Reader(FormatReader):
    def __init__(self: _typing.Self, header: _typing.Any, stream: _typing.Any, log: _typing.Any, options: _typing.Any) -> None:
        self.header = header
        self.stream = stream
        self.log = log
        self.options = options

    def extract_content(self: _typing.Self, output_dir: _typing.Any) -> _typing.Any:
        self.log.info("Extracting PDF...")

        pdf = PersistentTemporaryFile(".pdf")
        pdf.close()
        pdf = open(pdf, "wb")
        for x in memory_range(self.header.section_count()):
            pdf.write(self.header.section_data(x))
        pdf.close()

        from LiuXin_alpha.customize.ui import plugin_for_input_format

        pdf_plugin = plugin_for_input_format("pdf")
        for opt in pdf_plugin.options:
            if not hasattr(self.options, opt.option.name):
                setattr(self.options, opt.option.name, opt.recommended_value)

        return pdf_plugin.convert(open(pdf, "rb"), self.options, "pdf", self.log, {})

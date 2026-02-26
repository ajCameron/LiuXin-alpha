# -*- coding: utf-8 -*-

from __future__ import annotations

import os

from LiuXin_alpha.customize.conversion import InputFormatPlugin

__license__ = "GPL v3"
__copyright__ = "2011, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


class AZW4Input(InputFormatPlugin):
    """
    AZW4 files are Amazon's print replica ebook format.
    """

    name = "AZW4 Input"
    author = "John Schember"
    description = "Convert AZW4 to HTML"
    file_types = {"azw4"}

    def convert(self, stream, options, file_ext, log, accelerators):
        from LiuXin_alpha.file_formats.azw4.reader import Reader

        # AZW4 handling is byte-pattern based and does not need the PDB header.
        reader = Reader(None, stream, log, options)
        return reader.extract_content(os.getcwd())

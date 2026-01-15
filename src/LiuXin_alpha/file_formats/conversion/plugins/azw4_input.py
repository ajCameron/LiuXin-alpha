# -*- coding: utf-8 -*-

import os

from LiuXin.customize.conversion import InputFormatPlugin

__license__ = "GPL v3"
__copyright__ = "2011, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


class AZW4Input(InputFormatPlugin):
    """
    AZW4 files are Amazon's print replica ebook format - DJVU for kindle.
    """

    name = "AZW4 Input"
    author = "John Schember"
    description = "Convert AZW4 to HTML"
    file_types = {"azw4"}

    def convert(self, stream, options, file_ext, log, accelerators):
        from LiuXin.file_formats.pdb.header import PdbHeaderReader
        from LiuXin.file_formats.azw4.reader import Reader

        header = PdbHeaderReader(stream)
        reader = Reader(header, stream, log, options)
        opf = reader.extract_content(os.getcwdu())

        return opf

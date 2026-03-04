# -*- coding: utf-8 -*-

import os

from LiuXin_alpha.customize.conversion import InputFormatPlugin
from LiuXin_alpha.file_formats.conversion.plugins._workdir import (
    choose_conversion_workdir,
)

__license__ = "GPL 3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


class RBInput(InputFormatPlugin):

    name = "RB Input"
    author = "John Schember"
    description = "Convert RB files to HTML"
    file_types = {"rb"}

    def convert(self, stream, options, file_ext, log, accelerators):
        from LiuXin_alpha.file_formats.rb.reader import Reader

        reader = Reader(stream, log, options.input_encoding)
        opf = reader.extract_content(choose_conversion_workdir("_rb_input"))

        return opf

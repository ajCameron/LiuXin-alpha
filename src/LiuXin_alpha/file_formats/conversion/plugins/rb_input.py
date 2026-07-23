# -*- coding: utf-8 -*-

from __future__ import annotations

import typing as _typing
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

    def convert(self: _typing.Self, stream: _typing.Any, options: _typing.Any, file_ext: _typing.Any, log: _typing.Any, accelerators: _typing.Any) -> _typing.Any:
        from LiuXin_alpha.file_formats.rb.reader import Reader

        reader = Reader(stream, log, options.input_encoding)
        opf = reader.extract_content(choose_conversion_workdir("_rb_input"))

        return opf

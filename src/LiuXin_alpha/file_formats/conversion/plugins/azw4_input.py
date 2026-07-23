# -*- coding: utf-8 -*-

from __future__ import annotations

import typing as _typing

import os

from LiuXin_alpha.customize.conversion import InputFormatPlugin
from LiuXin_alpha.file_formats.conversion.plugins._workdir import (
    choose_conversion_workdir,
)

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

    def convert(self: _typing.Self, stream: _typing.Any, options: _typing.Any, file_ext: _typing.Any, log: _typing.Any, accelerators: _typing.Any) -> _typing.Any:
        from LiuXin_alpha.file_formats.azw4.reader import Reader

        # AZW4 handling is byte-pattern based and does not need the PDB header.
        reader = Reader(None, stream, log, options)
        return reader.extract_content(choose_conversion_workdir("_azw4_input"))

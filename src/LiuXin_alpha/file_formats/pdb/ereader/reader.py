# -*- coding: utf-8 -*-

"""
Read content from ereader pdb file.
"""
from __future__ import annotations

import typing as _typing

from LiuXin_alpha.file_formats.pdb.ereader import EreaderError
from LiuXin_alpha.file_formats.pdb.ereader.reader132 import Reader132
from LiuXin_alpha.file_formats.pdb.ereader.reader202 import Reader202
from LiuXin_alpha.file_formats.pdb.formatreader import FormatReader

__license__ = "GPL v3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


class Reader(FormatReader):
    def __init__(self: _typing.Self, header: _typing.Any, stream: _typing.Any, log: _typing.Any, options: _typing.Any) -> None:
        record0_size = len(header.section_data(0))

        if record0_size == 132:
            self.reader = Reader132(header, stream, log, options)
        elif record0_size in (116, 202):
            self.reader = Reader202(header, stream, log, options)
        else:
            raise EreaderError("Size mismatch. eReader header record size %s KB is not supported." % record0_size)

    def extract_content(self: _typing.Self, output_dir: _typing.Any) -> _typing.Any:
        return self.reader.extract_content(output_dir)

    def dump_pml(self: _typing.Self) -> _typing.Any:
        return self.reader.dump_pml()

    def dump_images(self: _typing.Self, out_dir: _typing.Any) -> _typing.Any:
        return self.reader.dump_images(out_dir)

# -*- coding: utf-8 -*-
from __future__ import annotations

import typing as _typing
import os

from LiuXin_alpha.customize.conversion import OutputFormatPlugin, OptionRecommendation

from LiuXin_alpha.utils.localization import _

__license__ = "GPL 3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


class RBOutput(OutputFormatPlugin):

    name = "RB Output"
    author = "John Schember"
    file_type = "rb"

    options = {
        OptionRecommendation(
            name="inline_toc",
            recommended_value=False,
            level=OptionRecommendation.LOW,
            option_help=_("Add Table of Contents to beginning of the book."),
        ),
    }

    def convert(self: _typing.Self, oeb_book: _typing.Any, output_path: _typing.Any, input_plugin: _typing.Any, opts: _typing.Any, log: _typing.Any) -> None:
        from LiuXin_alpha.file_formats.rb.writer import RBWriter

        close = False
        if not hasattr(output_path, "write"):
            close = True
            if not os.path.exists(os.path.dirname(output_path)) and os.path.dirname(output_path) != "":
                os.makedirs(os.path.dirname(output_path))
            out_stream = open(output_path, "wb")
        else:
            out_stream = output_path

        writer = RBWriter(opts, log)

        out_stream.seek(0)
        out_stream.truncate()

        writer.write_content(oeb_book, out_stream, oeb_book.metadata)

        if close:
            out_stream.close()

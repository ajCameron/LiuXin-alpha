# -*- coding: utf-8 -*-

from __future__ import annotations

import typing as _typing
import io

from LiuXin_alpha.customize.conversion import InputFormatPlugin

__license__ = "GPL 3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


class TCRInput(InputFormatPlugin):

    name = "TCR Input"
    author = "John Schember"
    description = "Convert TCR files to HTML"
    file_types = {"tcr"}

    def convert(self: _typing.Self, stream: _typing.Any, options: _typing.Any, file_ext: _typing.Any, log: _typing.Any, accelerators: _typing.Any) -> _typing.Any:
        from LiuXin_alpha.file_formats.compression.tcr import decompress

        log.info("Decompressing text...")
        raw_txt = decompress(stream)
        if isinstance(raw_txt, bytes):
            input_encoding = getattr(options, "input_encoding", None) or "utf-8"
            raw_txt = raw_txt.decode(input_encoding, "replace")

        log.info("Converting text to OEB...")
        stream = io.StringIO(raw_txt)

        from LiuXin_alpha.customize.ui import plugin_for_input_format

        txt_plugin = plugin_for_input_format("txt")
        for opt in getattr(txt_plugin, "options", ()):
            if not hasattr(options, opt.option.name):
                setattr(options, opt.option.name, opt.recommended_value)

        stream.seek(0)
        return txt_plugin.convert(stream, options, "txt", log, accelerators)

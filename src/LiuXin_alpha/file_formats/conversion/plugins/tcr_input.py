# -*- coding: utf-8 -*-

from LiuXin.customize.conversion import InputFormatPlugin

# Py2/Py3
from LiuXin.utils.lx_libraries.liuxin_six import six_cStringIO

__license__ = "GPL 3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


class TCRInput(InputFormatPlugin):

    name = "TCR Input"
    author = "John Schember"
    description = "Convert TCR files to HTML"
    file_types = {"tcr"}

    def convert(self, stream, options, file_ext, log, accelerators):
        from LiuXin.file_formats.compression.tcr import decompress

        log.info("Decompressing text...")
        raw_txt = decompress(stream)

        log.info("Converting text to OEB...")
        stream = six_cStringIO(raw_txt)

        from LiuXin.customize.ui import plugin_for_input_format

        txt_plugin = plugin_for_input_format("txt")
        for opt in txt_plugin.options:
            if not hasattr(self.options, opt.option.name):
                setattr(options, opt.option.name, opt.recommended_value)

        stream.seek(0)
        return txt_plugin.convert(stream, options, "txt", log, accelerators)

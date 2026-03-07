# -*- coding: utf-8 -*-

"""
Dedicated headless PDF output plugin.
"""

from LiuXin_alpha.file_formats.conversion.plugins.pdf_output import PDFOutput

__license__ = "GPL 3"
__copyright__ = "2026, LiuXin contributors"
__docformat__ = "restructuredtext en"


class PDFHeadlessOutput(PDFOutput):
    name = "PDF Output (Headless)"
    file_type = "pdfheadless"

    def convert(self, oeb_book, output_path, input_plugin, opts, log):
        setattr(opts, "pdf_engine_mode", "headless")
        return super().convert(oeb_book, output_path, input_plugin, opts, log)

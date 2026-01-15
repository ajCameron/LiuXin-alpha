#!/usr/bin/env python
# vim:fileencoding=utf-8

from __future__ import unicode_literals, division, absolute_import, print_function

from LiuXin.customize.conversion import InputFormatPlugin, OptionRecommendation

from LiuXin.utils.localization import trans as _

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"


class DOCXInput(InputFormatPlugin):

    name = "DOCX Input"
    author = "Kovid Goyal"
    description = _("Convert DOCX files (.docx and .docm) to HTML")
    file_types = {"docx", "docm"}

    options = {
        OptionRecommendation(
            name="docx_no_cover",
            recommended_value=False,
            option_help=_(
                "Normally, if a large image is present at the start of the document that "
                "looks like a cover, "
                "it will be removed from the document and used as the cover for created ebook. "
                "This option turns off that behavior."
            ),
        ),
    }

    recommendations = {("page_breaks_before", "/", OptionRecommendation.MED)}

    def convert(self, stream, options, file_ext, log, accelerators):
        from LiuXin.file_formats.docx.to_html import Convert

        return Convert(stream, detect_cover=not options.docx_no_cover, log=log)()

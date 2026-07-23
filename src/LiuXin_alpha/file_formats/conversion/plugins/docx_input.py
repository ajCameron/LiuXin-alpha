#!/usr/bin/env python
# vim:fileencoding=utf-8

from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

from LiuXin_alpha.customize.conversion import InputFormatPlugin, OptionRecommendation

from LiuXin_alpha.utils.localization import trans as _

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

    def convert(self: _typing.Self, stream: _typing.Any, options: _typing.Any, file_ext: _typing.Any, log: _typing.Any, accelerators: _typing.Any) -> _typing.Any:
        from LiuXin_alpha.file_formats.docx.to_html import Convert

        return Convert(stream, detect_cover=not options.docx_no_cover, log=log)()

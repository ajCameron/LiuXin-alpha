#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

from __future__ import with_statement
from __future__ import annotations

import typing as _typing

from LiuXin_alpha.customize.conversion import OutputFormatPlugin

__license__ = "GPL v3"
__copyright__ = "2009, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


class LITOutput(OutputFormatPlugin):

    name = "LIT Output"
    author = "Marshall T. Vandegrift"
    file_type = "lit"

    def convert(self: _typing.Self, oeb_book: _typing.Any, output_path: _typing.Any, input_plugin: _typing.Any, opts: _typing.Any, log: _typing.Any) -> None:

        from LiuXin_alpha.file_formats.lit.writer import LitWriter
        from LiuXin_alpha.file_formats.oeb.transforms.htmltoc import HTMLTOCAdder
        from LiuXin_alpha.file_formats.oeb.transforms.manglecase import CaseMangler
        from LiuXin_alpha.file_formats.oeb.transforms.rasterize import SVGRasterizer
        from LiuXin_alpha.file_formats.oeb.transforms.split import Split

        self.log, self.opts, self.oeb = log, opts, oeb_book

        split = Split(split_on_page_breaks=True, max_flow_size=0, remove_css_pagebreaks=False)
        split(self.oeb, self.opts)

        tocadder = HTMLTOCAdder()
        tocadder(oeb_book, opts)
        mangler = CaseMangler()
        mangler(oeb_book, opts)
        rasterizer = SVGRasterizer()
        rasterizer(oeb_book, opts)
        lit = LitWriter(self.opts)
        lit(oeb_book, output_path)

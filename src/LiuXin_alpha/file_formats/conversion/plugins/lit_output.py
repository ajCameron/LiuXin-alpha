#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Convert LIT content from the normalized ebook conversion pipeline.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise lit output through a consuming regression::

        python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py
"""
from __future__ import with_statement
from __future__ import annotations

import typing as _typing

from LiuXin_alpha.customize.conversion import OutputFormatPlugin

__license__ = "GPL v3"
__copyright__ = "2009, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


class LITOutput(OutputFormatPlugin):

    """
    Provide the litoutput contract for validated ebook processing.

    Example:
        Exercise LITOutput through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py
    """
    name = "LIT Output"
    author = "Marshall T. Vandegrift"
    file_type = "lit"

    def convert(self: _typing.Self, oeb_book: _typing.Any, output_path: _typing.Any, input_plugin: _typing.Any, opts: _typing.Any, log: _typing.Any) -> None:

        """
        Convert the supplied source into the stage's normalized output representation.

        Example:
            Exercise LITOutput.convert through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py


        :param oeb_book: Value supplied for oeb book under the utility contract.
        :param output_path: Value supplied for output path under the utility contract.
        :param input_plugin: Value supplied for input plugin under the utility contract.
        :param opts: Value supplied for opts under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
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

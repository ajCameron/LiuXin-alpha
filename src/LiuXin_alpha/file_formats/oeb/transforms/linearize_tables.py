#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Linearize HTML tables for restricted reading devices.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise linearize tables through a consuming regression::

        python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
"""
from __future__ import with_statement
from __future__ import annotations

import typing as _typing

from LiuXin_alpha.file_formats.oeb.base import OEB_DOCS, XPath, XHTML

__license__ = "GPL v3"
__copyright__ = "2009, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


class LinearizeTables(object):
    """
    Provide the linearizetables contract for validated ebook processing.

    Example:
        Exercise LinearizeTables through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
    """
    def linearize(self: _typing.Self, root: _typing.Any) -> None:
        """
        Perform the linearize operation under explicit file-format and conversion rules.

        Example:
            Exercise LinearizeTables.linearize through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param root: Root directory that bounds path resolution or traversal.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for x in XPath(
            "//h:table|//h:td|//h:tr|//h:th|//h:caption|" "//h:tbody|//h:tfoot|//h:thead|//h:colgroup|//h:col"
        )(root):
            x.tag = XHTML("div")
            for attr in (
                "style",
                "font",
                "valign",
                "colspan",
                "width",
                "height",
                "rowspan",
                "summary",
                "align",
                "cellspacing",
                "cellpadding",
                "frames",
                "rules",
                "border",
            ):
                if attr in x.attrib:
                    del x.attrib[attr]

    def __call__(self: _typing.Self, oeb: _typing.Any, context: _typing.Any) -> None:
        """
        Perform the call operation under explicit file-format and conversion rules.

        Example:
            Exercise LinearizeTables.  call   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param oeb: Value supplied for oeb under the utility contract.
        :param context: Value supplied for context under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for x in oeb.manifest.items:
            if x.media_type in OEB_DOCS:
                self.linearize(x.data)

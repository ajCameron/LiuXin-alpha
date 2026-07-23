#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

from __future__ import with_statement
from __future__ import annotations

import typing as _typing

from LiuXin_alpha.file_formats.oeb.base import OEB_DOCS, XPath, XHTML

__license__ = "GPL v3"
__copyright__ = "2009, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


class LinearizeTables(object):
    def linearize(self: _typing.Self, root: _typing.Any) -> None:
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
        for x in oeb.manifest.items:
            if x.media_type in OEB_DOCS:
                self.linearize(x.data)

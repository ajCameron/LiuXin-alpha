#!/usr/bin/env python2
# vim:fileencoding=utf-8
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"


class Theme(object):
    def __init__(self: _typing.Self, namespace: _typing.Any) -> None:
        self.major_latin_font = "Cambria"
        self.minor_latin_font = "Calibri"
        self.namespace = namespace

    def __call__(self: _typing.Self, root: _typing.Any) -> None:
        for fs in self.namespace.XPath("//a:fontScheme")(root):
            for mj in self.namespace.XPath("./a:majorFont")(fs):
                for l in self.namespace.XPath("./a:latin[@typeface]")(mj):
                    self.major_latin_font = l.get("typeface")
            for mj in self.namespace.XPath("./a:minorFont")(fs):
                for l in self.namespace.XPath("./a:latin[@typeface]")(mj):
                    self.minor_latin_font = l.get("typeface")

    def resolve_font_family(self: _typing.Self, ff: _typing.Any) -> _typing.Any:
        if ff.startswith("|"):
            ff = ff[1:-1]
            ff = self.major_latin_font if ff.startswith("major") else self.minor_latin_font
        return ff

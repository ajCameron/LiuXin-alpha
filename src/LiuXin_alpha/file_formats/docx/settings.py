#!/usr/bin/env python2
# vim:fileencoding=utf-8

from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"


class Settings(object):
    def __init__(self: _typing.Self, namespace: _typing.Any) -> None:
        self.default_tab_stop = 720 / 20
        self.namespace = namespace

    def __call__(self: _typing.Self, root: _typing.Any) -> None:
        for dts in self.namespace.XPath("//w:defaultTabStop[@w:val]")(root):
            try:
                self.default_tab_stop = int(self.namespace.get(dts, "w:val")) / 20
            except (ValueError, TypeError, AttributeError):
                pass

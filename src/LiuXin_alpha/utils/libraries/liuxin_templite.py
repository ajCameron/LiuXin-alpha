#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Small template engine used by legacy conversion templates.
"""

from __future__ import annotations

import re
import sys

from LiuXin_alpha.utils.localization import trans as _


class Templite(object):
    auto_emit = re.compile(r"(^['\"])|(^[a-zA-Z0-9_\[\]'\"]+$)")

    def __init__(self, template, start="${", end="}$"):
        if len(start) != 2 or len(end) != 2:
            raise ValueError("each delimiter must be two characters long")
        delimiter = re.compile("%s(.*?)%s" % (re.escape(start), re.escape(end)), re.DOTALL)
        offset = 0
        tokens = []
        for i, part in enumerate(delimiter.split(template)):
            part = part.replace("\\".join(list(start)), start)
            part = part.replace("\\".join(list(end)), end)
            if i % 2 == 0:
                if not part:
                    continue
                part = part.replace("\\", "\\\\").replace('"', '\\"')
                part = "\t" * offset + 'emit("""%s""")' % part
            else:
                part = part.rstrip()
                if not part:
                    continue
                if part.lstrip().startswith(":"):
                    if not offset:
                        raise SyntaxError("no block statement to terminate: ${%s}$" % part)
                    offset -= 1
                    part = part.lstrip()[1:]
                    if not part.endswith(":"):
                        continue
                elif self.auto_emit.match(part.lstrip()):
                    part = "emit(%s)" % part.lstrip()
                lines = part.splitlines()
                margin = min(len(l) - len(l.lstrip()) for l in lines if l.strip())
                part = "\n".join("\t" * offset + l[margin:] for l in lines)
                if part.endswith(":"):
                    offset += 1
            tokens.append(part)
        if offset:
            raise SyntaxError("%i block statement(s) not terminated" % offset)
        self.__code = compile("\n".join(tokens), "<templite %r>" % template[:20], "exec")

    def render(self, __namespace=None, **kw):
        namespace = {"_": _}
        if __namespace:
            namespace.update(__namespace)
        if kw:
            namespace.update(kw)
        namespace["emit"] = self.write

        __stdout = sys.stdout
        sys.stdout = self
        self.__output = []
        eval(self.__code, namespace)
        sys.stdout = __stdout
        return "".join(self.__output)

    def write(self, *args):
        for a in args:
            self.__output.append(str(a))

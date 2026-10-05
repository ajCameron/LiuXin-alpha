#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Compile and render small text templates with explicit context lookup.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise liuxin templite through a consuming regression::

        python -m pytest -q tests/scripts/test_docstring_migration.py
"""

from __future__ import annotations

import re
import sys

from LiuXin_alpha.utils.localization import trans as _


class Templite(object):
    """
    Provide the Templite utility contract with explicit state and cleanup behavior.

    Example:
        Exercise Templite through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py
    """
    auto_emit = re.compile(r"(^['\"])|(^[a-zA-Z0-9_\[\]'\"]+$)")

    def __init__(self, template, start="${", end="}$"):
        """
        Initialize and validate the Templite state.

        Example:
            Exercise Templite.  init   through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param template: Template expression parsed or evaluated.
        :param start: Value supplied for start under the utility contract.
        :param end: Value supplied for end under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
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
        """
        Perform the render utility operation under explicit compatibility rules.

        Example:
            Exercise Templite.render through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param __namespace: Value supplied for namespace under the utility contract.
        :param kw: Value supplied for kw under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Forward the write operation while preserving adapter ownership rules.

        Example:
            Exercise Templite.write through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for a in args:
            self.__output.append(str(a))

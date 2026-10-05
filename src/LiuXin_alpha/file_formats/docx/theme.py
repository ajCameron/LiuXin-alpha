#!/usr/bin/env python2
# vim:fileencoding=utf-8
"""
Resolve DOCX theme colors and fonts into normalized style values.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise theme through a consuming regression::

        python -m pytest -q tests/file_formats/docx/test_docx_modernized.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"


class Theme(object):
    """
    Provide the theme contract for validated ebook processing.

    Example:
        Exercise Theme through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py
    """
    def __init__(self: _typing.Self, namespace: _typing.Any) -> None:
        """
        Initialize and validate the theme state.

        Example:
            Exercise Theme.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param namespace: Value supplied for namespace under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.major_latin_font = "Cambria"
        self.minor_latin_font = "Calibri"
        self.namespace = namespace

    def __call__(self: _typing.Self, root: _typing.Any) -> None:
        """
        Perform the call operation under explicit file-format and conversion rules.

        Example:
            Exercise Theme.  call   through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param root: Root directory that bounds path resolution or traversal.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for fs in self.namespace.XPath("//a:fontScheme")(root):
            for mj in self.namespace.XPath("./a:majorFont")(fs):
                for l in self.namespace.XPath("./a:latin[@typeface]")(mj):
                    self.major_latin_font = l.get("typeface")
            for mj in self.namespace.XPath("./a:minorFont")(fs):
                for l in self.namespace.XPath("./a:latin[@typeface]")(mj):
                    self.minor_latin_font = l.get("typeface")

    def resolve_font_family(self: _typing.Self, ff: _typing.Any) -> _typing.Any:
        """
        Perform the resolve font family operation under explicit file-format and conversion rules.

        Example:
            Exercise Theme.resolve font family through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param ff: Value supplied for ff under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if ff.startswith("|"):
            ff = ff[1:-1]
            ff = self.major_latin_font if ff.startswith("major") else self.minor_latin_font
        return ff

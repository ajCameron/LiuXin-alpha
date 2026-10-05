#!/usr/bin/env python2
# vim:fileencoding=utf-8

"""
Read DOCX document settings that affect layout and conversion behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise settings through a consuming regression::

        python -m pytest -q tests/file_formats/docx/test_docx_modernized.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"


class Settings(object):
    """
    Provide the settings contract for validated ebook processing.

    Example:
        Exercise Settings through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py
    """
    def __init__(self: _typing.Self, namespace: _typing.Any) -> None:
        """
        Initialize and validate the settings state.

        Example:
            Exercise Settings.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param namespace: Value supplied for namespace under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.default_tab_stop = 720 / 20
        self.namespace = namespace

    def __call__(self: _typing.Self, root: _typing.Any) -> None:
        """
        Perform the call operation under explicit file-format and conversion rules.

        Example:
            Exercise Settings.  call   through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param root: Root directory that bounds path resolution or traversal.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for dts in self.namespace.XPath("//w:defaultTabStop[@w:val]")(root):
            try:
                self.default_tab_stop = int(self.namespace.get(dts, "w:val")) / 20
            except (ValueError, TypeError, AttributeError):
                pass

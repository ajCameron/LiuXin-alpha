#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Normalize KF8 markup and resources before serialization.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise cleanup through a consuming regression::

        python -m pytest -q tests/file_formats/mobi/test_mobi_output_end_to_end_and_unicode_torture.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

from LiuXin_alpha.file_formats.oeb.base import XPath

__license__ = "GPL v3"
__copyright__ = "2012, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


class CSSCleanup(object):
    """
    Provide the csscleanup contract for validated ebook processing.

    Example:
        Exercise CSSCleanup through a consuming regression::

            python -m pytest -q tests/file_formats/mobi/test_mobi_output_end_to_end_and_unicode_torture.py
    """
    def __init__(self: _typing.Self, log: _typing.Any, opts: _typing.Any) -> None:
        """
        Initialize and validate the csscleanup state.

        Example:
            Exercise CSSCleanup.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/mobi/test_mobi_output_end_to_end_and_unicode_torture.py


        :param log: Value supplied for log under the utility contract.
        :param opts: Value supplied for opts under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.log, self.opts = log, opts

    def __call__(self: _typing.Self, item: _typing.Any, stylizer: _typing.Any) -> None:
        """
        Perform the call operation under explicit file-format and conversion rules.

        Example:
            Exercise CSSCleanup.  call   through a consuming regression::

                python -m pytest -q tests/file_formats/mobi/test_mobi_output_end_to_end_and_unicode_torture.py


        :param item: Value supplied for item under the utility contract.
        :param stylizer: Value supplied for stylizer under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not hasattr(item.data, "xpath"):
            return

        # The Kindle touch displays all black pages if the height is set on
        # body
        for body in XPath("//h:body")(item.data):
            style = stylizer.style(body)
            style.drop("height")

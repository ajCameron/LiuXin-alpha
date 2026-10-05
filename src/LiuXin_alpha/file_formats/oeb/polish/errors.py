#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:fdm=marker:ai

"""
Define failures raised while polishing EPUB/OEB containers.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise errors through a consuming regression::

        python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

from LiuXin_alpha.file_formats import DRMError as _DRMError

from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"
__docformat__ = "restructuredtext en"


class InvalidBook(ValueError):
    """
    Provide the invalidbook contract for validated ebook processing.

    Example:
        Exercise InvalidBook through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    pass


class DRMError(_DRMError):
    """
    Report a drmerror encountered while processing an ebook format.

    Example:
        Exercise DRMError through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the drmerror state.

        Example:
            Exercise DRMError.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :return: None; validated state is stored on the receiving object.
        """
        super(DRMError, self).__init__(_("This file is locked with DRM. It cannot be edited."))


class MalformedMarkup(ValueError):
    """
    Provide the malformedmarkup contract for validated ebook processing.

    Example:
        Exercise MalformedMarkup through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    pass

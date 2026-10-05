#!/usr/bin/env  python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Expose the supported mobi compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/mobi/test_mobi_modernized.py
"""
from __future__ import annotations
__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal <kovid at kovidgoyal.net>"


class MobiError(Exception):
    """
    Report a mobierror encountered while processing an ebook format.

    Example:
        Exercise MobiError through a consuming regression::

            python -m pytest -q tests/file_formats/mobi/test_mobi_modernized.py
    """
    pass


# That might be a bit small on the PW, but Amazon/KG 2.5 still uses these values, even when delivered to a PW
MAX_THUMB_SIZE = 16 * 1024
MAX_THUMB_DIMEN = (180, 240)

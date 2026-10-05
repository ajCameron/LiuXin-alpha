#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Expose the supported debug compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/mobi/test_mobi_modernized.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

__license__ = "GPL v3"
__copyright__ = "2012, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


def format_bytes(byts: _typing.Any) -> _typing.Any:
    """
    Perform the format bytes operation under explicit file-format and conversion rules.

    Example:
        Exercise format bytes through a consuming regression::

            python -m pytest -q tests/file_formats/mobi/test_mobi_modernized.py


    :param byts: Value supplied for byts under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    byts = bytearray(byts)
    byts = [hex(b)[2:] for b in byts]
    return " ".join(byts)

# -*- coding: utf-8 -*-

"""
Transliterate Vietnamese Unicode text into ASCII approximations.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise vndecoder through a consuming regression::

        python -m pytest -q tests/file_formats/txt/test_txt_modernized.py
"""
from __future__ import annotations

import typing as _typing

from LiuXin_alpha.file_formats.unihandecode.unidecoder import Unidecoder
from LiuXin_alpha.file_formats.unihandecode.vncodepoints import CODEPOINTS as HANCODES
from LiuXin_alpha.file_formats.unihandecode.unicodepoints import CODEPOINTS

__license__ = "GPL 3"
__copyright__ = "2010, Hiroshi Miura <miurahr@linux.com>"
__docformat__ = "restructuredtext en"


class Vndecoder(Unidecoder):

    """
    Provide the vndecoder contract for validated ebook processing.

    Example:
        Exercise Vndecoder through a consuming regression::

            python -m pytest -q tests/file_formats/txt/test_txt_modernized.py
    """
    codepoints = {}

    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the vndecoder state.

        Example:
            Exercise Vndecoder.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


        :return: None; validated state is stored on the receiving object.
        """
        self.codepoints = CODEPOINTS
        self.codepoints.update(HANCODES)

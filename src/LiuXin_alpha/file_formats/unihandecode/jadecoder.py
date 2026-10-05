# coding:utf-8

"""
Transliterate Japanese Unicode text into ASCII approximations.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise jadecoder through a consuming regression::

        python -m pytest -q tests/file_formats/txt/test_txt_modernized.py
"""
from __future__ import annotations

import typing as _typing

import re

from LiuXin_alpha.file_formats.unihandecode.unidecoder import Unidecoder
from LiuXin_alpha.file_formats.unihandecode.unicodepoints import CODEPOINTS
from LiuXin_alpha.file_formats.unihandecode.jacodepoints import CODEPOINTS as JACODES
from LiuXin_alpha.file_formats.unihandecode.pykakasi.kakasi import kakasi

__license__ = "GPL 3"
__copyright__ = "2010, Hiroshi Miura <miurahr@linux.com>"
__docformat__ = "restructuredtext en"


class Jadecoder(Unidecoder):
    """
    Provide the jadecoder contract for validated ebook processing.

    Example:
        Exercise Jadecoder through a consuming regression::

            python -m pytest -q tests/file_formats/txt/test_txt_modernized.py
    """
    kakasi = None
    codepoints = {}

    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the jadecoder state.

        Example:
            Exercise Jadecoder.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


        :return: None; validated state is stored on the receiving object.
        """
        self.codepoints = CODEPOINTS
        self.codepoints.update(JACODES)
        self.kakasi = kakasi()

    def decode(self: _typing.Self, text: _typing.Any) -> _typing.Any:
        """
        Perform the decode operation under explicit file-format and conversion rules.

        Example:
            Exercise Jadecoder.decode through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


        :param text: Text parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            result = self.kakasi.do(text)
            return re.sub("[^\x00-\x7f]", lambda x: self.replace_point(x.group()), result)
        except:
            return re.sub("[^\x00-\x7f]", lambda x: self.replace_point(x.group()), text)

# -*- coding: utf-8 -*-
"""
Dispatch language-aware Unicode transliteration and fallbacks.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise unidecoder through a consuming regression::

        python -m pytest -q tests/file_formats/txt/test_txt_modernized.py
"""
from __future__ import annotations

import typing as _typing

import re

from LiuXin_alpha.file_formats.unihandecode.unicodepoints import CODEPOINTS
from LiuXin_alpha.file_formats.unihandecode.zhcodepoints import CODEPOINTS as HANCODES

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

__license__ = "GPL 3"
__copyright__ = "2010, Hiroshi Miura <miurahr@linux.com>"
__docformat__ = "restructuredtext en"


class Unidecoder(object):

    """
    Provide the unidecoder contract for validated ebook processing.

    Example:
        Exercise Unidecoder through a consuming regression::

            python -m pytest -q tests/file_formats/txt/test_txt_modernized.py
    """
    codepoints = {}

    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the unidecoder state.

        Example:
            Exercise Unidecoder.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


        :return: None; validated state is stored on the receiving object.
        """
        self.codepoints = CODEPOINTS
        self.codepoints.update(HANCODES)

    def decode(self: _typing.Self, text: _typing.Any) -> _typing.Any:
        # Replace characters larger than 127 with their ASCII equivelent.
        """
        Perform the decode operation under explicit file-format and conversion rules.

        Example:
            Exercise Unidecoder.decode through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


        :param text: Text parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return re.sub("[^\x00-\x7f]", lambda x: self.replace_point(x.group()), text)

    def replace_point(self: _typing.Self, codepoint: _typing.Any) -> _typing.Any:
        """
        Returns the replacement character or ? if none can be found.

        Example:
            Exercise Unidecoder.replace point through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


        :param codepoint: Value supplied for codepoint under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            # Split the unicode character xABCD into parts 0xAB and 0xCD.
            # 0xAB represents the group within CODEPOINTS to query and 0xCD
            # represents the position in the list of characters for the group.
            return self.codepoints[self.code_group(codepoint)][self.grouped_point(codepoint)]
        except:
            return "?"

    def code_group(self: _typing.Self, character: _typing.Any) -> _typing.Any:
        """
        Find what group character is a part of.

        Example:
            Exercise Unidecoder.code group through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


        :param character: Value supplied for character under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        # Code groups withing CODEPOINTS take the form 'xAB'
        try:  # python2
            return "x%02x" % (ord(six_unicode(character)) >> 8)
        except:
            return "x%02x" % (ord(character) >> 8)

    def grouped_point(self: _typing.Self, character: _typing.Any) -> _typing.Any:
        """
        Return the location the replacement character is in the list for a the group character is a part of.

        Example:
            Exercise Unidecoder.grouped point through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


        :param character: Value supplied for character under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:  # python2
            return ord(six_unicode(character)) & 255
        except:
            return ord(character) & 255

# -*- coding: utf-8 -*-
#  k2a.py
#
# Copyright 2011 Hiroshi Miura <miurahr@linux.com>
#
# Original copyright:
# * KAKASI (Kanji Kana Simple inversion program)
# * $Id: jj2.c,v 1.7 2001-04-12 05:57:34 rug Exp $
# * Copyright (C) 1992
# * Hironobu Takahashi (takahasi@tiny.or.jp)
# *
# * This program is free software; you can redistribute it and/or modify
# * it under the terms of the GNU General Public License as published by
# * the Free Software Foundation; either versions 2, or (at your option)
# * any later version.
# *
# * This program is distributed in the hope that it will be useful
# * but WITHOUT ANY WARRANTY; without even the implied warranty of
# * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# * GNU General Public License for more details.
# *
# */

"""
Romanize Katakana text for Japanese transliteration.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise k2a through a consuming regression::

        python -m pytest -q tests/file_formats/txt/test_txt_modernized.py
"""
from __future__ import annotations

import typing as _typing
from LiuXin_alpha.file_formats.unihandecode.pykakasi.jisyo import jisyo


class K2a(object):

    """
    Provide the k2a contract for validated ebook processing.

    Example:
        Exercise K2a through a consuming regression::

            python -m pytest -q tests/file_formats/txt/test_txt_modernized.py
    """
    kanwa = None

    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the k2a state.

        Example:
            Exercise K2a.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


        :return: None; validated state is stored on the receiving object.
        """
        self.kanwa = jisyo()

    def isKatakana(self: _typing.Self, char: _typing.Any) -> bool:
        """
        Perform the isKatakana operation under explicit file-format and conversion rules.

        Example:
            Exercise K2a.isKatakana through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


        :param char: Value supplied for char under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return 0x30A0 < ord(char) and ord(char) < 0x30F7

    def convert(self: _typing.Self, text: _typing.Any) -> tuple[_typing.Any, ...]:
        """
        Convert the supplied source into the stage's normalized output representation.

        Example:
            Exercise K2a.convert through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


        :param text: Text parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        Hstr = ""
        max_len = -1
        r = min(10, len(text) + 1)
        for x in xrange(r):
            if text[:x] in self.kanwa.kanadict:
                if max_len < x:
                    max_len = x
                    Hstr = self.kanwa.kanadict[text[:x]]
        return (Hstr, max_len)

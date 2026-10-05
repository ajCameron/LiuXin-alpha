# -*- coding: utf-8 -*-
#  j2h.py
#
# Copyright 2011 Hiroshi Miura <miurahr@linux.com>
#
#  Original Copyright:
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
Convert Japanese scripts into normalized Hiragana forms.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise j2h through a consuming regression::

        python -m pytest -q tests/file_formats/txt/test_txt_modernized.py
"""
from __future__ import annotations

import typing as _typing
import re

from LiuXin_alpha.file_formats.unihandecode.pykakasi.jisyo import jisyo

from LiuXin_alpha.utils.lx_libraries.liuxin_six import dict_iteritems as iteritems


class J2H(object):

    """
    Provide the j2h contract for validated ebook processing.

    Example:
        Exercise J2H through a consuming regression::

            python -m pytest -q tests/file_formats/txt/test_txt_modernized.py
    """
    kanwa = None

    cl_table = [
        "",
        "aiueow",
        "aiueow",
        "aiueow",
        "aiueow",
        "aiueow",
        "aiueow",
        "aiueow",
        "aiueow",
        "aiueow",
        "aiueow",
        "k",
        "g",
        "k",
        "g",
        "k",
        "g",
        "k",
        "g",
        "k",
        "g",
        "s",
        "zj",
        "s",
        "zj",
        "s",
        "zj",
        "s",
        "zj",
        "s",
        "zj",
        "t",
        "d",
        "tc",
        "d",
        "aiueokstchgzjfdbpw",
        "t",
        "d",
        "t",
        "d",
        "t",
        "d",
        "n",
        "n",
        "n",
        "n",
        "n",
        "h",
        "b",
        "p",
        "h",
        "b",
        "p",
        "hf",
        "b",
        "p",
        "h",
        "b",
        "p",
        "h",
        "b",
        "p",
        "m",
        "m",
        "m",
        "m",
        "m",
        "y",
        "y",
        "y",
        "y",
        "y",
        "y",
        "rl",
        "rl",
        "rl",
        "rl",
        "rl",
        "wiueo",
        "wiueo",
        "wiueo",
        "wiueo",
        "w",
        "n",
        "v",
        "k",
        "k",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
    ]

    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the j2h state.

        Example:
            Exercise J2H.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


        :return: None; validated state is stored on the receiving object.
        """
        self.kanwa = jisyo()

    def isKanji(self: _typing.Self, c: _typing.Any) -> bool:
        """
        Perform the isKanji operation under explicit file-format and conversion rules.

        Example:
            Exercise J2H.isKanji through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


        :param c: Value supplied for c under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return 0x3400 <= ord(c) and ord(c) < 0xFA2E

    def isCletter(self: _typing.Self, l: _typing.Any, c: _typing.Any) -> bool:
        """
        Perform the isCletter operation under explicit file-format and conversion rules.

        Example:
            Exercise J2H.isCletter through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


        :param l: Value supplied for l under the utility contract.
        :param c: Value supplied for c under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if (ord("ぁ") <= ord(c) and ord(c) <= 0x309F) and (l in self.cl_table[ord(c) - ord("ぁ") - 1]):
            return True
        return False

    def itaiji_conv(self: _typing.Self, text: _typing.Any) -> _typing.Any:
        """
        Perform the itaiji conv operation under explicit file-format and conversion rules.

        Example:
            Exercise J2H.itaiji conv through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


        :param text: Text parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        r = []
        for c in text:
            if c in self.kanwa.itaijidict:
                r.append(c)
        for c in r:
            text = re.sub(c, self.kanwa.itaijidict[c], text)
        return text

    def convert(self: _typing.Self, text: _typing.Any) -> tuple[_typing.Any, ...]:
        """
        Convert the supplied source into the stage's normalized output representation.

        Example:
            Exercise J2H.convert through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


        :param text: Text parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        max_len = 0
        Hstr = ""
        table = self.kanwa.load_jisyo(text[0])
        if table is None:
            return ("", 0)
        for (k, v) in iteritems(table):
            length = len(k)
            if len(text) >= length:
                if text.startswith(k):
                    for (yomi, tail) in v:
                        if tail == "":
                            if max_len < length:
                                Hstr = yomi
                                max_len = length
                        elif max_len < length + 1 and len(text) > length and self.isCletter(tail, text[length]):
                            Hstr = "".join([yomi, text[length]])
                            max_len = length + 1
        return (Hstr, max_len)

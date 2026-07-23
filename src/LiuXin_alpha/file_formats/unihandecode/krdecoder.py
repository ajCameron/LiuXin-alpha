# -*- coding: utf-8 -*-

"""
Decode unicode text to an ASCII representation of the text in Korean.
Based on unidecoder.
"""
from __future__ import annotations

import typing as _typing

from LiuXin_alpha.file_formats.unihandecode.unidecoder import Unidecoder
from LiuXin_alpha.file_formats.unihandecode.krcodepoints import CODEPOINTS as HANCODES
from LiuXin_alpha.file_formats.unihandecode.unicodepoints import CODEPOINTS

__license__ = "GPL 3"
__copyright__ = "2010, Hiroshi Miura <miurahr@linux.com>"
__docformat__ = "restructuredtext en"


class Krdecoder(Unidecoder):

    codepoints = {}

    def __init__(self: _typing.Self) -> None:
        self.codepoints = CODEPOINTS
        self.codepoints.update(HANCODES)

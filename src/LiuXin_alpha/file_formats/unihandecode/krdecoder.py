# -*- coding: utf-8 -*-

"""
Decode unicode text to an ASCII representation of the text in Korean.
Based on unidecoder.
"""

from LiuXin.file_formats.unihandecode.unidecoder import Unidecoder
from LiuXin.file_formats.unihandecode.krcodepoints import CODEPOINTS as HANCODES
from LiuXin.file_formats.unihandecode.unicodepoints import CODEPOINTS

__license__ = "GPL 3"
__copyright__ = "2010, Hiroshi Miura <miurahr@linux.com>"
__docformat__ = "restructuredtext en"


class Krdecoder(Unidecoder):

    codepoints = {}

    def __init__(self):
        self.codepoints = CODEPOINTS
        self.codepoints.update(HANCODES)

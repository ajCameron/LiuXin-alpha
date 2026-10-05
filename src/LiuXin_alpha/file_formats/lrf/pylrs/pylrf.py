#!/usr/bin/env python
"""
Serialize PyLRS document structures into LRF binary objects and streams.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise pylrf through a consuming regression::

        python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
"""
from __future__ import annotations

import typing as _typing
import struct
import zlib
import codecs
import os

from LiuXin_alpha.file_formats.lrf.pylrs.pylrfopt import tagListOptimizer

# Py2/Py3 compatibility
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems
from LiuXin_alpha.utils.libraries.liuxin_six import six_cStringIO

PYLRF_VERSION = "1.0"

#
# Acknowledgement:
#   This software would not have been possible without the pioneering
#   efforts of the author of lrf2lrs.py, Igor Skochinsky.
#
# Copyright (c) 2007 Mike Higgins (Falstaff)
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

#
# Change History:
#
# V1.0 06 Feb 2007
# Initial Release.

#
# Current limitations and bugs:
#   Never "scrambles" any streams (even if asked to).  This does not seem
#   to hurt anything.
#
#   Not based on any official documentation, so many assumptions had to be made.
#
#   Can be used to create lrf files that can lock up an eBook reader.
#   This is your only warning.
#
#   Unsupported objects: Canvas, Window, PopUpWindow, Sound, Import,
#                        SoundStream, ObjectInfo
#
#   The only button type supported is JumpButton.
#
#   Unsupported tags: SoundStop, Wait, pos on BlockSpace (and those used by
#                     unsupported objects).
#
#   Tags supporting Japanese text and Asian layout have not been tested.
#
#   Tested on Python 2.4 and 2.5, Windows XP and Sony PRS-500.
#
#   Commented even less than pylrs, but not very useful when called directly,
#   anyway.
#


class LrfError(Exception):
    """
    Report a lrferror encountered while processing an ebook format.

    Example:
        Exercise LrfError through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    pass


def writeByte(f: _typing.Any, byte: _typing.Any) -> None:
    """
    Perform the writeByte operation under explicit file-format and conversion rules.

    Example:
        Exercise writeByte through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


    :param f: Value supplied for f under the utility contract.
    :param byte: Value supplied for byte under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    f.write(struct.pack("<B", byte))


def writeWord(f: _typing.Any, word: _typing.Any) -> None:
    """
    Perform the writeWord operation under explicit file-format and conversion rules.

    Example:
        Exercise writeWord through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


    :param f: Value supplied for f under the utility contract.
    :param word: Value supplied for word under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    if int(word) > 65535:
        raise LrfError("Cannot encode a number greater than 65535 in a word.")
    if int(word) < 0:
        raise LrfError("Cannot encode a number < 0 in a word: " + str(word))
    f.write(struct.pack("<H", int(word)))


def writeSignedWord(f: _typing.Any, sword: _typing.Any) -> None:
    """
    Perform the writeSignedWord operation under explicit file-format and conversion rules.

    Example:
        Exercise writeSignedWord through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


    :param f: Value supplied for f under the utility contract.
    :param sword: Value supplied for sword under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    f.write(struct.pack("<h", int(float(sword))))


def writeWords(f: _typing.Any, *words: _typing.Any) -> None:
    """
    Perform the writeWords operation under explicit file-format and conversion rules.

    Example:
        Exercise writeWords through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


    :param f: Value supplied for f under the utility contract.
    :param words: Value supplied for words under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    f.write(struct.pack("<%dH" % len(words), *words))


def writeDWord(f: _typing.Any, dword: _typing.Any) -> None:
    """
    Perform the writeDWord operation under explicit file-format and conversion rules.

    Example:
        Exercise writeDWord through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


    :param f: Value supplied for f under the utility contract.
    :param dword: Value supplied for dword under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    f.write(struct.pack("<I", int(dword)))


def writeDWords(f: _typing.Any, *dwords: _typing.Any) -> None:
    """
    Perform the writeDWords operation under explicit file-format and conversion rules.

    Example:
        Exercise writeDWords through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


    :param f: Value supplied for f under the utility contract.
    :param dwords: Value supplied for dwords under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    f.write(struct.pack("<%dI" % len(dwords), *dwords))


def writeQWord(f: _typing.Any, qword: _typing.Any) -> None:
    """
    Perform the writeQWord operation under explicit file-format and conversion rules.

    Example:
        Exercise writeQWord through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


    :param f: Value supplied for f under the utility contract.
    :param qword: Value supplied for qword under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    f.write(struct.pack("<Q", qword))


def writeZeros(f: _typing.Any, nZeros: _typing.Any) -> None:
    """
    Perform the writeZeros operation under explicit file-format and conversion rules.

    Example:
        Exercise writeZeros through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


    :param f: Value supplied for f under the utility contract.
    :param nZeros: Value supplied for nZeros under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    f.write(b"\x00" * nZeros)


def writeString(f: _typing.Any, str: _typing.Any) -> None:
    """
    Perform the writeString operation under explicit file-format and conversion rules.

    Example:
        Exercise writeString through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


    :param f: Value supplied for f under the utility contract.
    :param str: Value supplied for str under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    if isinstance(str, (bytes, bytearray, memoryview)):
        f.write(bytes(str))
    else:
        f.write(str.encode("latin-1", "replace"))


def writeIdList(f: _typing.Any, idList: _typing.Any) -> None:
    """
    Perform the writeIdList operation under explicit file-format and conversion rules.

    Example:
        Exercise writeIdList through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


    :param f: Value supplied for f under the utility contract.
    :param idList: Value supplied for idList under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    writeWord(f, len(idList))
    writeDWords(f, *idList)


def writeColor(f: _typing.Any, color: _typing.Any) -> None:
    # TODO: allow color names, web format
    """
    Perform the writeColor operation under explicit file-format and conversion rules.

    Example:
        Exercise writeColor through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


    :param f: Value supplied for f under the utility contract.
    :param color: Value supplied for color under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    f.write(struct.pack(">I", int(color, 0)))


def writeLineWidth(f: _typing.Any, width: _typing.Any) -> None:
    """
    Perform the writeLineWidth operation under explicit file-format and conversion rules.

    Example:
        Exercise writeLineWidth through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


    :param f: Value supplied for f under the utility contract.
    :param width: Value supplied for width under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    writeWord(f, int(width))


def writeUnicode(f: _typing.Any, string: _typing.Any, encoding: _typing.Any) -> None:
    """
    Perform the writeUnicode operation under explicit file-format and conversion rules.

    Example:
        Exercise writeUnicode through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


    :param f: Value supplied for f under the utility contract.
    :param string: Value supplied for string under the utility contract.
    :param encoding: Value supplied for encoding under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    if isinstance(string, (bytes, bytearray, memoryview)):
        string = bytes(string).decode(encoding, "replace")
    elif not isinstance(string, str):
        string = str(string)
    string = string.encode("utf-16-le")
    length = len(string)
    if length > 65535:
        raise LrfError("Cannot write strings longer than 65535 characters.")
    writeWord(f, length)
    writeString(f, string)


def writeRaw(f: _typing.Any, string: _typing.Any, encoding: _typing.Any) -> None:
    """
    Perform the writeRaw operation under explicit file-format and conversion rules.

    Example:
        Exercise writeRaw through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


    :param f: Value supplied for f under the utility contract.
    :param string: Value supplied for string under the utility contract.
    :param encoding: Value supplied for encoding under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    if isinstance(string, (bytes, bytearray, memoryview)):
        string = bytes(string).decode(encoding, "replace")
    elif not isinstance(string, str):
        string = str(string)

    string = string.encode("utf-16-le")
    writeString(f, string)


def writeRubyAA(f: _typing.Any, rubyAA: _typing.Any) -> None:
    """
    Perform the writeRubyAA operation under explicit file-format and conversion rules.

    Example:
        Exercise writeRubyAA through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


    :param f: Value supplied for f under the utility contract.
    :param rubyAA: Value supplied for rubyAA under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    ralign, radjust = rubyAA
    radjust = {"line-edge": 0x10, "none": 0}[radjust]
    ralign = {"start": 1, "center": 2}[ralign]
    writeWord(f, ralign | radjust)


def writeBgImage(f: _typing.Any, bgInfo: _typing.Any) -> None:
    """
    Perform the writeBgImage operation under explicit file-format and conversion rules.

    Example:
        Exercise writeBgImage through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


    :param f: Value supplied for f under the utility contract.
    :param bgInfo: Value supplied for bgInfo under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    imode, iid = bgInfo
    imode = {"pfix": 0, "fix": 1, "tile": 2, "centering": 3}[imode]
    writeWord(f, imode)
    writeDWord(f, iid)


def writeEmpDots(f: _typing.Any, dotsInfo: _typing.Any, encoding: _typing.Any) -> None:
    """
    Perform the writeEmpDots operation under explicit file-format and conversion rules.

    Example:
        Exercise writeEmpDots through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


    :param f: Value supplied for f under the utility contract.
    :param dotsInfo: Value supplied for dotsInfo under the utility contract.
    :param encoding: Value supplied for encoding under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    ref_dots_font, dots_font_name, dots_code = dotsInfo
    writeDWord(f, ref_dots_font)
    LrfTag("fontfacename", dots_font_name).write(f, encoding)
    writeWord(f, int(dots_code, 0))


def writeRuledLine(f: _typing.Any, lineInfo: _typing.Any) -> None:
    """
    Perform the writeRuledLine operation under explicit file-format and conversion rules.

    Example:
        Exercise writeRuledLine through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


    :param f: Value supplied for f under the utility contract.
    :param lineInfo: Value supplied for lineInfo under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    line_length, line_type, line_width, lineColor = lineInfo
    writeWord(f, line_length)
    writeWord(f, LINE_TYPE_ENCODING[line_type])
    writeWord(f, line_width)
    writeColor(f, lineColor)


LRF_SIGNATURE = "L\x00R\x00F\x00\x00\x00"

# XOR_KEY = 48
XOR_KEY = 65024  # that's what lrf2lrs says -- not used, anyway...

LRF_VERSION = 1000  # is 999 for librie? lrf2lrs uses 1000

IMAGE_TYPE_ENCODING = dict(GIF=0x14, PNG=0x12, BMP=0x13, JPEG=0x11, JPG=0x11)

OBJECT_TYPE_ENCODING = dict(
    PageTree=0x01,
    Page=0x02,
    Header=0x03,
    Footer=0x04,
    PageAtr=0x05,
    PageStyle=0x05,
    Block=0x06,
    BlockAtr=0x07,
    BlockStyle=0x07,
    MiniPage=0x08,
    TextBlock=0x0A,
    Text=0x0A,
    TextAtr=0x0B,
    TextStyle=0x0B,
    ImageBlock=0x0C,
    Image=0x0C,
    Canvas=0x0D,
    ESound=0x0E,
    ImageStream=0x11,
    Import=0x12,
    Button=0x13,
    Window=0x14,
    PopUpWindow=0x15,
    Sound=0x16,
    SoundStream=0x17,
    Font=0x19,
    ObjectInfo=0x1A,
    BookAtr=0x1C,
    BookStyle=0x1C,
    SimpleTextBlock=0x1D,
    TOC=0x1E,
)

LINE_TYPE_ENCODING = {
    "none": 0,
    "solid": 0x10,
    "dashed": 0x20,
    "double": 0x30,
    "dotted": 0x40,
}

BINDING_DIRECTION_ENCODING = dict(Lr=1, Rl=16)


TAG_INFO = dict(
    rawtext=(0, writeRaw),
    ObjectStart=(0xF500, "<IH"),
    ObjectEnd=(0xF501,),
    # InfoLink (0xF502)
    Link=(0xF503, "<I"),
    StreamSize=(0xF504, writeDWord),
    StreamData=(0xF505, writeString),
    StreamEnd=(0xF506,),
    oddheaderid=(0xF507, writeDWord),
    evenheaderid=(0xF508, writeDWord),
    oddfooterid=(0xF509, writeDWord),
    evenfooterid=(0xF50A, writeDWord),
    ObjectList=(0xF50B, writeIdList),
    fontsize=(0xF511, writeSignedWord),
    fontwidth=(0xF512, writeSignedWord),
    fontescapement=(0xF513, writeSignedWord),
    fontorientation=(0xF514, writeSignedWord),
    fontweight=(0xF515, writeWord),
    fontfacename=(0xF516, writeUnicode),
    textcolor=(0xF517, writeColor),
    textbgcolor=(0xF518, writeColor),
    wordspace=(0xF519, writeSignedWord),
    letterspace=(0xF51A, writeSignedWord),
    baselineskip=(0xF51B, writeSignedWord),
    linespace=(0xF51C, writeSignedWord),
    parindent=(0xF51D, writeSignedWord),
    parskip=(0xF51E, writeSignedWord),
    # F51F, F520
    topmargin=(0xF521, writeWord),
    headheight=(0xF522, writeWord),
    headsep=(0xF523, writeWord),
    oddsidemargin=(0xF524, writeWord),
    textheight=(0xF525, writeWord),
    textwidth=(0xF526, writeWord),
    canvaswidth=(0xF551, writeWord),
    canvasheight=(0xF552, writeWord),
    footspace=(0xF527, writeWord),
    footheight=(0xF528, writeWord),
    bgimage=(0xF529, writeBgImage),
    setemptyview=(0xF52A, {"show": 1, "empty": 0}, writeWord),
    pageposition=(0xF52B, {"any": 0, "upper": 1, "lower": 2}, writeWord),
    evensidemargin=(0xF52C, writeWord),
    framemode=(0xF52E, {"None": 0, "curve": 2, "square": 1}, writeWord),
    blockwidth=(0xF531, writeWord),
    blockheight=(0xF532, writeWord),
    blockrule=(
        0xF533,
        {
            "horz-fixed": 0x14,
            "horz-adjustable": 0x12,
            "vert-fixed": 0x41,
            "vert-adjustable": 0x21,
            "block-fixed": 0x44,
            "block-adjustable": 0x22,
        },
        writeWord,
    ),
    bgcolor=(0xF534, writeColor),
    layout=(0xF535, {"TbRl": 0x41, "LrTb": 0x34}, writeWord),
    framewidth=(0xF536, writeWord),
    framecolor=(0xF537, writeColor),
    topskip=(0xF538, writeWord),
    sidemargin=(0xF539, writeWord),
    footskip=(0xF53A, writeWord),
    align=(0xF53C, {"head": 1, "center": 4, "foot": 8}, writeWord),
    column=(0xF53D, writeWord),
    columnsep=(0xF53E, writeSignedWord),
    minipagewidth=(0xF541, writeWord),
    minipageheight=(0xF542, writeWord),
    yspace=(0xF546, writeWord),
    xspace=(0xF547, writeWord),
    PutObj=(0xF549, "<HHI"),
    ImageRect=(0xF54A, "<HHHH"),
    ImageSize=(0xF54B, "<HH"),
    RefObjId=(0xF54C, "<I"),
    PageDiv=(0xF54E, "<HIHI"),
    StreamFlags=(0xF554, writeWord),
    Comment=(0xF555, writeUnicode),
    FontFilename=(0xF559, writeUnicode),
    PageList=(0xF55C, writeIdList),
    FontFacename=(0xF55D, writeUnicode),
    buttonflags=(0xF561, writeWord),
    PushButtonStart=(0xF566,),
    PushButtonEnd=(0xF567,),
    buttonactions=(0xF56A,),
    endbuttonactions=(0xF56B,),
    jumpto=(0xF56C, "<II"),
    RuledLine=(0xF573, writeRuledLine),
    rubyaa=(0xF575, writeRubyAA),
    rubyoverhang=(0xF576, {"none": 0, "auto": 1}, writeWord),
    empdotsposition=(0xF577, {"before": 1, "after": 2}, writeWord),
    empdots=(0xF578, writeEmpDots),
    emplineposition=(0xF579, {"before": 1, "after": 2}, writeWord),
    emplinetype=(0xF57A, LINE_TYPE_ENCODING, writeWord),
    ChildPageTree=(0xF57B, "<I"),
    ParentPageTree=(0xF57C, "<I"),
    Italic=(0xF581,),
    ItalicEnd=(0xF582,),
    pstart=(0xF5A1, writeDWord),  # what goes in the dword? refesound
    pend=(0xF5A2,),
    CharButton=(0xF5A7, writeDWord),
    CharButtonEnd=(0xF5A8,),
    Rubi=(0xF5A9,),
    RubiEnd=(0xF5AA,),
    Oyamoji=(0xF5AB,),
    OyamojiEnd=(0xF5AC,),
    Rubimoji=(0xF5AD,),
    RubimojiEnd=(0xF5AE,),
    Yoko=(0xF5B1,),
    YokoEnd=(0xF5B2,),
    Tate=(0xF5B3,),
    TateEnd=(0xF5B4,),
    Nekase=(0xF5B5,),
    NekaseEnd=(0xF5B6,),
    Sup=(0xF5B7,),
    SupEnd=(0xF5B8,),
    Sub=(0xF5B9,),
    SubEnd=(0xF5BA,),
    NoBR=(0xF5BB,),
    NoBREnd=(0xF5BC,),
    EmpDots=(0xF5BD,),
    EmpDotsEnd=(0xF5BE,),
    EmpLine=(0xF5C1,),
    EmpLineEnd=(0xF5C2,),
    DrawChar=(0xF5C3, "<H"),
    DrawCharEnd=(0xF5C4,),
    Box=(0xF5C6, LINE_TYPE_ENCODING, writeWord),
    BoxEnd=(0xF5C7,),
    Space=(0xF5CA, writeSignedWord),
    textstring=(0xF5CC, writeUnicode),
    Plot=(0xF5D1, "<HHII"),
    CR=(0xF5D2,),
    RegisterFont=(0xF5D8, writeDWord),
    setwaitprop=(0xF5DA, {"replay": 1, "noreplay": 2}, writeWord),
    charspace=(0xF5DD, writeSignedWord),
    textlinewidth=(0xF5F1, writeLineWidth),
    linecolor=(0xF5F2, writeColor),
)


class ObjectTableEntry(object):
    """
    Provide the objecttableentry contract for validated ebook processing.

    Example:
        Exercise ObjectTableEntry through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    def __init__(self: _typing.Self, objId: _typing.Any, offset: _typing.Any, size: _typing.Any) -> None:
        """
        Initialize and validate the objecttableentry state.

        Example:
            Exercise ObjectTableEntry.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param objId: Value supplied for objId under the utility contract.
        :param offset: Value supplied for offset under the utility contract.
        :param size: Value supplied for size under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.objId = objId
        self.offset = offset
        self.size = size

    def write(self: _typing.Self, f: _typing.Any) -> None:
        """
        Perform the write operation under explicit file-format and conversion rules.

        Example:
            Exercise ObjectTableEntry.write through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param f: Value supplied for f under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        writeDWords(f, self.objId, self.offset, self.size, 0)


class LrfTag(object):
    """
    Provide the lrftag contract for validated ebook processing.

    Example:
        Exercise LrfTag through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    def __init__(self: _typing.Self, name: _typing.Any, *parameters: _typing.Any) -> None:
        """
        Initialize and validate the lrftag state.

        Example:
            Exercise LrfTag.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param parameters: Value supplied for parameters under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        try:
            tag_info = TAG_INFO[name]
        except KeyError:
            raise LrfError("tag name %s not recognized" % name)

        self.name = name
        self.type = tag_info[0]
        self.format = tag_info[1:]

        if len(parameters) > 1:
            raise LrfError("only one parameter allowed on tag %s" % name)

        if len(parameters) == 0:
            self.parameter = None
        else:
            self.parameter = parameters[0]

    def write(self: _typing.Self, lrf: _typing.Any, encoding: _typing.Any = None) -> None:
        """
        Perform the write operation under explicit file-format and conversion rules.

        Example:
            Exercise LrfTag.write through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrf: Value supplied for lrf under the utility contract.
        :param encoding: Value supplied for encoding under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.type != 0:
            writeWord(lrf, self.type)

        p = self.parameter
        if p is None:
            return

        # print "   Writing tag", self.name
        for f in self.format:
            if isinstance(f, dict):
                p = f[p]
            elif isinstance(f, str):
                if isinstance(p, tuple):
                    writeString(lrf, struct.pack(f, *p))
                else:
                    writeString(lrf, struct.pack(f, p))
            else:
                if f in [writeUnicode, writeRaw, writeEmpDots]:
                    if encoding is None:
                        raise LrfError("Tag requires encoding")
                    f(lrf, p, encoding)
                else:
                    f(lrf, p)


STREAM_SCRAMBLED = 0x200
STREAM_COMPRESSED = 0x100
STREAM_FORCE_COMPRESSED = 0x8100
STREAM_TOC = 0x0051


class LrfStreamBase(object):
    """
    Provide the lrfstreambase contract for validated ebook processing.

    Example:
        Exercise LrfStreamBase through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    def __init__(self: _typing.Self, streamFlags: _typing.Any, streamData: _typing.Any = None) -> None:
        """
        Initialize and validate the lrfstreambase state.

        Example:
            Exercise LrfStreamBase.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param streamFlags: Value supplied for streamFlags under the utility contract.
        :param streamData: Value supplied for streamData under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.streamFlags = streamFlags
        self.streamData = streamData

    def setStreamData(self: _typing.Self, streamData: _typing.Any) -> None:
        """
        Perform the setStreamData operation under explicit file-format and conversion rules.

        Example:
            Exercise LrfStreamBase.setStreamData through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param streamData: Value supplied for streamData under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.streamData = streamData

    def getStreamTags(self: _typing.Self, optimize: bool = False) -> list[_typing.Any]:
        # tags:
        #   StreamFlags
        #   StreamSize
        #   StreamStart
        #   (data)
        #   StreamEnd
        #
        # if flags & 0x200, stream is scrambled
        # if flags & 0x100, stream is compressed

        """
        Perform the getStreamTags operation under explicit file-format and conversion rules.

        Example:
            Exercise LrfStreamBase.getStreamTags through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param optimize: Value supplied for optimize under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        flags = self.streamFlags
        stream_buffer = self.streamData

        # implement scramble?  I never scramble anything...

        if flags & STREAM_FORCE_COMPRESSED == STREAM_FORCE_COMPRESSED:
            optimize = False

        if flags & STREAM_COMPRESSED == STREAM_COMPRESSED:
            uncomp_len = len(stream_buffer)
            comp_stream_buffer = zlib.compress(stream_buffer)
            if optimize and uncomp_len <= len(comp_stream_buffer) + 4:
                flags &= ~STREAM_COMPRESSED
            else:
                stream_buffer = struct.pack("<I", uncomp_len) + comp_stream_buffer

        return [
            LrfTag("StreamFlags", flags & 0x01FF),
            LrfTag("StreamSize", len(stream_buffer)),
            LrfTag("StreamData", stream_buffer),
            LrfTag("StreamEnd"),
        ]


class LrfTagStream(LrfStreamBase):
    """
    Provide the lrftagstream contract for validated ebook processing.

    Example:
        Exercise LrfTagStream through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    def __init__(self: _typing.Self, streamFlags: _typing.Any, streamTags: _typing.Any = None) -> None:
        """
        Initialize and validate the lrftagstream state.

        Example:
            Exercise LrfTagStream.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param streamFlags: Value supplied for streamFlags under the utility contract.
        :param streamTags: Value supplied for streamTags under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        LrfStreamBase.__init__(self, streamFlags)
        if streamTags is None:
            self.tags = []
        else:
            self.tags = streamTags[:]

    def appendLrfTag(self: _typing.Self, tag: _typing.Any) -> None:
        """
        Perform the appendLrfTag operation under explicit file-format and conversion rules.

        Example:
            Exercise LrfTagStream.appendLrfTag through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param tag: Value supplied for tag under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.tags.append(tag)

    def getStreamTags(self: _typing.Self, encoding: _typing.Any, optimizeTags: bool = False, optimizeCompression: bool = False) -> _typing.Any:
        """
        Perform the getStreamTags operation under explicit file-format and conversion rules.

        Example:
            Exercise LrfTagStream.getStreamTags through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param encoding: Value supplied for encoding under the utility contract.
        :param optimizeTags: Value supplied for optimizeTags under the utility contract.
        :param optimizeCompression: Value supplied for optimizeCompression under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        stream = six_cStringIO()
        if optimizeTags:
            tagListOptimizer(self.tags)

        for tag in self.tags:
            tag.write(stream, encoding)

        self.streamData = stream.getvalue()
        stream.close()
        return LrfStreamBase.getStreamTags(self, optimize=optimizeCompression)


class LrfFileStream(LrfStreamBase):
    """
    Provide the lrffilestream contract for validated ebook processing.

    Example:
        Exercise LrfFileStream through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    def __init__(self: _typing.Self, streamFlags: _typing.Any, filename: _typing.Any) -> None:
        """
        Initialize and validate the lrffilestream state.

        Example:
            Exercise LrfFileStream.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param streamFlags: Value supplied for streamFlags under the utility contract.
        :param filename: Filename used for type inference or archive output.
        :return: None; validated state is stored on the receiving object.
        """
        LrfStreamBase.__init__(self, streamFlags)
        with open(filename, "rb") as open_file:
            self.streamData = open_file.read()


class LrfObject(object):
    """
    Provide the lrfobject contract for validated ebook processing.

    Example:
        Exercise LrfObject through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    def __init__(self: _typing.Self, name: _typing.Any, objId: _typing.Any) -> None:
        """
        Initialize and validate the lrfobject state.

        Example:
            Exercise LrfObject.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param objId: Value supplied for objId under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        if objId <= 0:
            raise LrfError("invalid objId for " + name)

        self.name = name
        self.objId = objId
        self.tags = []
        try:
            self.type = OBJECT_TYPE_ENCODING[name]
        except KeyError:
            raise LrfError("object name %s not recognized" % name)

    def __str__(self: _typing.Self) -> _typing.Any:
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise LrfObject.  str   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "LRFObject: " + self.name + ", " + str(self.objId)

    def appendLrfTag(self: _typing.Self, tag: _typing.Any) -> None:
        """
        Perform the appendLrfTag operation under explicit file-format and conversion rules.

        Example:
            Exercise LrfObject.appendLrfTag through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param tag: Value supplied for tag under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.tags.append(tag)

    def appendLrfTags(self: _typing.Self, tagList: _typing.Any) -> None:
        """
        Perform the appendLrfTags operation under explicit file-format and conversion rules.

        Example:
            Exercise LrfObject.appendLrfTags through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param tagList: Value supplied for tagList under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.tags.extend(tagList)

    # deprecated old name
    append = appendLrfTag

    def appendTagDict(self: _typing.Self, tagDict: _typing.Any, genClass: _typing.Any = None) -> None:
        #
        # This code does not really belong here, I think.  But it
        # belongs somewhere, so here it is.
        #
        """
        Perform the appendTagDict operation under explicit file-format and conversion rules.

        Example:
            Exercise LrfObject.appendTagDict through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param tagDict: Value supplied for tagDict under the utility contract.
        :param genClass: Value supplied for genClass under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        composites = {}
        for name, value in iteritems(tagDict):
            if name == "rubyAlignAndAdjust":
                continue
            if name in {
                "bgimagemode",
                "bgimageid",
                "rubyalign",
                "rubyadjust",
                "empdotscode",
                "empdotsfontname",
                "refempdotsfont",
            }:
                composites[name] = value
            else:
                self.append(LrfTag(name, value))

        if "rubyalign" in composites or "rubyadjust" in composites:
            ralign = composites.get("rubyalign", "none")
            radjust = composites.get("rubyadjust", "start")
            self.append(LrfTag("rubyaa", (ralign, radjust)))

        if "bgimagemode" in composites or "bgimageid" in composites:
            imode = composites.get("bgimagemode", "fix")
            iid = composites.get("bgimageid", 0)

            # for some reason, page style uses 0 for "fix" we call this pfix to differentiate it
            if genClass == "PageStyle" and imode == "fix":
                imode = "pfix"

            self.append(LrfTag("bgimage", (imode, iid)))

        if "empdotscode" in composites or "empdotsfontname" in composites or "refempdotsfont" in composites:
            dotscode = composites.get("empdotscode", "0x002E")
            dotsfontname = composites.get("empdotsfontname", "Dutch801 Rm BT Roman")
            refdotsfont = composites.get("refempdotsfont", 0)
            self.append(LrfTag("empdots", (refdotsfont, dotsfontname, dotscode)))

    def write(self: _typing.Self, lrf: _typing.Any, encoding: _typing.Any = None) -> None:
        # print "Writing object", self.name
        """
        Perform the write operation under explicit file-format and conversion rules.

        Example:
            Exercise LrfObject.write through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrf: Value supplied for lrf under the utility contract.
        :param encoding: Value supplied for encoding under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        LrfTag("ObjectStart", (self.objId, self.type)).write(lrf)

        for tag in self.tags:
            tag.write(lrf, encoding)

        LrfTag("ObjectEnd").write(lrf)


class LrfToc(LrfObject):
    """
    Table of contents. Format of toc is: [ (pageid, objid, string)...]

    Example:
        Exercise LrfToc through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """

    def __init__(self: _typing.Self, objId: _typing.Any, toc: _typing.Any, se: _typing.Any) -> None:
        """
        Initialize and validate the lrftoc state.

        Example:
            Exercise LrfToc.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param objId: Value supplied for objId under the utility contract.
        :param toc: Value supplied for toc under the utility contract.
        :param se: Value supplied for se under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        LrfObject.__init__(self, "TOC", objId)
        streamData = self._makeTocStream(toc, se)
        self._makeStreamTags(streamData)

    def _makeStreamTags(self: _typing.Self, streamData: _typing.Any) -> None:
        """
        Perform the makeStreamTags operation under explicit file-format and conversion rules.

        Example:
            Exercise LrfToc. makeStreamTags through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param streamData: Value supplied for streamData under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        stream = LrfStreamBase(STREAM_TOC, streamData)
        self.tags.extend(stream.getStreamTags())

    def _makeTocStream(self: _typing.Self, toc: _typing.Any, se: _typing.Any) -> _typing.Any:
        """
        Perform the makeTocStream operation under explicit file-format and conversion rules.

        Example:
            Exercise LrfToc. makeTocStream through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param toc: Value supplied for toc under the utility contract.
        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        stream = six_cStringIO()
        nEntries = len(toc)

        writeDWord(stream, nEntries)

        lastOffset = 0
        writeDWord(stream, lastOffset)
        for i in range(nEntries - 1):
            pageId, objId, label = toc[i]
            entryLen = 4 + 4 + 2 + len(label) * 2
            lastOffset += entryLen
            writeDWord(stream, lastOffset)

        for entry in toc:
            pageId, objId, label = entry
            if pageId <= 0:
                raise LrfError("page id invalid in toc: " + label)
            if objId <= 0:
                raise LrfError("textblock id invalid in toc: " + label)

            writeDWord(stream, pageId)
            writeDWord(stream, objId)
            writeUnicode(stream, label, se)

        streamData = stream.getvalue()
        stream.close()
        return streamData


class LrfWriter(object):
    """
    Provide the lrfwriter contract for validated ebook processing.

    Example:
        Exercise LrfWriter through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    def __init__(self: _typing.Self, sourceEncoding: _typing.Any) -> None:
        """
        Initialize and validate the lrfwriter state.

        Example:
            Exercise LrfWriter.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param sourceEncoding: Value supplied for sourceEncoding under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.sourceEncoding = sourceEncoding

        # The following flags are just to have a place to remember these
        # values.  The flags must still be passed to the appropriate classes
        # in order to have them work.

        self.saveStreamTags = False  # used only in testing -- hogs memory

        # highly experimental -- set to True at your own risk
        self.optimizeTags = False
        self.optimizeCompression = False

        # End of placeholders

        self.rootObjId = 0
        self.rootObj = None
        self.binding = 1  # 1=front to back, 16=back to front
        self.dpi = 1600
        self.width = 600
        self.height = 800
        self.colorDepth = 24
        self.tocObjId = 0
        self.docInfoXml = ""
        self.thumbnailEncoding = "JPEG"
        self.thumbnailData = b""
        self.objects = []
        self.objectTable = []

    def getSourceEncoding(self: _typing.Self) -> _typing.Any:
        """
        Perform the getSourceEncoding operation under explicit file-format and conversion rules.

        Example:
            Exercise LrfWriter.getSourceEncoding through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.sourceEncoding

    def toUnicode(self: _typing.Self, string: _typing.Any) -> _typing.Any:
        """
        Perform the toUnicode operation under explicit file-format and conversion rules.

        Example:
            Exercise LrfWriter.toUnicode through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param string: Value supplied for string under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if isinstance(string, (bytes, bytearray, memoryview)):
            string = bytes(string).decode(self.sourceEncoding, "replace")
        elif not isinstance(string, str):
            string = str(string)

        return string

    def getDocInfoXml(self: _typing.Self) -> _typing.Any:
        """
        Perform the getDocInfoXml operation under explicit file-format and conversion rules.

        Example:
            Exercise LrfWriter.getDocInfoXml through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.docInfoXml

    def setPageTreeId(self: _typing.Self, objId: _typing.Any) -> None:
        """
        Perform the setPageTreeId operation under explicit file-format and conversion rules.

        Example:
            Exercise LrfWriter.setPageTreeId through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param objId: Value supplied for objId under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.pageTreeId = objId

    def getPageTreeId(self: _typing.Self) -> _typing.Any:
        """
        Perform the getPageTreeId operation under explicit file-format and conversion rules.

        Example:
            Exercise LrfWriter.getPageTreeId through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.pageTreeId

    def setRootObject(self: _typing.Self, obj: _typing.Any) -> None:
        """
        Perform the setRootObject operation under explicit file-format and conversion rules.

        Example:
            Exercise LrfWriter.setRootObject through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param obj: Value supplied for obj under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.rootObjId != 0:
            raise LrfError("root object already set")

        self.rootObjId = obj.objId
        self.rootObj = obj

    def registerFontId(self: _typing.Self, id: _typing.Any) -> None:
        """
        Perform the registerFontId operation under explicit file-format and conversion rules.

        Example:
            Exercise LrfWriter.registerFontId through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param id: Value supplied for id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.rootObj is None:
            raise LrfError("can't register font -- no root object")

        self.rootObj.append(LrfTag("RegisterFont", id))

    def setTocObject(self: _typing.Self, obj: _typing.Any) -> None:
        """
        Perform the setTocObject operation under explicit file-format and conversion rules.

        Example:
            Exercise LrfWriter.setTocObject through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param obj: Value supplied for obj under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.tocObjId != 0:
            raise LrfError("toc object already set")

        self.tocObjId = obj.objId

    def setThumbnailFile(self: _typing.Self, filename: _typing.Any, encoding: _typing.Any = None) -> None:
        """
        Perform the setThumbnailFile operation under explicit file-format and conversion rules.

        Example:
            Exercise LrfWriter.setThumbnailFile through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param filename: Filename used for type inference or archive output.
        :param encoding: Value supplied for encoding under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        with open(filename, "rb") as thumb_file:
            self.thumbnailData = thumb_file.read()

        if encoding is None:
            encoding = os.path.splitext(filename)[1][1:]

        encoding = encoding.upper()
        if encoding not in IMAGE_TYPE_ENCODING:
            raise LrfError("unknown image type: " + encoding)

        self.thumbnailEncoding = encoding

    def append(self: _typing.Self, obj: _typing.Any) -> None:
        """
        Perform the append operation under explicit file-format and conversion rules.

        Example:
            Exercise LrfWriter.append through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param obj: Value supplied for obj under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.objects.append(obj)

    def addLrfObject(self: _typing.Self, objId: _typing.Any) -> None:
        """
        Perform the addLrfObject operation under explicit file-format and conversion rules.

        Example:
            Exercise LrfWriter.addLrfObject through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param objId: Value supplied for objId under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def writeFile(self: _typing.Self, lrf: _typing.Any) -> None:
        """
        Perform the writeFile operation under explicit file-format and conversion rules.

        Example:
            Exercise LrfWriter.writeFile through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrf: Value supplied for lrf under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.rootObjId == 0:
            raise LrfError("no root object has been set")

        self.writeHeader(lrf)
        self.writeObjects(lrf)
        self.updateObjectTableOffset(lrf)
        self.updateTocObjectOffset(lrf)
        self.writeObjectTable(lrf)

    def writeHeader(self: _typing.Self, lrf: _typing.Any) -> None:
        """
        Perform the writeHeader operation under explicit file-format and conversion rules.

        Example:
            Exercise LrfWriter.writeHeader through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrf: Value supplied for lrf under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        writeString(lrf, LRF_SIGNATURE)
        writeWord(lrf, LRF_VERSION)
        writeWord(lrf, XOR_KEY)
        writeDWord(lrf, self.rootObjId)
        writeQWord(lrf, len(self.objects))
        writeQWord(lrf, 0)  # 0x18 objectTableOffset -- will be updated
        writeZeros(lrf, 4)  # 0x20 unknown
        writeWord(lrf, self.binding)
        writeDWord(lrf, self.dpi)
        writeWords(lrf, self.width, self.height, self.colorDepth)
        writeZeros(lrf, 20)  # 0x30 unknown
        writeDWord(lrf, self.tocObjId)
        writeDWord(lrf, 0)  # 0x48 tocObjectOffset -- will be updated
        doc_info_xml = codecs.BOM_LE + self.docInfoXml.encode("utf-16-le")
        comp_doc_info = zlib.compress(doc_info_xml)
        writeWord(lrf, len(comp_doc_info) + 4)
        writeWord(lrf, IMAGE_TYPE_ENCODING[self.thumbnailEncoding])
        writeDWord(lrf, len(self.thumbnailData))
        writeDWord(lrf, len(doc_info_xml))
        writeString(lrf, comp_doc_info)
        writeString(lrf, self.thumbnailData)

    def writeObjects(self: _typing.Self, lrf: _typing.Any) -> None:
        # also appends object entries to the object table
        """
        Perform the writeObjects operation under explicit file-format and conversion rules.

        Example:
            Exercise LrfWriter.writeObjects through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrf: Value supplied for lrf under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.objectTable = []
        for obj in self.objects:
            obj_start = lrf.tell()
            obj.write(lrf, self.sourceEncoding)
            obj_end = lrf.tell()
            self.objectTable.append(ObjectTableEntry(obj.objId, obj_start, obj_end - obj_start))

    def updateObjectTableOffset(self: _typing.Self, lrf: _typing.Any) -> None:
        # update the offset of the object table
        """
        Perform the updateObjectTableOffset operation under explicit file-format and conversion rules.

        Example:
            Exercise LrfWriter.updateObjectTableOffset through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrf: Value supplied for lrf under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        table_offset = lrf.tell()
        lrf.seek(0x18, 0)
        writeQWord(lrf, table_offset)
        lrf.seek(0, 2)

    def updateTocObjectOffset(self: _typing.Self, lrf: _typing.Any) -> None:
        """
        Perform the updateTocObjectOffset operation under explicit file-format and conversion rules.

        Example:
            Exercise LrfWriter.updateTocObjectOffset through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrf: Value supplied for lrf under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.tocObjId == 0:
            return

        for entry in self.objectTable:
            if entry.objId == self.tocObjId:
                lrf.seek(0x48, 0)
                writeDWord(lrf, entry.offset)
                lrf.seek(0, 2)
                break
        else:
            raise LrfError("toc object not in object table")

    def writeObjectTable(self: _typing.Self, lrf: _typing.Any) -> None:
        """
        Perform the writeObjectTable operation under explicit file-format and conversion rules.

        Example:
            Exercise LrfWriter.writeObjectTable through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrf: Value supplied for lrf under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for tableEntry in self.objectTable:
            tableEntry.write(lrf)

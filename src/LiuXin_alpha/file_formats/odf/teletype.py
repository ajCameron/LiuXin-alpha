# -*- coding: utf-8 -*-
#
#   Create and extract text from ODF, handling whitespace correctly.
#   Copyright (C) 2008 J. David Eisenberg
#
#   This program is free software; you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation; either version 2 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License along
#   with this program; if not, write to the Free Software Foundation, Inc.,
#   51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.


"""
Extract and replace plain text within ODF element trees.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise teletype through a consuming regression::

        python -m pytest -q tests/file_formats/odf/test_odf_full_stack_unicode_torture.py
"""
from __future__ import annotations

import typing as _typing

from LiuXin_alpha.file_formats.odf.element import Node
import LiuXin_alpha.file_formats.odf.opendocument
from LiuXin_alpha.file_formats.odf.text import S, LineBreak, Tab


class WhitespaceText(object):
    """
    Provide the whitespacetext contract for validated ebook processing.

    Example:
        Exercise WhitespaceText through a consuming regression::

            python -m pytest -q tests/file_formats/odf/test_odf_full_stack_unicode_torture.py
    """
    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the whitespacetext state.

        Example:
            Exercise WhitespaceText.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_full_stack_unicode_torture.py


        :return: None; validated state is stored on the receiving object.
        """
        self.textBuffer = []
        self.spaceCount = 0

    def addTextToElement(self: _typing.Self, odfElement: _typing.Any, s: _typing.Any) -> None:
        """
        Process an input string, inserting <text:tab> elements for ' ', <text:line-break> elements for ' ', and <text:s> elements for runs of more than one blank. These will be added to the given element.

        Example:
            Exercise WhitespaceText.addTextToElement through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_full_stack_unicode_torture.py


        :param odfElement: Value supplied for odfElement under the utility contract.
        :param s: Value supplied for s under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        i = 0
        ch = " "

        # When we encounter a tab or newline, we can immediately
        # dump any accumulated text and then emit the appropriate
        # ODF element.
        #
        # When we encounter a space, we add it to the text buffer,
        # and then collect more spaces.  If there are more spaces
        # after the first one, we dump the text buffer and then
        # then emit the appropriate <text:s> element.

        while i < len(s):
            ch = s[i]
            if ch == "\t":
                self._emitTextBuffer(odfElement)
                odfElement.addElement(Tab())
                i += 1
            elif ch == "\n":
                self._emitTextBuffer(odfElement)
                odfElement.addElement(LineBreak())
                i += 1
            elif ch == " ":
                self.textBuffer.append(" ")
                i += 1
                self.spaceCount = 0
                while i < len(s) and (s[i] == " "):
                    self.spaceCount += 1
                    i += 1
                if self.spaceCount > 0:
                    self._emitTextBuffer(odfElement)
                    self._emitSpaces(odfElement)
            else:
                self.textBuffer.append(ch)
                i += 1

        self._emitTextBuffer(odfElement)

    def _emitTextBuffer(self: _typing.Self, odfElement: _typing.Any) -> None:
        """
        Creates a Text Node whose contents are the current textBuffer. Side effect: clears the text buffer.

        Example:
            Exercise WhitespaceText. emitTextBuffer through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_full_stack_unicode_torture.py


        :param odfElement: Value supplied for odfElement under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if len(self.textBuffer) > 0:
            odfElement.addText("".join(self.textBuffer))
        self.textBuffer = []

    def _emitSpaces(self: _typing.Self, odfElement: _typing.Any) -> None:
        """
        Creates a <text:s> element for the current spaceCount. Side effect: sets spaceCount back to zero

        Example:
            Exercise WhitespaceText. emitSpaces through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_full_stack_unicode_torture.py


        :param odfElement: Value supplied for odfElement under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.spaceCount > 0:
            spaceElement = S(c=self.spaceCount)
            odfElement.addElement(spaceElement)
        self.spaceCount = 0


def addTextToElement(odfElement: _typing.Any, s: _typing.Any) -> None:
    """
    Perform the addTextToElement operation under explicit file-format and conversion rules.

    Example:
        Exercise addTextToElement through a consuming regression::

            python -m pytest -q tests/file_formats/odf/test_odf_full_stack_unicode_torture.py


    :param odfElement: Value supplied for odfElement under the utility contract.
    :param s: Value supplied for s under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    wst = WhitespaceText()
    wst.addTextToElement(odfElement, s)


def extractText(odfElement: _typing.Any) -> _typing.Any:
    """
    Extract text content from an Element, with whitespace represented properly. Returns the text, with tabs, spaces, and newlines correctly evaluated. This method recursively descends through the children of the given element, accumulating text and "unwrapping" <text:s>, <text:tab>, and <text:line-break> elements along the way.

    Example:
        Exercise extractText through a consuming regression::

            python -m pytest -q tests/file_formats/odf/test_odf_full_stack_unicode_torture.py


    :param odfElement: Value supplied for odfElement under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    result = []

    if len(odfElement.childNodes) != 0:
        for child in odfElement.childNodes:
            if child.nodeType == Node.TEXT_NODE:
                result.append(child.data)
            elif child.nodeType == Node.ELEMENT_NODE:
                subElement = child
                tagName = subElement.qname
                if tagName == (
                    "urn:oasis:names:tc:opendocument:xmlns:text:1.0",
                    "line-break",
                ):
                    result.append("\n")
                elif tagName == (
                    "urn:oasis:names:tc:opendocument:xmlns:text:1.0",
                    "tab",
                ):
                    result.append("\t")
                elif tagName == (
                    "urn:oasis:names:tc:opendocument:xmlns:text:1.0",
                    "s",
                ):
                    c = subElement.getAttribute("c")
                    if c:
                        spaceCount = int(c)
                    else:
                        spaceCount = 1

                    result.append(" " * spaceCount)
                else:
                    result.append(extractText(subElement))
    return "".join(result)

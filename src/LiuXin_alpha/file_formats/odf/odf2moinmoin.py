# -*- coding: utf-8 -*-
# Copyright (C) 2006-2008 Søren Roug, European Environment Agency
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
#
# See http://trac.edgewall.org/wiki/WikiFormatting
#
# Contributor(s):
#

"""
Translate ODF content into MoinMoin wiki markup.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise odf2moinmoin through a consuming regression::

        python -m pytest -q tests/file_formats/odf/test_odf_modernized.py
"""
from __future__ import annotations

import typing as _typing
import sys, zipfile, xml.dom.minidom
from LiuXin_alpha.file_formats.odf.namespaces import nsdict
from LiuXin_alpha.file_formats.odf.elementtypes import *

IGNORED_TAGS = [
    "draw:a" "draw:g",
    "draw:line",
    "draw:object-ole",
    "office:annotation",
    "presentation:notes",
    "svg:desc",
] + [nsdict[item[0]] + ":" + item[1] for item in empty_elements]

INLINE_TAGS = [nsdict[item[0]] + ":" + item[1] for item in inline_elements]


class TextProps:
    """
    Holds properties for a text style.

    Example:
        Exercise TextProps through a consuming regression::

            python -m pytest -q tests/file_formats/odf/test_odf_modernized.py
    """

    def __init__(self: _typing.Self) -> None:

        """
        Initialize and validate the textprops state.

        Example:
            Exercise TextProps.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :return: None; validated state is stored on the receiving object.
        """
        self.italic = False
        self.bold = False
        self.fixed = False
        self.underlined = False
        self.strikethrough = False
        self.superscript = False
        self.subscript = False

    def setItalic(self: _typing.Self, value: _typing.Any) -> None:
        """
        Perform the setItalic operation under explicit file-format and conversion rules.

        Example:
            Exercise TextProps.setItalic through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param value: Value normalized, stored, formatted or returned.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if value == "italic":
            self.italic = True
        elif value == "normal":
            self.italic = False

    def setBold(self: _typing.Self, value: _typing.Any) -> None:
        """
        Perform the setBold operation under explicit file-format and conversion rules.

        Example:
            Exercise TextProps.setBold through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param value: Value normalized, stored, formatted or returned.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if value == "bold":
            self.bold = True
        elif value == "normal":
            self.bold = False

    def setFixed(self: _typing.Self, value: _typing.Any) -> None:
        """
        Perform the setFixed operation under explicit file-format and conversion rules.

        Example:
            Exercise TextProps.setFixed through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param value: Value normalized, stored, formatted or returned.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.fixed = value

    def setUnderlined(self: _typing.Self, value: _typing.Any) -> None:
        """
        Perform the setUnderlined operation under explicit file-format and conversion rules.

        Example:
            Exercise TextProps.setUnderlined through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param value: Value normalized, stored, formatted or returned.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if value and value != "none":
            self.underlined = True

    def setStrikethrough(self: _typing.Self, value: _typing.Any) -> None:
        """
        Perform the setStrikethrough operation under explicit file-format and conversion rules.

        Example:
            Exercise TextProps.setStrikethrough through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param value: Value normalized, stored, formatted or returned.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if value and value != "none":
            self.strikethrough = True

    def setPosition(self: _typing.Self, value: _typing.Any) -> None:
        """
        Perform the setPosition operation under explicit file-format and conversion rules.

        Example:
            Exercise TextProps.setPosition through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param value: Value normalized, stored, formatted or returned.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if value is None or value == "":
            return
        posisize = value.split(" ")
        textpos = posisize[0]
        if textpos.find("%") == -1:
            if textpos == "sub":
                self.superscript = False
                self.subscript = True
            elif textpos == "super":
                self.superscript = True
                self.subscript = False
        else:
            itextpos = int(textpos[: textpos.find("%")])
            if itextpos > 10:
                self.superscript = False
                self.subscript = True
            elif itextpos < -10:
                self.superscript = True
                self.subscript = False

    def __str__(self: _typing.Self) -> _typing.Any:

        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise TextProps.  str   through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "[italic=%s, bold=i%s, fixed=%s]" % (
            str(self.italic),
            str(self.bold),
            str(self.fixed),
        )


class ParagraphProps:
    """
    Holds properties of a paragraph style.

    Example:
        Exercise ParagraphProps through a consuming regression::

            python -m pytest -q tests/file_formats/odf/test_odf_modernized.py
    """

    def __init__(self: _typing.Self) -> None:

        """
        Initialize and validate the paragraphprops state.

        Example:
            Exercise ParagraphProps.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :return: None; validated state is stored on the receiving object.
        """
        self.blockquote = False
        self.headingLevel = 0
        self.code = False
        self.title = False
        self.indented = 0

    def setIndented(self: _typing.Self, value: _typing.Any) -> None:
        """
        Perform the setIndented operation under explicit file-format and conversion rules.

        Example:
            Exercise ParagraphProps.setIndented through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param value: Value normalized, stored, formatted or returned.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.indented = value

    def setHeading(self: _typing.Self, level: _typing.Any) -> None:
        """
        Perform the setHeading operation under explicit file-format and conversion rules.

        Example:
            Exercise ParagraphProps.setHeading through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param level: Value supplied for level under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.headingLevel = level

    def setTitle(self: _typing.Self, value: _typing.Any) -> None:
        """
        Perform the setTitle operation under explicit file-format and conversion rules.

        Example:
            Exercise ParagraphProps.setTitle through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param value: Value normalized, stored, formatted or returned.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.title = value

    def setCode(self: _typing.Self, value: _typing.Any) -> None:
        """
        Perform the setCode operation under explicit file-format and conversion rules.

        Example:
            Exercise ParagraphProps.setCode through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param value: Value normalized, stored, formatted or returned.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.code = value

    def __str__(self: _typing.Self) -> _typing.Any:

        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise ParagraphProps.  str   through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "[bq=%s, h=%d, code=%s]" % (
            str(self.blockquote),
            self.headingLevel,
            str(self.code),
        )


class ListProperties:
    """
    Holds properties for a list style.

    Example:
        Exercise ListProperties through a consuming regression::

            python -m pytest -q tests/file_formats/odf/test_odf_modernized.py
    """

    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the listproperties state.

        Example:
            Exercise ListProperties.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :return: None; validated state is stored on the receiving object.
        """
        self.ordered = False

    def setOrdered(self: _typing.Self, value: _typing.Any) -> None:
        """
        Perform the setOrdered operation under explicit file-format and conversion rules.

        Example:
            Exercise ListProperties.setOrdered through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param value: Value normalized, stored, formatted or returned.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.ordered = value


class ODF2MoinMoin(object):
    """
    Provide the odf2moinmoin contract for validated ebook processing.

    Example:
        Exercise ODF2MoinMoin through a consuming regression::

            python -m pytest -q tests/file_formats/odf/test_odf_modernized.py
    """
    def __init__(self: _typing.Self, filepath: _typing.Any) -> None:
        """
        Initialize and validate the odf2moinmoin state.

        Example:
            Exercise ODF2MoinMoin.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param filepath: Value supplied for filepath under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.footnotes = []
        self.footnoteCounter = 0
        self.textStyles = {"Standard": TextProps()}
        self.paragraphStyles = {"Standard": ParagraphProps()}
        self.listStyles = {}
        self.fixedFonts = []
        self.hasTitle = 0
        self.lastsegment = None

        # Tags
        self.elements = {
            "draw:page": self.textToString,
            "draw:frame": self.textToString,
            "draw:image": self.draw_image,
            "draw:text-box": self.textToString,
            "text:a": self.text_a,
            "text:note": self.text_note,
        }
        for tag in IGNORED_TAGS:
            self.elements[tag] = self.do_nothing

        for tag in INLINE_TAGS:
            self.elements[tag] = self.inline_markup
        self.elements["text:line-break"] = self.text_line_break
        self.elements["text:s"] = self.text_s
        self.elements["text:tab"] = self.text_tab

        self.load(filepath)

    def processFontDeclarations(self: _typing.Self, fontDecl: _typing.Any) -> None:
        """
        Extracts necessary font information from a font-declaration element.

        Example:
            Exercise ODF2MoinMoin.processFontDeclarations through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param fontDecl: Value supplied for fontDecl under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for fontFace in fontDecl.getElementsByTagName("style:font-face"):
            if fontFace.getAttribute("style:font-pitch") == "fixed":
                self.fixedFonts.append(fontFace.getAttribute("style:name"))

    def extractTextProperties(self: _typing.Self, style: _typing.Any, parent: _typing.Any = None) -> _typing.Any:
        """
        Extracts text properties from a style element.

        Example:
            Exercise ODF2MoinMoin.extractTextProperties through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param style: Value supplied for style under the utility contract.
        :param parent: Value supplied for parent under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        textProps = TextProps()

        if parent:
            parentProp = self.textStyles.get(parent, None)
            if parentProp:
                textProp = parentProp

        textPropEl = style.getElementsByTagName("style:text-properties")
        if not textPropEl:
            return textProps

        textPropEl = textPropEl[0]

        textProps.setItalic(textPropEl.getAttribute("fo:font-style"))
        textProps.setBold(textPropEl.getAttribute("fo:font-weight"))
        textProps.setUnderlined(textPropEl.getAttribute("style:text-underline-style"))
        textProps.setStrikethrough(textPropEl.getAttribute("style:text-line-through-style"))
        textProps.setPosition(textPropEl.getAttribute("style:text-position"))

        if textPropEl.getAttribute("style:font-name") in self.fixedFonts:
            textProps.setFixed(True)

        return textProps

    def extractParagraphProperties(self: _typing.Self, style: _typing.Any, parent: _typing.Any = None) -> _typing.Any:
        """
        Extracts paragraph properties from a style element.

        Example:
            Exercise ODF2MoinMoin.extractParagraphProperties through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param style: Value supplied for style under the utility contract.
        :param parent: Value supplied for parent under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        paraProps = ParagraphProps()

        name = style.getAttribute("style:name")

        if name.startswith("Heading_20_"):
            level = name[11:]
            try:
                level = int(level)
                paraProps.setHeading(level)
            except:
                level = 0

        if name == "Title":
            paraProps.setTitle(True)

        paraPropEl = style.getElementsByTagName("style:paragraph-properties")
        if paraPropEl:
            paraPropEl = paraPropEl[0]
            leftMargin = paraPropEl.getAttribute("fo:margin-left")
            if leftMargin:
                try:
                    leftMargin = float(leftMargin[:-2])
                    if leftMargin > 0.01:
                        paraProps.setIndented(True)
                except:
                    pass

        textProps = self.extractTextProperties(style)
        if textProps.fixed:
            paraProps.setCode(True)

        return paraProps

    def processStyles(self: _typing.Self, styleElements: _typing.Any) -> None:
        """
        Runs through "style" elements extracting necessary information.

        Example:
            Exercise ODF2MoinMoin.processStyles through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param styleElements: Value supplied for styleElements under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

        for style in styleElements:

            name = style.getAttribute("style:name")

            if name == "Standard":
                continue

            family = style.getAttribute("style:family")
            parent = style.getAttribute("style:parent-style-name")

            if family == "text":
                self.textStyles[name] = self.extractTextProperties(style, parent)

            elif family == "paragraph":
                self.paragraphStyles[name] = self.extractParagraphProperties(style, parent)
                self.textStyles[name] = self.extractTextProperties(style, parent)

    def processListStyles(self: _typing.Self, listStyleElements: _typing.Any) -> None:

        """
        Perform the processListStyles operation under explicit file-format and conversion rules.

        Example:
            Exercise ODF2MoinMoin.processListStyles through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param listStyleElements: Value supplied for listStyleElements under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for style in listStyleElements:
            name = style.getAttribute("style:name")

            prop = ListProperties()
            if style.hasChildNodes():
                subitems = [
                    el
                    for el in style.childNodes
                    if el.nodeType == xml.dom.Node.ELEMENT_NODE and el.tagName == "text:list-level-style-number"
                ]
                if len(subitems) > 0:
                    prop.setOrdered(True)

            self.listStyles[name] = prop

    def load(self: _typing.Self, filepath: _typing.Any) -> None:
        """
        Loads an ODT file.

        Example:
            Exercise ODF2MoinMoin.load through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param filepath: Value supplied for filepath under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

        zip = zipfile.ZipFile(filepath)

        styles_doc = xml.dom.minidom.parseString(zip.read("styles.xml"))
        fontfacedecls = styles_doc.getElementsByTagName("office:font-face-decls")
        if fontfacedecls:
            self.processFontDeclarations(fontfacedecls[0])
        self.processStyles(styles_doc.getElementsByTagName("style:style"))
        self.processListStyles(styles_doc.getElementsByTagName("text:list-style"))

        self.content = xml.dom.minidom.parseString(zip.read("content.xml"))
        fontfacedecls = self.content.getElementsByTagName("office:font-face-decls")
        if fontfacedecls:
            self.processFontDeclarations(fontfacedecls[0])

        self.processStyles(self.content.getElementsByTagName("style:style"))
        self.processListStyles(self.content.getElementsByTagName("text:list-style"))

    def compressCodeBlocks(self: _typing.Self, text: _typing.Any) -> _typing.Any:
        """
        Removes extra blank lines from code blocks.

        Example:
            Exercise ODF2MoinMoin.compressCodeBlocks through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param text: Text parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        return text
        lines = text.split("\n")
        buffer = []
        numLines = len(lines)
        for i in range(numLines):

            if (
                lines[i].strip()
                or i == numLines - 1
                or i == 0
                or not (lines[i - 1].startswith("    ") and lines[i + 1].startswith("    "))
            ):
                buffer.append("\n" + lines[i])

        return "".join(buffer)

    # -----------------------------------
    def do_nothing(self: _typing.Self, node: _typing.Any) -> str:
        """
        Perform the do nothing operation under explicit file-format and conversion rules.

        Example:
            Exercise ODF2MoinMoin.do nothing through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param node: Value supplied for node under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return ""

    def draw_image(self: _typing.Self, node: _typing.Any) -> _typing.Any:
        """
        Perform the draw image operation under explicit file-format and conversion rules.

        Example:
            Exercise ODF2MoinMoin.draw image through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param node: Value supplied for node under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        link = node.getAttribute("xlink:href")
        if link and link[:2] == "./":  # Indicates a sub-object, which isn't supported
            return "%s\n" % link
        if link and link[:9] == "Pictures/":
            link = link[9:]
        return "[[Image(%s)]]\n" % link

    def text_a(self: _typing.Self, node: _typing.Any) -> _typing.Any:
        """
        Perform the text a operation under explicit file-format and conversion rules.

        Example:
            Exercise ODF2MoinMoin.text a through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param node: Value supplied for node under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        text = self.textToString(node)
        link = node.getAttribute("xlink:href")
        if link.strip() == text.strip():
            return "[%s] " % link.strip()
        else:
            return "[%s %s] " % (link.strip(), text.strip())

    def text_line_break(self: _typing.Self, node: _typing.Any) -> str:
        """
        Perform the text line break operation under explicit file-format and conversion rules.

        Example:
            Exercise ODF2MoinMoin.text line break through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param node: Value supplied for node under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "[[BR]]"

    def text_note(self: _typing.Self, node: _typing.Any) -> _typing.Any:
        """
        Perform the text note operation under explicit file-format and conversion rules.

        Example:
            Exercise ODF2MoinMoin.text note through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param node: Value supplied for node under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        cite = node.getElementsByTagName("text:note-citation")[0].childNodes[0].nodeValue
        body = node.getElementsByTagName("text:note-body")[0].childNodes[0]
        self.footnotes.append((cite, self.textToString(body)))
        return "^%s^" % cite

    def text_s(self: _typing.Self, node: _typing.Any) -> _typing.Any:
        """
        Perform the text s operation under explicit file-format and conversion rules.

        Example:
            Exercise ODF2MoinMoin.text s through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param node: Value supplied for node under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            num = int(node.getAttribute("text:c"))
            return " " * num
        except:
            return " "

    def text_tab(self: _typing.Self, node: _typing.Any) -> str:
        """
        Perform the text tab operation under explicit file-format and conversion rules.

        Example:
            Exercise ODF2MoinMoin.text tab through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param node: Value supplied for node under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "    "

    def inline_markup(self: _typing.Self, node: _typing.Any) -> _typing.Any:
        """
        Perform the inline markup operation under explicit file-format and conversion rules.

        Example:
            Exercise ODF2MoinMoin.inline markup through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param node: Value supplied for node under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        text = self.textToString(node)

        if not text.strip():
            return ""  # don't apply styles to white space

        styleName = node.getAttribute("text:style-name")
        style = self.textStyles.get(styleName, TextProps())

        if style.fixed:
            return "`" + text + "`"

        mark = []
        if style:
            if style.italic:
                mark.append("''")
            if style.bold:
                mark.append("'''")
            if style.underlined:
                mark.append("__")
            if style.strikethrough:
                mark.append("~~")
            if style.superscript:
                mark.append("^")
            if style.subscript:
                mark.append(",,")
        revmark = mark[:]
        revmark.reverse()
        return "%s%s%s" % ("".join(mark), text, "".join(revmark))

    # -----------------------------------
    def listToString(self: _typing.Self, listElement: _typing.Any, indent: int = 0) -> _typing.Any:

        """
        Perform the listToString operation under explicit file-format and conversion rules.

        Example:
            Exercise ODF2MoinMoin.listToString through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param listElement: Value supplied for listElement under the utility contract.
        :param indent: Value supplied for indent under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.lastsegment = listElement.tagName
        buffer = []

        styleName = listElement.getAttribute("text:style-name")
        props = self.listStyles.get(styleName, ListProperties())

        i = 0
        for item in listElement.childNodes:
            buffer.append(" " * indent)
            i += 1
            if props.ordered:
                number = str(i)
                number = " " + number + ". "
                buffer.append(" 1. ")
            else:
                buffer.append(" * ")
            subitems = [el for el in item.childNodes if el.tagName in ["text:p", "text:h", "text:list"]]
            for subitem in subitems:
                if subitem.tagName == "text:list":
                    buffer.append("\n")
                    buffer.append(self.listToString(subitem, indent + 3))
                else:
                    buffer.append(self.paragraphToString(subitem, indent + 3))
                self.lastsegment = subitem.tagName
            self.lastsegment = item.tagName
            buffer.append("\n")

        return "".join(buffer)

    def tableToString(self: _typing.Self, tableElement: _typing.Any) -> _typing.Any:
        """
        MoinMoin uses || to delimit table cells

        Example:
            Exercise ODF2MoinMoin.tableToString through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param tableElement: Value supplied for tableElement under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        self.lastsegment = tableElement.tagName
        buffer = []

        for item in tableElement.childNodes:
            self.lastsegment = item.tagName
            if item.tagName == "table:table-header-rows":
                buffer.append(self.tableToString(item))
            if item.tagName == "table:table-row":
                buffer.append("\n||")
                for cell in item.childNodes:
                    buffer.append(self.inline_markup(cell))
                    buffer.append("||")
                    self.lastsegment = cell.tagName
        return "".join(buffer)

    def toString(self: _typing.Self) -> _typing.Any:
        """
        Converts the document to a string. FIXME: Result from second call differs from first call

        Example:
            Exercise ODF2MoinMoin.toString through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        body = self.content.getElementsByTagName("office:body")[0]
        text = body.childNodes[0]

        buffer = []

        paragraphs = [
            el
            for el in text.childNodes
            if el.tagName
            in [
                "draw:page",
                "text:p",
                "text:h",
                "text:section",
                "text:list",
                "table:table",
            ]
        ]

        for paragraph in paragraphs:
            if paragraph.tagName == "text:list":
                text = self.listToString(paragraph)
            elif paragraph.tagName == "text:section":
                text = self.textToString(paragraph)
            elif paragraph.tagName == "table:table":
                text = self.tableToString(paragraph)
            else:
                text = self.paragraphToString(paragraph)
            if text:
                buffer.append(text)

        if self.footnotes:

            buffer.append("----")
            for cite, body in self.footnotes:
                buffer.append("%s: %s" % (cite, body))

        buffer.append("")
        return self.compressCodeBlocks("\n".join(buffer))

    def textToString(self: _typing.Self, element: _typing.Any) -> _typing.Any:

        """
        Perform the textToString operation under explicit file-format and conversion rules.

        Example:
            Exercise ODF2MoinMoin.textToString through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param element: Value supplied for element under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        buffer = []

        for node in element.childNodes:

            if node.nodeType == xml.dom.Node.TEXT_NODE:
                buffer.append(node.nodeValue)

            elif node.nodeType == xml.dom.Node.ELEMENT_NODE:
                tag = node.tagName

                if tag in ("draw:text-box", "draw:frame"):
                    buffer.append(self.textToString(node))

                elif tag in ("text:p", "text:h"):
                    text = self.paragraphToString(node)
                    if text:
                        buffer.append(text)
                elif tag == "text:list":
                    buffer.append(self.listToString(node))
                else:
                    method = self.elements.get(tag)
                    if method:
                        buffer.append(method(node))
                    else:
                        buffer.append(" {" + tag + "} ")

        return "".join(buffer)

    def paragraphToString(self: _typing.Self, paragraph: _typing.Any, indent: int = 0) -> _typing.Any:

        """
        Perform the paragraphToString operation under explicit file-format and conversion rules.

        Example:
            Exercise ODF2MoinMoin.paragraphToString through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param paragraph: Value supplied for paragraph under the utility contract.
        :param indent: Value supplied for indent under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        dummyParaProps = ParagraphProps()

        style_name = paragraph.getAttribute("text:style-name")
        paraProps = self.paragraphStyles.get(style_name, dummyParaProps)
        text = self.inline_markup(paragraph)

        if paraProps and not paraProps.code:
            text = text.strip()

        if paragraph.tagName == "text:p" and self.lastsegment == "text:p":
            text = "\n" + text

        self.lastsegment = paragraph.tagName

        if paraProps.title:
            self.hasTitle = 1
            return "= " + text + " =\n"

        outlinelevel = paragraph.getAttribute("text:outline-level")
        if outlinelevel:

            level = int(outlinelevel)
            if self.hasTitle:
                level += 1

            if level >= 1:
                return "=" * level + " " + text + " " + "=" * level + "\n"

        elif paraProps.code:
            return "{{{\n" + text + "\n}}}\n"

        if paraProps.indented:
            return self.wrapParagraph(text, indent=indent, blockquote=True)

        else:
            return self.wrapParagraph(text, indent=indent)

    def wrapParagraph(self: _typing.Self, text: _typing.Any, indent: int = 0, blockquote: bool = False) -> _typing.Any:

        """
        Perform the wrapParagraph operation under explicit file-format and conversion rules.

        Example:
            Exercise ODF2MoinMoin.wrapParagraph through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param text: Text parsed, normalized or rendered.
        :param indent: Value supplied for indent under the utility contract.
        :param blockquote: Value supplied for blockquote under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        counter = 0
        buffer = []
        LIMIT = 50

        if blockquote:
            buffer.append("  ")

        return "".join(buffer) + text
        # Unused from here
        for token in text.split():

            if counter > LIMIT - indent:
                buffer.append("\n" + " " * indent)
                if blockquote:
                    buffer.append("  ")
                counter = 0

            buffer.append(token + " ")
            counter += len(token)

        return "".join(buffer)

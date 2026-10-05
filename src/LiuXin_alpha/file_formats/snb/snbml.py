# -*- coding: utf-8 -*-

"""
Translate SNB markup into normalized OEB content.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise snbml through a consuming regression::

        python -m pytest -q tests/file_formats/snb/test_snb_modernized.py
"""
from __future__ import annotations

import typing as _typing

import os
import re
from collections.abc import Sequence
from typing import Protocol

from lxml import etree  # pyright: ignore[reportMissingImports]

from LiuXin_alpha.utils.libraries.liuxin_six import six_string_types
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

__license__ = "GPL 3"
__copyright__ = "2010, Li Fanxi <lifanxi@freemindworld.com>"
__docformat__ = "restructuredtext en"


class _Logger(Protocol):
    """
    Provide the logger contract for validated ebook processing.

    Example:
        Exercise  Logger through a consuming regression::

            python -m pytest -q tests/file_formats/snb/test_snb_modernized.py
    """
    def debug(self: _typing.Self, message: object) -> object:
        """
        Perform the debug operation under explicit file-format and conversion rules.

        Example:
            Exercise  Logger.debug through a consuming regression::

                python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


        :param message: Value supplied for message under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    def info(self: _typing.Self, message: object) -> object:
        """
        Perform the info operation under explicit file-format and conversion rules.

        Example:
            Exercise  Logger.info through a consuming regression::

                python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


        :param message: Value supplied for message under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...


def ProcessFileName(fileName: str) -> str:
    """
    Flatten the filepath.

    Example:
        Exercise ProcessFileName through a consuming regression::

            python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


    :param fileName: Value supplied for fileName under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    fileName = fileName.replace("/", "_").replace("\\", "_").replace(os.sep, "_")
    # Handle bookmark for HTML file
    fileName = fileName.replace("#", "_")
    # Make it lower case
    fileName = fileName.lower()
    # Change all images to jpg
    root, ext = os.path.splitext(fileName)
    if ext in [".jpeg", ".jpg", ".gif", ".svg", ".png"]:
        fileName = root + ".jpg"
    return fileName


BLOCK_TAGS = [
    "div",
    "p",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "li",
    "tr",
]

BLOCK_STYLES = [
    "block",
]

SPACE_TAGS = [
    "td",
]

CALIBRE_SNB_IMG_TAG = "<$$calibre_snb_temp_img$$>"
CALIBRE_SNB_BM_TAG = "<$$calibre_snb_bm_tag$$>"
CALIBRE_SNB_PRE_TAG = "<$$calibre_snb_pre_tag$$>"


class SNBMLizer(object):

    """
    Provide the snbmlizer contract for validated ebook processing.

    Example:
        Exercise SNBMLizer through a consuming regression::

            python -m pytest -q tests/file_formats/snb/test_snb_modernized.py
    """
    curSubItem = ""
    #    curText = [ ]

    def __init__(self: _typing.Self, log: _Logger) -> None:
        """
        Initialize and validate the snbmlizer state.

        Example:
            Exercise SNBMLizer.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


        :param log: Value supplied for log under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.log = log
        self.oeb_book: _typing.Any = None
        self.opts: _typing.Any = None
        self.item: _typing.Any = None
        self.subitems: Sequence[tuple[str, str]] = ()

    def extract_content(
        self: _typing.Self,
        oeb_book: _typing.Any,
        item: _typing.Any,
        subitems: Sequence[tuple[str, str]],
        opts: _typing.Any,
    ) -> dict[str, _typing.Any]:
        """
        Extract content under the format's safety and compatibility rules.

        Example:
            Exercise SNBMLizer.extract content through a consuming regression::

                python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


        :param oeb_book: Value supplied for oeb book under the utility contract.
        :param item: Value supplied for item under the utility contract.
        :param subitems: Value supplied for subitems under the utility contract.
        :param opts: Value supplied for opts under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.log.info("Converting XHTML to SNBC...")
        self.oeb_book = oeb_book
        self.opts = opts
        self.item = item
        self.subitems = subitems
        return self.mlize()

    def merge_content(
        self: _typing.Self,
        old_tree: _typing.Any,
        oeb_book: _typing.Any,
        item: _typing.Any,
        subitems: Sequence[tuple[str, str]],
        opts: _typing.Any,
    ) -> None:
        """
        Perform the merge content operation under explicit file-format and conversion rules.

        Example:
            Exercise SNBMLizer.merge content through a consuming regression::

                python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


        :param old_tree: Value supplied for old tree under the utility contract.
        :param oeb_book: Value supplied for oeb book under the utility contract.
        :param item: Value supplied for item under the utility contract.
        :param subitems: Value supplied for subitems under the utility contract.
        :param opts: Value supplied for opts under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        newTrees = self.extract_content(oeb_book, item, subitems, opts)
        body = old_tree.find(".//body")
        if body is not None:
            for subName in newTrees:
                newbody = newTrees[subName].find(".//body")
                for entity in newbody:
                    body.append(entity)

    def mlize(self: _typing.Self) -> dict[str, _typing.Any]:
        """
        Perform the mlize operation under explicit file-format and conversion rules.

        Example:
            Exercise SNBMLizer.mlize through a consuming regression::

                python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        from LiuXin_alpha.file_formats.oeb.base import XHTML
        from LiuXin_alpha.file_formats.oeb.stylizer import Stylizer

        output = [""]
        stylizer = Stylizer(
            self.item.data,
            self.item.href,
            self.oeb_book,
            self.opts,
            self.opts.output_profile,
        )
        content = six_unicode(etree.tostring(self.item.data.find(XHTML("body")), encoding=six_unicode))
        #        content = self.remove_newlines(content)
        trees = {}
        for subitem, subtitle in self.subitems:
            snbcTree = etree.Element("snbc")
            snbcHead = etree.SubElement(snbcTree, "head")
            etree.SubElement(snbcHead, "title").text = subtitle
            if self.opts and self.opts.snb_hide_chapter_name:
                etree.SubElement(snbcHead, "hidetitle").text = "true"
            etree.SubElement(snbcTree, "body")
            trees[subitem] = snbcTree
        output.append("%s%s\n\n" % (CALIBRE_SNB_BM_TAG, ""))
        output += self.dump_text(self.subitems, etree.fromstring(content), stylizer)[0]
        output = self.cleanup_text("".join(output))

        subitem = ""
        bodyTree = trees[subitem].find(".//body")
        for line in output.splitlines():
            pos = line.find(CALIBRE_SNB_PRE_TAG)
            if pos == -1:
                line = line.strip(" \t\n\r\u3000")
            else:
                etree.SubElement(bodyTree, "text").text = etree.CDATA(line[pos + len(CALIBRE_SNB_PRE_TAG) :])
                continue
            if len(line) != 0:
                if line.find(CALIBRE_SNB_IMG_TAG) == 0:
                    prefix = ProcessFileName(os.path.dirname(self.item.href))
                    if prefix != "":
                        etree.SubElement(bodyTree, "img").text = prefix + "_" + line[len(CALIBRE_SNB_IMG_TAG) :]
                    else:
                        etree.SubElement(bodyTree, "img").text = line[len(CALIBRE_SNB_IMG_TAG) :]
                elif line.find(CALIBRE_SNB_BM_TAG) == 0:
                    subitem = line[len(CALIBRE_SNB_BM_TAG) :]
                    bodyTree = trees[subitem].find(".//body")
                else:
                    if self.opts and not self.opts.snb_dont_indent_first_line:
                        prefix = "\u3000\u3000"
                    else:
                        prefix = ""
                    etree.SubElement(bodyTree, "text").text = etree.CDATA(six_unicode(prefix + line))
                if self.opts and self.opts.snb_insert_empty_line:
                    etree.SubElement(bodyTree, "text").text = etree.CDATA("")

        return trees

    def remove_newlines(self: _typing.Self, text: str) -> str:
        """
        Perform the remove newlines operation under explicit file-format and conversion rules.

        Example:
            Exercise SNBMLizer.remove newlines through a consuming regression::

                python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


        :param text: Text parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.log.debug("\tRemove newlines for processing...")
        text = text.replace("\r\n", " ")
        text = text.replace("\n", " ")
        text = text.replace("\r", " ")

        return text

    def cleanup_text(self: _typing.Self, text: str) -> str:
        """
        Perform the cleanup text operation under explicit file-format and conversion rules.

        Example:
            Exercise SNBMLizer.cleanup text through a consuming regression::

                python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


        :param text: Text parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.log.debug("\tClean up text...")
        # Replace bad characters.
        text = text.replace("\xc2", "")
        text = text.replace("\xa0", " ")
        text = text.replace("\xa9", "(C)")

        # Replace tabs, vertical tags and form feeds with single space.
        text = text.replace("\t+", " ")
        text = text.replace("\v+", " ")
        text = text.replace("\f+", " ")

        # Single line paragraph.
        text = re.sub("(?<=.)%s(?=.)" % os.linesep, " ", text)

        # Remove multiple spaces.
        # text = re.sub('[ ]{2,}', ' ', text)

        # Remove excessive newlines.
        text = re.sub("\n[ ]+\n", "\n\n", text)
        if self.opts.remove_paragraph_spacing:
            text = re.sub("\n{2,}", "\n", text)
            text = re.sub("(?imu)^(?=.)", "\t", text)
        else:
            text = re.sub("\n{3,}", "\n\n", text)

        # Replace spaces at the beginning and end of lines
        text = re.sub("(?imu)^[ ]+", "", text)
        text = re.sub("(?imu)[ ]+$", "", text)

        if self.opts.snb_max_line_length:
            max_length = self.opts.snb_max_line_length
            if getattr(self.opts, "max_line_length", max_length) < 25:  # and not self.opts.force_max_line_length:
                max_length = 25
            short_lines = []
            lines = text.splitlines()
            for line in lines:
                while len(line) > max_length:
                    space = line.rfind(" ", 0, max_length)
                    if space != -1:
                        # Space was found.
                        short_lines.append(line[:space])
                        line = line[space + 1 :]
                    else:
                        # Space was not found.
                        if False and self.opts.force_max_line_length:
                            # Force breaking at max_lenght.
                            short_lines.append(line[:max_length])
                            line = line[max_length:]
                        else:
                            # Look for the first space after max_length.
                            space = line.find(" ", max_length, len(line))
                            if space != -1:
                                # Space was found.
                                short_lines.append(line[:space])
                                line = line[space + 1 :]
                            else:
                                # No space was found cannot break line.
                                short_lines.append(line)
                                line = ""
                # Add the text that was less than max_lengh to the list
                short_lines.append(line)
            text = "\n".join(short_lines)

        return text

    def dump_text(
        self: _typing.Self,
        subitems: Sequence[tuple[str, str]],
        elem: _typing.Any,
        stylizer: _typing.Any,
        end: str = "",
        pre: bool = False,
        li: str = "",
    ) -> list[str] | tuple[list[str], str]:
        """
        Perform the dump text operation under explicit file-format and conversion rules.

        Example:
            Exercise SNBMLizer.dump text through a consuming regression::

                python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


        :param subitems: Value supplied for subitems under the utility contract.
        :param elem: Value supplied for elem under the utility contract.
        :param stylizer: Value supplied for stylizer under the utility contract.
        :param end: Value supplied for end under the utility contract.
        :param pre: Value supplied for pre under the utility contract.
        :param li: Value supplied for li under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        from LiuXin_alpha.file_formats.oeb.base import XHTML_NS, barename, namespace

        if not isinstance(elem.tag, six_string_types) or namespace(elem.tag) != XHTML_NS:
            p = elem.getparent()
            if p is not None and isinstance(p.tag, six_string_types) and namespace(p.tag) == XHTML_NS and elem.tail:
                return [elem.tail]
            return [""]

        text = [""]
        style = stylizer.style(elem)

        if elem.attrib.get("id") is not None and elem.attrib["id"] in [href for href, title in subitems]:
            if self.curSubItem is not None and self.curSubItem != elem.attrib["id"]:
                self.curSubItem = elem.attrib["id"]
                text.append("\n\n%s%s\n\n" % (CALIBRE_SNB_BM_TAG, self.curSubItem))

        if style["display"] in ("none", "oeb-page-head", "oeb-page-foot") or style["visibility"] == "hidden":
            if hasattr(elem, "tail") and elem.tail:
                return [elem.tail]
            return [""]

        tag = barename(elem.tag)
        in_block = False

        # Are we in a paragraph block?
        if tag in BLOCK_TAGS or style["display"] in BLOCK_STYLES:
            in_block = True
            if not end.endswith("\n\n") and hasattr(elem, "text") and elem.text:
                text.append("\n\n")

        if tag in SPACE_TAGS:
            if not end.endswith("u ") and hasattr(elem, "text") and elem.text:
                text.append(" ")

        if tag == "img":
            text.append("\n\n%s%s\n\n" % (CALIBRE_SNB_IMG_TAG, ProcessFileName(elem.attrib["src"])))

        if tag == "br":
            text.append("\n\n")

        if tag == "li":
            li = "- "

        pre = tag == "pre" or pre
        # Process tags that contain text.
        if hasattr(elem, "text") and elem.text:
            if pre:
                text.append(("\n\n%s" % CALIBRE_SNB_PRE_TAG).join((li + elem.text).splitlines()))
            else:
                text.append(li + elem.text)
            li = ""

        for item in elem:
            en = ""
            if len(text) >= 2:
                en = text[-1][-2:]
            t = self.dump_text(subitems, item, stylizer, en, pre, li)[0]
            text += t

        if in_block:
            text.append("\n\n")

        if hasattr(elem, "tail") and elem.tail:
            if pre:
                text.append(("\n\n%s" % CALIBRE_SNB_PRE_TAG).join(elem.tail.splitlines()))
            else:
                text.append(li + elem.tail)
            li = ""

        return text, li

# -*- coding: utf-8 -*-

"""
Generate RocketBook markup from normalized book content.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise rbml through a consuming regression::

        python -m pytest -q tests/file_formats/rb/test_rb_modernized.py
"""
from __future__ import annotations

import typing as _typing

import re
from collections.abc import MutableMapping
from typing import Protocol

from LiuXin_alpha.file_formats.rb import unique_name

from LiuXin_alpha.utils.calibre import prepare_string_for_xml
from LiuXin_alpha.utils.libraries.liuxin_six import six_string_types
from LiuXin_alpha.utils.localization import trans as _


__license__ = "GPL 3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"

TAGS = [
    "b",
    "big",
    "blockquote",
    "br",
    "center",
    "code",
    "div",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "hr",
    "i",
    "li",
    "ol",
    "p",
    "pre",
    "small",
    "sub",
    "sup",
    "ul",
]

LINK_TAGS = [
    "a",
]

IMAGE_TAGS = [
    "img",
]

STYLES = [
    ("font-weight", {"bold": "b", "bolder": "b"}),
    ("font-style", {"italic": "i"}),
    ("text-align", {"center": "center"}),
]


class _Logger(Protocol):
    """
    Provide the logger contract for validated ebook processing.

    Example:
        Exercise  Logger through a consuming regression::

            python -m pytest -q tests/file_formats/rb/test_rb_modernized.py
    """
    def debug(self: _typing.Self, message: object) -> object:
        """
        Perform the debug operation under explicit file-format and conversion rules.

        Example:
            Exercise  Logger.debug through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


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

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :param message: Value supplied for message under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    def warn(self: _typing.Self, message: object) -> object:
        """
        Perform the warn operation under explicit file-format and conversion rules.

        Example:
            Exercise  Logger.warn through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :param message: Value supplied for message under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    def warning(self: _typing.Self, message: object) -> object:
        """
        Perform the warning operation under explicit file-format and conversion rules.

        Example:
            Exercise  Logger.warning through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :param message: Value supplied for message under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...


class RBMLizer(object):
    """
    Provide the rbmlizer contract for validated ebook processing.

    Example:
        Exercise RBMLizer through a consuming regression::

            python -m pytest -q tests/file_formats/rb/test_rb_modernized.py
    """
    def __init__(
        self: _typing.Self,
        log: _Logger,
        name_map: MutableMapping[str, str] | None = None,
    ) -> None:
        """
        Initialize and validate the rbmlizer state.

        Example:
            Exercise RBMLizer.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :param log: Value supplied for log under the utility contract.
        :param name_map: Value supplied for name map under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.log = log
        self.name_map = {} if name_map is None else name_map
        self.link_hrefs: dict[str, str] = {}
        self.oeb_book: _typing.Any = None
        self.opts: _typing.Any = None

    def extract_content(
        self: _typing.Self,
        oeb_book: _typing.Any,
        opts: _typing.Any,
    ) -> str:
        """
        Extract content under the format's safety and compatibility rules.

        Example:
            Exercise RBMLizer.extract content through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :param oeb_book: Value supplied for oeb book under the utility contract.
        :param opts: Value supplied for opts under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.log.info("Converting XHTML to RB markup...")
        self.oeb_book = oeb_book
        self.opts = opts
        return self.mlize_spine()

    def mlize_spine(self: _typing.Self) -> str:
        """
        Perform the mlize spine operation under explicit file-format and conversion rules.

        Example:
            Exercise RBMLizer.mlize spine through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.link_hrefs = {}
        output = ["<HTML><HEAD><TITLE></TITLE></HEAD><BODY>"]
        output.append(self.get_cover_page())
        output.append("ghji87yhjko0Caliblre-toc-placeholder-for-insertion-later8ujko0987yjk")
        output.append(self.get_text())
        output.append("</BODY></HTML>")
        output = "".join(output).replace(
            "ghji87yhjko0Caliblre-toc-placeholder-for-insertion-later8ujko0987yjk",
            self.get_toc(),
        )
        output = self.clean_text(output)
        return output

    def get_cover_page(self: _typing.Self) -> str:
        """
        Return cover page under the format's safety and compatibility rules.

        Example:
            Exercise RBMLizer.get cover page through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        from LiuXin_alpha.file_formats.oeb.stylizer import Stylizer
        from LiuXin_alpha.file_formats.oeb.base import XHTML

        output = ""
        if "cover" in self.oeb_book.guide:
            if self.name_map.get(self.oeb_book.guide["cover"].href, None):
                output += '<IMG SRC="%s">' % self.name_map[self.oeb_book.guide["cover"].href]
        if "titlepage" in self.oeb_book.guide:
            self.log.debug("Generating cover page...")
            href = self.oeb_book.guide["titlepage"].href
            item = self.oeb_book.manifest.hrefs[href]
            if item.spine_position is None:
                stylizer = Stylizer(
                    item.data,
                    item.href,
                    self.oeb_book,
                    self.opts,
                    self.opts.output_profile,
                )
                output += "".join(self.dump_text(item.data.find(XHTML("body")), stylizer, item))
        return output

    def get_toc(self: _typing.Self) -> str:
        """
        Return toc under the format's safety and compatibility rules.

        Example:
            Exercise RBMLizer.get toc through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        toc = [""]
        if self.opts.inline_toc:
            self.log.debug("Generating table of contents...")
            toc.append("<H1>%s</H1><UL>\n" % _("Table of Contents:"))
            for item in self.oeb_book.toc:
                if item.href in self.link_hrefs.keys():
                    toc.append('<LI><A HREF="#%s">%s</A></LI>\n' % (self.link_hrefs[item.href], item.title))
                else:
                    log_warn = getattr(self.log, "warning", None) or getattr(self.log, "warn", None)
                    if log_warn is not None:
                        log_warn("Ignoring toc item: %s not found in document." % item)
            toc.append("</UL>")
        return "".join(toc)

    def get_text(self: _typing.Self) -> str:
        """
        Return text under the format's safety and compatibility rules.

        Example:
            Exercise RBMLizer.get text through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        from LiuXin_alpha.file_formats.oeb.stylizer import Stylizer
        from LiuXin_alpha.file_formats.oeb.base import XHTML

        output = [""]
        for item in self.oeb_book.spine:
            self.log.debug("Converting %s to RocketBook HTML..." % item.href)
            stylizer = Stylizer(item.data, item.href, self.oeb_book, self.opts, self.opts.output_profile)
            output.append(self.add_page_anchor(item))
            output += self.dump_text(item.data.find(XHTML("body")), stylizer, item)
        return "".join(output)

    def add_page_anchor(self: _typing.Self, page: _typing.Any) -> str:
        """
        Perform the add page anchor operation under explicit file-format and conversion rules.

        Example:
            Exercise RBMLizer.add page anchor through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :param page: Value supplied for page under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.get_anchor(page, "")

    def get_anchor(
        self: _typing.Self,
        page: _typing.Any,
        aid: str,
    ) -> str:
        """
        Return anchor under the format's safety and compatibility rules.

        Example:
            Exercise RBMLizer.get anchor through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :param page: Value supplied for page under the utility contract.
        :param aid: Value supplied for aid under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        aid = "%s#%s" % (page.href, aid)
        if aid not in self.link_hrefs.keys():
            self.link_hrefs[aid] = "calibre_link-%s" % len(self.link_hrefs.keys())
        aid = self.link_hrefs[aid]
        return '<A NAME="%s"></A>' % aid

    def clean_text(self: _typing.Self, text: str) -> str:
        # Remove anchors that do not have links
        """
        Perform the clean text operation under explicit file-format and conversion rules.

        Example:
            Exercise RBMLizer.clean text through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :param text: Text parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        anchors = set(re.findall(r'(?<=<A NAME=").+?(?="></A>)', text))
        links = set(re.findall(r'(?<=<A HREF="#).+?(?=">)', text))
        for unused in anchors.difference(links):
            text = text.replace('<A NAME="%s"></A>' % unused, "")

        return text

    def dump_text(
        self: _typing.Self,
        elem: _typing.Any,
        stylizer: _typing.Any,
        page: _typing.Any,
        tag_stack: list[str] | None = None,
    ) -> list[str]:
        """
        Perform the dump text operation under explicit file-format and conversion rules.

        Example:
            Exercise RBMLizer.dump text through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :param elem: Value supplied for elem under the utility contract.
        :param stylizer: Value supplied for stylizer under the utility contract.
        :param page: Value supplied for page under the utility contract.
        :param tag_stack: Value supplied for tag stack under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        from LiuXin_alpha.file_formats.oeb.base import XHTML_NS, barename, namespace
        if tag_stack is None:
            tag_stack = []

        if not isinstance(elem.tag, six_string_types) or namespace(elem.tag) != XHTML_NS:
            p = elem.getparent()
            if p is not None and isinstance(p.tag, six_string_types) and namespace(p.tag) == XHTML_NS and elem.tail:
                return [elem.tail]
            return [""]

        text = [""]
        style = stylizer.style(elem)

        if style["display"] in ("none", "oeb-page-head", "oeb-page-foot") or style["visibility"] == "hidden":
            if hasattr(elem, "tail") and elem.tail:
                return [elem.tail]
            return [""]

        tag = barename(elem.tag)
        tag_count = 0

        # Process tags that need special processing and that do not have inner
        # text. Usually these require an argument
        if tag in IMAGE_TAGS:
            if elem.attrib.get("src", None):
                if page.abshref(elem.attrib["src"]) not in self.name_map.keys():
                    self.name_map[page.abshref(elem.attrib["src"])] = unique_name(
                        "%s" % len(self.name_map.keys()), self.name_map.keys()
                    )
                text.append('<IMG SRC="%s">' % self.name_map[page.abshref(elem.attrib["src"])])

        rb_tag = tag.upper() if tag in TAGS else None
        if rb_tag:
            tag_count += 1
            text.append("<%s>" % rb_tag)
            tag_stack.append(rb_tag)

        # Anchors links
        if tag in LINK_TAGS:
            href = elem.get("href")
            if href:
                href = page.abshref(href)
                if "://" not in href:
                    if "#" not in href:
                        href += "#"
                    if href not in self.link_hrefs.keys():
                        self.link_hrefs[href] = "calibre_link-%s" % len(self.link_hrefs.keys())
                    href = self.link_hrefs[href]
                    text.append('<A HREF="#%s">' % href)
                tag_count += 1
                tag_stack.append("A")

        # Anchor ids
        id_name = elem.get("id")
        if id_name:
            text.append(self.get_anchor(page, id_name))

        # Processes style information
        for s in STYLES:
            style_tag = s[1].get(style[s[0]], None)
            if style_tag:
                style_tag = style_tag.upper()
                tag_count += 1
                text.append("<%s>" % style_tag)
                tag_stack.append(style_tag)

        # Process tags that contain text.
        if hasattr(elem, "text") and elem.text:
            text.append(prepare_string_for_xml(elem.text))

        for item in elem:
            text += self.dump_text(item, stylizer, page, tag_stack)

        close_tag_list = []
        for i in range(0, tag_count):
            close_tag_list.insert(0, tag_stack.pop())

        text += self.close_tags(close_tag_list)

        if hasattr(elem, "tail") and elem.tail:
            text.append(prepare_string_for_xml(elem.tail))

        return text

    def close_tags(
        self: _typing.Self,
        tags: list[str],
    ) -> list[str]:
        """
        Perform the close tags operation under explicit file-format and conversion rules.

        Example:
            Exercise RBMLizer.close tags through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :param tags: Value supplied for tags under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        text = [""]
        for i in range(0, len(tags)):
            tag = tags.pop()
            text.append("</%s>" % tag)

        return text

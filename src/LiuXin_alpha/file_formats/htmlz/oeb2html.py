# -*- coding: utf-8 -*-

"""
Serialize normalized OEB content and resources into an HTMLZ archive.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise oeb2html through a consuming regression::

        python -m pytest -q tests/file_formats/htmlz/test_htmlz_modernized.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

"""
Transform OEB content into a single (more or less) HTML file.
"""

import os
import re

from functools import partial

try:
    from lxml import html as lxml_html  # type: ignore
except Exception:  # pragma: no cover - runtime without lxml
    lxml_html = None

from LiuXin_alpha.file_formats.oeb.base import (
    XHTML,
    XHTML_NS,
    barename,
    namespace,
    OEB_IMAGES,
    XLINK,
    rewrite_links,
    urlnormalize,
)
from LiuXin_alpha.file_formats.oeb.stylizer import Stylizer

from LiuXin_alpha.utils.calibre import prepare_string_for_xml
from LiuXin_alpha.utils.logging import default_log

# Py2/Py3 compatibility
from LiuXin_alpha.utils.libraries.liuxin_six import six_string_types
from LiuXin_alpha.utils.libraries.liuxin_six import six_urldefrag as urldefrag

__license__ = "GPL 3"
__copyright__ = "2011, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"

SELF_CLOSING_TAGS = {
    "area",
    "base",
    "basefont",
    "br",
    "hr",
    "input",
    "img",
    "link",
    "meta",
}


class OEB2HTML(object):
    """
    Base class. All subclasses should implement dump_text to actually transform content. Also, callers should use oeb2html to get the transformed html. links and images can be retrieved after calling oeb2html to get the mapping of OEB links and images to the new names used in the html returned by oeb2html. Images will always be referenced as if they are in an images directory.

    Example:
        Exercise OEB2HTML through a consuming regression::

            python -m pytest -q tests/file_formats/htmlz/test_htmlz_modernized.py
    """

    def __init__(self: _typing.Self, log: _typing.Any = None) -> None:
        """
        Initialize and validate the oeb2html state.

        Example:
            Exercise OEB2HTML.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/htmlz/test_htmlz_modernized.py


        :param log: Value supplied for log under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.log = default_log if log is None else log
        self.links = {}
        self.images = {}

    def oeb2html(self: _typing.Self, oeb_book: _typing.Any, opts: _typing.Any) -> _typing.Any:
        """
        Perform the oeb2html operation under explicit file-format and conversion rules.

        Example:
            Exercise OEB2HTML.oeb2html through a consuming regression::

                python -m pytest -q tests/file_formats/htmlz/test_htmlz_modernized.py


        :param oeb_book: Value supplied for oeb book under the utility contract.
        :param opts: Value supplied for opts under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.log.info("Converting OEB book to HTML...")
        self.opts = opts
        self.links = {}
        self.images = {}
        self.base_hrefs = [item.href for item in oeb_book.spine]
        self.map_resources(oeb_book)

        return self.mlize_spine(oeb_book)

    def mlize_spine(self: _typing.Self, oeb_book: _typing.Any) -> _typing.Any:
        """
        Perform the mlize spine operation under explicit file-format and conversion rules.

        Example:
            Exercise OEB2HTML.mlize spine through a consuming regression::

                python -m pytest -q tests/file_formats/htmlz/test_htmlz_modernized.py


        :param oeb_book: Value supplied for oeb book under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        output = ['<html><head><meta http-equiv="Content-Type" content="text/html;charset=utf-8" /></head><body>']
        for item in oeb_book.spine:
            self.log.debug("Converting %s to HTML..." % item.href)
            self.rewrite_ids(item.data, item)
            rewrite_links(item.data, partial(self.rewrite_link, page=item))
            stylizer = Stylizer(item.data, item.href, oeb_book, self.opts)
            output += self.dump_text(item.data.find(XHTML("body")), stylizer, item)
            output.append("\n\n")
        output.append("</body></html>")
        return "".join(output)

    def dump_text(self: _typing.Self, elem: _typing.Any, stylizer: _typing.Any, page: _typing.Any) -> None:
        """
        Perform the dump text operation under explicit file-format and conversion rules.

        Example:
            Exercise OEB2HTML.dump text through a consuming regression::

                python -m pytest -q tests/file_formats/htmlz/test_htmlz_modernized.py


        :param elem: Value supplied for elem under the utility contract.
        :param stylizer: Value supplied for stylizer under the utility contract.
        :param page: Value supplied for page under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def get_link_id(self: _typing.Self, href: _typing.Any, id: str = "") -> _typing.Any:
        """
        Return link id under the format's safety and compatibility rules.

        Example:
            Exercise OEB2HTML.get link id through a consuming regression::

                python -m pytest -q tests/file_formats/htmlz/test_htmlz_modernized.py


        :param href: Value supplied for href under the utility contract.
        :param id: Value supplied for id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if id:
            href += "#%s" % id
        if href not in self.links:
            self.links[href] = "#calibre_link-%s" % len(self.links.keys())
        return self.links[href]

    def map_resources(self: _typing.Self, oeb_book: _typing.Any) -> None:
        """
        Perform the map resources operation under explicit file-format and conversion rules.

        Example:
            Exercise OEB2HTML.map resources through a consuming regression::

                python -m pytest -q tests/file_formats/htmlz/test_htmlz_modernized.py


        :param oeb_book: Value supplied for oeb book under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        link_attrs_fallback = {
            "href",
            "src",
            "data",
            "action",
            "longdesc",
            "poster",
            "cite",
            "usemap",
            "background",
            "profile",
        }
        for item in oeb_book.manifest:
            if item.media_type in OEB_IMAGES:
                if item.href not in self.images:
                    ext = os.path.splitext(item.href)[1]
                    fname = "%s%s" % (len(self.images), ext)
                    fname = fname.zfill(10)
                    self.images[item.href] = fname
            if item in oeb_book.spine:
                self.get_link_id(item.href)
                root = item.data.find(XHTML("body"))
                if lxml_html is None:
                    link_attrs = set(link_attrs_fallback)
                else:
                    link_attrs = set(lxml_html.defs.link_attrs)
                link_attrs.add(XLINK("href"))
                for el in root.iter():
                    attribs = el.attrib
                    try:
                        if not isinstance(el.tag, six_string_types):
                            continue
                    except:
                        continue
                    for attr in attribs:
                        if attr in link_attrs:
                            href = item.abshref(attribs[attr])
                            href, attr_id = urldefrag(href)
                            if href in self.base_hrefs:
                                self.get_link_id(href, attr_id)

    def rewrite_link(self: _typing.Self, url: _typing.Any, page: _typing.Any = None) -> _typing.Any:
        """
        Perform the rewrite link operation under explicit file-format and conversion rules.

        Example:
            Exercise OEB2HTML.rewrite link through a consuming regression::

                python -m pytest -q tests/file_formats/htmlz/test_htmlz_modernized.py


        :param url: Value supplied for url under the utility contract.
        :param page: Value supplied for page under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not page:
            return url
        abs_url = page.abshref(urlnormalize(url))
        if abs_url in self.images:
            return "images/%s" % self.images[abs_url]
        if abs_url in self.links:
            return self.links[abs_url]
        return url

    def rewrite_ids(self: _typing.Self, root: _typing.Any, page: _typing.Any) -> None:
        """
        Perform the rewrite ids operation under explicit file-format and conversion rules.

        Example:
            Exercise OEB2HTML.rewrite ids through a consuming regression::

                python -m pytest -q tests/file_formats/htmlz/test_htmlz_modernized.py


        :param root: Root directory that bounds path resolution or traversal.
        :param page: Value supplied for page under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for el in root.iter():
            try:
                tag = el.tag
            except UnicodeDecodeError:
                continue
            if tag == XHTML("body"):
                el.attrib["id"] = self.get_link_id(page.href)[1:]
                continue
            if "id" in el.attrib:
                el.attrib["id"] = self.get_link_id(page.href, el.attrib["id"])[1:]

    def get_css(self: _typing.Self, oeb_book: _typing.Any) -> _typing.Any:
        """
        Return css under the format's safety and compatibility rules.

        Example:
            Exercise OEB2HTML.get css through a consuming regression::

                python -m pytest -q tests/file_formats/htmlz/test_htmlz_modernized.py


        :param oeb_book: Value supplied for oeb book under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        css_parts = []
        for item in oeb_book.manifest:
            if item.media_type == "text/css":
                css_text = getattr(item.data, "cssText", "")
                if isinstance(css_text, bytes):
                    css_text = css_text.decode("utf-8", "replace")
                css_parts.append(css_text)
        return "\n\n".join(x for x in css_parts if x)

    def prepare_string_for_html(self: _typing.Self, raw: _typing.Any) -> _typing.Any:
        """
        Perform the prepare string for html operation under explicit file-format and conversion rules.

        Example:
            Exercise OEB2HTML.prepare string for html through a consuming regression::

                python -m pytest -q tests/file_formats/htmlz/test_htmlz_modernized.py


        :param raw: Value supplied for raw under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        raw = prepare_string_for_xml(raw)
        raw = raw.replace("\u00ad", "&shy;")
        raw = raw.replace("\u2014", "&mdash;")
        raw = raw.replace("\u2013", "&ndash;")
        raw = raw.replace("\u00a0", "&nbsp;")
        return raw


class OEB2HTMLNoCSSizer(OEB2HTML):
    """
    This will remap a small number of CSS styles to equivalent HTML tags.

    Example:
        Exercise OEB2HTMLNoCSSizer through a consuming regression::

            python -m pytest -q tests/file_formats/htmlz/test_htmlz_modernized.py
    """

    def dump_text(self: _typing.Self, elem: _typing.Any, stylizer: _typing.Any, page: _typing.Any) -> _typing.Any:
        """
        Perform the dump text operation under explicit file-format and conversion rules.

        Example:
            Exercise OEB2HTMLNoCSSizer.dump text through a consuming regression::

                python -m pytest -q tests/file_formats/htmlz/test_htmlz_modernized.py


        :param elem: Value supplied for elem under the utility contract.
        :param stylizer: Value supplied for stylizer under the utility contract.
        :param page: Value supplied for page under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        # We can only processes tags. If there isn't a tag return any text.
        if not isinstance(elem.tag, six_string_types) or namespace(elem.tag) != XHTML_NS:
            p = elem.getparent()
            if p is not None and isinstance(p.tag, six_string_types) and namespace(p.tag) == XHTML_NS and elem.tail:
                return [elem.tail]
            return [""]

        # Setup our variables.
        text = [""]
        style = stylizer.style(elem)
        tags = []
        tag = barename(elem.tag)
        attribs = elem.attrib

        if tag == "body":
            tag = "div"
        tags.append(tag)

        # Ignore anything that is set to not be displayed.
        if style["display"] in ("none", "oeb-page-head", "oeb-page-foot") or style["visibility"] == "hidden":
            return [""]

        # Remove attributes we won't want.
        if "class" in attribs:
            del attribs["class"]
        if "style" in attribs:
            del attribs["style"]

        # Turn the rest of the attributes into a string we can write with the tag.
        at = ""
        for k, v in attribs.items():
            at += ' %s="%s"' % (k, prepare_string_for_xml(v, attribute=True))

        # Write the tag.
        text.append("<%s%s" % (tag, at))
        if tag in SELF_CLOSING_TAGS:
            text.append(" />")
        else:
            text.append(">")

        # Turn styles into tags.
        if style["font-weight"] in ("bold", "bolder"):
            text.append("<b>")
            tags.append("b")
        if style["font-style"] == "italic":
            text.append("<i>")
            tags.append("i")
        if style["text-decoration"] == "underline":
            text.append("<u>")
            tags.append("u")
        if style["text-decoration"] == "line-through":
            text.append("<s>")
            tags.append("s")

        # Process tags that contain text.
        if hasattr(elem, "text") and elem.text:
            text.append(self.prepare_string_for_html(elem.text))

        # Recurse down into tags within the tag we are in.
        for item in elem:
            text += self.dump_text(item, stylizer, page)

        # Close all open tags.
        tags.reverse()
        for t in tags:
            if t not in SELF_CLOSING_TAGS:
                text.append("</%s>" % t)

        # Add the text that is outside of the tag.
        if hasattr(elem, "tail") and elem.tail:
            text.append(self.prepare_string_for_html(elem.tail))

        return text


class OEB2HTMLInlineCSSizer(OEB2HTML):
    """
    Turns external CSS classes into inline style attributes.

    Example:
        Exercise OEB2HTMLInlineCSSizer through a consuming regression::

            python -m pytest -q tests/file_formats/htmlz/test_htmlz_modernized.py
    """

    def dump_text(self: _typing.Self, elem: _typing.Any, stylizer: _typing.Any, page: _typing.Any) -> _typing.Any:
        """
        Perform the dump text operation under explicit file-format and conversion rules.

        Example:
            Exercise OEB2HTMLInlineCSSizer.dump text through a consuming regression::

                python -m pytest -q tests/file_formats/htmlz/test_htmlz_modernized.py


        :param elem: Value supplied for elem under the utility contract.
        :param stylizer: Value supplied for stylizer under the utility contract.
        :param page: Value supplied for page under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        # We can only processes tags. If there isn't a tag return any text.
        if not isinstance(elem.tag, six_string_types) or namespace(elem.tag) != XHTML_NS:
            p = elem.getparent()
            if p is not None and isinstance(p.tag, six_string_types) and namespace(p.tag) == XHTML_NS and elem.tail:
                return [elem.tail]
            return [""]

        # Setup our variables.
        text = [""]
        style = stylizer.style(elem)
        tags = []
        tag = barename(elem.tag)
        attribs = elem.attrib

        style_a = "%s" % style
        style_a = style_a if style_a else ""
        if tag == "body":
            # Change the body to a div so we can merge multiple files.
            tag = "div"
            # Add page-break-brefore: always because renders typically treat a new file (we're merging files)
            # as a page break and remove all other page break types that might be set.
            style_a = "page-break-before: always; %s" % re.sub("page-break-[^:]+:[^;]+;?", "", style_a)
        # Remove unnecessary spaces.
        style_a = re.sub(r"\s{2,}", " ", style_a).strip()
        tags.append(tag)

        # Remove attributes we won't want.
        if "class" in attribs:
            del attribs["class"]
        if "style" in attribs:
            del attribs["style"]

        # Turn the rest of the attributes into a string we can write with the tag.
        at = ""
        for k, v in attribs.items():
            at += ' %s="%s"' % (k, prepare_string_for_xml(v, attribute=True))

        # Turn style into strings for putting in the tag.
        style_t = ""
        if style_a:
            style_t = ' style="%s"' % style_a.replace('"', "'")

        # Write the tag.
        text.append("<%s%s%s" % (tag, at, style_t))
        if tag in SELF_CLOSING_TAGS:
            text.append(" />")
        else:
            text.append(">")

        # Process tags that contain text.
        if hasattr(elem, "text") and elem.text:
            text.append(self.prepare_string_for_html(elem.text))

        # Recurse down into tags within the tag we are in.
        for item in elem:
            text += self.dump_text(item, stylizer, page)

        # Close all open tags.
        tags.reverse()
        for t in tags:
            if t not in SELF_CLOSING_TAGS:
                text.append("</%s>" % t)

        # Add the text that is outside of the tag.
        if hasattr(elem, "tail") and elem.tail:
            text.append(self.prepare_string_for_html(elem.tail))

        return text


class OEB2HTMLClassCSSizer(OEB2HTML):
    """
    Use CSS classes. css_style option can specify whether to use inline classes (style tag in the head) or reference an external CSS file called style.css.

    Example:
        Exercise OEB2HTMLClassCSSizer through a consuming regression::

            python -m pytest -q tests/file_formats/htmlz/test_htmlz_modernized.py
    """

    def mlize_spine(self: _typing.Self, oeb_book: _typing.Any) -> _typing.Any:
        """
        Perform the mlize spine operation under explicit file-format and conversion rules.

        Example:
            Exercise OEB2HTMLClassCSSizer.mlize spine through a consuming regression::

                python -m pytest -q tests/file_formats/htmlz/test_htmlz_modernized.py


        :param oeb_book: Value supplied for oeb book under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        output = []
        for item in oeb_book.spine:
            self.log.debug("Converting %s to HTML..." % item.href)
            self.rewrite_ids(item.data, item)
            rewrite_links(item.data, partial(self.rewrite_link, page=item))
            stylizer = Stylizer(item.data, item.href, oeb_book, self.opts)
            output += self.dump_text(item.data.find(XHTML("body")), stylizer, item)
            output.append("\n\n")
        if self.opts.htmlz_class_style == "external":
            css = '<link href="style.css" rel="stylesheet" type="text/css" />'
        else:
            css = '<style type="text/css">' + self.get_css(oeb_book) + "</style>"
        output = (
            ['<html><head><meta http-equiv="Content-Type" content="text/html;charset=utf-8" />']
            + [css]
            + ["</head><body>"]
            + output
            + ["</body></html>"]
        )
        return "".join(output)

    def dump_text(self: _typing.Self, elem: _typing.Any, stylizer: _typing.Any, page: _typing.Any) -> _typing.Any:
        """
        Perform the dump text operation under explicit file-format and conversion rules.

        Example:
            Exercise OEB2HTMLClassCSSizer.dump text through a consuming regression::

                python -m pytest -q tests/file_formats/htmlz/test_htmlz_modernized.py


        :param elem: Value supplied for elem under the utility contract.
        :param stylizer: Value supplied for stylizer under the utility contract.
        :param page: Value supplied for page under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        # We can only processes tags. If there isn't a tag return any text.
        if not isinstance(elem.tag, six_string_types) or namespace(elem.tag) != XHTML_NS:
            p = elem.getparent()
            if p is not None and isinstance(p.tag, six_string_types) and namespace(p.tag) == XHTML_NS and elem.tail:
                return [elem.tail]
            return [""]

        # Setup our variables.
        text = [""]
        tags = []
        tag = barename(elem.tag)
        attribs = elem.attrib

        if tag == "body":
            tag = "div"
        tags.append(tag)

        # Remove attributes we won't want.
        if "style" in attribs:
            del attribs["style"]

        # Turn the rest of the attributes into a string we can write with the tag.
        at = ""
        for k, v in attribs.items():
            at += ' %s="%s"' % (k, prepare_string_for_xml(v, attribute=True))

        # Write the tag.
        text.append("<%s%s" % (tag, at))
        if tag in SELF_CLOSING_TAGS:
            text.append(" />")
        else:
            text.append(">")

        # Process tags that contain text.
        if hasattr(elem, "text") and elem.text:
            text.append(self.prepare_string_for_html(elem.text))

        # Recurse down into tags within the tag we are in.
        for item in elem:
            text += self.dump_text(item, stylizer, page)

        # Close all open tags.
        tags.reverse()
        for t in tags:
            if t not in SELF_CLOSING_TAGS:
                text.append("</%s>" % t)

        # Add the text that is outside of the tag.
        if hasattr(elem, "tail") and elem.tail:
            text.append(self.prepare_string_for_html(elem.tail))

        return text


def oeb2html_no_css(oeb_book: _typing.Any, log: _typing.Any, opts: _typing.Any) -> tuple[_typing.Any, ...]:
    """
    Perform the oeb2html no css operation under explicit file-format and conversion rules.

    Example:
        Exercise oeb2html no css through a consuming regression::

            python -m pytest -q tests/file_formats/htmlz/test_htmlz_modernized.py


    :param oeb_book: Value supplied for oeb book under the utility contract.
    :param log: Value supplied for log under the utility contract.
    :param opts: Value supplied for opts under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    local_izer = OEB2HTMLNoCSSizer(log)
    local_html = local_izer.oeb2html(oeb_book, opts)
    images = local_izer.images
    return local_html, images


def oeb2html_inline_css(oeb_book: _typing.Any, log: _typing.Any, opts: _typing.Any) -> tuple[_typing.Any, ...]:
    """
    Perform the oeb2html inline css operation under explicit file-format and conversion rules.

    Example:
        Exercise oeb2html inline css through a consuming regression::

            python -m pytest -q tests/file_formats/htmlz/test_htmlz_modernized.py


    :param oeb_book: Value supplied for oeb book under the utility contract.
    :param log: Value supplied for log under the utility contract.
    :param opts: Value supplied for opts under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    local_izer = OEB2HTMLInlineCSSizer(log)
    local_html = local_izer.oeb2html(oeb_book, opts)
    images = local_izer.images
    return local_html, images


def oeb2html_class_css(oeb_book: _typing.Any, log: _typing.Any, opts: _typing.Any) -> tuple[_typing.Any, ...]:
    """
    Perform the oeb2html class css operation under explicit file-format and conversion rules.

    Example:
        Exercise oeb2html class css through a consuming regression::

            python -m pytest -q tests/file_formats/htmlz/test_htmlz_modernized.py


    :param oeb_book: Value supplied for oeb book under the utility contract.
    :param log: Value supplied for log under the utility contract.
    :param opts: Value supplied for opts under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    local_izer = OEB2HTMLClassCSSizer(log)
    setattr(opts, "htmlz_class_style", "inline")
    local_html = local_izer.oeb2html(oeb_book, opts)
    images = local_izer.images
    return local_html, images

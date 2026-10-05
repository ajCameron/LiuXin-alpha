#!/usr/bin/env python
# vim:fileencoding=utf-8

"""
Apply stable human-readable formatting to EPUB/OEB resources.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise pretty through a consuming regression::

        python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

import textwrap

try:
    from itertools import imap
except ImportError:
    imap = map

from LiuXin_alpha.utils.text import as_unicode as force_unicode

from LiuXin_alpha.file_formats.oeb.base import (
    serialize,
    OEB_DOCS,
    barename,
    OEB_STYLES,
    XPNSMAP,
    XHTML,
    SVG,
)
from LiuXin_alpha.file_formats.oeb.polish.container import OPF_NAMESPACES
from LiuXin_alpha.file_formats.oeb.polish.utils import guess_type

from LiuXin_alpha.utils.language_tools.icu import sort_key

# Py2/Py3 emulation layer
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"


def isspace(x: _typing.Any) -> _typing.Any:
    """
    Tests to see if the given character could possible be rendered as a space

    Example:
        Exercise isspace through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param x: Value supplied for x under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return not x.strip("\u0009\u000a\u000c\u000d\u0020")


def pretty_xml_tree(elem: _typing.Any, level: int = 0, indent: str = "  ") -> None:
    """
    XML beautifier, assumes that elements that have children do not have textual content. Also assumes that there is no text immediately after closing tags. These are true for opf/ncx and container.xml files. If either of the assumptions are violated, there should be no data loss, but pretty printing wont produce optimal results.

    Example:
        Exercise pretty xml tree through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param elem: Value supplied for elem under the utility contract.
    :param level: Value supplied for level under the utility contract.
    :param indent: Value supplied for indent under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    if (not elem.text and len(elem) > 0) or (elem.text and isspace(elem.text)):
        elem.text = "\n" + (indent * (level + 1))
    for i, child in enumerate(elem):
        pretty_xml_tree(child, level=level + 1, indent=indent)
        if not child.tail or isspace(child.tail):
            l = level + 1
            if i == len(elem) - 1:
                l -= 1
            child.tail = "\n" + (indent * l)


def pretty_opf_string(root: _typing.Any) -> _typing.Any:
    """
    Provides a prettified string representation of the opf document with the given root.

    Example:
        Exercise pretty opf string through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from lxml import etree

    return etree.tostring(root, pretty_print=True)


def pretty_opf(root: _typing.Any) -> None:

    # Put all dc: tags first starting with title and author. Preserve order for
    # the rest.
    """
    Perform the pretty opf operation under explicit file-format and conversion rules.

    Example:
        Exercise pretty opf through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    def dckey(local_x: _typing.Any) -> _typing.Any:
        """
        Perform the dckey operation under explicit file-format and conversion rules.

        Example:
            Exercise pretty opf.dckey through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param local_x: Value supplied for local x under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return {"title": 0, "creator": 1}.get(barename(local_x.tag), 2)

    for metadata in root.xpath("//opf:metadata", namespaces=OPF_NAMESPACES):
        dc_tags = metadata.xpath('./*[namespace-uri()="%s"]' % OPF_NAMESPACES["dc"])
        dc_tags.sort(key=dckey)
        for x in reversed(dc_tags):
            metadata.insert(0, x)

    # Group items in the manifest
    spine_ids = root.xpath("//opf:spine/opf:itemref/@idref", namespaces=OPF_NAMESPACES)
    spine_ids = {x: i for i, x in enumerate(spine_ids)}

    def manifest_key(local_x: _typing.Any) -> tuple[_typing.Any, ...]:
        """
        Perform the manifest key operation under explicit file-format and conversion rules.

        Example:
            Exercise pretty opf.manifest key through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param local_x: Value supplied for local x under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        mt = local_x.get("media-type", "")
        href = local_x.get("href", "")
        ext = href.rpartition(".")[-1].lower()
        cat = 1000
        if mt in OEB_DOCS:
            cat = 0
        elif mt == guess_type("a.ncx"):
            cat = 1
        elif mt in OEB_STYLES:
            cat = 2
        elif mt.startswith("image/"):
            cat = 3
        elif ext in {"otf", "ttf", "woff"}:
            cat = 4
        elif mt.startswith("audio/"):
            cat = 5
        elif mt.startswith("video/"):
            cat = 6

        if cat == 0:
            i = spine_ids.get(local_x.get("id", None), 1000000000)
        else:
            i = sort_key(href)
        return cat, i

    for manifest in root.xpath("//opf:manifest", namespaces=OPF_NAMESPACES):
        try:
            children = sorted(manifest, key=manifest_key)
        except AttributeError:
            continue  # There are comments so dont sort since that would mess up the comments
        for x in reversed(children):
            manifest.insert(0, x)


SVG_TAG = SVG("svg")

BLOCK_TAGS = frozenset(
    imap(
        XHTML,
        (
            "address",
            "article",
            "aside",
            "audio",
            "blockquote",
            "body",
            "canvas",
            "dd",
            "div",
            "dl",
            "dt",
            "fieldset",
            "figcaption",
            "figure",
            "footer",
            "form",
            "h1",
            "h2",
            "h3",
            "h4",
            "h5",
            "h6",
            "header",
            "hgroup",
            "hr",
            "li",
            "noscript",
            "ol",
            "output",
            "p",
            "pre",
            "script",
            "section",
            "style",
            "table",
            "tbody",
            "td",
            "tfoot",
            "thead",
            "tr",
            "ul",
            "video",
            "img",
        ),
    )
) | {SVG_TAG}


def isblock(x: _typing.Any) -> bool:
    """
    Perform the isblock operation under explicit file-format and conversion rules.

    Example:
        Exercise isblock through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param x: Value supplied for x under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if callable(x.tag) or not x.tag:
        return True
    if x.tag in BLOCK_TAGS:
        return True
    return False


def has_only_blocks(x: _typing.Any) -> bool:
    """
    Return whether has only blocks holds for the supplied ebook data.

    Example:
        Exercise has only blocks through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param x: Value supplied for x under the utility contract.
    :return: True when the documented condition holds; otherwise False.
    """
    if hasattr(x.tag, "split") and len(x) == 0:
        # Tag with no children,
        return False
    if x.text and not isspace(x.text):
        return False
    for child in x:
        if not isblock(child) or (child.tail and not isspace(child.tail)):
            return False
    return True


def indent_for_tag(x: _typing.Any) -> _typing.Any:
    """
    Perform the indent for tag operation under explicit file-format and conversion rules.

    Example:
        Exercise indent for tag through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param x: Value supplied for x under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    prev = x.getprevious()
    x = x.getparent().text if prev is None else prev.tail
    if not x:
        return ""
    s = x.rpartition("\n")[-1]
    return s if isspace(s) else ""


def set_indent(elem: _typing.Any, attr: _typing.Any, indent: _typing.Any) -> None:
    """
    Set indent under the format's safety and compatibility rules.

    Example:
        Exercise set indent through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param elem: Value supplied for elem under the utility contract.
    :param attr: Value supplied for attr under the utility contract.
    :param indent: Value supplied for indent under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    x = getattr(elem, attr)
    if not x:
        x = indent
    else:
        lines = x.splitlines()
        if isspace(lines[-1]):
            lines[-1] = indent
        else:
            lines.append(indent)
        x = "\n".join(lines)
    setattr(elem, attr, x)


def pretty_block(parent: _typing.Any, level: int = 1, indent: str = "  ") -> None:
    """
    Surround block tags with blank lines and recurse into child block tags that contain only other block tags.

    Example:
        Exercise pretty block through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param parent: Value supplied for parent under the utility contract.
    :param level: Value supplied for level under the utility contract.
    :param indent: Value supplied for indent under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    if not parent.text or isspace(parent.text):
        parent.text = ""
    nn = "\n" if hasattr(parent.tag, "strip") and barename(parent.tag) in {"tr", "td", "th"} else "\n\n"
    parent.text = parent.text + nn + (indent * level)
    for i, child in enumerate(parent):
        if isblock(child) and has_only_blocks(child):
            pretty_block(child, level=level + 1, indent=indent)
        elif child.tag == SVG_TAG:
            pretty_xml_tree(child, level=level, indent=indent)
        l = level
        if i == len(parent) - 1:
            l -= 1
        if not child.tail or isspace(child.tail):
            child.tail = ""
        child.tail = child.tail + nn + (indent * l)


def pretty_script_or_style(container: _typing.Any, child: _typing.Any) -> None:
    """
    Perform the pretty script or style operation under explicit file-format and conversion rules.

    Example:
        Exercise pretty script or style through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param container: Value supplied for container under the utility contract.
    :param child: Value supplied for child under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    if child.text:
        indent = indent_for_tag(child)
        if child.tag.endswith("style"):
            child.text = force_unicode(pretty_css(container, "", child.text), "utf-8")
        child.text = textwrap.dedent(child.text)
        child.text = "\n" + "\n".join([(indent + x) if x else "" for x in child.text.splitlines()])
        set_indent(child, "text", indent)


def pretty_html_tree(container: _typing.Any, root: _typing.Any) -> None:
    """
    Perform the pretty html tree operation under explicit file-format and conversion rules.

    Example:
        Exercise pretty html tree through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param container: Value supplied for container under the utility contract.
    :param root: Root directory that bounds path resolution or traversal.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    root.text = "\n\n"
    for child in root:
        child.tail = "\n\n"
        if hasattr(child.tag, "endswith") and child.tag.endswith("}head"):
            pretty_xml_tree(child)
    for body in root.findall("h:body", namespaces=XPNSMAP):
        pretty_block(body)
        # Special case the handling of a body that contains a single block tag
        # with all content. In this case we prettify the containing block tag
        # even if it has non block children.
        if (
            len(body) == 1
            and not callable(body[0].tag)
            and isblock(body[0])
            and not has_only_blocks(body[0])
            and barename(body[0].tag) != "pre"
            and len(body[0]) > 0
        ):
            pretty_block(body[0], level=2)

    if container is not None:
        # Handle <script> and <style> tags
        for child in root.xpath('//*[local-name()="script" or local-name()="style"]'):
            pretty_script_or_style(container, child)


def fix_html(container: _typing.Any, raw: _typing.Any) -> _typing.Any:
    """
    Fix any parsing errors in the HTML represented as a string in raw. Fixing is done using the HTML5 parsing algorithm.

    Example:
        Exercise fix html through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param container: Value supplied for container under the utility contract.
    :param raw: Value supplied for raw under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    root = container.parse_xhtml(raw)
    return serialize(root, "text/html")


def pretty_html(container: _typing.Any, name: _typing.Any, raw: _typing.Any) -> _typing.Any:
    """
    Pretty print the HTML represented as a string in raw

    Example:
        Exercise pretty html through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param container: Value supplied for container under the utility contract.
    :param name: Field, file, function or resource name addressed by the operation.
    :param raw: Value supplied for raw under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    root = container.parse_xhtml(raw)
    pretty_html_tree(container, root)
    return serialize(root, "text/html")


def pretty_css(container: _typing.Any, name: _typing.Any, raw: _typing.Any) -> _typing.Any:
    """
    Pretty print the CSS represented as a string in raw

    Example:
        Exercise pretty css through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param container: Value supplied for container under the utility contract.
    :param name: Field, file, function or resource name addressed by the operation.
    :param raw: Value supplied for raw under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    sheet = container.parse_css(raw)
    return serialize(sheet, "text/css")


def pretty_xml(container: _typing.Any, name: _typing.Any, raw: _typing.Any) -> _typing.Any:
    """
    Pretty print the XML represented as a string in raw. If ``name`` is the name of the OPF, extra OPF-specific prettying is performed.

    Example:
        Exercise pretty xml through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param container: Value supplied for container under the utility contract.
    :param name: Field, file, function or resource name addressed by the operation.
    :param raw: Value supplied for raw under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    root = container.parse_xml(raw)
    if name == container.opf_name:
        pretty_opf(root)
    pretty_xml_tree(root)
    return serialize(root, "text/xml")


def fix_all_html(container: _typing.Any) -> None:
    """
    Fix any parsing errors in all HTML files in the container. Fixing is done using the HTML5 parsing algorithm.

    Example:
        Exercise fix all html through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param container: Value supplied for container under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    for name, mt in iteritems(container.mime_map):
        if mt in OEB_DOCS:
            container.parsed(name)
            container.dirty(name)


def pretty_all(container: _typing.Any) -> None:
    """
    Pretty print all HTML/CSS/XML files in the container

    Example:
        Exercise pretty all through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param container: Value supplied for container under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    for name, mt in iteritems(container.mime_map):
        prettied = False
        if mt in OEB_DOCS:
            pretty_html_tree(container, container.parsed(name))
            prettied = True
        elif mt in OEB_STYLES:
            container.parsed(name)
            prettied = True
        elif name == container.opf_name:
            root = container.parsed(name)
            pretty_opf(root)
            pretty_xml_tree(root)
            prettied = True
        elif mt in {guess_type("a.ncx"), guess_type("a.xml")}:
            pretty_xml_tree(container.parsed(name))
            prettied = True
        if prettied:
            container.dirty(name)

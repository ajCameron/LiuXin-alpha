"""
Model OEB books, manifests, spines, guides, metadata and navigation.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise base through a consuming regression::

        python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
"""
from __future__ import annotations

import typing as _typing

import logging
import os
import re
import uuid
from collections import defaultdict
from itertools import count
from urllib.parse import unquote

from LiuXin_alpha.utils.libraries.liuxin_etree import etree

try:
    from lxml import html  # type: ignore
except Exception:  # pragma: no cover - runtime without lxml
    class _MissingLxmlHtml:
        """
        Provide the missinglxmlhtml contract for validated ebook processing.

        Example:
            Exercise  MissingLxmlHtml through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
        """
        def __getattr__(self: _typing.Self, name: _typing.Any) -> None:
            """
            Perform the getattr operation under explicit file-format and conversion rules.

            Example:
                Exercise  MissingLxmlHtml.  getattr   through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :param name: Field, file, function or resource name addressed by the operation.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            raise ImportError("lxml.html is unavailable in this runtime")

    html = _MissingLxmlHtml()

from LiuXin_alpha.file_formats.conversion.preprocess import CSSPreProcessor
from LiuXin_alpha.file_formats.oeb.parse_utils import (
    barename,
    XHTML_NS,
    RECOVER_PARSER,
    namespace,
    XHTML,
    parse_html,
    NotHTML,
)

from LiuXin_alpha.constants import filesystem_encoding, __version__

from LiuXin_alpha.utils.text import isbytestring, as_unicode
from LiuXin_alpha.utils.mine_types import get_types_map

from LiuXin_alpha.utils.libraries.calibre_chardet import xml_to_unicode
from LiuXin_alpha.utils.localization import translate, trans as _
from LiuXin_alpha.utils.libraries.cleantext import clean_xml_chars
from LiuXin_alpha.utils.text.icu import title_case as icu_title

# Py2/Py3 compatability layer
from LiuXin_alpha.utils.libraries.liuxin_six import (
    memory_range,
    six_cmp,
    six_string_types,
    six_unicode,
    six_unicode as unicode,
    six_urldefrag as urldefrag,
    six_urlparse as urlparse,
    six_urlunparse as urlunparse,
    six_urljoin as urljoin)


__license__ = "GPL v3"
__copyright__ = "2008, Marshall T. Vandegrift <llasram@gmail.com>"
__docformat__ = "restructuredtext en"

XML_NS = "http://www.w3.org/XML/1998/namespace"
OEB_DOC_NS = "http://openebook.org/namespaces/oeb-document/1.0/"
OPF1_NS = "http://openebook.org/namespaces/oeb-package/1.0/"
OPF2_NS = "http://www.idpf.org/2007/opf"
OPF_NSES = {OPF1_NS, OPF2_NS}
DC09_NS = "http://purl.org/metadata/dublin_core"
DC10_NS = "http://purl.org/dc/elements/1.0/"
DC11_NS = "http://purl.org/dc/elements/1.1/"
DC_NSES = {DC09_NS, DC10_NS, DC11_NS}
XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"
DCTERMS_NS = "http://purl.org/dc/terms/"
NCX_NS = "http://www.daisy.org/z3986/2005/ncx/"
SVG_NS = "http://www.w3.org/2000/svg"
XLINK_NS = "http://www.w3.org/1999/xlink"
CALIBRE_NS = "http://calibre.kovidgoyal.net/2009/metadata"
RE_NS = "http://exslt.org/regular-expressions"
MBP_NS = "http://www.mobipocket.com"
EPUB_NS = "http://www.idpf.org/2007/ops"

XPNSMAP = {
    "h": XHTML_NS,
    "o1": OPF1_NS,
    "o2": OPF2_NS,
    "d09": DC09_NS,
    "d10": DC10_NS,
    "d11": DC11_NS,
    "xsi": XSI_NS,
    "dt": DCTERMS_NS,
    "ncx": NCX_NS,
    "svg": SVG_NS,
    "xl": XLINK_NS,
    "re": RE_NS,
    "mbp": MBP_NS,
    "calibre": CALIBRE_NS,
    "epub": EPUB_NS,
}

OPF1_NSMAP = {"dc": DC11_NS, "oebpackage": OPF1_NS}
OPF2_NSMAP = {
    "opf": OPF2_NS,
    "dc": DC11_NS,
    "dcterms": DCTERMS_NS,
    "xsi": XSI_NS,
    "calibre": CALIBRE_NS,
}


def XML(name: _typing.Any) -> _typing.Any:
    """
    Makes a name in the XML namespace.

    Example:
        Exercise XML through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param name: Field, file, function or resource name addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return "{%s}%s" % (XML_NS, name)


def OPF(name: _typing.Any) -> _typing.Any:
    """
    Perform the OPF operation under explicit file-format and conversion rules.

    Example:
        Exercise OPF through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param name: Field, file, function or resource name addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return "{%s}%s" % (OPF2_NS, name)


def DC(name: _typing.Any) -> _typing.Any:
    """
    Names in the Dublin core metadata namespace

    Example:
        Exercise DC through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param name: Field, file, function or resource name addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return "{%s}%s" % (DC11_NS, name)


def XSI(name: _typing.Any) -> _typing.Any:
    """
    Perform the XSI operation under explicit file-format and conversion rules.

    Example:
        Exercise XSI through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param name: Field, file, function or resource name addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return "{%s}%s" % (XSI_NS, name)


def DCTERMS(name: _typing.Any) -> _typing.Any:
    """
    dcterms is a more specified version of Dublin Core. For more information on the semantic differences. http://wiki.dublincore.org/index.php/FAQ/DC_and_DCTERMS_Namespaces

    Example:
        Exercise DCTERMS through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param name: Field, file, function or resource name addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return "{%s}%s" % (DCTERMS_NS, name)


def NCX(name: _typing.Any) -> _typing.Any:
    """
    Perform the NCX operation under explicit file-format and conversion rules.

    Example:
        Exercise NCX through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param name: Field, file, function or resource name addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return "{%s}%s" % (NCX_NS, name)


def SVG(name: _typing.Any) -> _typing.Any:
    """
    Perform the SVG operation under explicit file-format and conversion rules.

    Example:
        Exercise SVG through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param name: Field, file, function or resource name addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return "{%s}%s" % (SVG_NS, name)


def XLINK(name: _typing.Any) -> _typing.Any:
    """
    Perform the XLINK operation under explicit file-format and conversion rules.

    Example:
        Exercise XLINK through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param name: Field, file, function or resource name addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return "{%s}%s" % (XLINK_NS, name)


def CALIBRE(name: _typing.Any) -> _typing.Any:
    """
    Makes a name in the calibre namespace.

    Example:
        Exercise CALIBRE through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param name: Field, file, function or resource name addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return "{%s}%s" % (CALIBRE_NS, name)


_css_url_re = re.compile(r'url\s*\([\'"]{0,1}(.*?)[\'"]{0,1}\)', re.I)
_css_import_re = re.compile(r'@import "(.*?)"')
_archive_re = re.compile(r"[^ ]+")

# Tags that should not be self closed in epub output
self_closing_bad_tags = {
    "a",
    "abbr",
    "address",
    "article",
    "aside",
    "audio",
    "b",
    "bdo",
    "blockquote",
    "body",
    "button",
    "cite",
    "code",
    "dd",
    "del",
    "details",
    "dfn",
    "div",
    "dl",
    "dt",
    "em",
    "fieldset",
    "figcaption",
    "figure",
    "footer",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "header",
    "hgroup",
    "i",
    "ins",
    "kbd",
    "label",
    "legend",
    "li",
    "map",
    "mark",
    "meter",
    "nav",
    "ol",
    "output",
    "p",
    "pre",
    "progress",
    "q",
    "rp",
    "rt",
    "samp",
    "section",
    "select",
    "small",
    "span",
    "strong",
    "sub",
    "summary",
    "sup",
    "textarea",
    "time",
    "ul",
    "var",
    "video",
    "title",
    "script",
    "style",
}

_self_closing_pat = re.compile(
    r"<(?P<tag>%s)(?=[\s/])(?P<arg>[^>]*)/>" % ("|".join(self_closing_bad_tags)),
    re.IGNORECASE,
)
_self_closing_pat_bytes = re.compile(
    br"<(?P<tag>%s)(?=[\s/])(?P<arg>[^>]*)/>"
    % b"|".join(x.encode("ascii") for x in self_closing_bad_tags),
    re.IGNORECASE,
)


def close_self_closing_tags(raw: _typing.Any) -> _typing.Any:
    """
    Perform the close self closing tags operation under explicit file-format and conversion rules.

    Example:
        Exercise close self closing tags through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param raw: Value supplied for raw under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(raw, bytes):
        return _self_closing_pat_bytes.sub(br"<\g<tag>\g<arg>></\g<tag>>", raw)
    return _self_closing_pat.sub(r"<\g<tag>\g<arg>></\g<tag>>", raw)


def uuid_id() -> _typing.Any:
    """
    Perform the uuid id operation under explicit file-format and conversion rules.

    Example:
        Exercise uuid id through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return "u" + six_unicode(uuid.uuid4())


def itercsslinks(raw: _typing.Any) -> _typing.Iterator[_typing.Any]:
    """
    Perform the itercsslinks operation under explicit file-format and conversion rules.

    Example:
        Exercise itercsslinks through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param raw: Value supplied for raw under the utility contract.
    :return: An iterator yielding the normalized values described above.
    """
    for match in _css_url_re.finditer(raw):
        yield match.group(1), match.start(1)
    for match in _css_import_re.finditer(raw):
        yield match.group(1), match.start(1)


def iterlinks(root: _typing.Any, find_links_in_css: bool = True) -> _typing.Iterator[_typing.Any]:
    """
    Iterate over all links in a OEB Document.

    Example:
        Exercise iterlinks through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param find_links_in_css: Value supplied for find links in css under the utility
        contract.
    :return: An iterator yielding the normalized values described above.
    """
    assert etree.iselement(root)
    link_attrs = set(html.defs.link_attrs) | {XLINK("href"), "poster"}

    for el in root.iter():
        attribs = el.attrib
        try:
            tag = el.tag
        except UnicodeDecodeError:
            continue

        if tag == XHTML("object"):
            codebase = None
            # <object> tags have attributes that are relative to
            # codebase
            if "codebase" in attribs:
                codebase = el.get("codebase")
                yield (el, "codebase", codebase, 0)
            for attrib in "classid", "data":
                if attrib in attribs:
                    value = el.get(attrib)
                    if codebase is not None:
                        value = urljoin(codebase, value)
                    yield (el, attrib, value, 0)
            if "archive" in attribs:
                for match in _archive_re.finditer(el.get("archive")):
                    value = match.group(0)
                    if codebase is not None:
                        value = urljoin(codebase, value)
                    yield (el, "archive", value, match.start())
        else:
            for attr in attribs:
                if attr in link_attrs:
                    yield (el, attr, attribs[attr], 0)

        if not find_links_in_css:
            continue
        if tag == XHTML("style") and el.text:
            for match in _css_url_re.finditer(el.text):
                yield (el, None, match.group(1), match.start(1))
            for match in _css_import_re.finditer(el.text):
                yield (el, None, match.group(1), match.start(1))
        if "style" in attribs:
            for match in _css_url_re.finditer(attribs["style"]):
                yield (el, "style", match.group(1), match.start(1))


def make_links_absolute(root: _typing.Any, base_url: _typing.Any) -> None:
    """
    Make all links in the document absolute, given the ``base_url`` for the document (the full URL where the document came from)

    Example:
        Exercise make links absolute through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param base_url: Value supplied for base url under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    def link_repl(href: _typing.Any) -> _typing.Any:
        """
        Perform the link repl operation under explicit file-format and conversion rules.

        Example:
            Exercise make links absolute.link repl through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param href: Value supplied for href under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return urljoin(base_url, href)

    rewrite_links(root, link_repl)


def resolve_base_href(root: _typing.Any) -> None:
    """
    Perform the resolve base href operation under explicit file-format and conversion rules.

    Example:
        Exercise resolve base href through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    base_href = None
    basetags = root.xpath("//base[@href]|//h:base[@href]", namespaces=XPNSMAP)
    for b in basetags:
        base_href = b.get("href")
        b.drop_tree()
    if not base_href:
        return
    make_links_absolute(root, base_href, resolve_base_href=False)


def rewrite_links(root: _typing.Any, link_repl_func: _typing.Any, resolve_base_href: bool = False) -> None:
    """
    Rewrite all the links in the document. For each link ``link_repl_func(link)`` will be called, and the return value will replace the old link.

    Example:
        Exercise rewrite links through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param link_repl_func: Value supplied for link repl func under the utility contract.
    :param resolve_base_href: Value supplied for resolve base href under the utility
        contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    try:
        from cssutils import replaceUrls, log, CSSParser
    except ImportError:
        replaceUrls = log = CSSParser = None
    else:
        log.setLevel(logging.WARN)
        log.raiseExceptions = False

    if resolve_base_href:
        resolve_base_href(root)
    for el, attrib, link, pos in iterlinks(root, find_links_in_css=False):
        new_link = link_repl_func(link.strip())
        if new_link == link:
            continue
        if new_link is None:
            # Remove the attribute or element content
            if attrib is None:
                el.text = ""
            else:
                del el.attrib[attrib]
            continue
        if attrib is None:
            new = el.text[:pos] + new_link + el.text[pos + len(link) :]
            el.text = new
        else:
            cur = el.attrib[attrib]
            if not pos and len(cur) == len(link):
                # Most common case
                el.attrib[attrib] = new_link
            else:
                new = cur[:pos] + new_link + cur[pos + len(link) :]
                el.attrib[attrib] = new

    if CSSParser is None:
        # Fallback CSS URL rewriting without cssutils. This handles url(...)
        # and @import references in <style> and inline style attributes.
        def replace_css_links(text: _typing.Any) -> _typing.Any:
            """
            Perform the replace css links operation under explicit file-format and conversion rules.

            Example:
                Exercise rewrite links.replace css links through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :param text: Text parsed, normalized or rendered.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if not text:
                return text
            links = list(itercsslinks(text))
            if not links:
                return text
            for link, pos in reversed(links):
                new_link = link_repl_func(link.strip())
                if new_link == link:
                    continue
                if new_link is None:
                    new_link = ""
                if isinstance(new_link, bytes):
                    new_link = new_link.decode("utf-8", "replace")
                text = text[:pos] + new_link + text[pos + len(link) :]
            return text

        for el in root.iter():
            try:
                tag = el.tag
            except UnicodeDecodeError:
                continue
            if tag == XHTML("style") and el.text and (_css_url_re.search(el.text) is not None or "@import" in el.text):
                el.text = replace_css_links(el.text)
            style = el.attrib.get("style")
            if style and _css_url_re.search(style) is not None:
                el.attrib["style"] = replace_css_links(style)
        return

    parser = CSSParser(raiseExceptions=False, log=_css_logger, fetcher=lambda x: (None, None))
    for el in root.iter():
        try:
            tag = el.tag
        except UnicodeDecodeError:
            continue

        if tag == XHTML("style") and el.text and (_css_url_re.search(el.text) is not None or "@import" in el.text):
            stylesheet = parser.parseString(el.text, validate=False)
            replaceUrls(stylesheet, link_repl_func)
            repl = stylesheet.cssText
            if isbytestring(repl):
                repl = repl.decode("utf-8")
            el.text = "\n" + clean_xml_chars(repl) + "\n"

        if "style" in el.attrib:
            text = el.attrib["style"]
            if _css_url_re.search(text) is not None:
                try:
                    stext = parser.parseStyle(text, validate=False)
                except:
                    # Parsing errors are raised by cssutils
                    continue
                replaceUrls(stext, link_repl_func)
                repl = stext.cssText.replace("\n", " ").replace("\r", " ")
                if isbytestring(repl):
                    repl = repl.decode("utf-8")
                el.attrib["style"] = repl


types_map = get_types_map()
EPUB_MIME = types_map[".epub"]
XHTML_MIME = types_map[".xhtml"]
CSS_MIME = types_map[".css"]
NCX_MIME = types_map[".ncx"]
OPF_MIME = types_map[".opf"]
PAGE_MAP_MIME = "application/oebps-page-map+xml"
OEB_DOC_MIME = "text/x-oeb1-document"
OEB_CSS_MIME = "text/x-oeb1-css"
OPENTYPE_MIME = types_map[".otf"]
GIF_MIME = types_map[".gif"]
JPEG_MIME = types_map[".jpeg"]
PNG_MIME = types_map[".png"]
SVG_MIME = types_map[".svg"]
BINARY_MIME = "application/octet-stream"

XHTML_CSS_NAMESPACE = '@namespace "%s";\n' % XHTML_NS

# OEB document types allowed in the manifest
OEB_STYLES = {CSS_MIME, OEB_CSS_MIME, "text/x-oeb-css", "xhtml/css"}
OEB_DOCS = {XHTML_MIME, "text/html", OEB_DOC_MIME, "text/x-oeb-document"}
OEB_RASTER_IMAGES = {GIF_MIME, JPEG_MIME, PNG_MIME}
OEB_IMAGES = {GIF_MIME, JPEG_MIME, PNG_MIME, SVG_MIME}

MS_COVER_TYPE = "other.ms-coverimage-standard"

ENTITY_RE = re.compile(r"&([a-zA-Z_:][a-zA-Z0-9.-_:]+);")
COLLAPSE_RE = re.compile(r"[ \t\r\n\v]+")
QNAME_RE = re.compile(r"^[{][^{}]+[}][^{}]+$")
PREFIXNAME_RE = re.compile(r"^[^:]+[:][^:]+")
XMLDECL_RE = re.compile(r"^\s*<[?]xml.*?[?]>")
CSSURL_RE = re.compile(r"""url[(](?P<q>["']?)(?P<url>[^)]+)(?P=q)[)]""")


def element(parent: _typing.Any, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
    """
    Return an element, optionally under a parent. Pass parent = None to get a top level element.

    Example:
        Exercise element through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param parent: Value supplied for parent under the utility contract.
    :param args: Positional values forwarded to the compatibility implementation.
    :param kwargs: Keyword values forwarded to the compatibility implementation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if parent is not None:
        return etree.SubElement(parent, *args, **kwargs)
    return etree.Element(*args, **kwargs)


def prefixname(name: _typing.Any, nsrmap: _typing.Any) -> _typing.Any:
    """
    Makes an element name with the correct prefix.

    Example:
        Exercise prefixname through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param name: Field, file, function or resource name addressed by the operation.
    :param nsrmap: Value supplied for nsrmap under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not isqname(name):
        return name
    ns = namespace(name)
    if ns not in nsrmap:
        return name
    prefix = nsrmap[ns]
    if not prefix:
        return barename(name)
    return ":".join((prefix, barename(name)))


def isprefixname(name: _typing.Any) -> bool:
    """
    True if the name is prefixed - False otherwise

    Example:
        Exercise isprefixname through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param name: Field, file, function or resource name addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    # Uses an re to check if there's an : in the name
    return name and PREFIXNAME_RE.match(name) is not None


def qname(name: _typing.Any, nsmap: _typing.Any) -> _typing.Any:
    """
    Perform the qname operation under explicit file-format and conversion rules.

    Example:
        Exercise qname through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param name: Field, file, function or resource name addressed by the operation.
    :param nsmap: Value supplied for nsmap under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not isprefixname(name):
        return name
    prefix, local = name.split(":", 1)
    if prefix not in nsmap:
        return name
    return "{%s}%s" % (nsmap[prefix], local)


def isqname(name: _typing.Any) -> bool:
    """
    Perform the isqname operation under explicit file-format and conversion rules.

    Example:
        Exercise isqname through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param name: Field, file, function or resource name addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return name and QNAME_RE.match(name) is not None


def XPath(expr: _typing.Any) -> _typing.Any:
    """
    Perform the XPath operation under explicit file-format and conversion rules.

    Example:
        Exercise XPath through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param expr: Value supplied for expr under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return etree.XPath(expr, namespaces=XPNSMAP)


def xpath(elem: _typing.Any, expr: _typing.Any) -> _typing.Any:
    """
    Perform the xpath operation under explicit file-format and conversion rules.

    Example:
        Exercise xpath through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param elem: Value supplied for elem under the utility contract.
    :param expr: Value supplied for expr under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return elem.xpath(expr, namespaces=XPNSMAP)


def xml2str(root: _typing.Any, pretty_print: bool = False, strip_comments: bool = False, with_tail: bool = True) -> _typing.Any:
    """
    Render the xml document as a string

    Example:
        Exercise xml2str through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param pretty_print: Value supplied for pretty print under the utility contract.
    :param strip_comments: Value supplied for strip comments under the utility contract.
    :param with_tail: Value supplied for with tail under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not strip_comments:
        # -- in comments trips up adobe digital editions
        for x in root.iterdescendants():
            if getattr(x, "tag", None) is etree.Comment and x.text and "--" in x.text:
                x.text = x.text.replace("--", "__")
    ans = etree.tostring(
        root,
        encoding="utf-8",
        xml_declaration=True,
        pretty_print=pretty_print,
        with_tail=with_tail,
    )

    if strip_comments:
        ans = re.compile(r"<!--.*?-->", re.DOTALL).sub("", ans)

    return ans


def xml2unicode(root: _typing.Any, pretty_print: bool = False) -> _typing.Any:
    """
    Perform the xml2unicode operation under explicit file-format and conversion rules.

    Example:
        Exercise xml2unicode through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param pretty_print: Value supplied for pretty print under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return etree.tostring(root, pretty_print=pretty_print)


def xml2text(elem: _typing.Any) -> _typing.Any:
    """
    Perform the xml2text operation under explicit file-format and conversion rules.

    Example:
        Exercise xml2text through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param elem: Value supplied for elem under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return etree.tostring(elem, method="text", encoding=unicode, with_tail=False)


def serialize(data: _typing.Any, media_type: _typing.Any, pretty_print: bool = False) -> _typing.Any:
    """
    Perform the serialize operation under explicit file-format and conversion rules.

    Example:
        Exercise serialize through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param data: Value supplied for data under the utility contract.
    :param media_type: Value supplied for media type under the utility contract.
    :param pretty_print: Value supplied for pretty print under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(data, etree._Element):
        is_oeb_doc = media_type in OEB_DOCS
        if is_oeb_doc:
            for style in data.iterfind(".//{http://www.w3.org/1999/xhtml}style"):
                if style.text and re.search(r"[<>&]", style.text) is not None:
                    style.text = etree.CDATA(style.text)
        ans = xml2str(data, pretty_print=pretty_print)
        if is_oeb_doc:
            # Convert self closing div|span|a|video|audio|iframe|etc tags to normally closed ones, as they are
            # interpreted incorrectly by some browser based renderers
            ans = close_self_closing_tags(ans)
        return ans
    if isinstance(data, unicode):
        return data.encode("utf-8")
    if hasattr(data, "cssText"):
        data = data.cssText
        if isinstance(data, unicode):
            data = data.encode("utf-8")
        return data + b"\n"
    return bytes(data)


class SimpleCSSRule(object):
    """
    Provide the simplecssrule contract for validated ebook processing.

    Example:
        Exercise SimpleCSSRule through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
    """
    STYLE_RULE = 1
    CHARSET_RULE = 2

    def __init__(self: _typing.Self, css_text: _typing.Any) -> None:
        """
        Initialize and validate the simplecssrule state.

        Example:
            Exercise SimpleCSSRule.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param css_text: Value supplied for css text under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.cssText = css_text
        stripped = css_text.lstrip().lower()
        self.type = self.CHARSET_RULE if stripped.startswith("@charset") else self.STYLE_RULE


class SimpleCSSStyleSheet(object):
    """
    Provide the simplecssstylesheet contract for validated ebook processing.

    Example:
        Exercise SimpleCSSStyleSheet through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
    """
    def __init__(self: _typing.Self, text: str = "") -> None:
        """
        Initialize and validate the simplecssstylesheet state.

        Example:
            Exercise SimpleCSSStyleSheet.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param text: Text parsed, normalized or rendered.
        :return: None; validated state is stored on the receiving object.
        """
        self.namespaces = {}
        self.cssRules = []
        self.cssText = ""
        self.set_css_text(text)

    def _parse_rules(self: _typing.Self, text: _typing.Any) -> _typing.Any:
        """
        Parse rules under the format's safety and compatibility rules.

        Example:
            Exercise SimpleCSSStyleSheet. parse rules through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param text: Text parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        matches = re.findall(r"@charset\s+[^;]+;|[^{}]+{[^{}]*}", text, flags=re.I | re.S)
        if not matches:
            stripped = text.strip()
            matches = [stripped] if stripped else []
        return [SimpleCSSRule(x.strip()) for x in matches if x.strip()]

    def set_css_text(self: _typing.Self, text: _typing.Any) -> None:
        """
        Set css text under the format's safety and compatibility rules.

        Example:
            Exercise SimpleCSSStyleSheet.set css text through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param text: Text parsed, normalized or rendered.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.cssText = text or ""
        self.cssRules = self._parse_rules(self.cssText)

    def __iter__(self: _typing.Self) -> _typing.Any:
        """
        Perform the iter operation under explicit file-format and conversion rules.

        Example:
            Exercise SimpleCSSStyleSheet.  iter   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return iter(self.cssRules)

    def add(self: _typing.Self, rule: _typing.Any) -> None:
        """
        Perform the add operation under explicit file-format and conversion rules.

        Example:
            Exercise SimpleCSSStyleSheet.add through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param rule: Value supplied for rule under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.cssRules.append(SimpleCSSRule(getattr(rule, "cssText", six_unicode(rule))))
        self.cssText = "\n".join(r.cssText for r in self.cssRules)

    def deleteRule(self: _typing.Self, index: _typing.Any) -> None:
        """
        Perform the deleteRule operation under explicit file-format and conversion rules.

        Example:
            Exercise SimpleCSSStyleSheet.deleteRule through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param index: Value supplied for index under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        del self.cssRules[index]
        self.cssText = "\n".join(r.cssText for r in self.cssRules)


ASCII_CHARS = set(chr(x) for x in memory_range(128))
UNIBYTE_CHARS = set(chr(x) for x in memory_range(256))
URL_SAFE = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ" "abcdefghijklmnopqrstuvwxyz" "0123456789" "_.-/~")
URL_UNSAFE = [ASCII_CHARS - URL_SAFE, UNIBYTE_CHARS - URL_SAFE]


def urlquote(href: _typing.Any) -> _typing.Any:
    """
    Quote URL-unsafe characters, allowing IRI-safe characters. That is, this function returns valid IRIs not valid URIs. In particular, IRIs can contain non-ascii characters.

    Example:
        Exercise urlquote through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param href: Value supplied for href under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    result = []
    unsafe = 0 if isinstance(href, unicode) else 1
    unsafe = URL_UNSAFE[unsafe]
    for char in href:
        if char in unsafe:
            char = "%%%02x" % ord(char)
        result.append(char)
    return "".join(result)


def urlunquote(href: _typing.Any, error_handling: str = "strict") -> _typing.Any:
    """
    Perform the urlunquote operation under explicit file-format and conversion rules.

    Example:
        Exercise urlunquote through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param href: Value supplied for href under the utility contract.
    :param error_handling: Value supplied for error handling under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(href, bytes):
        href = href.decode("utf-8", error_handling)
    return unquote(href, errors=error_handling)


def urlnormalize(href: _typing.Any) -> _typing.Any:
    """
    Convert a URL into normalized form, with all and only URL-unsafe characters URL quoted.

    Example:
        Exercise urlnormalize through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param href: Value supplied for href under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    parts = urlparse(href)
    if not parts.scheme or parts.scheme == "file":
        path, frag = urldefrag(href)
        parts = ("", "", path, "", "", frag)
    parts = (part.replace("\\", "/") for part in parts)
    parts = (urlunquote(part) for part in parts)
    parts = (urlquote(part) for part in parts)
    return urlunparse(parts)


def extract(elem: _typing.Any) -> None:
    """
    Removes this element from the tree, including its children and text. The tail text is joined to the previous element or parent.

    Example:
        Exercise extract through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param elem: Value supplied for elem under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    parent = elem.getparent()
    if parent is not None:
        if elem.tail:
            previous = elem.getprevious()
            if previous is None:
                parent.text = (parent.text or "") + elem.tail
            else:
                previous.tail = (previous.tail or "") + elem.tail
        parent.remove(elem)


class DummyHandler(logging.Handler):
    """
    Dummy logging handler - passes the message on to the log if the log is set, otherwise does nothing.

    Example:
        Exercise DummyHandler through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
    """

    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the dummyhandler state.

        Example:
            Exercise DummyHandler.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: None; validated state is stored on the receiving object.
        """
        logging.Handler.__init__(self, logging.WARNING)
        self.setFormatter(logging.Formatter("%(message)s"))
        self.log = None

    def emit(self: _typing.Self, record: _typing.Any) -> None:
        """
        Perform the emit operation under explicit file-format and conversion rules.

        Example:
            Exercise DummyHandler.emit through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param record: Value supplied for record under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.log is not None:
            msg = self.format(record)
            f = self.log.error if record.levelno >= logging.ERROR else self.log.warn
            f(msg)


_css_logger = logging.getLogger("calibre.css")
_css_logger.setLevel(logging.WARNING)
_css_log_handler = DummyHandler()
_css_logger.addHandler(_css_log_handler)


class OEBError(Exception):
    """
    Generic OEB-processing error.

    Example:
        Exercise OEBError through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
    """

    pass


class NullContainer(object):
    """
    An empty container.

    Example:
        Exercise NullContainer through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
    """

    def __init__(self: _typing.Self, log: _typing.Any) -> None:
        """
        Initialize and validate the nullcontainer state.

        Example:
            Exercise NullContainer.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param log: Value supplied for log under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.log = log

    def read(self: _typing.Self, path: _typing.Any) -> None:
        """
        Perform the read operation under explicit file-format and conversion rules.

        Example:
            Exercise NullContainer.read through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise OEBError("Attempt to read from NullContainer")

    def write(self: _typing.Self, path: _typing.Any) -> None:
        """
        Perform the write operation under explicit file-format and conversion rules.

        Example:
            Exercise NullContainer.write through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise OEBError("Attempt to write to NullContainer")

    def exists(self: _typing.Self, path: _typing.Any) -> bool:
        """
        Perform the exists operation under explicit file-format and conversion rules.

        Example:
            Exercise NullContainer.exists through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return False

    def namelist(self: _typing.Self) -> list[_typing.Any]:
        """
        Perform the namelist operation under explicit file-format and conversion rules.

        Example:
            Exercise NullContainer.namelist through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return []


class DirContainer(object):
    """
    Filesystem directory container. Contains pointer to files which are stored on the file system.

    Example:
        Exercise DirContainer through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
    """

    def __init__(self: _typing.Self, path: _typing.Any, log: _typing.Any, ignore_opf: bool = False) -> None:
        """
        Initialize and validate the dircontainer state.

        Example:
            Exercise DirContainer.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :param log: Value supplied for log under the utility contract.
        :param ignore_opf: Value supplied for ignore opf under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.log = log
        # `isbytestring()` in this codebase currently treats str as bytes-like.
        # Only decode actual bytes here.
        if isinstance(path, bytes):
            path = path.decode(filesystem_encoding)
        self.opfname = None
        ext = os.path.splitext(path)[1].lower()
        if ext == ".opf":
            self.opfname = os.path.basename(path)
            self.rootdir = os.path.dirname(path)
            return

        self.rootdir = path
        if not ignore_opf:
            for path in self.namelist():
                ext = os.path.splitext(path)[1].lower()
                if ext == ".opf":
                    self.opfname = path
                    return

    def _unquote(self: _typing.Self, path: _typing.Any) -> _typing.Any:
        """
        Transforms a path into an actual path which (hopefully) points to a resource on the system.

        Example:
            Exercise DirContainer. unquote through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if isinstance(path, bytes):
            path = path.decode("utf-8", "replace")
        return urlunquote(path)

    def read(self: _typing.Self, path: _typing.Any) -> _typing.Any:
        """
        Read and returns the binary data for a given path.

        Example:
            Exercise DirContainer.read through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if path is None:
            path = self.opfname
        path = os.path.join(self.rootdir, self._unquote(path))
        with open(path, "rb") as f:
            return f.read()

    def write(self: _typing.Self, path: _typing.Any, data: _typing.Any) -> _typing.Any:
        """
        Write data out to the system.

        Example:
            Exercise DirContainer.write through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :param data: Value supplied for data under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        path = os.path.join(self.rootdir, self._unquote(path))
        target_dir = os.path.dirname(path)
        if not os.path.isdir(target_dir):
            os.makedirs(target_dir)
        with open(path, "wb") as f:
            return f.write(data)

    def exists(self: _typing.Self, path: _typing.Any) -> _typing.Any:
        """
        Checks to see if the given path exists

        Example:
            Exercise DirContainer.exists through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not path:
            return False
        try:
            path = os.path.join(self.rootdir, self._unquote(path))
        except ValueError:  # Happens if path contains quoted special chars
            return False
        try:
            return os.path.isfile(path)
        except UnicodeEncodeError:
            # On linux, if LANG is unset, the os.stat call tries to encode the unicode path using ASCII
            # To replicate try:
            # LANG=en_US.ASCII python -c "import os; os.stat(u'Espa\xf1a')"
            return os.path.isfile(path.encode(filesystem_encoding))

    def namelist(self: _typing.Self) -> _typing.Any:
        """
        Returns the names of all the files in the root - will always return posix style relative paths (paths separated by /).

        Example:
            Exercise DirContainer.namelist through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        names = []
        base = self.rootdir
        if isinstance(base, bytes):
            base = base.decode(filesystem_encoding, "replace")
        for root, dirs, files in os.walk(base):
            for fname in files:
                fname = os.path.join(root, fname)
                if isinstance(fname, bytes):
                    try:
                        fname = fname.decode(filesystem_encoding, "replace")
                    except Exception:
                        continue
                fname = fname.replace("\\", "/")
                names.append(fname)
        return names


class Metadata(object):
    """
    A collection of OEB data model metadata.

    Example:
        Exercise Metadata through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
    """

    DC_TERMS = {
        "contributor",
        "coverage",
        "creator",
        "date",
        "description",
        "format",
        "identifier",
        "language",
        "publisher",
        "relation",
        "rights",
        "source",
        "subject",
        "title",
        "type",
    }
    CALIBRE_TERMS = {
        "series",
        "series_index",
        "rating",
        "timestamp",
        "publication_type",
        "title_sort",
    }
    OPF_ATTRS = {
        "role": OPF("role"),
        "file-as": OPF("file-as"),
        "scheme": OPF("scheme"),
        "event": OPF("event"),
        "type": XSI("type"),
        "lang": XML("lang"),
        "id": "id",
    }
    OPF1_NSMAP = {"dc": DC11_NS, "oebpackage": OPF1_NS}
    OPF2_NSMAP = {
        "opf": OPF2_NS,
        "dc": DC11_NS,
        "dcterms": DCTERMS_NS,
        "xsi": XSI_NS,
        "calibre": CALIBRE_NS,
    }

    class Item(object):
        """
        An item of OEB data model metadata.

        Example:
            Exercise Metadata.Item through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
        """

        item_type = "MetadataItem"

        class Attribute(object):
            """
            Smart accessor for the attributes of an OEB metadata item

            Example:
                Exercise Metadata.Item.Attribute through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
            """

            def __init__(self: _typing.Self, attr: _typing.Any, allowed: _typing.Any = None) -> None:
                """
                Initialize and validate the attribute state.

                Example:
                    Exercise Metadata.Item.Attribute.  init   through a consuming regression::

                        python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


                :param attr: Value supplied for attr under the utility contract.
                :param allowed: Value supplied for allowed under the utility contract.
                :return: None; validated state is stored on the receiving object.
                """
                if not callable(attr):
                    attr_, attr = attr, lambda term: attr_
                self.attr = attr
                self.allowed = allowed

            def term_attr(self: _typing.Self, obj: _typing.Any) -> _typing.Any:
                """
                Perform the term attr operation under explicit file-format and conversion rules.

                Example:
                    Exercise Metadata.Item.Attribute.term attr through a consuming regression::

                        python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


                :param obj: Value supplied for obj under the utility contract.
                :return: The normalized value, metadata record, path, stream result or collection
                    described above.
                """
                term = obj.term
                if namespace(term) != DC11_NS:
                    term = OPF("meta")
                allowed = self.allowed
                if allowed is not None and term not in allowed:
                    raise AttributeError(
                        "attribute %r not valid for metadata term %r" % (self.attr(term), barename(obj.term))
                    )
                return self.attr(term)

            def __get__(self: _typing.Self, obj: _typing.Any, cls: _typing.Any) -> _typing.Any:
                """
                Perform the get operation under explicit file-format and conversion rules.

                Example:
                    Exercise Metadata.Item.Attribute.  get   through a consuming regression::

                        python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


                :param obj: Value supplied for obj under the utility contract.
                :param cls: Value supplied for cls under the utility contract.
                :return: The normalized value, metadata record, path, stream result or collection
                    described above.
                """
                if obj is None:
                    return None
                return obj.attrib.get(self.term_attr(obj), "")

            def __set__(self: _typing.Self, obj: _typing.Any, value: _typing.Any) -> None:
                """
                Perform the set operation under explicit file-format and conversion rules.

                Example:
                    Exercise Metadata.Item.Attribute.  set   through a consuming regression::

                        python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


                :param obj: Value supplied for obj under the utility contract.
                :param value: Value normalized, stored, formatted or returned.
                :return: None; the operation mutates state, writes output or performs cleanup in
                    place.
                """
                obj.attrib[self.term_attr(obj)] = value

        def __init__(self: _typing.Self, term: _typing.Any, value: _typing.Any, attrib: _typing.Any = None, nsmap: _typing.Any = None, **kwargs: _typing.Any) -> None:
            """
            Initialize and validate the item state.

            Example:
                Exercise Metadata.Item.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :param term: Value supplied for term under the utility contract.
            :param value: Value normalized, stored, formatted or returned.
            :param attrib: Value supplied for attrib under the utility contract.
            :param nsmap: Value supplied for nsmap under the utility contract.
            :param kwargs: Keyword values forwarded to the compatibility implementation.
            :return: None; validated state is stored on the receiving object.
            """
            # Make sure that the value being stored is not of the wrong type
            hash(value)
            if attrib is None:
                attrib = {}
            if nsmap is None:
                nsmap = {}

            self.attrib = attrib = dict(attrib)
            self.nsmap = nsmap = dict(nsmap)
            attrib.update(kwargs)
            if namespace(term) == OPF2_NS:
                term = barename(term)
            ns = namespace(term)
            local = barename(term).lower()
            if local in Metadata.DC_TERMS and (not ns or ns in DC_NSES):
                # Anything looking like Dublin Core is coerced
                term = DC(local)
            elif local in Metadata.CALIBRE_TERMS and ns in (CALIBRE_NS, ""):
                # Ditto for Calibre-specific metadata
                term = CALIBRE(local)
            self.term = term
            self.value = value
            for attr, value in list(attrib.items()):
                if isprefixname(value):
                    attrib[attr] = qname(value, nsmap)
                nsattr = Metadata.OPF_ATTRS.get(attr, attr)
                if nsattr == OPF("scheme") and namespace(term) != DC11_NS:
                    # The opf:meta element takes @scheme, not @opf:scheme
                    nsattr = "scheme"
                if attr != nsattr:
                    attrib[nsattr] = attrib.pop(attr)

        @property
        def name(self: _typing.Self) -> _typing.Any:
            """
            Perform the name operation under explicit file-format and conversion rules.

            Example:
                Exercise Metadata.Item.name through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.term

        @property
        def content(self: _typing.Self) -> _typing.Any:
            """
            Perform the content operation under explicit file-format and conversion rules.

            Example:
                Exercise Metadata.Item.content through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.value

        @content.setter
        def content(self: _typing.Self, value: _typing.Any) -> None:
            """
            Perform the content operation under explicit file-format and conversion rules.

            Example:
                Exercise Metadata.Item.content through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :param value: Value normalized, stored, formatted or returned.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.value = value

        scheme = Attribute(
            lambda term: "scheme" if term == OPF("meta") else OPF("scheme"),
            [DC("identifier"), OPF("meta")],
        )
        file_as = Attribute(OPF("file-as"), [DC("creator"), DC("contributor"), DC("title")])
        role = Attribute(OPF("role"), [DC("creator"), DC("contributor")])
        event = Attribute(OPF("event"), [DC("date")])
        id = Attribute("id")
        type = Attribute(XSI("type"), [DC("date"), DC("format"), DC("type")])
        lang = Attribute(
            XML("lang"),
            [
                DC("contributor"),
                DC("coverage"),
                DC("creator"),
                DC("publisher"),
                DC("relation"),
                DC("rights"),
                DC("source"),
                DC("subject"),
                OPF("meta"),
            ],
        )

        def __getitem__(self: _typing.Self, key: _typing.Any) -> _typing.Any:
            """
            Perform the getitem operation under explicit file-format and conversion rules.

            Example:
                Exercise Metadata.Item.  getitem   through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :param key: Metadata, identifier or local-variable key.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.attrib[key]

        def __setitem__(self: _typing.Self, key: _typing.Any, value: _typing.Any) -> None:
            """
            Perform the setitem operation under explicit file-format and conversion rules.

            Example:
                Exercise Metadata.Item.  setitem   through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :param key: Metadata, identifier or local-variable key.
            :param value: Value normalized, stored, formatted or returned.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.attrib[key] = value

        def __contains__(self: _typing.Self, key: _typing.Any) -> bool:
            """
            Perform the contains operation under explicit file-format and conversion rules.

            Example:
                Exercise Metadata.Item.  contains   through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :param key: Metadata, identifier or local-variable key.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return key in self.attrib

        def get(self: _typing.Self, key: _typing.Any, default: _typing.Any = None) -> _typing.Any:
            """
            Perform the get operation under explicit file-format and conversion rules.

            Example:
                Exercise Metadata.Item.get through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :param key: Metadata, identifier or local-variable key.
            :param default: Value supplied for default under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.attrib.get(key, default)

        def __repr__(self: _typing.Self) -> _typing.Any:
            """
            Perform the repr operation under explicit file-format and conversion rules.

            Example:
                Exercise Metadata.Item.  repr   through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return "Item(term=%r, value=%r, attrib=%r)" % (
                barename(self.term),
                self.value,
                self.attrib,
            )

        def __str__(self: _typing.Self) -> _typing.Any:
            """
            Perform the str operation under explicit file-format and conversion rules.

            Example:
                Exercise Metadata.Item.  str   through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            val = six_unicode(self.value)
            if isinstance(val, bytes):
                val = val.decode("utf-8", "replace")
            return val

        def __unicode__(self: _typing.Self) -> _typing.Any:
            """
            Perform the unicode operation under explicit file-format and conversion rules.

            Example:
                Exercise Metadata.Item.  unicode   through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return as_unicode(self.value)

        def to_opf1(self: _typing.Self, dcmeta: _typing.Any = None, xmeta: _typing.Any = None, nsrmap: _typing.Any = None) -> _typing.Any:

            """
            Perform the to opf1 operation under explicit file-format and conversion rules.

            Example:
                Exercise Metadata.Item.to opf1 through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :param dcmeta: Value supplied for dcmeta under the utility contract.
            :param xmeta: Value supplied for xmeta under the utility contract.
            :param nsrmap: Value supplied for nsrmap under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if nsrmap is None:
                nsrmap = {}

            attrib = {}
            for key, value in self.attrib.items():
                if namespace(key) == OPF2_NS:
                    key = barename(key)
                attrib[key] = prefixname(value, nsrmap)
            if namespace(self.term) == DC11_NS:
                name = DC(icu_title(barename(self.term)))
                elem = element(dcmeta, name, attrib=attrib)
                elem.text = self.value
            else:
                elem = element(xmeta, "meta", attrib=attrib)
                elem.attrib["name"] = prefixname(self.term, nsrmap)
                elem.attrib["content"] = prefixname(self.value, nsrmap)
            return elem

        def to_opf2(self: _typing.Self, parent: _typing.Any = None, nsrmap: _typing.Any = None) -> _typing.Any:

            """
            Perform the to opf2 operation under explicit file-format and conversion rules.

            Example:
                Exercise Metadata.Item.to opf2 through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :param parent: Value supplied for parent under the utility contract.
            :param nsrmap: Value supplied for nsrmap under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if nsrmap is None:
                nsrmap = {}

            attrib = {}
            for key, value in self.attrib.items():
                attrib[key] = prefixname(value, nsrmap)
            if namespace(self.term) == DC11_NS:
                elem = element(parent, self.term, attrib=attrib)
                try:
                    elem.text = self.value
                except:
                    elem.text = repr(self.value)
            else:
                elem = element(parent, OPF("meta"), attrib=attrib)
                elem.attrib["name"] = prefixname(self.term, nsrmap)
                elem.attrib["content"] = prefixname(self.value, nsrmap)
            return elem

    def __init__(self: _typing.Self, oeb: _typing.Any) -> None:
        """
        Initialize and validate the metadata state.

        Example:
            Exercise Metadata.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param oeb: Value supplied for oeb under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.oeb = oeb
        self.items = defaultdict(list)

    def add(self: _typing.Self, term: _typing.Any, value: _typing.Any, attrib: _typing.Any = None, nsmap: _typing.Any = None, **kwargs: _typing.Any) -> _typing.Any:
        """
        Add a new metadata item.

        Example:
            Exercise Metadata.add through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param term: Value supplied for term under the utility contract.
        :param value: Value normalized, stored, formatted or returned.
        :param attrib: Value supplied for attrib under the utility contract.
        :param nsmap: Value supplied for nsmap under the utility contract.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if attrib is None:
            attrib = {}
        if nsmap is None:
            nsmap = {}

        item = self.Item(term, value, attrib, nsmap, **kwargs)
        items = self.items[barename(item.term)]
        items.append(item)
        return item

    def iterkeys(self: _typing.Self) -> _typing.Iterator[_typing.Any]:
        """
        Perform the iterkeys operation under explicit file-format and conversion rules.

        Example:
            Exercise Metadata.iterkeys through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: An iterator yielding the normalized values described above.
        """
        for key in self.items:
            yield key

    __iter__ = iterkeys

    def clear(self: _typing.Self, key: _typing.Any) -> None:
        """
        Perform the clear operation under explicit file-format and conversion rules.

        Example:
            Exercise Metadata.clear through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param key: Metadata, identifier or local-variable key.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        l = self.items[key]
        for x in list(l):
            l.remove(x)

    def filter(self: _typing.Self, key: _typing.Any, predicate: _typing.Any) -> None:
        """
        Perform the filter operation under explicit file-format and conversion rules.

        Example:
            Exercise Metadata.filter through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param key: Metadata, identifier or local-variable key.
        :param predicate: Value supplied for predicate under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        l = self.items[key]
        for x in list(l):
            if predicate(x):
                l.remove(x)

    def __getitem__(self: _typing.Self, key: _typing.Any) -> _typing.Any:
        """
        Perform the getitem operation under explicit file-format and conversion rules.

        Example:
            Exercise Metadata.  getitem   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param key: Metadata, identifier or local-variable key.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.items[key]

    def __contains__(self: _typing.Self, key: _typing.Any) -> bool:
        """
        Perform the contains operation under explicit file-format and conversion rules.

        Example:
            Exercise Metadata.  contains   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param key: Metadata, identifier or local-variable key.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return key in self.items

    def __getattr__(self: _typing.Self, term: _typing.Any) -> _typing.Any:
        """
        Perform the getattr operation under explicit file-format and conversion rules.

        Example:
            Exercise Metadata.  getattr   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param term: Value supplied for term under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.items[term]

    @property
    def _nsmap(self: _typing.Self) -> _typing.Any:
        """
        Perform the nsmap operation under explicit file-format and conversion rules.

        Example:
            Exercise Metadata. nsmap through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        nsmap = {}
        for term in self.items:
            for item in self.items[term]:
                nsmap.update(item.nsmap)
        return nsmap

    @property
    def _opf1_nsmap(self: _typing.Self) -> _typing.Any:
        """
        Perform the opf1 nsmap operation under explicit file-format and conversion rules.

        Example:
            Exercise Metadata. opf1 nsmap through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        nsmap = self._nsmap
        for key, value in nsmap.items():
            if value in OPF_NSES or value in DC_NSES:
                del nsmap[key]
        return nsmap

    @property
    def _opf2_nsmap(self: _typing.Self) -> _typing.Any:
        """
        Perform the opf2 nsmap operation under explicit file-format and conversion rules.

        Example:
            Exercise Metadata. opf2 nsmap through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        nsmap = self._nsmap
        nsmap.update(OPF2_NSMAP)
        return nsmap

    def to_opf1(self: _typing.Self, parent: _typing.Any = None) -> _typing.Any:
        """
        Perform the to opf1 operation under explicit file-format and conversion rules.

        Example:
            Exercise Metadata.to opf1 through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param parent: Value supplied for parent under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        nsmap = self._opf1_nsmap
        nsrmap = dict((value, key) for key, value in nsmap.items())
        elem = element(parent, "metadata", nsmap=nsmap)
        dcmeta = element(elem, "dc-metadata", nsmap=OPF1_NSMAP)
        xmeta = element(elem, "x-metadata")
        for term in self.items:
            for item in self.items[term]:
                item.to_opf1(dcmeta, xmeta, nsrmap=nsrmap)
        if "ms-chaptertour" not in self.items:
            chaptertour = self.Item("ms-chaptertour", "chaptertour")
            chaptertour.to_opf1(dcmeta, xmeta, nsrmap=nsrmap)
        return elem

    def to_opf2(self: _typing.Self, parent: _typing.Any = None) -> _typing.Any:
        """
        Perform the to opf2 operation under explicit file-format and conversion rules.

        Example:
            Exercise Metadata.to opf2 through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param parent: Value supplied for parent under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        nsmap = self._opf2_nsmap
        nsrmap = dict((value, key) for key, value in nsmap.items())
        elem = element(parent, OPF("metadata"), nsmap=nsmap)
        for term in self.items:
            for item in self.items[term]:
                item.to_opf2(elem, nsrmap=nsrmap)
        return elem


class Manifest(object):
    """
    Collection of files composing an OEB data model book.

    Example:
        Exercise Manifest through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
    """

    class Item(object):
        """
        A representation of OEB data model book content file.

        Example:
            Exercise Manifest.Item through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
        """

        item_type = "ManifestItem"

        NUM_RE = re.compile("^(.*)([0-9][0-9.]*)(?=[.]|$)")

        def __init__(self: _typing.Self, oeb: _typing.Any, id: _typing.Any, href: _typing.Any, media_type: _typing.Any, fallback: _typing.Any = None, loader: _typing.Any = str, data: _typing.Any = None) -> None:
            """
            Initialize and validate the item state.

            Example:
                Exercise Manifest.Item.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :param oeb: Value supplied for oeb under the utility contract.
            :param id: Value supplied for id under the utility contract.
            :param href: Value supplied for href under the utility contract.
            :param media_type: Value supplied for media type under the utility contract.
            :param fallback: Value supplied for fallback under the utility contract.
            :param loader: Value supplied for loader under the utility contract.
            :param data: Value supplied for data under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            if href:
                href = six_unicode(href)
            self.oeb = oeb
            self.id = id
            self.href = self.path = urlnormalize(href)
            self.media_type = media_type
            self.fallback = fallback
            self.override_css_fetch = None
            self.spine_position = None
            self.linear = True
            if loader is None and data is None:
                loader = oeb.container.read
            self._loader = loader
            self._data = data

        def __repr__(self: _typing.Self) -> _typing.Any:
            """
            Perform the repr operation under explicit file-format and conversion rules.

            Example:
                Exercise Manifest.Item.  repr   through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return "Item(id=%r, href=%r, media_type=%r)" % (
                self.id,
                self.href,
                self.media_type,
            )

        # Parsing {{{
        def _parse_xml(self: _typing.Self, data: _typing.Any) -> _typing.Any:
            """
            Parse xml under the format's safety and compatibility rules.

            Example:
                Exercise Manifest.Item. parse xml through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :param data: Value supplied for data under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            data = xml_to_unicode(data, strip_encoding_pats=True, assume_utf8=True, resolve_entities=True)[0]
            if not data:
                return None
            return etree.fromstring(data, parser=RECOVER_PARSER)

        def _parse_xhtml(self: _typing.Self, data: _typing.Any) -> _typing.Any:
            """
            Parse xhtml under the format's safety and compatibility rules.

            Example:
                Exercise Manifest.Item. parse xhtml through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :param data: Value supplied for data under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            orig_data = data
            fname = urlunquote(self.href)
            self.oeb.log.debug("Parsing", fname, "...")
            self.oeb.html_preprocessor.current_href = self.href
            try:
                data = parse_html(
                    data,
                    log=self.oeb.log,
                    decoder=self.oeb.decode,
                    preprocessor=self.oeb.html_preprocessor,
                    filename=fname,
                    non_html_file_tags={"ncx"},
                )
            except NotHTML:
                return self._parse_xml(orig_data)
            return data

        def _parse_txt(self: _typing.Self, data: _typing.Any) -> _typing.Any:
            """
            Parse data as a string.

            Example:
                Exercise Manifest.Item. parse txt through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :param data: Value supplied for data under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if "<html>" in data:
                return self._parse_xhtml(data)

            self.oeb.log.debug("Converting", self.href, "...")

            from LiuXin_alpha.file_formats.txt.processor import convert_markdown

            title = self.oeb.metadata.title
            if title:
                title = six_unicode(title[0])
            else:
                title = _("Unknown")

            return self._parse_xhtml(convert_markdown(data, title=title))

        def _parse_css(self: _typing.Self, data: _typing.Any) -> _typing.Any:
            """
            Parse css under the format's safety and compatibility rules.

            Example:
                Exercise Manifest.Item. parse css through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :param data: Value supplied for data under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.oeb.log.debug("Parsing", self.href, "...")
            data = self.oeb.decode(data)
            data = self.oeb.css_preprocessor(data, add_namespace=True)
            try:
                from cssutils import CSSParser, log, resolveImports
                from cssutils.css import CSSRule
            except ModuleNotFoundError:
                return SimpleCSSStyleSheet(data)

            log.setLevel(logging.WARN)
            log.raiseExceptions = False
            parser = CSSParser(
                loglevel=logging.WARNING,
                fetcher=self.override_css_fetch or self._fetch_css,
                log=_css_logger,
            )
            data = parser.parseString(data, href=self.href, validate=False)
            data = resolveImports(data)
            data.namespaces["h"] = XHTML_NS
            for rule in tuple(data.cssRules.rulesOfType(CSSRule.PAGE_RULE)):
                data.cssRules.remove(rule)
            return data

        def _fetch_css(self: _typing.Self, path: _typing.Any) -> tuple[_typing.Any, ...]:
            """
            Perform the fetch css operation under explicit file-format and conversion rules.

            Example:
                Exercise Manifest.Item. fetch css through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :param path: Filesystem path read, written, normalized or validated by the
                operation.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            hrefs = self.oeb.manifest.hrefs
            if path not in hrefs:
                self.oeb.logger.warn("CSS import of missing file %r" % path)
                return None, None
            item = hrefs[path]
            if item.media_type not in OEB_STYLES:
                self.oeb.logger.warn("CSS import of non-CSS file %r" % path)
                return None, None
            data = item.data.cssText
            enc = None if isinstance(data, unicode) else "utf-8"
            return enc, data

        # }}}

        @property
        def data(self: _typing.Self) -> _typing.Any:
            """
            Provides MIME type sensitive access to the manifest entry's associated content.

            Example:
                Exercise Manifest.Item.data through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            data = self._data
            if data is None:
                if self._loader is None:
                    return None
                data = self._loader(getattr(self, "html_input_href", self.href))
            if not isinstance(data, (str, bytes)):
                pass  # already parsed
            elif self.media_type.lower() in OEB_DOCS:
                data = self._parse_xhtml(data)
            elif self.media_type.lower()[-4:] in ("+xml", "/xml"):
                data = self._parse_xml(data)
            elif self.media_type.lower() in OEB_STYLES:
                data = self._parse_css(data)
            elif self.media_type.lower() == "text/plain":
                self.oeb.log.warn("%s contains data in TXT format" % self.href, "converting to HTML")
                data = self._parse_txt(data)
                self.media_type = XHTML_MIME
            self._data = data
            return data

        @data.setter
        def data(self: _typing.Self, value: _typing.Any) -> None:
            """
            Perform the data operation under explicit file-format and conversion rules.

            Example:
                Exercise Manifest.Item.data through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :param value: Value normalized, stored, formatted or returned.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self._data = value

        @data.deleter
        def data(self: _typing.Self) -> None:
            """
            Perform the data operation under explicit file-format and conversion rules.

            Example:
                Exercise Manifest.Item.data through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self._data = None

        def unload_data_from_memory(self: _typing.Self, memory: _typing.Any = None) -> None:
            """
            Write the internal _data cache out to memory.

            Example:
                Exercise Manifest.Item.unload data from memory through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :param memory: Value supplied for memory under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if isinstance(self._data, (str, bytes)):
                if memory is None:
                    from LiuXin_alpha.utils.ptempfiles import PersistentTemporaryFile

                    pt = PersistentTemporaryFile(suffix="_oeb_base_mem_unloader.img")
                    with pt:
                        pt.write(self._data)
                    self.oeb._temp_files.append(pt.name)

                    def loader(*args: _typing.Any) -> _typing.Any:
                        """
                        Perform the loader operation under explicit file-format and conversion rules.

                        Example:
                            Exercise Manifest.Item.unload data from memory.loader through a consuming regression::

                                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


                        :param args: Positional values forwarded to the compatibility implementation.
                        :return: The normalized value, metadata record, path, stream result or collection
                            described above.
                        """
                        with open(pt.name, "rb") as f:
                            ans = f.read()
                        os.remove(pt.name)
                        return ans

                    self._loader = loader
                else:

                    def loader2(*args: _typing.Any) -> _typing.Any:
                        """
                        Perform the loader2 operation under explicit file-format and conversion rules.

                        Example:
                            Exercise Manifest.Item.unload data from memory.loader2 through a consuming regression::

                                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


                        :param args: Positional values forwarded to the compatibility implementation.
                        :return: The normalized value, metadata record, path, stream result or collection
                            described above.
                        """
                        with open(memory, "rb") as f:
                            ans = f.read()
                        return ans

                    self._loader = loader2
                self._data = None

        def __str__(self: _typing.Self) -> _typing.Any:
            """
            Perform the str operation under explicit file-format and conversion rules.

            Example:
                Exercise Manifest.Item.  str   through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            text = serialize(self.data, self.media_type, pretty_print=self.oeb.pretty_print)
            if isinstance(text, bytes):
                return text.decode("utf-8", "replace")
            return six_unicode(text)

        def __unicode__(self: _typing.Self) -> _typing.Any:
            """
            Perform the unicode operation under explicit file-format and conversion rules.

            Example:
                Exercise Manifest.Item.  unicode   through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            data = self.data
            if isinstance(data, etree._Element):
                return xml2unicode(data, pretty_print=self.oeb.pretty_print)
            if isinstance(data, unicode):
                return data
            if hasattr(data, "cssText"):
                return data.cssText
            return six_unicode(data)

        def __eq__(self: _typing.Self, other: _typing.Any) -> bool:
            """
            Perform the eq operation under explicit file-format and conversion rules.

            Example:
                Exercise Manifest.Item.  eq   through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :param other: Value supplied for other under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return id(self) == id(other)

        def __hash__(self: _typing.Self) -> _typing.Any:
            """
            Perform the hash operation under explicit file-format and conversion rules.

            Example:
                Exercise Manifest.Item.  hash   through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return id(self)

        def __ne__(self: _typing.Self, other: _typing.Any) -> _typing.Any:
            """
            Perform the ne operation under explicit file-format and conversion rules.

            Example:
                Exercise Manifest.Item.  ne   through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :param other: Value supplied for other under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return not self.__eq__(other)

        def __cmp__(self: _typing.Self, other: _typing.Any) -> _typing.Any:
            """
            Perform the cmp operation under explicit file-format and conversion rules.

            Example:
                Exercise Manifest.Item.  cmp   through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :param other: Value supplied for other under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            result = six_cmp(self.spine_position, other.spine_position)
            if result != 0:
                return result
            smatch = self.NUM_RE.search(self.href)
            sref = smatch.group(1) if smatch else self.href
            snum = float(smatch.group(2)) if smatch else 0.0
            skey = (sref, snum, self.id)
            omatch = self.NUM_RE.search(other.href)
            oref = omatch.group(1) if omatch else other.href
            onum = float(omatch.group(2)) if omatch else 0.0
            okey = (oref, onum, other.id)
            return six_cmp(skey, okey)

        def relhref(self: _typing.Self, href: _typing.Any) -> _typing.Any:
            """
            Convert the URL provided in :param:`href` from a book-absolute reference to a reference relative to this manifest item.

            Example:
                Exercise Manifest.Item.relhref through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :param href: Value supplied for href under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if urlparse(href).scheme:
                return href
            if "/" not in self.href:
                return href
            base = os.path.dirname(self.href).split("/")
            target, frag = urldefrag(href)
            target = target.split("/")
            # Code assumes that the index is always set by this for loop
            index = 0
            for index in memory_range(min(len(base), len(target))):
                if base[index] != target[index]:
                    break
            else:
                index += 1
            relhref = ([".."] * (len(base) - index)) + target[index:]
            relhref = "/".join(relhref)
            if frag:
                relhref = "#".join((relhref, frag))
            return relhref

        def abshref(self: _typing.Self, href: _typing.Any) -> _typing.Any:
            """
            Convert the URL provided in :param:`href` from a reference relative to this manifest item to a book-absolute reference.

            Example:
                Exercise Manifest.Item.abshref through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :param href: Value supplied for href under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            purl = urlparse(href)
            scheme = purl.scheme
            if scheme and scheme != "file":
                return href
            purl = list(purl)
            purl[0] = ""
            href = urlunparse(purl)
            path, frag = urldefrag(href)
            if not path:
                if frag:
                    return "#".join((self.href, frag))
                else:
                    return self.href
            if "/" not in self.href:
                return href
            dirname = os.path.dirname(self.href)
            href = os.path.join(dirname, href)
            href = os.path.normpath(href).replace("\\", "/")
            return href

    def __init__(self: _typing.Self, oeb: _typing.Any) -> None:
        """
        Initialize and validate the manifest state.

        Example:
            Exercise Manifest.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param oeb: Value supplied for oeb under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.oeb = oeb
        self.items = set()
        self.ids = {}
        self.hrefs = {}

    def add(self: _typing.Self, id: _typing.Any, href: _typing.Any, media_type: _typing.Any, fallback: _typing.Any = None, loader: _typing.Any = None, data: _typing.Any = None) -> _typing.Any:
        """
        Add a new item to the book manifest.

        Example:
            Exercise Manifest.add through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param id: Value supplied for id under the utility contract.
        :param href: Value supplied for href under the utility contract.
        :param media_type: Value supplied for media type under the utility contract.
        :param fallback: Value supplied for fallback under the utility contract.
        :param loader: Value supplied for loader under the utility contract.
        :param data: Value supplied for data under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        item = self.Item(self.oeb, id, href, media_type, fallback, loader, data)
        self.items.add(item)
        self.ids[item.id] = item
        self.hrefs[item.href] = item
        return item

    def remove(self: _typing.Self, item: _typing.Any) -> None:
        """
        Removes :param:`item` from the manifest.

        Example:
            Exercise Manifest.remove through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param item: Value supplied for item under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if item in self.ids:
            item = self.ids[item]
        del self.ids[item.id]
        if item.href in self.hrefs:
            del self.hrefs[item.href]
        self.items.remove(item)
        if item in self.oeb.spine:
            self.oeb.spine.remove(item)

    def remove_duplicate_item(self: _typing.Self, item: _typing.Any) -> None:
        """
        Perform the remove duplicate item operation under explicit file-format and conversion rules.

        Example:
            Exercise Manifest.remove duplicate item through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param item: Value supplied for item under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if item in self.ids:
            item = self.ids[item]
        del self.ids[item.id]
        self.items.remove(item)

    def generate(self: _typing.Self, id: _typing.Any = None, href: _typing.Any = None) -> tuple[_typing.Any, ...]:
        """
        Generate a new unique identifier and/or internal path for use in creating a new manifest item, using the provided :param:`id` and/or :param:`href` as bases.

        Example:
            Exercise Manifest.generate through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param id: Value supplied for id under the utility contract.
        :param href: Value supplied for href under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if id is not None:
            base = id
            index = 1
            while id in self.ids:
                id = base + str(index)
                index += 1
        if href is not None:
            href = urlnormalize(href)
            base, ext = os.path.splitext(href)
            index = 1
            lhrefs = set([x.lower() for x in self.hrefs])
            while href.lower() in lhrefs:
                href = base + str(index) + ext
                index += 1
        return id, six_unicode(href)

    def __iter__(self: _typing.Self) -> _typing.Iterator[_typing.Any]:
        """
        Perform the iter operation under explicit file-format and conversion rules.

        Example:
            Exercise Manifest.  iter   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: An iterator yielding the normalized values described above.
        """
        for item in self.items:
            yield item

    def __len__(self: _typing.Self) -> _typing.Any:
        """
        Perform the len operation under explicit file-format and conversion rules.

        Example:
            Exercise Manifest.  len   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return len(self.items)

    def values(self: _typing.Self) -> _typing.Any:
        """
        Perform the values operation under explicit file-format and conversion rules.

        Example:
            Exercise Manifest.values through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return list(self.items)

    def __contains__(self: _typing.Self, item: _typing.Any) -> bool:
        """
        Perform the contains operation under explicit file-format and conversion rules.

        Example:
            Exercise Manifest.  contains   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param item: Value supplied for item under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return item in self.items

    def to_opf1(self: _typing.Self, parent: _typing.Any = None) -> _typing.Any:
        """
        Perform the to opf1 operation under explicit file-format and conversion rules.

        Example:
            Exercise Manifest.to opf1 through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param parent: Value supplied for parent under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        elem = element(parent, "manifest")
        for item in self.items:
            media_type = item.media_type
            if media_type in OEB_DOCS:
                media_type = OEB_DOC_MIME
            elif media_type in OEB_STYLES:
                media_type = OEB_CSS_MIME
            attrib = {
                "id": item.id,
                "href": urlunquote(item.href),
                "media-type": media_type,
            }
            if item.fallback:
                attrib["fallback"] = item.fallback
            element(elem, "item", attrib=attrib)
        return elem

    def to_opf2(self: _typing.Self, parent: _typing.Any = None) -> _typing.Any:
        """
        Perform the to opf2 operation under explicit file-format and conversion rules.

        Example:
            Exercise Manifest.to opf2 through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param parent: Value supplied for parent under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        elem = element(parent, OPF("manifest"))
        for item in sorted(self.items, key=lambda x: x.href):
            media_type = item.media_type
            if media_type in OEB_DOCS:
                media_type = XHTML_MIME
            elif media_type in OEB_STYLES:
                media_type = CSS_MIME
            attrib = {
                "id": item.id,
                "href": urlunquote(item.href),
                "media-type": media_type,
            }
            if item.fallback:
                attrib["fallback"] = item.fallback
            element(elem, OPF("item"), attrib=attrib)
        return elem

    @property
    def main_stylesheet(self: _typing.Self) -> _typing.Any:
        """
        Perform the main stylesheet operation under explicit file-format and conversion rules.

        Example:
            Exercise Manifest.main stylesheet through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = getattr(self, "_main_stylesheet", None)
        if ans is None:
            for item in self:
                if item.media_type.lower() in OEB_STYLES:
                    ans = item
                    break
        return ans

    @main_stylesheet.setter
    def main_stylesheet(self: _typing.Self, item: _typing.Any) -> None:
        """
        Perform the main stylesheet operation under explicit file-format and conversion rules.

        Example:
            Exercise Manifest.main stylesheet through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param item: Value supplied for item under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._main_stylesheet = item


class Spine(object):
    """
    Collection of manifest items composing an OEB data model book's main textual content.

    Example:
        Exercise Spine through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
    """

    def __init__(self: _typing.Self, oeb: _typing.Any) -> None:
        """
        Initialize and validate the spine state.

        Example:
            Exercise Spine.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param oeb: Value supplied for oeb under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.oeb = oeb
        self.items = []
        self.page_progression_direction = None

    def _linear(self: _typing.Self, linear: _typing.Any) -> _typing.Any:
        """
        Perform the linear operation under explicit file-format and conversion rules.

        Example:
            Exercise Spine. linear through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param linear: Value supplied for linear under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if isinstance(linear, six_string_types):
            linear = linear.lower()
        if linear is None or linear in ("yes", "true"):
            linear = True
        elif linear in ("no", "false"):
            linear = False
        return linear

    def add(self: _typing.Self, item: _typing.Any, linear: _typing.Any = None) -> _typing.Any:
        """
        Append :param:`item` to the end of the `Spine`.

        Example:
            Exercise Spine.add through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param item: Value supplied for item under the utility contract.
        :param linear: Value supplied for linear under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        item.linear = self._linear(linear)
        item.spine_position = len(self.items)
        self.items.append(item)
        return item

    def insert(self: _typing.Self, index: _typing.Any, item: _typing.Any, linear: _typing.Any) -> _typing.Any:
        """
        Insert :param:`item` at position :param:`index` in the `Spine`.

        Example:
            Exercise Spine.insert through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param index: Value supplied for index under the utility contract.
        :param item: Value supplied for item under the utility contract.
        :param linear: Value supplied for linear under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        item.linear = self._linear(linear)
        item.spine_position = index
        self.items.insert(index, item)
        for i in memory_range(index, len(self.items)):
            self.items[i].spine_position = i
        return item

    def remove(self: _typing.Self, item: _typing.Any) -> None:
        """
        Remove :param:`item` from the `Spine`.

        Example:
            Exercise Spine.remove through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param item: Value supplied for item under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        index = item.spine_position
        self.items.pop(index)
        for i in memory_range(index, len(self.items)):
            self.items[i].spine_position = i
        item.spine_position = None

    def index(self: _typing.Self, item: _typing.Any) -> _typing.Any:
        """
        Perform the index operation under explicit file-format and conversion rules.

        Example:
            Exercise Spine.index through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param item: Value supplied for item under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        for i, x in enumerate(self):
            if item == x:
                return i
        return -1

    def __iter__(self: _typing.Self) -> _typing.Iterator[_typing.Any]:
        """
        Perform the iter operation under explicit file-format and conversion rules.

        Example:
            Exercise Spine.  iter   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: An iterator yielding the normalized values described above.
        """
        for item in self.items:
            yield item

    def __getitem__(self: _typing.Self, index: _typing.Any) -> _typing.Any:
        """
        Perform the getitem operation under explicit file-format and conversion rules.

        Example:
            Exercise Spine.  getitem   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param index: Value supplied for index under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.items[index]

    def __len__(self: _typing.Self) -> _typing.Any:
        """
        Perform the len operation under explicit file-format and conversion rules.

        Example:
            Exercise Spine.  len   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return len(self.items)

    def __contains__(self: _typing.Self, item: _typing.Any) -> bool:
        """
        Perform the contains operation under explicit file-format and conversion rules.

        Example:
            Exercise Spine.  contains   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param item: Value supplied for item under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return item in self.items

    def to_opf1(self: _typing.Self, parent: _typing.Any = None) -> _typing.Any:
        """
        Perform the to opf1 operation under explicit file-format and conversion rules.

        Example:
            Exercise Spine.to opf1 through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param parent: Value supplied for parent under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        elem = element(parent, "spine")
        for item in self.items:
            if item.linear:
                element(elem, "itemref", attrib={"idref": item.id})
        return elem

    def to_opf2(self: _typing.Self, parent: _typing.Any = None) -> _typing.Any:
        """
        Perform the to opf2 operation under explicit file-format and conversion rules.

        Example:
            Exercise Spine.to opf2 through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param parent: Value supplied for parent under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        elem = element(parent, OPF("spine"))
        for item in self.items:
            attrib = {"idref": item.id}
            if not item.linear:
                attrib["linear"] = "no"
            element(elem, OPF("itemref"), attrib=attrib)
        return elem


class Guide(object):
    """
    Collection of references to standard frequently-occurring sections within an OEB data model book.

    Example:
        Exercise Guide through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
    """

    class Reference(object):
        """
        Reference to a standard book section.

        Example:
            Exercise Guide.Reference through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
        """

        _TYPES_TITLES = [
            ("cover", _("Cover")),
            ("title-page", _("Title Page")),
            ("toc", _("Table of Contents")),
            ("index", _("Index")),
            ("glossary", _("Glossary")),
            ("acknowledgements", _("Acknowledgements")),
            ("bibliography", _("Bibliography")),
            ("colophon", _("Colophon")),
            ("copyright-page", _("Copyright")),
            ("dedication", _("Dedication")),
            ("epigraph", _("Epigraph")),
            ("foreword", _("Foreword")),
            ("loi", _("List of Illustrations")),
            ("lot", _("List of Tables")),
            ("notes", _("Notes")),
            ("preface", _("Preface")),
            ("text", _("Main Text")),
        ]
        TYPES = set(t for t, _ in _TYPES_TITLES)  # noqa
        TITLES = dict(_TYPES_TITLES)
        ORDER = dict((t, i) for i, (t, _) in enumerate(_TYPES_TITLES))  # noqa

        def __init__(self: _typing.Self, oeb: _typing.Any, type: _typing.Any, title: _typing.Any, href: _typing.Any) -> None:
            """
            Initialize and validate the reference state.

            Example:
                Exercise Guide.Reference.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :param oeb: Value supplied for oeb under the utility contract.
            :param type: Value supplied for type under the utility contract.
            :param title: Value supplied for title under the utility contract.
            :param href: Value supplied for href under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            self.oeb = oeb
            if type.lower() in self.TYPES:
                local_type = type.lower()
            elif type not in self.TYPES and not type.startswith("other."):
                local_type = "other." + type
            else:
                local_type = type
            if not title and local_type in self.TITLES:
                title = oeb.translate(self.TITLES[local_type])
            self.type = local_type
            self.title = title
            self.href = urlnormalize(href)

        def __repr__(self: _typing.Self) -> _typing.Any:
            """
            Perform the repr operation under explicit file-format and conversion rules.

            Example:
                Exercise Guide.Reference.  repr   through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return "Reference(type=%r, title=%r, href=%r)" % (
                self.type,
                self.title,
                self.href,
            )

        @property
        def _order(self: _typing.Self) -> _typing.Any:
            """
            Perform the order operation under explicit file-format and conversion rules.

            Example:
                Exercise Guide.Reference. order through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.ORDER.get(self.type, self.type)

        @property
        def _sort_key(self: _typing.Self) -> tuple[int, int | str]:
            """
            Return a Python 3-safe ordering key for guide references.

            Example:
                Exercise Guide.Reference. sort key through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if self.type in self.ORDER:
                return (0, self.ORDER[self.type])
            return (1, self.type)

        def __cmp__(self: _typing.Self, other: _typing.Any) -> _typing.Any:
            """
            Perform the cmp operation under explicit file-format and conversion rules.

            Example:
                Exercise Guide.Reference.  cmp   through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :param other: Value supplied for other under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if not isinstance(other, Guide.Reference):
                return NotImplemented
            return six_cmp(self._order, other._order)

        @property
        def item(self: _typing.Self) -> _typing.Any:
            """
            The manifest item associated with this reference.

            Example:
                Exercise Guide.Reference.item through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            path = urldefrag(self.href)[0]
            hrefs = self.oeb.manifest.hrefs
            return hrefs.get(path, None)

    def __init__(self: _typing.Self, oeb: _typing.Any) -> None:
        """
        Initialize and validate the guide state.

        Example:
            Exercise Guide.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param oeb: Value supplied for oeb under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.oeb = oeb
        self.refs = {}

    def add(self: _typing.Self, type: _typing.Any, title: _typing.Any, href: _typing.Any) -> _typing.Any:
        """
        Add a new reference to the `Guide`.

        Example:
            Exercise Guide.add through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param type: Value supplied for type under the utility contract.
        :param title: Value supplied for title under the utility contract.
        :param href: Value supplied for href under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if href:
            href = six_unicode(href)
        ref = self.Reference(self.oeb, type, title, href)
        self.refs[type] = ref
        return ref

    def remove(self: _typing.Self, type: _typing.Any) -> _typing.Any:
        """
        Perform the remove operation under explicit file-format and conversion rules.

        Example:
            Exercise Guide.remove through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param type: Value supplied for type under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.refs.pop(type, None)

    def iterkeys(self: _typing.Self) -> _typing.Iterator[_typing.Any]:
        """
        Perform the iterkeys operation under explicit file-format and conversion rules.

        Example:
            Exercise Guide.iterkeys through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: An iterator yielding the normalized values described above.
        """
        for item_type in self.refs:
            yield item_type

    __iter__ = iterkeys

    def values(self: _typing.Self) -> _typing.Any:
        """
        Perform the values operation under explicit file-format and conversion rules.

        Example:
            Exercise Guide.values through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return sorted(self.refs.values(), key=lambda ref: ref._sort_key)

    def items(self: _typing.Self) -> _typing.Iterator[_typing.Any]:
        """
        Perform the items operation under explicit file-format and conversion rules.

        Example:
            Exercise Guide.items through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: An iterator yielding the normalized values described above.
        """
        for item_type, ref in self.refs.items():
            yield item_type, ref

    def __getitem__(self: _typing.Self, key: _typing.Any) -> _typing.Any:
        """
        Perform the getitem operation under explicit file-format and conversion rules.

        Example:
            Exercise Guide.  getitem   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param key: Metadata, identifier or local-variable key.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.refs[key]

    def __delitem__(self: _typing.Self, key: _typing.Any) -> None:
        """
        Perform the delitem operation under explicit file-format and conversion rules.

        Example:
            Exercise Guide.  delitem   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param key: Metadata, identifier or local-variable key.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        del self.refs[key]

    def __contains__(self: _typing.Self, key: _typing.Any) -> bool:
        """
        Perform the contains operation under explicit file-format and conversion rules.

        Example:
            Exercise Guide.  contains   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param key: Metadata, identifier or local-variable key.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return key in self.refs

    def __len__(self: _typing.Self) -> _typing.Any:
        """
        Perform the len operation under explicit file-format and conversion rules.

        Example:
            Exercise Guide.  len   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return len(self.refs)

    def to_opf1(self: _typing.Self, parent: _typing.Any = None) -> _typing.Any:
        """
        Perform the to opf1 operation under explicit file-format and conversion rules.

        Example:
            Exercise Guide.to opf1 through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param parent: Value supplied for parent under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        elem = element(parent, "guide")
        for ref in self.refs.values():
            attrib = {"type": ref.type, "href": urlunquote(ref.href)}
            if ref.title:
                attrib["title"] = ref.title
            element(elem, "reference", attrib=attrib)
        return elem

    def to_opf2(self: _typing.Self, parent: _typing.Any = None) -> _typing.Any:
        """
        Perform the to opf2 operation under explicit file-format and conversion rules.

        Example:
            Exercise Guide.to opf2 through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param parent: Value supplied for parent under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        elem = element(parent, OPF("guide"))
        for ref in self.refs.values():
            attrib = {"type": ref.type, "href": urlunquote(ref.href)}
            if ref.title:
                attrib["title"] = ref.title
            element(elem, OPF("reference"), attrib=attrib)
        return elem


class TOC(object):
    """
    Represents a hierarchical table of contents or navigation tree for accessing arbitrary semantic sections within an OEB data model book.

    Example:
        Exercise TOC through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
    """

    def __init__(
        self: _typing.Self,
        title: _typing.Any = None,
        href: _typing.Any = None,
        klass: _typing.Any = None,
        id: _typing.Any = None,
        play_order: _typing.Any = None,
        author: _typing.Any = None,
        description: _typing.Any = None,
        toc_thumbnail: _typing.Any = None,
    ) -> None:
        """
        Initialize and validate the toc state.

        Example:
            Exercise TOC.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param title: Value supplied for title under the utility contract.
        :param href: Value supplied for href under the utility contract.
        :param klass: Value supplied for klass under the utility contract.
        :param id: Value supplied for id under the utility contract.
        :param play_order: Value supplied for play order under the utility contract.
        :param author: Value supplied for author under the utility contract.
        :param description: Value supplied for description under the utility contract.
        :param toc_thumbnail: Value supplied for toc thumbnail under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.title = title
        self.href = urlnormalize(href) if href else href
        self.klass = klass
        self.id = id
        self.nodes = []
        self.play_order = 0
        if play_order is None:
            play_order = self.next_play_order()
        self.play_order = play_order
        self.author = author
        self.description = description
        self.toc_thumbnail = toc_thumbnail

    def add(
        self: _typing.Self,
        title: _typing.Any,
        href: _typing.Any,
        klass: _typing.Any = None,
        id: _typing.Any = None,
        play_order: int = 0,
        author: _typing.Any = None,
        description: _typing.Any = None,
        toc_thumbnail: _typing.Any = None,
    ) -> _typing.Any:
        """
        Create and return a new sub-node of this node.

        Example:
            Exercise TOC.add through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param title: Value supplied for title under the utility contract.
        :param href: Value supplied for href under the utility contract.
        :param klass: Value supplied for klass under the utility contract.
        :param id: Value supplied for id under the utility contract.
        :param play_order: Value supplied for play order under the utility contract.
        :param author: Value supplied for author under the utility contract.
        :param description: Value supplied for description under the utility contract.
        :param toc_thumbnail: Value supplied for toc thumbnail under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        node = TOC(title, href, klass, id, play_order, author, description, toc_thumbnail)
        self.nodes.append(node)
        return node

    def remove(self: _typing.Self, node: _typing.Any) -> bool:
        """
        Perform the remove operation under explicit file-format and conversion rules.

        Example:
            Exercise TOC.remove through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param node: Value supplied for node under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        for child in self.nodes:
            if child is node:
                self.nodes.remove(child)
                return True
            else:
                if child.remove(node):
                    return True
        return False

    def iter(self: _typing.Self) -> _typing.Iterator[_typing.Any]:
        """
        Iterate over this node and all descendants in depth-first order.

        Example:
            Exercise TOC.iter through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: An iterator yielding the normalized values described above.
        """
        yield self
        for child in self.nodes:
            for node in child.iter():
                yield node

    def count(self: _typing.Self) -> _typing.Any:
        """
        Perform the count operation under explicit file-format and conversion rules.

        Example:
            Exercise TOC.count through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return len(list(self.iter())) - 1

    def next_play_order(self: _typing.Self) -> _typing.Any:
        """
        Perform the next play order operation under explicit file-format and conversion rules.

        Example:
            Exercise TOC.next play order through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        entries = [x.play_order for x in self.iter()]
        base = max(entries) if entries else 0
        return base + 1

    def has_href(self: _typing.Self, href: _typing.Any) -> bool:
        """
        Return whether has href holds for the supplied ebook data.

        Example:
            Exercise TOC.has href through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param href: Value supplied for href under the utility contract.
        :return: True when the documented condition holds; otherwise False.
        """
        for x in self.iter():
            if x.href == href:
                return True
        return False

    def has_text(self: _typing.Self, text: _typing.Any) -> bool:
        """
        Return whether has text holds for the supplied ebook data.

        Example:
            Exercise TOC.has text through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param text: Text parsed, normalized or rendered.
        :return: True when the documented condition holds; otherwise False.
        """
        for x in self.iter():
            if x.title and x.title.lower() == text.lower():
                return True
        return False

    def iterdescendants(self: _typing.Self, breadth_first: bool = False) -> _typing.Iterator[_typing.Any]:
        """
        Iterate over all descendant nodes in depth-first order.

        Example:
            Exercise TOC.iterdescendants through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param breadth_first: Value supplied for breadth first under the utility contract.
        :return: An iterator yielding the normalized values described above.
        """
        if breadth_first:
            for child in self.nodes:
                yield child
            for child in self.nodes:
                for node in child.iterdescendants(breadth_first=True):
                    yield node
        else:
            for child in self.nodes:
                for node in child.iter():
                    yield node

    def __iter__(self: _typing.Self) -> _typing.Iterator[_typing.Any]:
        """
        Iterate over all immediate child nodes.

        Example:
            Exercise TOC.  iter   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: An iterator yielding the normalized values described above.
        """
        for node in self.nodes:
            yield node

    def __getitem__(self: _typing.Self, index: _typing.Any) -> _typing.Any:
        """
        Perform the getitem operation under explicit file-format and conversion rules.

        Example:
            Exercise TOC.  getitem   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param index: Value supplied for index under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.nodes[index]

    def autolayer(self: _typing.Self) -> None:
        """
        Make sequences of children pointing to the same content file into children of the first node referencing that file.

        Example:
            Exercise TOC.autolayer through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        prev = None
        for node in list(self.nodes):
            if prev and urldefrag(prev.href)[0] == urldefrag(node.href)[0]:
                self.nodes.remove(node)
                prev.nodes.append(node)
            else:
                prev = node

    def depth(self: _typing.Self) -> _typing.Any:
        """
        The maximum depth of the navigation tree rooted at this node.

        Example:
            Exercise TOC.depth through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return max(node.depth() for node in self.nodes) + 1
        except ValueError:
            return 1

    def get_lines(self: _typing.Self, lvl: int = 0) -> _typing.Any:
        """
        Return lines under the format's safety and compatibility rules.

        Example:
            Exercise TOC.get lines through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param lvl: Value supplied for lvl under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = [("\t" * lvl) + "TOC: %s --> %s" % (self.title, self.href)]
        for child in self:
            ans.extend(child.get_lines(lvl + 1))
        return ans

    def __str__(self: _typing.Self) -> _typing.Any:
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise TOC.  str   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "\n".join(self.get_lines())

    def __unicode__(self: _typing.Self) -> _typing.Any:
        """
        Perform the unicode operation under explicit file-format and conversion rules.

        Example:
            Exercise TOC.  unicode   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "\n".join(self.get_lines())

    def to_opf1(self: _typing.Self, tour: _typing.Any) -> _typing.Any:
        """
        Perform the to opf1 operation under explicit file-format and conversion rules.

        Example:
            Exercise TOC.to opf1 through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param tour: Value supplied for tour under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        for node in self.nodes:
            element(
                tour,
                "site",
                attrib={"title": node.title, "href": urlunquote(node.href)},
            )
            node.to_opf1(tour)
        return tour

    def to_ncx(self: _typing.Self, parent: _typing.Any = None) -> _typing.Any:
        """
        Perform the to ncx operation under explicit file-format and conversion rules.

        Example:
            Exercise TOC.to ncx through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param parent: Value supplied for parent under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if parent is None:
            parent = etree.Element(NCX("navMap"))
        for node in self.nodes:
            node_id = node.id or uuid_id()
            po = node.play_order
            if po == 0:
                po = 1
            attrib = {"id": node_id, "playOrder": str(po)}
            if node.klass:
                attrib["class"] = node.klass
            point = element(parent, NCX("navPoint"), attrib=attrib)
            label = etree.SubElement(point, NCX("navLabel"))
            title = node.title
            if title:
                title = re.sub(r"\s+", " ", title)
            element(label, NCX("text")).text = title
            # Do not unescape this URL as ADE requires it to be escaped to
            # handle semi colons and other special characters in the file names
            element(point, NCX("content"), src=node.href)
            node.to_ncx(point)
        return parent

    def rationalize_play_orders(self: _typing.Self) -> None:
        """
        Ensure that all nodes with the same play_order have the same href and with different play_orders have different hrefs.

        Example:
            Exercise TOC.rationalize play orders through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        def po_node(n: _typing.Any) -> _typing.Any:
            """
            Perform the po node operation under explicit file-format and conversion rules.

            Example:
                Exercise TOC.rationalize play orders.po node through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :param n: Value supplied for n under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            for x in self.iter():
                if x is n:
                    return
                if x.play_order == n.play_order:
                    return x

        def href_node(n: _typing.Any) -> _typing.Any:
            """
            Perform the href node operation under explicit file-format and conversion rules.

            Example:
                Exercise TOC.rationalize play orders.href node through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :param n: Value supplied for n under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            for loc_x in self.iter():
                if loc_x is n:
                    return
                if loc_x.href == n.href:
                    return loc_x

        for local_x in self.iter():
            y = po_node(local_x)
            if y is not None:
                if local_x.href != y.href:
                    local_x.play_order = getattr(href_node(local_x), "play_order", self.next_play_order())
            y = href_node(local_x)
            if y is not None:
                local_x.play_order = y.play_order


class PageList(object):
    """
    Collection of named "pages" to mapped positions within an OEB data model book's textual content.

    Example:
        Exercise PageList through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
    """

    class Page(object):
        """
        Represents a mapping between a page name and a position within the book content.

        Example:
            Exercise PageList.Page through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
        """

        TYPES = {"front", "normal", "special"}

        def __init__(self: _typing.Self, name: _typing.Any, href: _typing.Any, type: str = "normal", klass: _typing.Any = None, id: _typing.Any = None) -> None:
            """
            Initialize and validate the page state.

            Example:
                Exercise PageList.Page.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :param name: Field, file, function or resource name addressed by the operation.
            :param href: Value supplied for href under the utility contract.
            :param type: Value supplied for type under the utility contract.
            :param klass: Value supplied for klass under the utility contract.
            :param id: Value supplied for id under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            self.name = six_unicode(name)
            self.href = urlnormalize(href)
            self.type = type if type in self.TYPES else "normal"
            self.id = id
            self.klass = klass

    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the pagelist state.

        Example:
            Exercise PageList.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: None; validated state is stored on the receiving object.
        """
        self.pages = []

    def add(self: _typing.Self, name: _typing.Any, href: _typing.Any, type: str = "normal", klass: _typing.Any = None, id: _typing.Any = None) -> _typing.Any:
        """
        Create a new page and add it to the `PageList`.

        Example:
            Exercise PageList.add through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param href: Value supplied for href under the utility contract.
        :param type: Value supplied for type under the utility contract.
        :param klass: Value supplied for klass under the utility contract.
        :param id: Value supplied for id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        page = self.Page(name, href, type, klass, id)
        self.pages.append(page)
        return page

    def __len__(self: _typing.Self) -> _typing.Any:
        """
        Perform the len operation under explicit file-format and conversion rules.

        Example:
            Exercise PageList.  len   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return len(self.pages)

    def __iter__(self: _typing.Self) -> _typing.Iterator[_typing.Any]:
        """
        Perform the iter operation under explicit file-format and conversion rules.

        Example:
            Exercise PageList.  iter   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: An iterator yielding the normalized values described above.
        """
        for page in self.pages:
            yield page

    def __getitem__(self: _typing.Self, index: _typing.Any) -> _typing.Any:
        """
        Perform the getitem operation under explicit file-format and conversion rules.

        Example:
            Exercise PageList.  getitem   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param index: Value supplied for index under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.pages[index]

    def pop(self: _typing.Self, index: _typing.Any = -1) -> _typing.Any:
        """
        Perform the pop operation under explicit file-format and conversion rules.

        Example:
            Exercise PageList.pop through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param index: Value supplied for index under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.pages.pop(index)

    def remove(self: _typing.Self, page: _typing.Any) -> _typing.Any:
        """
        Perform the remove operation under explicit file-format and conversion rules.

        Example:
            Exercise PageList.remove through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param page: Value supplied for page under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.pages.remove(page)

    def to_ncx(self: _typing.Self, parent: _typing.Any = None) -> _typing.Any:
        """
        Perform the to ncx operation under explicit file-format and conversion rules.

        Example:
            Exercise PageList.to ncx through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param parent: Value supplied for parent under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        plist = element(parent, NCX("pageList"), id=uuid_id())
        values = dict((t, count(1)) for t in ("front", "normal", "special"))
        for page in self.pages:
            id = page.id or uuid_id()
            page_type = page.type
            value = str(next(values[page_type]))
            attrib = {"id": id, "value": value, "type": page_type, "playOrder": "0"}
            if page.klass:
                attrib["class"] = page.klass
            ptarget = element(plist, NCX("pageTarget"), attrib=attrib)
            label = element(ptarget, NCX("navLabel"))
            element(label, NCX("text")).text = page.name
            element(ptarget, NCX("content"), src=page.href)
        return plist

    def to_page_map(self: _typing.Self) -> _typing.Any:
        """
        Perform the to page map operation under explicit file-format and conversion rules.

        Example:
            Exercise PageList.to page map through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        pmap = etree.Element(OPF("page-map"), nsmap={None: OPF2_NS})
        for page in self.pages:
            element(pmap, OPF("page"), name=page.name, href=page.href)
        return pmap


class OEBBook(object):
    """
    Representation of a book in the IDPF OEB data model.

    Example:
        Exercise OEBBook through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
    """

    COVER_SVG_XP = XPath("h:body//svg:svg[position() = 1]")
    COVER_OBJECT_XP = XPath("h:body//h:object[@data][position() = 1]")

    def __init__(
        self: _typing.Self,
        logger: _typing.Any,
        html_preprocessor: _typing.Any,
        css_preprocessor: _typing.Any = CSSPreProcessor(),
        encoding: str = "utf-8",
        pretty_print: bool = False,
        input_encoding: str = "utf-8",
    ) -> None:
        """
        Create empty book. Arguments:

        Example:
            Exercise OEBBook.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param logger: Value supplied for logger under the utility contract.
        :param html_preprocessor: Value supplied for html preprocessor under the utility
            contract.
        :param css_preprocessor: Value supplied for css preprocessor under the utility
            contract.
        :param encoding: Value supplied for encoding under the utility contract.
        :param pretty_print: Value supplied for pretty print under the utility contract.
        :param input_encoding: Value supplied for input encoding under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        _css_log_handler.log = logger
        self.encoding = encoding
        self.input_encoding = input_encoding
        self.html_preprocessor = html_preprocessor
        self.css_preprocessor = css_preprocessor
        self.pretty_print = pretty_print
        self.logger = self.log = logger
        self.version = "2.0"
        self.container = NullContainer(self.log)
        self.metadata = Metadata(self)
        self.uid = None
        self.manifest = Manifest(self)
        self.spine = Spine(self)
        self.guide = Guide(self)
        self.toc = TOC()
        self.pages = PageList()
        self.auto_generated_toc = True
        self._temp_files = []

    def clean_temp_files(self: _typing.Self) -> None:
        """
        Perform the clean temp files operation under explicit file-format and conversion rules.

        Example:
            Exercise OEBBook.clean temp files through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for path in self._temp_files:
            try:
                os.remove(path)
            except Exception as e:
                self.log.exception("Unable to clean temporary files - {}".format(str(e)))

    @classmethod
    def generate(cls: type[_typing.Self], opts: _typing.Any) -> _typing.Any:
        """
        Generate an OEBBook instance from command-line options.

        Example:
            Exercise OEBBook.generate through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param opts: Value supplied for opts under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        encoding = opts.encoding
        pretty_print = opts.pretty_print
        return cls(encoding=encoding, pretty_print=pretty_print)

    def translate(self: _typing.Self, text: _typing.Any) -> _typing.Any:
        """
        Translate :param:`text` into the book's primary language.

        Example:
            Exercise OEBBook.translate through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param text: Text parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        lang = str(self.metadata.language[0])
        lang = lang.split("-", 1)[0].lower()
        return translate(lang, text)

    def decode(self: _typing.Self, data: _typing.Any) -> _typing.Any:
        """
        Automatically decode :param:`data` into a `unicode` object.

        Example:
            Exercise OEBBook.decode through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param data: Value supplied for data under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        def fix_data(d: _typing.Any) -> _typing.Any:
            """
            Perform the fix data operation under explicit file-format and conversion rules.

            Example:
                Exercise OEBBook.decode.fix data through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


            :param d: Value supplied for d under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return d.replace("\r\n", "\n").replace("\r", "\n")

        if isinstance(data, unicode):
            return fix_data(data)
        bom_enc = None
        if data[:4] in ("\0\0\xfe\xff", "\xff\xfe\0\0"):
            bom_enc = {"\0\0\xfe\xff": "utf-32-be", "\xff\xfe\0\0": "utf-32-le"}[data[:4]]
            data = data[4:]
        elif data[:2] in ("\xff\xfe", "\xfe\xff"):
            bom_enc = {"\xff\xfe": "utf-16-le", "\xfe\xff": "utf-16-be"}[data[:2]]
            data = data[2:]
        elif data[:3] == "\xef\xbb\xbf":
            bom_enc = "utf-8"
            data = data[3:]

        if bom_enc is not None:
            try:
                return fix_data(data.decode(bom_enc))
            except UnicodeDecodeError:
                pass

        if self.input_encoding:
            try:
                return fix_data(data.decode(self.input_encoding, "replace"))
            except UnicodeDecodeError:
                pass

        try:
            return fix_data(data.decode("utf-8"))
        except UnicodeDecodeError:
            pass

        data, _ = xml_to_unicode(data)
        return fix_data(data)

    def to_opf1(self: _typing.Self) -> dict[_typing.Any, _typing.Any]:
        """
        Produce OPF 1.2 representing the book's metadata and structure.

        Example:
            Exercise OEBBook.to opf1 through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        package = etree.Element("package", attrib={"unique-identifier": self.uid.id})
        self.metadata.to_opf1(package)
        self.manifest.to_opf1(package)
        self.spine.to_opf1(package)
        tours = element(package, "tours")
        tour = element(tours, "tour", attrib={"id": "chaptertour", "title": "Chapter Tour"})
        self.toc.to_opf1(tour)
        self.guide.to_opf1(package)
        return {OPF_MIME: ("content.opf", package)}

    def _update_playorder(self: _typing.Self, ncx: _typing.Any) -> None:
        """
        Perform the update playorder operation under explicit file-format and conversion rules.

        Example:
            Exercise OEBBook. update playorder through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param ncx: Value supplied for ncx under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        hrefs = set(map(urlnormalize, xpath(ncx, "//ncx:content/@src")))
        playorder = {}
        next_item = 1
        selector = XPath("h:body//*[@id or @name]")
        for item in self.spine:
            base = item.href
            if base in hrefs:
                playorder[base] = next_item
                next_item += 1
            for elem in selector(item.data):
                added = False
                for attr in ("id", "name"):
                    attr_id = elem.get(attr)
                    if not attr_id:
                        continue
                    href = "#".join([base, attr_id])
                    if href in hrefs:
                        playorder[href] = next_item
                        added = True
                if added:
                    next_item += 1
        selector = XPath("ncx:content/@src")
        for i, elem in enumerate(xpath(ncx, "//*[@playOrder and ./ncx:content[@src]]")):
            href = urlnormalize(selector(elem)[0])
            order = playorder.get(href, i)
            elem.attrib["playOrder"] = str(order)
        return

    def _to_ncx(self: _typing.Self) -> _typing.Any:
        """
        Perform the to ncx operation under explicit file-format and conversion rules.

        Example:
            Exercise OEBBook. to ncx through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        lang = six_unicode(self.metadata.language[0])
        lang = lang.replace("_", "-")

        ncx = etree.Element(
            NCX("ncx"),
            attrib={"version": "2005-1", XML("lang"): lang},
            nsmap={None: NCX_NS},
        )
        head = etree.SubElement(ncx, NCX("head"))
        etree.SubElement(head, NCX("meta"), name="dtb:uid", content=six_unicode(self.uid))
        etree.SubElement(head, NCX("meta"), name="dtb:depth", content=str(self.toc.depth()))

        generator = "".join(["calibre (", __version__, ")"])
        etree.SubElement(head, NCX("meta"), name="dtb:generator", content=generator)
        etree.SubElement(head, NCX("meta"), name="dtb:totalPageCount", content=str(len(self.pages)))

        maxpnum = etree.SubElement(head, NCX("meta"), name="dtb:maxPageNumber", content="0")

        title = etree.SubElement(ncx, NCX("docTitle"))
        text = etree.SubElement(title, NCX("text"))
        text.text = six_unicode(self.metadata.title[0])
        navmap = etree.SubElement(ncx, NCX("navMap"))
        self.toc.to_ncx(navmap)

        if len(self.pages) > 0:
            plist = self.pages.to_ncx(ncx)
            value = max(int(x) for x in xpath(plist, "//@value"))
            maxpnum.attrib["content"] = str(value)
        self._update_playorder(ncx)

        return ncx

    def to_opf2(self: _typing.Self, page_map: bool = False) -> _typing.Any:
        """
        Produce OPF 2.0 representing the book's metadata and structure.

        Example:
            Exercise OEBBook.to opf2 through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param page_map: Value supplied for page map under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        results = {}
        package = etree.Element(
            OPF("package"),
            attrib={"version": "2.0", "unique-identifier": self.uid.id},
            nsmap={None: OPF2_NS},
        )
        self.metadata.to_opf2(package)
        manifest = self.manifest.to_opf2(package)
        spine = self.spine.to_opf2(package)
        self.guide.to_opf2(package)
        results[OPF_MIME] = ("content.opf", package)
        local_id, href = self.manifest.generate("ncx", "toc.ncx")
        etree.SubElement(
            manifest,
            OPF("item"),
            id=local_id,
            href=href,
            attrib={"media-type": NCX_MIME},
        )
        spine.attrib["toc"] = local_id
        results[NCX_MIME] = (href, self._to_ncx())
        if page_map and len(self.pages) > 0:
            local_id, href = self.manifest.generate("page-map", "page-map.xml")
            etree.SubElement(
                manifest,
                OPF("item"),
                id=local_id,
                href=href,
                attrib={"media-type": PAGE_MAP_MIME},
            )
            spine.attrib["page-map"] = local_id
            results[PAGE_MAP_MIME] = (href, self.pages.to_page_map())
        if self.spine.page_progression_direction in {"ltr", "rtl"}:
            spine.attrib["page-progression-direction"] = self.spine.page_progression_direction
        return results

#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Parse XML and HTML resources under OEB safety and compatibility rules.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise parse utils through a consuming regression::

        python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
"""
from __future__ import (
    absolute_import,
    annotations,
    division,
    print_function,
    unicode_literals,
)

import re
import typing as _typing

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

from LiuXin_alpha.constants import filesystem_encoding, force_unicode
from LiuXin_alpha.utils.libraries.calibre_chardet import (
    strip_encoding_declarations,
    xml_to_unicode,
)
from LiuXin_alpha.utils.libraries.liuxin_html5lib.constants import (
    namespaces as html5_namespaces,
)
from LiuXin_alpha.utils.libraries.liuxin_six import (
    dict_iteritems as iteritems,
)
from LiuXin_alpha.utils.libraries.liuxin_six import (
    dict_itervalues as itervalues,
)

# Py2/Py3 compatability layer
from LiuXin_alpha.utils.libraries.liuxin_six import (
    six_string_types,
)
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.text.xml_utils import xml_replace_entities

__license__ = "GPL v3"
__copyright__ = "2011, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


RECOVER_PARSER = etree.XMLParser(recover=True, no_network=True)
XHTML_NS = "http://www.w3.org/1999/xhtml"
XMLNS_NS = "http://www.w3.org/2000/xmlns/"


class NotHTML(Exception):
    """
    Provide the nothtml contract for validated ebook processing.

    Example:
        Exercise NotHTML through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
    """
    def __init__(self: _typing.Self, root_tag: _typing.Any) -> None:
        """
        Initialize and validate the nothtml state.

        Example:
            Exercise NotHTML.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param root_tag: Value supplied for root tag under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        Exception.__init__(self, "Data is not HTML")
        self.root_tag = root_tag


def barename(name: _typing.Any) -> _typing.Any:
    """
    Perform the barename operation under explicit file-format and conversion rules.

    Example:
        Exercise barename through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param name: Field, file, function or resource name addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return name.rpartition("}")[-1]


def namespace(name: _typing.Any) -> _typing.Any:
    """
    Perform the namespace operation under explicit file-format and conversion rules.

    Example:
        Exercise namespace through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param name: Field, file, function or resource name addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return name.rpartition("}")[0][1:]


def XHTML(name: _typing.Any) -> _typing.Any:
    """
    Perform the XHTML operation under explicit file-format and conversion rules.

    Example:
        Exercise XHTML through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param name: Field, file, function or resource name addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return "{%s}%s" % (XHTML_NS, name)


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
    return elem.xpath(expr, namespaces={"h": XHTML_NS})


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
    return etree.XPath(expr, namespaces={"h": XHTML_NS})


META_XP = XPath('/h:html/h:head/h:meta[@http-equiv="Content-Type"]')


def merge_multiple_html_heads_and_bodies(root: _typing.Any, log: _typing.Any = None) -> _typing.Any:
    """
    Perform the merge multiple html heads and bodies operation under explicit file-format and conversion rules.

    Example:
        Exercise merge multiple html heads and bodies through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param log: Value supplied for log under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    heads, bodies = xpath(root, "//h:head"), xpath(root, "//h:body")
    if not (len(heads) > 1 or len(bodies) > 1):
        return root
    for child in root:
        root.remove(child)
    head = root.makeelement(XHTML("head"))
    body = root.makeelement(XHTML("body"))
    for h in heads:
        for x in h:
            head.append(x)
    for b in bodies:
        for x in b:
            body.append(x)
    root.append(head)
    root.append(body)
    if log is not None:
        log.warning("Merging multiple <head> and <body> sections")
    return root


def clone_element(elem: _typing.Any, nsmap: _typing.Any = None, in_context: bool = True) -> _typing.Any:
    """
    Perform the clone element operation under explicit file-format and conversion rules.

    Example:
        Exercise clone element through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param elem: Value supplied for elem under the utility contract.
    :param nsmap: Value supplied for nsmap under the utility contract.
    :param in_context: Value supplied for in context under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if nsmap is None:
        nsmap = {}
    if in_context:
        maker = elem.getroottree().getroot().makeelement
    else:
        maker = etree.Element
    nelem = maker(elem.tag, attrib=elem.attrib, nsmap=nsmap)
    nelem.text, nelem.tail = elem.text, elem.tail
    nelem.extend(elem)
    return nelem


def node_depth(node: _typing.Any) -> _typing.Any:
    """
    Perform the node depth operation under explicit file-format and conversion rules.

    Example:
        Exercise node depth through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param node: Value supplied for node under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    ans = 0
    p = node.getparent()
    while p is not None:
        ans += 1
        p = p.getparent()
    return ans


def fix_self_closing_cdata_tags(data: _typing.Any) -> _typing.Any:
    """
    Perform the fix self closing cdata tags operation under explicit file-format and conversion rules.

    Example:
        Exercise fix self closing cdata tags through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param data: Value supplied for data under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from LiuXin_alpha.utils.libraries.liuxin_html5lib.constants import (
        cdataElements,
        rcdataElements,
    )

    return re.sub(
        r"<\s*(%s)\s*[^>]*/\s*>" % ("|".join(cdataElements | rcdataElements)),
        r"<\1></\1>",
        data,
        flags=re.I,
    )


def html5_parse(data: _typing.Any, max_nesting_depth: int = 100) -> _typing.Any:
    """
    Perform the html5 parse operation under explicit file-format and conversion rules.

    Example:
        Exercise html5 parse through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param data: Value supplied for data under the utility contract.
    :param max_nesting_depth: Value supplied for max nesting depth under the utility
        contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    import warnings

    # Seems to require a specific version of the library - embedding it for sanity
    import LiuXin_alpha.utils.libraries.liuxin_html5lib as html5lib

    # HTML5 parsing algorithm idiocy: http://code.google.com/p/html5lib/issues/detail?id=195
    data = fix_self_closing_cdata_tags(data)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            data = html5lib.parse(data, treebuilder="lxml").getroot()
        except ValueError:
            from LiuXin_alpha.utils.libraries.cleantext import clean_xml_chars

            data = html5lib.parse(clean_xml_chars(data), treebuilder="lxml").getroot()

    # Check that the asinine HTML 5 algorithm did not result in a tree with insane nesting depths
    for x in data.iterdescendants():
        if isinstance(x.tag, six_string_types) and len(x) == 0:  # Leaf node
            depth = node_depth(x)
            if depth > max_nesting_depth:
                raise ValueError("liuxin_html5lib resulted in a tree with nesting depth > %d" % max_nesting_depth)

    # liuxin_html5lib has the most inelegant handling of namespaces I have ever seen
    # Try to reconstitute destroyed namespace info
    xmlns_declaration = "{%s}" % XMLNS_NS
    non_html5_namespaces = {}
    seen_namespaces = set()
    for elem in tuple(data.iter()):
        elem.attrib.pop("xmlns", None)
        # Set lang correctly
        xl = elem.attrib.pop("xmlU0003Alang", None)
        if xl is not None and "lang" not in elem.attrib:
            elem.attrib["lang"] = xl
        namespaces = {}
        for x in tuple(elem.attrib):
            if x.startswith("xmlnsU") or x.startswith(xmlns_declaration):
                # A namespace declaration
                val = elem.attrib.pop(x)
                if x.startswith("xmlnsU0003A"):
                    prefix = x[11:]
                    namespaces[prefix] = val

        remapped_namespaces = {}
        if namespaces:
            # Some destroyed namespace declarations were found
            p = elem.getparent()
            if p is None:
                # We handle the root node later
                non_html5_namespaces = namespaces
            else:
                idx = p.index(elem)
                p.remove(elem)
                elem = clone_element(elem, nsmap=namespaces)
                p.insert(idx, elem)
                remapped_namespaces = {ns: namespaces[ns] for ns in set(namespaces) - set(elem.nsmap)}

        if not isinstance(elem.tag, six_string_types):
            continue
        b = barename(elem.tag)
        idx = b.find("U0003A")
        if idx > -1:
            prefix, tag = b[:idx], b[idx + 6 :]
            ns = elem.nsmap.get(prefix, None)
            if ns is None:
                ns = non_html5_namespaces.get(prefix, None)
            if ns is None:
                ns = remapped_namespaces.get(prefix, None)
            if ns is not None:
                elem.tag = "{%s}%s" % (ns, tag)

        for b in tuple(elem.attrib):
            idx = b.find("U0003A")
            if idx > -1:
                prefix, tag = b[:idx], b[idx + 6 :]
                ns = elem.nsmap.get(prefix, None)
                if ns is None:
                    ns = non_html5_namespaces.get(prefix, None)
                if ns is None:
                    ns = remapped_namespaces.get(prefix, None)
                if ns is not None:
                    elem.attrib["{%s}%s" % (ns, tag)] = elem.attrib.pop(b)

        seen_namespaces |= set(itervalues(elem.nsmap))

    # nsmap = dict(html5lib.constants.namespaces)
    nsmap = dict(html5_namespaces)
    nsmap[None] = nsmap.pop("html")
    non_html5_namespaces.update(nsmap)
    nsmap = non_html5_namespaces

    data = clone_element(data, nsmap=nsmap, in_context=False)

    # Remove unused namespace declarations
    fnsmap = {k: v for k, v in iteritems(nsmap) if v in seen_namespaces and v != XMLNS_NS}
    return clone_element(data, nsmap=fnsmap, in_context=False)


def _html4_parse(data: _typing.Any, prefer_soup: bool = False) -> _typing.Any:
    """
    Parse an html4 document into a tree

    Example:
        Exercise  html4 parse through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param data: Value supplied for data under the utility contract.
    :param prefer_soup: Value supplied for prefer soup under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if prefer_soup:
        from LiuXin_alpha.utils.libraries.soupparser import fromstring

        data = fromstring(data)
    else:
        data = html.fromstring(data)
    data.attrib.pop("xmlns", None)
    for elem in data.iter(tag=etree.Comment):
        if elem.text:
            elem.text = elem.text.strip("-")
    data = etree.tostring(data, encoding="unicode")

    # Setting huge_tree=True causes crashes in windows with large files
    parser = etree.XMLParser(no_network=True)
    try:
        data = etree.fromstring(data, parser=parser)
    except etree.XMLSyntaxError:
        data = etree.fromstring(data, parser=RECOVER_PARSER)
    return data


def clean_word_doc(data: _typing.Any, log: _typing.Any) -> _typing.Any:
    """
    Perform the clean word doc operation under explicit file-format and conversion rules.

    Example:
        Exercise clean word doc through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param data: Value supplied for data under the utility contract.
    :param log: Value supplied for log under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    prefixes = []
    for match in re.finditer(r'xmlns:(\S+?)=".*?microsoft.*?"', data):
        prefixes.append(match.group(1))
    if prefixes:
        log.warning("Found microsoft markup, cleaning...")
        # Remove empty tags as they are not rendered by browsers but can become renderable HTML tags like <p/> if the
        # document is parsed by an HTML parser
        pat = re.compile(r"<(%s):([a-zA-Z0-9]+)[^>/]*?></\1:\2>" % ("|".join(prefixes)), re.DOTALL)
        data = pat.sub("", data)
        pat = re.compile(r"<(%s):([a-zA-Z0-9]+)[^>/]*?/>" % ("|".join(prefixes)))
        data = pat.sub("", data)
    return data


class HTML5Doc(ValueError):
    """
    Provide the html5doc contract for validated ebook processing.

    Example:
        Exercise HTML5Doc through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
    """
    pass


def check_for_html5(prefix: _typing.Any, root: _typing.Any) -> None:
    """
    Perform the check for html5 operation under explicit file-format and conversion rules.

    Example:
        Exercise check for html5 through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param prefix: Text prepended to the formatted or selected result.
    :param root: Root directory that bounds path resolution or traversal.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    if re.search(r"<!DOCTYPE\s+html\s*>", prefix, re.IGNORECASE) is not None:
        if root.xpath("//svg"):
            raise HTML5Doc("This document appears to be un-namespaced HTML 5, should be parsed by the HTML 5 parser")


def parse_html(
    data: _typing.Any,
    log: _typing.Any = None,
    decoder: _typing.Any = None,
    preprocessor: _typing.Any = None,
    filename: str = "<string>",
    non_html_file_tags: _typing.Any = frozenset(),
) -> _typing.Any:
    """
    Parse an html document into a tree for later use

    Example:
        Exercise parse html through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param data: Value supplied for data under the utility contract.
    :param log: Value supplied for log under the utility contract.
    :param decoder: Value supplied for decoder under the utility contract.
    :param preprocessor: Value supplied for preprocessor under the utility contract.
    :param filename: Filename used for type inference or archive output.
    :param non_html_file_tags: Value supplied for non html file tags under the utility
        contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if log is None:
        from LiuXin_alpha.utils.logging import default_log

        log = default_log

    filename = force_unicode(filename, enc=filesystem_encoding)

    if not isinstance(data, six_string_types):
        if decoder is not None:
            data = decoder(data)
        else:
            data = xml_to_unicode(data)[0]

    data = strip_encoding_declarations(data)
    if preprocessor is not None:
        data = preprocessor(data)

    # There could be null bytes in data if it had &#0; entities in it
    data = data.replace("\0", "")

    # Remove DOCTYPE declaration as it messes up parsing. In particular, it causes tostring to insert xmlns
    # declarations, which messes up the coercing logic
    pre = ""
    idx = data.find("<html")
    if idx == -1:
        idx = data.find("<HTML")
    has_html4_doctype = False
    if idx > -1:
        pre = data[:idx]
        data = data[idx:]
        if "<!DOCTYPE" in pre:  # Handle user defined entities
            has_html4_doctype = re.search(r"<!DOCTYPE\s+[^>]+HTML\s+4.0[^.]+>", pre) is not None
            # kindlegen produces invalid xhtml with uppercase attribute names
            # if fed HTML 4 with uppercase attribute names, so try to detect
            # and compensate for that.
            user_entities = {}
            for match in re.finditer(r"<!ENTITY\s+(\S+)\s+([^>]+)", pre):
                val = match.group(2)
                if val.startswith('"') and val.endswith('"'):
                    val = val[1:-1]
                user_entities[match.group(1)] = val
            if user_entities:
                pat = re.compile(r"&(%s);" % ("|".join(user_entities.keys())))
                data = pat.sub(lambda m: user_entities[m.group(1)], data)

    data = raw = clean_word_doc(data, log)

    # Setting huge_tree=True causes crashes in windows with large files
    parser = etree.XMLParser(no_network=True)

    # Try with more & more drastic measures to parse
    try:
        data = etree.fromstring(data, parser=parser)
        check_for_html5(pre, data)
    except (HTML5Doc, etree.XMLSyntaxError):
        log.debug("Initial parse failed, using more forgiving parsers")
        raw = data = xml_replace_entities(raw)
        try:
            data = etree.fromstring(data, parser=parser)
            check_for_html5(pre, data)
        except (HTML5Doc, etree.XMLSyntaxError):
            log.debug("Parsing %s as HTML" % filename)
            data = raw
            try:
                data = html5_parse(data)
            except Exception as e:
                log.exception("HTML 5 parsing failed, falling back to older parsers - %s", str(e))
                data = _html4_parse(data)

    if has_html4_doctype or data.tag == "HTML":
        # Lower case all tag and attribute names
        data.tag = data.tag.lower()
        for x in data.iterdescendants():
            try:
                x.tag = x.tag.lower()
                for key, val in list(iteritems(x.attrib)):
                    del x.attrib[key]
                    key = key.lower()
                    x.attrib[key] = val
            except:
                pass

    if barename(data.tag) != "html":
        if barename(data.tag) in non_html_file_tags:
            raise NotHTML(data.tag)
        log.warning("File %r does not appear to be (X)HTML" % filename)
        nroot = etree.fromstring("<html></html>")
        has_body = False
        for child in list(data):
            # This might screw up with python 3
            if isinstance(child.tag, six_string_types) and barename(child.tag) == "body":
                has_body = True
                break
        parent = nroot
        if not has_body:
            log.warning("File %r appears to be a HTML fragment" % filename)
            nroot = etree.fromstring("<html><body/></html>")
            parent = nroot[0]
        for child in list(data.iter()):
            oparent = child.getparent()
            if oparent is not None:
                oparent.remove(child)
            parent.append(child)
        data = nroot

    # Force into the XHTML namespace
    if not namespace(data.tag):
        log.warning("Forcing %s into XHTML namespace", filename)
        data.attrib["xmlns"] = XHTML_NS
        data = etree.tostring(data, encoding="unicode")

        try:
            data = etree.fromstring(data, parser=parser)
        except:
            data = data.replace(":=", "=").replace(":>", ">")
            data = data.replace("<http:/>", "")
            try:
                data = etree.fromstring(data, parser=parser)
            except etree.XMLSyntaxError:
                log.warning("Stripping comments from %s" % filename)
                data = re.compile(r"<!--.*?-->", re.DOTALL).sub("", data)
                data = data.replace("<?xml version='1.0' encoding='utf-8'?><o:p></o:p>", "")
                data = data.replace("<?xml version='1.0' encoding='utf-8'??>", "")
                try:
                    data = etree.fromstring(data, parser=RECOVER_PARSER)
                except etree.XMLSyntaxError:
                    log.warning("Stripping meta tags from %s" % filename)
                    data = re.sub(r"<meta\s+[^>]+?>", "", data)
                    data = etree.fromstring(data, parser=RECOVER_PARSER)
    elif namespace(data.tag) != XHTML_NS:
        # OEB_DOC_NS, but possibly others
        ns = namespace(data.tag)
        attrib = dict(data.attrib)
        nroot = etree.Element(XHTML("html"), nsmap={None: XHTML_NS}, attrib=attrib)
        for elem in data.iterdescendants():
            if isinstance(elem.tag, six_string_types) and namespace(elem.tag) == ns:
                elem.tag = XHTML(barename(elem.tag))
        for elem in data:
            nroot.append(elem)
        data = nroot

    fnsmap = {k: v for k, v in iteritems(data.nsmap) if v != XHTML_NS}
    fnsmap[None] = XHTML_NS
    if fnsmap != dict(data.nsmap):
        # Remove non default prefixes referring to the XHTML namespace
        data = clone_element(data, nsmap=fnsmap, in_context=False)

    data = merge_multiple_html_heads_and_bodies(data, log)
    # Ensure has a <head/>
    head = xpath(data, "/h:html/h:head")
    head = head[0] if head else None
    if head is None:
        log.warning("File %s missing <head/> element" % filename)
        head = etree.Element(XHTML("head"))
        data.insert(0, head)
        title = etree.SubElement(head, XHTML("title"))
        title.text = _("Unknown")
    elif not xpath(data, "/h:html/h:head/h:title"):
        title = etree.SubElement(head, XHTML("title"))
        title.text = _("Unknown")
    # Ensure <title> is not empty
    title = xpath(data, "/h:html/h:head/h:title")[0]
    if not title.text or not title.text.strip():
        title.text = _("Unknown")
    # Remove any encoding-specifying <meta/> elements
    for meta in META_XP(data):
        meta.getparent().remove(meta)
    meta = etree.SubElement(head, XHTML("meta"), attrib={"http-equiv": "Content-Type"})
    meta.set("content", "text/html; charset=utf-8")  # Ensure content is second attribute

    # Ensure has a <body/>
    if not xpath(data, "/h:html/h:body"):
        body = xpath(data, "//h:body")
        if body:
            body = body[0]
            body.getparent().remove(body)
            data.append(body)
        else:
            log.warning("File %s missing <body/> element" % filename)
            etree.SubElement(data, XHTML("body"))

    # Remove microsoft office markup
    r = [x for x in data.iterdescendants() if isinstance(x.tag, six_string_types) and "microsoft-com" in x.tag]
    for x in r:
        x.tag = XHTML("span")

    # Remove lang redefinition inserted by the amazing Microsoft Word!
    body = xpath(data, "/h:html/h:body")[0]
    for key in list(body.attrib.keys()):
        if key == "lang" or key.endswith("}lang"):
            body.attrib.pop(key)

    def remove_elem(local_elem: _typing.Any) -> None:
        """
        Perform the remove elem operation under explicit file-format and conversion rules.

        Example:
            Exercise parse html.remove elem through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param local_elem: Value supplied for local elem under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        p = local_elem.getparent()
        idx = p.index(local_elem) - 1
        p.remove(local_elem)
        if local_elem.tail:
            if idx < 0:
                if p.text is None:
                    p.text = ""
                p.text += local_elem.tail
            else:
                if p[idx].tail is None:
                    p[idx].tail = ""
                p[idx].tail += local_elem.tail

    # Remove hyperlinks with no content as they cause rendering artifacts in browser based renderers.
    # Also remove empty <b>, <u> and <i> tags
    for a in xpath(data, "//h:a[@href]|//h:i|//h:b|//h:u"):
        if a.get("id", None) is None and a.get("name", None) is None and len(a) == 0 and not a.text:
            remove_elem(a)

    # Convert <br>s with content into paragraphs as ADE can't handle
    # them
    for br in xpath(data, "//h:br"):
        if len(br) > 0 or br.text:
            br.tag = XHTML("div")

    # Remove any stray text in the <head> section and format it nicely
    data.text = "\n  "
    head = xpath(data, "//h:head")
    if head:
        head = head[0]
        head.text = "\n    "
        head.tail = "\n  "
        for child in head:
            child.tail = "\n    "
        # If there is a head at all then there should be at least one child tag
        try:
            child.tail = "\n  "
        except NameError:
            pass

    return data

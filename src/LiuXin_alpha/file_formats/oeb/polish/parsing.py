#!/usr/bin/env python
# vim:fileencoding=utf-8

"""
Parse and serialize EPUB/OEB markup under compatibility rules.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise parsing through a consuming regression::

        python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
"""
from __future__ import (
    absolute_import,
    annotations,
    division,
    print_function,
    unicode_literals,
)

import copy
import re
import typing as _typing
import warnings
from bisect import bisect
from functools import partial

from lxml.etree import (
    CommentBase,
    ElementBase,
    ElementDefaultClassLookup,
    XMLParser,
    fromstring,
)
from lxml.etree import (
    Element as LxmlElement,
)

from LiuXin_alpha.file_formats.oeb.parse_utils import fix_self_closing_cdata_tags
from LiuXin_alpha.utils.libraries.calibre_chardet import ENCODING_PATS, xml_to_unicode
from LiuXin_alpha.utils.libraries.cleantext import clean_xml_chars
from LiuXin_alpha.utils.libraries.liuxin_html5lib.constants import (
    EOF,
    namespaces,
    tableInsertModeElements,
)
from LiuXin_alpha.utils.libraries.liuxin_html5lib.html5parser import HTMLParser
from LiuXin_alpha.utils.libraries.liuxin_html5lib.ihatexml import (
    DataLossWarning,
    InfosetFilter,
)
from LiuXin_alpha.utils.libraries.liuxin_html5lib.treebuilders._base import (
    TreeBuilder as BaseTreeBuilder,
)
from LiuXin_alpha.utils.libraries.liuxin_six import iteritems
from LiuXin_alpha.utils.text.xml_utils import xml_replace_entities

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"

infoset_filter = InfosetFilter()
to_xml_name = infoset_filter.toXmlName
known_namespaces = {namespaces[k]: k for k in ("mathml", "svg", "xlink")}
html_ns = namespaces["html"]
xlink_ns = namespaces["xlink"]
xml_ns = namespaces["xmlns"]


class NamespacedHTMLPresent(ValueError):
    """
    Provide the namespacedhtmlpresent contract for validated ebook processing.

    Example:
        Exercise NamespacedHTMLPresent through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    def __init__(self: _typing.Self, prefix: _typing.Any) -> None:
        """
        Initialize and validate the namespacedhtmlpresent state.

        Example:
            Exercise NamespacedHTMLPresent.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param prefix: Text prepended to the formatted or selected result.
        :return: None; validated state is stored on the receiving object.
        """
        ValueError.__init__(self, prefix)
        self.prefix = prefix


# Nodes {{{
def ElementFactory(name: _typing.Any, namespace: _typing.Any = None, context: _typing.Any = None) -> _typing.Any:
    """
    Perform the ElementFactory operation under explicit file-format and conversion rules.

    Example:
        Exercise ElementFactory through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param name: Field, file, function or resource name addressed by the operation.
    :param namespace: Value supplied for namespace under the utility contract.
    :param context: Value supplied for context under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    context = context or create_lxml_context()
    ns = namespace or namespaces["html"]
    try:
        return context.makeelement("{%s}%s" % (ns, name), nsmap={None: ns})
    except ValueError:
        return context.makeelement("{%s}%s" % (ns, to_xml_name(name)), nsmap={None: ns})


class Element(ElementBase):

    """
    Implements the interface required by the liuxin_html5lib tree builders (see liuxin_html5lib.treebuilders._base.Node) on top of the lxml ElementBase class

    Example:
        Exercise Element through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """

    def __str__(self: _typing.Self) -> _typing.Any:
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise Element.  str   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        attrs = ""
        if self.attrib:
            attrs = " " + " ".join('%s="%s"' % (k, v) for k, v in iteritems(self.attrib))
        ns = self.tag.rpartition("}")[0][1:]
        prefix = {v: k for k, v in iteritems(self.nsmap)}.get(ns, "") or ""
        if prefix:
            prefix += ":"
        return "<%s%s%s (%s)>" % (
            prefix,
            getattr(self, "name", self.tag),
            attrs,
            hex(id(self)),
        )

    __repr__ = __str__

    def attributes(self: _typing.Self) -> _typing.Any:
        """
        Perform the attributes operation under explicit file-format and conversion rules.

        Example:
            Exercise Element.attributes through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.attrib

    @property
    def childNodes(self: _typing.Self) -> _typing.Any:
        """
        Perform the childNodes operation under explicit file-format and conversion rules.

        Example:
            Exercise Element.childNodes through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self

    @childNodes.setter
    def childNodes(self: _typing.Self, val: _typing.Any) -> None:
        """
        Perform the childNodes operation under explicit file-format and conversion rules.

        Example:
            Exercise Element.childNodes through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self[:] = list(val)

    def parent(self: _typing.Self) -> _typing.Any:
        """
        Perform the parent operation under explicit file-format and conversion rules.

        Example:
            Exercise Element.parent through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.getparent()

    def hasContent(self: _typing.Self) -> _typing.Any:
        """
        Perform the hasContent operation under explicit file-format and conversion rules.

        Example:
            Exercise Element.hasContent through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return bool(self.text or len(self))

    appendChild = ElementBase.append
    removeChild = ElementBase.remove

    def cloneNode(self: _typing.Self) -> _typing.Any:
        """
        Perform the cloneNode operation under explicit file-format and conversion rules.

        Example:
            Exercise Element.cloneNode through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = self.makeelement(self.tag, nsmap=self.nsmap, attrib=self.attrib)
        for x in ("name", "namespace", "nameTuple"):
            setattr(ans, x, getattr(self, x))
        return ans

    def insertBefore(self: _typing.Self, node: _typing.Any, ref_node: _typing.Any) -> None:
        """
        Perform the insertBefore operation under explicit file-format and conversion rules.

        Example:
            Exercise Element.insertBefore through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param node: Value supplied for node under the utility contract.
        :param ref_node: Value supplied for ref node under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.insert(self.index(ref_node), node)

    def insertText(self: _typing.Self, data: _typing.Any, insertBefore: _typing.Any = None) -> None:
        """
        Perform the insertText operation under explicit file-format and conversion rules.

        Example:
            Exercise Element.insertText through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param data: Value supplied for data under the utility contract.
        :param insertBefore: Value supplied for insertBefore under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        def append_text(el: _typing.Any, attr: _typing.Any) -> None:
            """
            Perform the append text operation under explicit file-format and conversion rules.

            Example:
                Exercise Element.insertText.append text through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


            :param el: Value supplied for el under the utility contract.
            :param attr: Value supplied for attr under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            try:
                setattr(el, attr, (getattr(el, attr) or "") + data)
            except ValueError:
                text = data.replace("\u000c", " ")
                try:
                    setattr(el, attr, (getattr(el, attr) or "") + text)
                except ValueError:
                    setattr(el, attr, (getattr(el, attr) or "") + clean_xml_chars(text))

        if len(self) == 0:
            append_text(self, "text")
        elif insertBefore is None:
            # Insert the text as the tail of the last child element
            el = self[-1]
            append_text(el, "tail")
        else:
            # Insert the text before the specified node
            index = self.index(insertBefore)
            if index > 0:
                el = self[index - 1]
                append_text(el, "tail")
            else:
                append_text(self, "text")

    def reparentChildren(self: _typing.Self, new_parent: _typing.Any) -> None:
        # Move self.text
        """
        Perform the reparentChildren operation under explicit file-format and conversion rules.

        Example:
            Exercise Element.reparentChildren through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param new_parent: Value supplied for new parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if len(new_parent) > 0:
            el = new_parent[-1]
            el.tail = (el.tail or "") + self.text
        else:
            if self.text:
                new_parent.text = (new_parent.text or "") + self.text
        self.text = None
        for child in self:
            new_parent.append(child)


class Comment(CommentBase):
    """
    Provide the comment contract for validated ebook processing.

    Example:
        Exercise Comment through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    @property
    def data(self: _typing.Self) -> _typing.Any:
        """
        Perform the data operation under explicit file-format and conversion rules.

        Example:
            Exercise Comment.data through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.text

    @data.setter
    def data(self: _typing.Self, val: _typing.Any) -> None:
        """
        Perform the data operation under explicit file-format and conversion rules.

        Example:
            Exercise Comment.data through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.text = val.replace("--", "- -")

    def parent(self: _typing.Self) -> _typing.Any:
        """
        Perform the parent operation under explicit file-format and conversion rules.

        Example:
            Exercise Comment.parent through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.getparent()

    def name(self: _typing.Self) -> None:
        """
        Perform the name operation under explicit file-format and conversion rules.

        Example:
            Exercise Comment.name through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return None

    def namespace(self: _typing.Self) -> None:
        """
        Perform the namespace operation under explicit file-format and conversion rules.

        Example:
            Exercise Comment.namespace through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return None

    def nameTuple(self: _typing.Self) -> tuple[_typing.Any, ...]:
        """
        Perform the nameTuple operation under explicit file-format and conversion rules.

        Example:
            Exercise Comment.nameTuple through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return None, None

    def childNodes(self: _typing.Self) -> list[_typing.Any]:
        """
        Perform the childNodes operation under explicit file-format and conversion rules.

        Example:
            Exercise Comment.childNodes through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return []

    def attributes(self: _typing.Self) -> dict[_typing.Any, _typing.Any]:
        """
        Perform the attributes operation under explicit file-format and conversion rules.

        Example:
            Exercise Comment.attributes through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return {}

    def hasContent(self: _typing.Self) -> _typing.Any:
        """
        Perform the hasContent operation under explicit file-format and conversion rules.

        Example:
            Exercise Comment.hasContent through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return bool(self.text)

    def no_op(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> None:
        """
        Perform the no op operation under explicit file-format and conversion rules.

        Example:
            Exercise Comment.no op through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    appendChild = no_op
    removeChild = no_op
    insertBefore = no_op
    reparentChildren = no_op

    def insertText(self: _typing.Self, text: _typing.Any, insertBefore: _typing.Any = None) -> None:
        """
        Perform the insertText operation under explicit file-format and conversion rules.

        Example:
            Exercise Comment.insertText through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param text: Text parsed, normalized or rendered.
        :param insertBefore: Value supplied for insertBefore under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.text = (self.text or "") + text.replace("--", "- -")

    def cloneNode(self: _typing.Self) -> _typing.Any:
        """
        Perform the cloneNode operation under explicit file-format and conversion rules.

        Example:
            Exercise Comment.cloneNode through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return copy.copy(self)


class Document(object):
    """
    Provide the document contract for validated ebook processing.

    Example:
        Exercise Document through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the document state.

        Example:
            Exercise Document.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :return: None; validated state is stored on the receiving object.
        """
        self.root = None
        self.doctype = None

    def appendChild(self: _typing.Self, child: _typing.Any) -> None:
        """
        Perform the appendChild operation under explicit file-format and conversion rules.

        Example:
            Exercise Document.appendChild through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param child: Value supplied for child under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if isinstance(child, ElementBase):
            self.root = child
        elif isinstance(child, DocType):
            self.doctype = child


class DocType(object):
    """
    Provide the doctype contract for validated ebook processing.

    Example:
        Exercise DocType through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    def __init__(self: _typing.Self, name: _typing.Any, public_id: _typing.Any, system_id: _typing.Any) -> None:
        """
        Initialize and validate the doctype state.

        Example:
            Exercise DocType.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param public_id: Value supplied for public id under the utility contract.
        :param system_id: Value supplied for system id under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.text = self.name = name
        self.public_id, self.system_id = public_id, system_id


def create_lxml_context() -> _typing.Any:
    """
    Create lxml context under the format's safety and compatibility rules.

    Example:
        Exercise create lxml context through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    parser = XMLParser(no_network=True)
    parser.set_element_class_lookup(ElementDefaultClassLookup(element=Element, comment=Comment))
    return parser


# }}}


def clean_attrib(name: _typing.Any, val: _typing.Any, nsmap: _typing.Any, attrib: _typing.Any, namespaced_attribs: _typing.Any) -> tuple[_typing.Any, ...]:

    """
    Perform the clean attrib operation under explicit file-format and conversion rules.

    Example:
        Exercise clean attrib through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param name: Field, file, function or resource name addressed by the operation.
    :param val: Template or metadata value evaluated by the operation.
    :param nsmap: Value supplied for nsmap under the utility contract.
    :param attrib: Value supplied for attrib under the utility contract.
    :param namespaced_attribs: Value supplied for namespaced attribs under the utility
        contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(name, tuple):
        prefix, name, ns = name
        if ns == xml_ns:
            if prefix is None:
                nsmap[None] = val
            else:
                nsmap[name] = val
            return None, True
        nsmap_changed = False
        if ns == xlink_ns and "xlink" not in nsmap:
            for prefix, nns in tuple(iteritems(nsmap)):
                if nns == xlink_ns:
                    del nsmap[prefix]
            nsmap["xlink"] = xlink_ns
            nsmap_changed = True
        return ("{%s}%s" % (ns, name)), nsmap_changed

    if ":" in name:
        prefix, name = name.partition(":")[0::2]
        if prefix == "xmlns":
            # Use an existing prefix for this namespace, if
            # possible
            existing = {x: k for k, x in iteritems(nsmap)}.get(val, False)
            if existing is not False:
                name = existing
            nsmap[name] = val
            return None, True
        if prefix == "xml":
            if name != "lang" or name in attrib:
                return None, False
            return name, False

        ns = nsmap.get(prefix, None)
        if ns is None:
            namespaced_attribs[(prefix, name)] = val
            return None, True
        return "{%s}%s" % (ns, name), False

    return name, False


def makeelement_ns(ctx: _typing.Any, namespace: _typing.Any, prefix: _typing.Any, name: _typing.Any, attrib: _typing.Any, nsmap: _typing.Any) -> _typing.Any:
    """
    Perform the makeelement ns operation under explicit file-format and conversion rules.

    Example:
        Exercise makeelement ns through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param ctx: Value supplied for ctx under the utility contract.
    :param namespace: Value supplied for namespace under the utility contract.
    :param prefix: Text prepended to the formatted or selected result.
    :param name: Field, file, function or resource name addressed by the operation.
    :param attrib: Value supplied for attrib under the utility contract.
    :param nsmap: Value supplied for nsmap under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    nns = attrib.pop("xmlns", None)
    if nns is not None:
        nsmap[None] = nns
    try:
        elem = ctx.makeelement("{%s}%s" % (namespace, name), nsmap=nsmap)
    except ValueError:
        elem = ctx.makeelement("{%s}%s" % (namespace, to_xml_name(name)), nsmap=nsmap)
    # Unfortunately, lxml randomizes attrib order if passed in the makeelement
    # constructor, therefore they have to be set one by one.
    nsmap_changed = False
    namespaced_attribs = {}
    for k, v in iteritems(attrib):
        try:
            elem.set(k, v)
        except (ValueError, TypeError):
            k, is_namespace = clean_attrib(k, v, nsmap, attrib, namespaced_attribs)
            nsmap_changed |= is_namespace
            if k is not None:
                try:
                    elem.set(k, v)
                except ValueError:
                    elem.set(to_xml_name(k), v)
    if nsmap_changed:
        nelem = ctx.makeelement(elem.tag, nsmap=nsmap)
        for k, v in elem.items():  # Only elem.items() preserves attrib order
            nelem.set(k, v)
        for (prefix, name), v in iteritems(namespaced_attribs):
            ns = nsmap.get(prefix, None)
            if ns is not None:
                try:
                    nelem.set("{%s}%s" % (ns, name), v)
                except ValueError:
                    nelem.set("{%s}%s" % (ns, to_xml_name(name)), v)
            else:
                nelem.set(to_xml_name("%s:%s" % (prefix, name)), v)
        elem = nelem

    # Handle namespace prefixed tag names
    if prefix is not None:
        namespace = nsmap.get(prefix, None)
        current_ns = elem.nsmap.get(elem.prefix, None)
        if namespace is not None and namespace != current_ns:
            nelem = ctx.makeelement("{%s}%s" % (nsmap[prefix], elem.tag.rpartition("}")[2]), nsmap=nsmap)
            for k, v in elem.items():
                nelem.set(k, v)
            elem = nelem

    # Ensure that svg and mathml elements get no namespace prefixes
    if elem.prefix is not None and namespace in known_namespaces:
        for k, v in tuple(iteritems(nsmap)):
            if v == namespace:
                del nsmap[k]
        nsmap[None] = namespace
        nelem = ctx.makeelement(elem.tag, nsmap=nsmap)
        for k, v in elem.items():
            nelem.set(k, v)
        elem = nelem

    return elem


class TreeBuilder(BaseTreeBuilder):

    """
    Provide the treebuilder contract for validated ebook processing.

    Example:
        Exercise TreeBuilder through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    elementClass = ElementFactory
    documentClass = Document
    doctypeClass = DocType

    def __init__(self: _typing.Self, namespaceHTMLElements: bool = True, linenumber_attribute: _typing.Any = None) -> None:
        """
        Initialize and validate the treebuilder state.

        Example:
            Exercise TreeBuilder.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param namespaceHTMLElements: Value supplied for namespaceHTMLElements under the
            utility contract.
        :param linenumber_attribute: Value supplied for linenumber attribute under the
            utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        BaseTreeBuilder.__init__(self, namespaceHTMLElements)
        self.linenumber_attribute = linenumber_attribute
        self.lxml_context = create_lxml_context()
        self.elementClass = partial(ElementFactory, context=self.lxml_context)
        self.proxy_cache = []

    def getDocument(self: _typing.Self) -> _typing.Any:
        """
        Perform the getDocument operation under explicit file-format and conversion rules.

        Example:
            Exercise TreeBuilder.getDocument through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.document.root

    # The following methods are re-implementations from BaseTreeBuilder to
    # handle namespaces properly.

    def insertRoot(self: _typing.Self, token: _typing.Any) -> None:
        """
        Perform the insertRoot operation under explicit file-format and conversion rules.

        Example:
            Exercise TreeBuilder.insertRoot through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param token: Value supplied for token under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        element = self.createElement(token, nsmap={None: namespaces["html"]})
        self.openElements.append(element)
        self.document.appendChild(element)

    def promote_elem(self: _typing.Self, elem: _typing.Any, tag_name: _typing.Any) -> None:
        """
        Add the paraphernalia to elem that the liuxin_html5lib infrastructure needs

        Example:
            Exercise TreeBuilder.promote elem through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param elem: Value supplied for elem under the utility contract.
        :param tag_name: Value supplied for tag name under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.proxy_cache.append(elem)
        elem.name = tag_name
        elem.namespace = elem.nsmap.get(elem.prefix, html_ns)
        elem.nameTuple = (elem.namespace, elem.name)

    def createElement(self: _typing.Self, token: _typing.Any, nsmap: _typing.Any = None) -> _typing.Any:
        """
        Create an element but don't insert it anywhere

        Example:
            Exercise TreeBuilder.createElement through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param token: Value supplied for token under the utility contract.
        :param nsmap: Value supplied for nsmap under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        nsmap = nsmap or {}
        name = token_name = token["name"]
        namespace = token.get("namespace", self.defaultNamespace)
        prefix = None
        if ":" in name:
            if name.endswith(":html"):
                raise NamespacedHTMLPresent(name.rpartition(":")[0])
            prefix, name = name.partition(":")[0::2]
            namespace = nsmap.get(prefix, namespace)
        elem = makeelement_ns(self.lxml_context, namespace, prefix, name, token["data"], nsmap)

        # Keep a reference to elem so that lxml does not delete and re-create
        # it, losing the name related attributes
        self.promote_elem(elem, token_name)
        position = token.get("position", None)
        if position is not None:
            # Unfortunately, libxml2 can only store line numbers upto 65535
            # (unsigned short). If you really need to workaround this, use the
            # patch here:
            # https://bug325533.bugzilla-attachments.gnome.org/attachment.cgi?id=56951
            # (replacing int with size_t) and patching lxml correspondingly to
            # get rid of the OverflowError
            try:
                elem.sourceline = position[0][0]
            except OverflowError:
                elem.sourceline = 65535
            if self.linenumber_attribute is not None:
                elem.set(self.linenumber_attribute, str(position[0][0]))
        return elem

    def insertElementNormal(self: _typing.Self, token: _typing.Any) -> _typing.Any:
        """
        Perform the insertElementNormal operation under explicit file-format and conversion rules.

        Example:
            Exercise TreeBuilder.insertElementNormal through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param token: Value supplied for token under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        parent = self.openElements[-1]
        element = self.createElement(token, parent.nsmap)
        parent.appendChild(element)
        self.openElements.append(element)
        return element

    def insertElementTable(self: _typing.Self, token: _typing.Any) -> _typing.Any:
        """
        Create an element and insert it into the tree

        Example:
            Exercise TreeBuilder.insertElementTable through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param token: Value supplied for token under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.openElements[-1].name not in tableInsertModeElements:
            return self.insertElementNormal(token)
        # We should be in the InTable mode. This means we want to do
        # special magic element rearranging
        parent, insertBefore = self.getTableMisnestedNodePosition()
        element = self.createElement(token, nsmap=parent.nsmap)
        if insertBefore is None:
            parent.appendChild(element)
        else:
            parent.insertBefore(element, insertBefore)
        self.openElements.append(element)
        return element

    def clone_node(self: _typing.Self, elem: _typing.Any, nsmap_update: _typing.Any) -> _typing.Any:
        """
        Perform the clone node operation under explicit file-format and conversion rules.

        Example:
            Exercise TreeBuilder.clone node through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param elem: Value supplied for elem under the utility contract.
        :param nsmap_update: Value supplied for nsmap update under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        assert len(elem) == 0
        nsmap = elem.nsmap.copy()
        nsmap.update(nsmap_update)
        nelem = self.lxml_context.makeelement(elem.tag, nsmap=nsmap)
        self.promote_elem(nelem, elem.tag.rpartition("}")[2])
        nelem.sourceline = elem.sourceline
        for k, v in elem.items():
            nelem.set(k, v)
        nelem.text, nelem.tail = elem.text, elem.tail
        return nelem

    def apply_html_attributes(self: _typing.Self, attrs: _typing.Any) -> None:
        """
        Perform the apply html attributes operation under explicit file-format and conversion rules.

        Example:
            Exercise TreeBuilder.apply html attributes through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param attrs: Value supplied for attrs under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not attrs:
            return
        html = self.openElements[0]
        for k, v in iteritems(attrs):
            if k not in html.attrib and k != "xmlns":
                try:
                    html.set(k, v)
                except TypeError:
                    pass
                except ValueError:
                    if k == "xmlns:xml":
                        continue
                    if k == "xml:lang" and "lang" not in html.attrib:
                        k = "lang"
                        html.set(k, v)
                        continue
                    if (
                        k.startswith("xmlns:")
                        and v not in known_namespaces
                        and v != namespaces["html"]
                        and len(html) == 0
                    ):
                        # We have a namespace declaration, the only way to add
                        # it to the existing html node is to replace it.
                        prefix = k[len("xmlns:") :]
                        if not prefix:
                            continue
                        self.openElements[0] = html = self.clone_node(html, {prefix: v})
                        self.document.appendChild(html)
                    else:
                        html.set(to_xml_name(k), v)

    def apply_body_attributes(self: _typing.Self, attrs: _typing.Any) -> None:
        """
        Perform the apply body attributes operation under explicit file-format and conversion rules.

        Example:
            Exercise TreeBuilder.apply body attributes through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param attrs: Value supplied for attrs under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not attrs:
            return
        body = self.openElements[1]
        for k, v in iteritems(attrs):
            if k not in body.attrib and k != "xmlns":
                try:
                    body.set(k, v)
                except TypeError:
                    pass
                except ValueError:
                    if k == "xmlns:xml":
                        continue
                    if k == "xml:lang" and "lang" not in body.attrib:
                        k = "lang"
                    body.set(to_xml_name(k), v)

    def insertComment(self: _typing.Self, token: _typing.Any, parent: _typing.Any = None) -> None:
        """
        Perform the insertComment operation under explicit file-format and conversion rules.

        Example:
            Exercise TreeBuilder.insertComment through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param token: Value supplied for token under the utility contract.
        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if parent is None:
            parent = self.openElements[-1]
        parent.appendChild(Comment(token["data"].replace("--", "- -")))


def makeelement(ctx: _typing.Any, name: _typing.Any, attrib: _typing.Any) -> _typing.Any:
    """
    Perform the makeelement operation under explicit file-format and conversion rules.

    Example:
        Exercise makeelement through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param ctx: Value supplied for ctx under the utility contract.
    :param name: Field, file, function or resource name addressed by the operation.
    :param attrib: Value supplied for attrib under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    attrib.pop("xmlns", None)
    try:
        elem = ctx.makeelement(name)
    except ValueError:
        elem = ctx.makeelement(to_xml_name(name))
    for k, v in iteritems(attrib):
        try:
            elem.set(k, v)
        except TypeError:
            elem.set(to_xml_name(k[1]), v)
        except ValueError:
            if k == "xml:lang" and "lang" not in attrib:
                k = "lang"
            elem.set(to_xml_name(k), v)
    return elem


class NoNamespaceTreeBuilder(TreeBuilder):
    """
    Provide the nonamespacetreebuilder contract for validated ebook processing.

    Example:
        Exercise NoNamespaceTreeBuilder through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    def __init__(self: _typing.Self, namespaceHTMLElements: bool = False, linenumber_attribute: _typing.Any = None) -> None:
        """
        Initialize and validate the nonamespacetreebuilder state.

        Example:
            Exercise NoNamespaceTreeBuilder.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param namespaceHTMLElements: Value supplied for namespaceHTMLElements under the
            utility contract.
        :param linenumber_attribute: Value supplied for linenumber attribute under the
            utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        BaseTreeBuilder.__init__(self, namespaceHTMLElements)
        self.linenumber_attribute = linenumber_attribute
        self.lxml_context = create_lxml_context()
        self.elementClass = partial(ElementFactory, context=self.lxml_context)
        self.proxy_cache = []

    def createElement(self: _typing.Self, token: _typing.Any, nsmap: _typing.Any = None) -> _typing.Any:
        """
        Perform the createElement operation under explicit file-format and conversion rules.

        Example:
            Exercise NoNamespaceTreeBuilder.createElement through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param token: Value supplied for token under the utility contract.
        :param nsmap: Value supplied for nsmap under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        name = token["name"].rpartition(":")[2]
        elem = makeelement(self.lxml_context, name, token["data"])
        # Keep a reference to elem so that lxml does not delete and re-create
        # it, losing _namespace
        self.proxy_cache.append(elem)
        elem.name = elem.tag
        elem.namespace = token.get("namespace", self.defaultNamespace)
        elem.nameTuple = (elem.namespace or html_ns, elem.name)
        position = token.get("position", None)
        if position is not None:
            try:
                elem.sourceline = position[0][0]
            except OverflowError:
                elem.sourceline = 65535
            if self.linenumber_attribute is not None:
                elem.set(self.linenumber_attribute, str(position[0][0]))
        return elem

    def apply_html_attributes(self: _typing.Self, attrs: _typing.Any) -> None:
        """
        Perform the apply html attributes operation under explicit file-format and conversion rules.

        Example:
            Exercise NoNamespaceTreeBuilder.apply html attributes through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param attrs: Value supplied for attrs under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not attrs:
            return
        html = self.openElements[0]
        for k, v in iteritems(attrs):
            if k not in html.attrib and k != "xmlns":
                try:
                    html.set(k, v)
                except ValueError:
                    if k == "xml:lang" and "lang" not in html.attrib:
                        k = "lang"
                    html.set(to_xml_name(k), v)

    def apply_body_attributes(self: _typing.Self, attrs: _typing.Any) -> None:
        """
        Perform the apply body attributes operation under explicit file-format and conversion rules.

        Example:
            Exercise NoNamespaceTreeBuilder.apply body attributes through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param attrs: Value supplied for attrs under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not attrs:
            return
        body = self.openElements[1]
        for k, v in iteritems(attrs):
            if k not in body.attrib and k != "xmlns":
                try:
                    body.set(k, v)
                except ValueError:
                    if k == "xml:lang" and "lang" not in body.attrib:
                        k = "lang"
                    body.set(to_xml_name(k), v)


# Input Stream {{{
_regex_cache = {}


class FastStream(object):

    """
    Provide the faststream contract for validated ebook processing.

    Example:
        Exercise FastStream through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    __slots__ = ("raw", "pos", "errors", "new_lines", "track_position", "charEncoding")

    def __init__(self: _typing.Self, raw: _typing.Any, track_position: bool = False) -> None:
        """
        Initialize and validate the faststream state.

        Example:
            Exercise FastStream.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param raw: Value supplied for raw under the utility contract.
        :param track_position: Value supplied for track position under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.raw = raw
        self.pos = 0
        self.errors = []
        self.charEncoding = ("utf-8", "certain")
        self.track_position = track_position
        if track_position:
            self.new_lines = tuple(m.start() + 1 for m in re.finditer(r"\n", raw))

    def reset(self: _typing.Self) -> None:
        """
        Perform the reset operation under explicit file-format and conversion rules.

        Example:
            Exercise FastStream.reset through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.pos = 0

    def char(self: _typing.Self) -> _typing.Any:
        """
        Perform the char operation under explicit file-format and conversion rules.

        Example:
            Exercise FastStream.char through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            ans = self.raw[self.pos]
        except IndexError:
            return EOF
        self.pos += 1
        return ans

    def unget(self: _typing.Self, char: _typing.Any) -> None:
        """
        Perform the unget operation under explicit file-format and conversion rules.

        Example:
            Exercise FastStream.unget through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param char: Value supplied for char under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if char is not None:
            self.pos = max(0, self.pos - 1)

    def charsUntil(self: _typing.Self, characters: _typing.Any, opposite: bool = False) -> _typing.Any:
        # Use a cache of regexps to find the required characters
        """
        Perform the charsUntil operation under explicit file-format and conversion rules.

        Example:
            Exercise FastStream.charsUntil through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param characters: Value supplied for characters under the utility contract.
        :param opposite: Value supplied for opposite under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            chars = _regex_cache[(characters, opposite)]
        except KeyError:
            regex = "".join(["\\x%02x" % ord(c) for c in characters])
            if not opposite:
                regex = "^%s" % regex
            chars = _regex_cache[(characters, opposite)] = re.compile("[%s]+" % regex)

        # Find the longest matching prefix
        m = chars.match(self.raw, self.pos)
        if m is None:
            return ""
        self.pos = m.end()
        return m.group()

    def position(self: _typing.Self) -> tuple[_typing.Any, ...]:
        """
        Perform the position operation under explicit file-format and conversion rules.

        Example:
            Exercise FastStream.position through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not self.track_position:
            return (-1, -1)
        pos = self.pos
        lnum = bisect(self.new_lines, pos)
        # lnum is the line from which the next char() will come, therefore the
        # current char is a \n and \n is given the line number of the line it
        # creates.
        try:
            offset = self.new_lines[lnum - 1] - pos
        except IndexError:
            offset = pos
        return (lnum + 1, offset)


# }}}

if len("\U0010FFFF") == 1:  # UCS4 build
    replace_chars = re.compile("[\uD800-\uDFFF]")
else:
    replace_chars = re.compile("([\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF])")


def parse_html5(
    raw: _typing.Any,
    decoder: _typing.Any = None,
    log: _typing.Any = None,
    discard_namespaces: bool = False,
    line_numbers: bool = True,
    linenumber_attribute: _typing.Any = None,
    replace_entities: bool = True,
    fix_newlines: bool = True,
) -> _typing.Any:
    """
    Parse html5 under the format's safety and compatibility rules.

    Example:
        Exercise parse html5 through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param raw: Value supplied for raw under the utility contract.
    :param decoder: Value supplied for decoder under the utility contract.
    :param log: Value supplied for log under the utility contract.
    :param discard_namespaces: Value supplied for discard namespaces under the utility
        contract.
    :param line_numbers: Value supplied for line numbers under the utility contract.
    :param linenumber_attribute: Value supplied for linenumber attribute under the
        utility contract.
    :param replace_entities: Value supplied for replace entities under the utility
        contract.
    :param fix_newlines: Value supplied for fix newlines under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if raw is None:
        raise ValueError("Cannot parse HTML5: raw input is None")
    if isinstance(raw, bytes):
        raw = xml_to_unicode(raw)[0] if decoder is None else decoder(raw)
    raw = fix_self_closing_cdata_tags(raw)  # TODO: Handle this in the parser
    if replace_entities:
        raw = xml_replace_entities(raw)
    if fix_newlines:
        raw = raw.replace("\r\n", "\n").replace("\r", "\n")
    raw = replace_chars.sub("", raw)

    stream_class = partial(FastStream, track_position=line_numbers)
    stream = stream_class(raw)
    builder = partial(
        NoNamespaceTreeBuilder if discard_namespaces else TreeBuilder,
        linenumber_attribute=linenumber_attribute,
    )
    while True:
        try:
            parser = HTMLParser(
                tree=builder,
                track_positions=line_numbers,
                namespaceHTMLElements=not discard_namespaces,
            )
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=DataLossWarning)
                try:
                    parser.parse(stream, parseMeta=False, useChardet=False)
                finally:
                    parser.tree.proxy_cache = None
        except NamespacedHTMLPresent as err:
            raw = re.sub(
                r"<\s*/{0,1}(%s:)" % re.escape(err.prefix),
                lambda m: m.group().replace(m.group(1), ""),
                raw,
                flags=re.I,
            )
            stream = stream_class(raw)
            continue
        break
    root = parser.tree.getDocument()
    if root is None:
        raise ValueError("Failed to parse correctly, no root element produced")
    if (discard_namespaces and root.tag != "html") or (
        not discard_namespaces and (root.tag != "{%s}%s" % (namespaces["html"], "html") or root.prefix)
    ):
        raise ValueError("Failed to parse correctly, root has tag: %s and prefix: %s" % (root.tag, root.prefix))
    return root


def strip_encoding_declarations(raw: _typing.Any) -> _typing.Any:
    # A custom encoding stripper that preserves line numbers
    """
    Perform the strip encoding declarations operation under explicit file-format and conversion rules.

    Example:
        Exercise strip encoding declarations through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param raw: Value supplied for raw under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    limit = 10 * 1024
    for pat in ENCODING_PATS:
        prefix = raw[:limit]
        suffix = raw[limit:]
        prefix = pat.sub(lambda m: "\n" * m.group().count("\n"), prefix)
        raw = prefix + suffix
    return raw


def parse(
    raw: _typing.Any,
    decoder: _typing.Any = None,
    log: _typing.Any = None,
    line_numbers: bool = True,
    linenumber_attribute: _typing.Any = None,
    replace_entities: bool = True,
    force_html5_parse: bool = False,
) -> _typing.Any:
    """
    Parse the supplied date text and return its normalized datetime value.

    Example:
        Exercise parse through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param raw: Value supplied for raw under the utility contract.
    :param decoder: Value supplied for decoder under the utility contract.
    :param log: Value supplied for log under the utility contract.
    :param line_numbers: Value supplied for line numbers under the utility contract.
    :param linenumber_attribute: Value supplied for linenumber attribute under the
        utility contract.
    :param replace_entities: Value supplied for replace entities under the utility
        contract.
    :param force_html5_parse: Value supplied for force html5 parse under the utility
        contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if raw is None:
        raise ValueError("Cannot parse markup: raw input is None")
    if isinstance(raw, bytes):
        raw = xml_to_unicode(raw)[0] if decoder is None else decoder(raw)
    if replace_entities:
        raw = xml_replace_entities(raw).replace("\0", "")  # Handle &#0;
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")

    # Remove any preamble before the opening html tag as it can cause problems,
    # especially doctypes, preserve the original linenumbers by inserting
    # newlines at the start
    pre = raw[:2048]
    for match in re.finditer(r"<\s*html", pre, flags=re.I):
        newlines = raw.count("\n", 0, match.start())
        raw = ("\n" * newlines) + raw[match.start() :]
        break

    raw = strip_encoding_declarations(raw)
    if force_html5_parse:
        return parse_html5(
            raw,
            log=log,
            line_numbers=line_numbers,
            linenumber_attribute=linenumber_attribute,
            replace_entities=False,
            fix_newlines=False,
        )
    try:
        parser = XMLParser(no_network=True)
        ans = fromstring(raw, parser=parser)
        if ans.tag != "{%s}html" % html_ns:
            raise ValueError("Root tag is not <html> in the XHTML namespace")
        if linenumber_attribute:
            for elem in ans.iter(LxmlElement):
                if elem.sourceline is not None:
                    elem.set(linenumber_attribute, str(elem.sourceline))
        return ans
    except Exception as e:
        if log is not None:
            log.exception(
                "Failed to parse as XML, parsing as tag soup",
                " - exception message: {}".format(str(e)),
            )
        return parse_html5(
            raw,
            log=log,
            line_numbers=line_numbers,
            linenumber_attribute=linenumber_attribute,
            replace_entities=False,
            fix_newlines=False,
        )


if __name__ == "__main__":

    from lxml import etree

    root = parse_html5(
        '\n<html><head><title>a\n</title><p b=1 c=2 a=0>&nbsp;\n<b>b<svg ass="wipe" viewbox="0">',
        discard_namespaces=False,
    )
    print(etree.tostring(root, encoding="utf-8"))
    print()

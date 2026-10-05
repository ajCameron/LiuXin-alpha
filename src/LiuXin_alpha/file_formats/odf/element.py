#!/usr/bin/env python
# Copyright (C) 2007-2010 Søren Roug, European Environment Agency
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
# Contributor(s):
#

# Note: This script has copied a lot of text from xml.dom.minidom.
# Whatever license applies to that file also applies to this file.
#

"""
Represent, validate and serialize namespace-aware ODF elements.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise element through a consuming regression::

        python -m pytest -q tests/file_formats/odf/test_odf_modernized.py
"""
from __future__ import annotations

import typing as _typing
import xml.dom
from xml.dom.minicompat import defproperty, EmptyNodeList
from LiuXin_alpha.file_formats.odf.namespaces import nsdict
from LiuXin_alpha.file_formats.odf import grammar
from LiuXin_alpha.file_formats.odf.attrconverters import AttrConverters

from LiuXin_alpha.utils.libraries.calibre_polyglot.builtins import unicode_type

# The following code is pasted form xml.sax.saxutils
# Tt makes it possible to run the code without the xml sax package installed
# To make it possible to have <rubbish> in your text elements, it is necessary to escape the texts


def _escape(data: _typing.Any, entities: dict[_typing.Any, _typing.Any] = {}) -> _typing.Any:
    """
    Escape &, <, and > in a string of data.

    Example:
        Exercise  escape through a consuming regression::

            python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


    :param data: Value supplied for data under the utility contract.
    :param entities: Value supplied for entities under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    data = data.replace("&", "&amp;")
    data = data.replace("<", "&lt;")
    data = data.replace(">", "&gt;")
    for chars, entity in entities.items():
        data = data.replace(chars, entity)
    return data


def _quoteattr(data: _typing.Any, entities: dict[_typing.Any, _typing.Any] = {}) -> _typing.Any:
    """
    Escape and quote an attribute value.

    Example:
        Exercise  quoteattr through a consuming regression::

            python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


    :param data: Value supplied for data under the utility contract.
    :param entities: Value supplied for entities under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    entities["\n"] = "&#10;"
    entities["\r"] = "&#12;"
    data = _escape(data, entities)
    if '"' in data:
        if "'" in data:
            data = '"%s"' % data.replace('"', "&quot;")
        else:
            data = "'%s'" % data
    else:
        data = '"%s"' % data
    return data


def _nssplit(qualifiedName: _typing.Any) -> _typing.Any:
    """
    Split a qualified name into namespace part and local part.

    Example:
        Exercise  nssplit through a consuming regression::

            python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


    :param qualifiedName: Value supplied for qualifiedName under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    fields = qualifiedName.split(":", 1)
    if len(fields) == 2:
        return fields
    else:
        return (None, fields[0])


def _nsassign(namespace: _typing.Any) -> _typing.Any:
    """
    Perform the nsassign operation under explicit file-format and conversion rules.

    Example:
        Exercise  nsassign through a consuming regression::

            python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


    :param namespace: Value supplied for namespace under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return nsdict.setdefault(namespace, "ns" + unicode_type(len(nsdict)))


# Exceptions


class IllegalChild(Exception):
    """
    Complains if you add an element to a parent where it is not allowed

    Example:
        Exercise IllegalChild through a consuming regression::

            python -m pytest -q tests/file_formats/odf/test_odf_modernized.py
    """


class IllegalText(Exception):
    """
    Complains if you add text or cdata to an element where it is not allowed

    Example:
        Exercise IllegalText through a consuming regression::

            python -m pytest -q tests/file_formats/odf/test_odf_modernized.py
    """


class Node(xml.dom.Node):
    """
    super class for more specific nodes

    Example:
        Exercise Node through a consuming regression::

            python -m pytest -q tests/file_formats/odf/test_odf_modernized.py
    """

    parentNode = None
    nextSibling = None
    previousSibling = None

    def hasChildNodes(self: _typing.Self) -> bool:
        """
        Tells whether this element has any children; text nodes, subelements, whatever.

        Example:
            Exercise Node.hasChildNodes through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.childNodes:
            return True
        else:
            return False

    def _get_childNodes(self: _typing.Self) -> _typing.Any:
        """
        Perform the get childNodes operation under explicit file-format and conversion rules.

        Example:
            Exercise Node. get childNodes through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.childNodes

    def _get_firstChild(self: _typing.Self) -> _typing.Any:
        """
        Perform the get firstChild operation under explicit file-format and conversion rules.

        Example:
            Exercise Node. get firstChild through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.childNodes:
            return self.childNodes[0]

    def _get_lastChild(self: _typing.Self) -> _typing.Any:
        """
        Perform the get lastChild operation under explicit file-format and conversion rules.

        Example:
            Exercise Node. get lastChild through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.childNodes:
            return self.childNodes[-1]

    def insertBefore(self: _typing.Self, newChild: _typing.Any, refChild: _typing.Any) -> _typing.Any:
        """
        Inserts the node newChild before the existing child node refChild. If refChild is null, insert newChild at the end of the list of children.

        Example:
            Exercise Node.insertBefore through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param newChild: Value supplied for newChild under the utility contract.
        :param refChild: Value supplied for refChild under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if newChild.nodeType not in self._child_node_types:
            raise IllegalChild(f"{newChild.tagName} cannot be child of {self.tagName}")
        if newChild.parentNode is not None:
            newChild.parentNode.removeChild(newChild)
        if refChild is None:
            self.appendChild(newChild)
        else:
            try:
                index = self.childNodes.index(refChild)
            except ValueError:
                raise xml.dom.NotFoundErr()
            self.childNodes.insert(index, newChild)
            newChild.nextSibling = refChild
            refChild.previousSibling = newChild
            if index:
                node = self.childNodes[index - 1]
                node.nextSibling = newChild
                newChild.previousSibling = node
            else:
                newChild.previousSibling = None
            newChild.parentNode = self
        return newChild

    def appendChild(self: _typing.Self, newChild: _typing.Any) -> _typing.Any:
        """
        Adds the node newChild to the end of the list of children of this node. If the newChild is already in the tree, it is first removed.

        Example:
            Exercise Node.appendChild through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param newChild: Value supplied for newChild under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if newChild.nodeType == self.DOCUMENT_FRAGMENT_NODE:
            for c in tuple(newChild.childNodes):
                self.appendChild(c)
            # The DOM does not clearly specify what to return in this case
            return newChild
        if newChild.nodeType not in self._child_node_types:
            raise IllegalChild(f"<{newChild.tagName}> is not allowed in {self.tagName}")
        if newChild.parentNode is not None:
            newChild.parentNode.removeChild(newChild)
        _append_child(self, newChild)
        newChild.nextSibling = None
        return newChild

    def removeChild(self: _typing.Self, oldChild: _typing.Any) -> _typing.Any:
        """
        Removes the child node indicated by oldChild from the list of children, and returns it.

        Example:
            Exercise Node.removeChild through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param oldChild: Value supplied for oldChild under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        # FIXME: update ownerDocument.element_dict or find other solution
        try:
            self.childNodes.remove(oldChild)
        except ValueError:
            raise xml.dom.NotFoundErr()
        if oldChild.nextSibling is not None:
            oldChild.nextSibling.previousSibling = oldChild.previousSibling
        if oldChild.previousSibling is not None:
            oldChild.previousSibling.nextSibling = oldChild.nextSibling
        oldChild.nextSibling = oldChild.previousSibling = None
        if self.ownerDocument:
            self.ownerDocument.clear_caches()
        oldChild.parentNode = None
        return oldChild

    def __unicode__(self: _typing.Self) -> _typing.Any:
        """
        Perform the unicode operation under explicit file-format and conversion rules.

        Example:
            Exercise Node.  unicode   through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        val = []
        for c in self.childNodes:
            val.append(str(c))
        return "".join(val)

    __str__ = __unicode__


defproperty(Node, "firstChild", doc="First child node, or None.")
defproperty(Node, "lastChild", doc="Last child node, or None.")


def _append_child(self: _typing.Any, node: _typing.Any) -> None:
    # fast path with less checks; usable by DOM builders if careful
    """
    Perform the append child operation under explicit file-format and conversion rules.

    Example:
        Exercise  append child through a consuming regression::

            python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


    :param self: Value supplied for self under the utility contract.
    :param node: Value supplied for node under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    childNodes = self.childNodes
    if childNodes:
        last = childNodes[-1]
        node.__dict__["previousSibling"] = last
        last.__dict__["nextSibling"] = node
    childNodes.append(node)
    node.__dict__["parentNode"] = self


class Childless:
    """
    Mixin that makes childless-ness easy to implement and avoids the complexity of the Node methods that deal with children.

    Example:
        Exercise Childless through a consuming regression::

            python -m pytest -q tests/file_formats/odf/test_odf_modernized.py
    """

    attributes = None
    childNodes = EmptyNodeList()
    firstChild = None
    lastChild = None

    def _get_firstChild(self: _typing.Self) -> None:
        """
        Perform the get firstChild operation under explicit file-format and conversion rules.

        Example:
            Exercise Childless. get firstChild through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return None

    def _get_lastChild(self: _typing.Self) -> None:
        """
        Perform the get lastChild operation under explicit file-format and conversion rules.

        Example:
            Exercise Childless. get lastChild through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return None

    def appendChild(self: _typing.Self, node: _typing.Any) -> None:
        """
        Raises an error

        Example:
            Exercise Childless.appendChild through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param node: Value supplied for node under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise xml.dom.HierarchyRequestErr(self.tagName + " nodes cannot have children")

    def hasChildNodes(self: _typing.Self) -> bool:
        """
        Perform the hasChildNodes operation under explicit file-format and conversion rules.

        Example:
            Exercise Childless.hasChildNodes through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return False

    def insertBefore(self: _typing.Self, newChild: _typing.Any, refChild: _typing.Any) -> None:
        """
        Raises an error

        Example:
            Exercise Childless.insertBefore through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param newChild: Value supplied for newChild under the utility contract.
        :param refChild: Value supplied for refChild under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise xml.dom.HierarchyRequestErr(self.tagName + " nodes do not have children")

    def removeChild(self: _typing.Self, oldChild: _typing.Any) -> None:
        """
        Raises an error

        Example:
            Exercise Childless.removeChild through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param oldChild: Value supplied for oldChild under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise xml.dom.NotFoundErr(self.tagName + " nodes do not have children")

    def replaceChild(self: _typing.Self, newChild: _typing.Any, oldChild: _typing.Any) -> None:
        """
        Raises an error

        Example:
            Exercise Childless.replaceChild through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param newChild: Value supplied for newChild under the utility contract.
        :param oldChild: Value supplied for oldChild under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise xml.dom.HierarchyRequestErr(self.tagName + " nodes do not have children")


class Text(Childless, Node):
    """
    Provide the text contract for validated ebook processing.

    Example:
        Exercise Text through a consuming regression::

            python -m pytest -q tests/file_formats/odf/test_odf_modernized.py
    """
    nodeType = Node.TEXT_NODE
    tagName = "Text"

    def __init__(self: _typing.Self, data: _typing.Any) -> None:
        """
        Initialize and validate the text state.

        Example:
            Exercise Text.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param data: Value supplied for data under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.data = data

    def __str__(self: _typing.Self) -> _typing.Any:
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise Text.  str   through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.data

    __unicode__ = __str__

    def toXml(self: _typing.Self, level: _typing.Any, f: _typing.Any) -> None:
        """
        Write XML in UTF-8

        Example:
            Exercise Text.toXml through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param level: Value supplied for level under the utility contract.
        :param f: Value supplied for f under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.data:
            f.write(_escape(str(self.data)))


class CDATASection(Text, Childless):
    """
    Provide the cdatasection contract for validated ebook processing.

    Example:
        Exercise CDATASection through a consuming regression::

            python -m pytest -q tests/file_formats/odf/test_odf_modernized.py
    """
    nodeType = Node.CDATA_SECTION_NODE

    def toXml(self: _typing.Self, level: _typing.Any, f: _typing.Any) -> None:
        """
        Generate XML output of the node. If the text contains "]]>", then escape it by going out of CDATA mode (]]>), then write the string and then go into CDATA mode again. (<![CDATA[)

        Example:
            Exercise CDATASection.toXml through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param level: Value supplied for level under the utility contract.
        :param f: Value supplied for f under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.data:
            f.write("<![CDATA[%s]]>" % self.data.replace("]]>", "]]>]]><![CDATA["))


class Element(Node):
    """
    Creates a arbitrary element and is intended to be subclassed not used on its own. This element is the base of every element it defines a class which resembles a xml-element. The main advantage of this kind of implementation is that you don't have to create a toXML method for every different object. Every element consists of an attribute, optional subelements, optional text and optional cdata.

    Example:
        Exercise Element through a consuming regression::

            python -m pytest -q tests/file_formats/odf/test_odf_modernized.py
    """

    nodeType = Node.ELEMENT_NODE
    namespaces = {}  # Due to shallow copy this is a static variable

    _child_node_types = (
        Node.ELEMENT_NODE,
        Node.PROCESSING_INSTRUCTION_NODE,
        Node.COMMENT_NODE,
        Node.TEXT_NODE,
        Node.CDATA_SECTION_NODE,
        Node.ENTITY_REFERENCE_NODE,
    )

    def __init__(
        self: _typing.Self, attributes: _typing.Any = None, text: _typing.Any = None, cdata: _typing.Any = None, qname: _typing.Any = None, qattributes: _typing.Any = None, check_grammar: bool = True, **args: _typing.Any
    ) -> None:
        """
        Initialize and validate the element state.

        Example:
            Exercise Element.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param attributes: Value supplied for attributes under the utility contract.
        :param text: Text parsed, normalized or rendered.
        :param cdata: Value supplied for cdata under the utility contract.
        :param qname: Value supplied for qname under the utility contract.
        :param qattributes: Value supplied for qattributes under the utility contract.
        :param check_grammar: Value supplied for check grammar under the utility contract.
        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; validated state is stored on the receiving object.
        """
        if qname is not None:
            self.qname = qname
        assert hasattr(self, "qname")
        self.ownerDocument = None
        self.childNodes = []
        self.allowed_children = grammar.allowed_children.get(self.qname)
        prefix = self.get_nsprefix(self.qname[0])
        self.tagName = prefix + ":" + self.qname[1]
        if text is not None:
            self.addText(text)
        if cdata is not None:
            self.addCDATA(cdata)

        allowed_attrs = self.allowed_attributes()
        self.attributes = {}
        # Load the attributes from the 'attributes' argument
        if attributes:
            for attr, value in attributes.items():
                self.setAttribute(attr, value)
        # Load the qualified attributes
        if qattributes:
            for attr, value in qattributes.items():
                self.setAttrNS(attr[0], attr[1], value)
        if allowed_attrs is not None:
            # Load the attributes from the 'args' argument
            for arg in args.keys():
                self.setAttribute(arg, args[arg])
        else:
            for arg in args.keys():  # If any attribute is allowed
                self.attributes[arg] = args[arg]
        if not check_grammar:
            return
        # Test that all mandatory attributes have been added.
        required = grammar.required_attributes.get(self.qname)
        if required:
            for r in required:
                if self.getAttrNS(r[0], r[1]) is None:
                    raise AttributeError(
                        "Required attribute missing: {} in <{}>".format(r[1].lower().replace("-", ""), self.tagName)
                    )

    def get_knownns(self: _typing.Self, prefix: _typing.Any) -> _typing.Any:
        """
        Odfpy maintains a list of known namespaces. In some cases a prefix is used, and we need to know which namespace it resolves to.

        Example:
            Exercise Element.get knownns through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param prefix: Text prepended to the formatted or selected result.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        global nsdict
        for ns, p in nsdict.items():
            if p == prefix:
                return ns
        return None

    def get_nsprefix(self: _typing.Self, namespace: _typing.Any) -> _typing.Any:
        """
        Odfpy maintains a list of known namespaces. In some cases we have a namespace URL, and needs to look up or assign the prefix for it.

        Example:
            Exercise Element.get nsprefix through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param namespace: Value supplied for namespace under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if namespace is None:
            namespace = ""
        prefix = _nsassign(namespace)
        if namespace not in self.namespaces:
            self.namespaces[namespace] = prefix
        return prefix

    def allowed_attributes(self: _typing.Self) -> _typing.Any:
        """
        Perform the allowed attributes operation under explicit file-format and conversion rules.

        Example:
            Exercise Element.allowed attributes through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return grammar.allowed_attributes.get(self.qname)

    def _setOwnerDoc(self: _typing.Self, element: _typing.Any) -> None:
        """
        Perform the setOwnerDoc operation under explicit file-format and conversion rules.

        Example:
            Exercise Element. setOwnerDoc through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param element: Value supplied for element under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        element.ownerDocument = self.ownerDocument
        for child in element.childNodes:
            self._setOwnerDoc(child)

    def addElement(self: _typing.Self, element: _typing.Any, check_grammar: bool = True) -> None:
        """
        adds an element to an Element

        Example:
            Exercise Element.addElement through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param element: Value supplied for element under the utility contract.
        :param check_grammar: Value supplied for check grammar under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if check_grammar and self.allowed_children is not None:
            if element.qname not in self.allowed_children:
                raise IllegalChild(f"<{element.tagName}> is not allowed in <{self.tagName}>")
        self.appendChild(element)
        self._setOwnerDoc(element)
        if self.ownerDocument:
            self.ownerDocument.rebuild_caches(element)

    def addText(self: _typing.Self, text: _typing.Any, check_grammar: bool = True) -> None:
        """
        Adds text to an element Setting check_grammar=False turns off grammar checking

        Example:
            Exercise Element.addText through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param text: Text parsed, normalized or rendered.
        :param check_grammar: Value supplied for check grammar under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if check_grammar and self.qname not in grammar.allows_text:
            raise IllegalText("The <%s> element does not allow text" % self.tagName)
        else:
            if text != "":
                self.appendChild(Text(text))

    def addCDATA(self: _typing.Self, cdata: _typing.Any, check_grammar: bool = True) -> None:
        """
        Adds CDATA to an element Setting check_grammar=False turns off grammar checking

        Example:
            Exercise Element.addCDATA through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param cdata: Value supplied for cdata under the utility contract.
        :param check_grammar: Value supplied for check grammar under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if check_grammar and self.qname not in grammar.allows_text:
            raise IllegalText("The <%s> element does not allow text" % self.tagName)
        else:
            self.appendChild(CDATASection(cdata))

    def removeAttribute(self: _typing.Self, attr: _typing.Any, check_grammar: bool = True) -> None:
        """
        Removes an attribute by name.

        Example:
            Exercise Element.removeAttribute through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param attr: Value supplied for attr under the utility contract.
        :param check_grammar: Value supplied for check grammar under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        allowed_attrs = self.allowed_attributes()
        if allowed_attrs is None:
            if isinstance(attr, tuple):
                prefix, localname = attr
                self.removeAttrNS(prefix, localname)
            else:
                raise AttributeError("Unable to add simple attribute - use (namespace, localpart)")
        else:
            # Construct a list of allowed arguments
            allowed_args = [a[1].lower().replace("-", "") for a in allowed_attrs]
            if check_grammar and attr not in allowed_args:
                raise AttributeError(f"Attribute {attr} is not allowed in <{self.tagName}>")
            i = allowed_args.index(attr)
            self.removeAttrNS(allowed_attrs[i][0], allowed_attrs[i][1])

    def setAttribute(self: _typing.Self, attr: _typing.Any, value: _typing.Any, check_grammar: bool = True) -> None:
        """
        Add an attribute to the element This is sort of a convenience method. All attributes in ODF have namespaces. The library knows what attributes are legal and then allows the user to provide the attribute as a keyword argument and the library will add the correct namespace. Must overwrite, If attribute already exists.

        Example:
            Exercise Element.setAttribute through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param attr: Value supplied for attr under the utility contract.
        :param value: Value normalized, stored, formatted or returned.
        :param check_grammar: Value supplied for check grammar under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        allowed_attrs = self.allowed_attributes()
        if allowed_attrs is None:
            if isinstance(attr, tuple):
                prefix, localname = attr
                self.setAttrNS(prefix, localname, value)
            else:
                raise AttributeError("Unable to add simple attribute - use (namespace, localpart)")
        else:
            # Construct a list of allowed arguments
            allowed_args = [a[1].lower().replace("-", "") for a in allowed_attrs]
            if check_grammar and attr not in allowed_args:
                raise AttributeError(f"Attribute {attr} is not allowed in <{self.tagName}>")
            i = allowed_args.index(attr)
            self.setAttrNS(allowed_attrs[i][0], allowed_attrs[i][1], value)

    def setAttrNS(self: _typing.Self, namespace: _typing.Any, localpart: _typing.Any, value: _typing.Any) -> None:
        """
        Add an attribute to the element In case you need to add an attribute the library doesn't know about then you must provide the full qualified name It will not check that the attribute is legal according to the schema. Must overwrite, If attribute already exists.

        Example:
            Exercise Element.setAttrNS through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param namespace: Value supplied for namespace under the utility contract.
        :param localpart: Value supplied for localpart under the utility contract.
        :param value: Value normalized, stored, formatted or returned.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        c = AttrConverters()
        self.attributes[(namespace, localpart)] = c.convert((namespace, localpart), value, self)

    def getAttrNS(self: _typing.Self, namespace: _typing.Any, localpart: _typing.Any) -> _typing.Any:
        """
        Perform the getAttrNS operation under explicit file-format and conversion rules.

        Example:
            Exercise Element.getAttrNS through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param namespace: Value supplied for namespace under the utility contract.
        :param localpart: Value supplied for localpart under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.attributes.get((namespace, localpart))

    def removeAttrNS(self: _typing.Self, namespace: _typing.Any, localpart: _typing.Any) -> None:
        """
        Perform the removeAttrNS operation under explicit file-format and conversion rules.

        Example:
            Exercise Element.removeAttrNS through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param namespace: Value supplied for namespace under the utility contract.
        :param localpart: Value supplied for localpart under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        del self.attributes[(namespace, localpart)]

    def getAttribute(self: _typing.Self, attr: _typing.Any) -> _typing.Any:
        """
        Get an attribute value. The method knows which namespace the attribute is in

        Example:
            Exercise Element.getAttribute through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param attr: Value supplied for attr under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        allowed_attrs = self.allowed_attributes()
        if allowed_attrs is None:
            if isinstance(attr, tuple):
                prefix, localname = attr
                return self.getAttrNS(prefix, localname)
            else:
                raise AttributeError("Unable to get simple attribute - use (namespace, localpart)")
        else:
            # Construct a list of allowed arguments
            allowed_args = [a[1].lower().replace("-", "") for a in allowed_attrs]
            i = allowed_args.index(attr)
            return self.getAttrNS(allowed_attrs[i][0], allowed_attrs[i][1])

    def write_open_tag(self: _typing.Self, level: _typing.Any, f: _typing.Any) -> None:
        """
        Write open tag under the format's safety and compatibility rules.

        Example:
            Exercise Element.write open tag through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param level: Value supplied for level under the utility contract.
        :param f: Value supplied for f under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        f.write("<" + self.tagName)
        if level == 0:
            for namespace, prefix in self.namespaces.items():
                f.write(" xmlns:" + prefix + '="' + _escape(unicode_type(namespace)) + '"')
        for qname in self.attributes.keys():
            prefix = self.get_nsprefix(qname[0])
            f.write(
                " "
                + _escape(unicode_type(prefix + ":" + qname[1]))
                + "="
                + _quoteattr(str(self.attributes[qname]))
            )
        f.write(">")

    def write_close_tag(self: _typing.Self, level: _typing.Any, f: _typing.Any) -> None:
        """
        Write close tag under the format's safety and compatibility rules.

        Example:
            Exercise Element.write close tag through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param level: Value supplied for level under the utility contract.
        :param f: Value supplied for f under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        f.write("</" + self.tagName + ">")

    def toXml(self: _typing.Self, level: _typing.Any, f: _typing.Any) -> None:
        """
        Generate XML stream out of the tree structure

        Example:
            Exercise Element.toXml through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param level: Value supplied for level under the utility contract.
        :param f: Value supplied for f under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        f.write("<" + self.tagName)
        if level == 0:
            for namespace, prefix in self.namespaces.items():
                f.write(" xmlns:" + prefix + '="' + _escape(unicode_type(namespace)) + '"')
        for qname in self.attributes.keys():
            prefix = self.get_nsprefix(qname[0])
            f.write(
                " "
                + _escape(unicode_type(prefix + ":" + qname[1]))
                + "="
                + _quoteattr(str(self.attributes[qname]))
            )
        if self.childNodes:
            f.write(">")
            for element in self.childNodes:
                element.toXml(level + 1, f)
            f.write("</" + self.tagName + ">")
        else:
            f.write("/>")

    def _getElementsByObj(self: _typing.Self, obj: _typing.Any, accumulator: _typing.Any) -> _typing.Any:
        """
        Perform the getElementsByObj operation under explicit file-format and conversion rules.

        Example:
            Exercise Element. getElementsByObj through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param obj: Value supplied for obj under the utility contract.
        :param accumulator: Value supplied for accumulator under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.qname == obj.qname:
            accumulator.append(self)
        for e in self.childNodes:
            if e.nodeType == Node.ELEMENT_NODE:
                accumulator = e._getElementsByObj(obj, accumulator)
        return accumulator

    def getElementsByType(self: _typing.Self, element: _typing.Any) -> _typing.Any:
        """
        Gets elements based on the type, which is function from text.py, draw.py etc.

        Example:
            Exercise Element.getElementsByType through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param element: Value supplied for element under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        obj = element(check_grammar=False)
        return self._getElementsByObj(obj, [])

    def isInstanceOf(self: _typing.Self, element: _typing.Any) -> bool:
        """
        This is a check to see if the object is an instance of a type

        Example:
            Exercise Element.isInstanceOf through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param element: Value supplied for element under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        obj = element(check_grammar=False)
        return self.qname == obj.qname

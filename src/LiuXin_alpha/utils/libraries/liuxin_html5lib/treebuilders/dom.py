"""
Build or walk HTML5 trees using the DOM compatibility interface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise dom through a consuming regression::

        python -m pytest -q tests/file_formats/html/test_html_modernized.py
"""
from __future__ import absolute_import, division, unicode_literals


from xml.dom import minidom, Node
import weakref

from LiuXin_alpha.utils.libraries.liuxin_html5lib.treebuilders import _base
from LiuXin_alpha.utils.libraries.liuxin_html5lib import constants
from LiuXin_alpha.utils.libraries.liuxin_html5lib.constants import namespaces
from LiuXin_alpha.utils.libraries.liuxin_html5lib.utils import moduleFactoryFactory


def getDomBuilder(DomImplementation):
    """
    Perform the getDomBuilder utility operation under explicit compatibility rules.

    Example:
        Exercise getDomBuilder through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param DomImplementation: Value supplied for DomImplementation under the utility
        contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    Dom = DomImplementation

    class AttrList(object):
        """
        Provide the AttrList utility contract with explicit state and cleanup behavior.

        Example:
            Exercise getDomBuilder.AttrList through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def __init__(self, element):
            """
            Initialize and validate the AttrList state.

            Example:
                Exercise getDomBuilder.AttrList.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param element: Value supplied for element under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            self.element = element

        def __iter__(self):
            """
            Expose iter behavior for the compatibility container.

            Example:
                Exercise getDomBuilder.AttrList.  iter   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return list(self.element.attributes.items()).__iter__()

        def __setitem__(self, name, value):
            """
            Perform the setitem utility operation under explicit compatibility rules.

            Example:
                Exercise getDomBuilder.AttrList.  setitem   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param name: Field, file, function or resource name addressed by the operation.
            :param value: Value normalized, stored, formatted or returned.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.element.setAttribute(name, value)

        def __len__(self):
            """
            Perform the len utility operation under explicit compatibility rules.

            Example:
                Exercise getDomBuilder.AttrList.  len   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return len(list(self.element.attributes.items()))

        def items(self):
            """
            Perform the items utility operation under explicit compatibility rules.

            Example:
                Exercise getDomBuilder.AttrList.items through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return [(item[0], item[1]) for item in list(self.element.attributes.items())]

        def keys(self):
            """
            Perform the keys utility operation under explicit compatibility rules.

            Example:
                Exercise getDomBuilder.AttrList.keys through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return list(self.element.attributes.keys())

        def __getitem__(self, name):
            """
            Expose getitem behavior for the compatibility container.

            Example:
                Exercise getDomBuilder.AttrList.  getitem   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param name: Field, file, function or resource name addressed by the operation.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.element.getAttribute(name)

        def __contains__(self, name):
            """
            Perform the contains utility operation under explicit compatibility rules.

            Example:
                Exercise getDomBuilder.AttrList.  contains   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param name: Field, file, function or resource name addressed by the operation.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if isinstance(name, tuple):
                raise NotImplementedError
            else:
                return self.element.hasAttribute(name)

    class NodeBuilder(_base.Node):
        """
        Provide the NodeBuilder utility contract with explicit state and cleanup behavior.

        Example:
            Exercise getDomBuilder.NodeBuilder through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def __init__(self, element):
            """
            Initialize and validate the NodeBuilder state.

            Example:
                Exercise getDomBuilder.NodeBuilder.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param element: Value supplied for element under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            _base.Node.__init__(self, element.nodeName)
            self.element = element

        namespace = property(lambda self: hasattr(self.element, "namespaceURI") and self.element.namespaceURI or None)

        def appendChild(self, node):
            """
            Perform the appendChild utility operation under explicit compatibility rules.

            Example:
                Exercise getDomBuilder.NodeBuilder.appendChild through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param node: Value supplied for node under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            node.parent = self
            self.element.appendChild(node.element)

        def insertText(self, data, insertBefore=None):
            """
            Perform the insertText utility operation under explicit compatibility rules.

            Example:
                Exercise getDomBuilder.NodeBuilder.insertText through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param data: Value supplied for data under the utility contract.
            :param insertBefore: Value supplied for insertBefore under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            text = self.element.ownerDocument.createTextNode(data)
            if insertBefore:
                self.element.insertBefore(text, insertBefore.element)
            else:
                self.element.appendChild(text)

        def insertBefore(self, node, refNode):
            """
            Perform the insertBefore utility operation under explicit compatibility rules.

            Example:
                Exercise getDomBuilder.NodeBuilder.insertBefore through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param node: Value supplied for node under the utility contract.
            :param refNode: Value supplied for refNode under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.element.insertBefore(node.element, refNode.element)
            node.parent = self

        def removeChild(self, node):
            """
            Perform the removeChild utility operation under explicit compatibility rules.

            Example:
                Exercise getDomBuilder.NodeBuilder.removeChild through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param node: Value supplied for node under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if node.element.parentNode == self.element:
                self.element.removeChild(node.element)
            node.parent = None

        def reparentChildren(self, newParent):
            """
            Perform the reparentChildren utility operation under explicit compatibility rules.

            Example:
                Exercise getDomBuilder.NodeBuilder.reparentChildren through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param newParent: Value supplied for newParent under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            while self.element.hasChildNodes():
                child = self.element.firstChild
                self.element.removeChild(child)
                newParent.element.appendChild(child)
            self.childNodes = []

        def getAttributes(self):
            """
            Perform the getAttributes utility operation under explicit compatibility rules.

            Example:
                Exercise getDomBuilder.NodeBuilder.getAttributes through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return AttrList(self.element)

        def setAttributes(self, attributes):
            """
            Perform the setAttributes utility operation under explicit compatibility rules.

            Example:
                Exercise getDomBuilder.NodeBuilder.setAttributes through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param attributes: Value supplied for attributes under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if attributes:
                for name, value in list(attributes.items()):
                    if isinstance(name, tuple):
                        if name[0] is not None:
                            qualifiedName = name[0] + ":" + name[1]
                        else:
                            qualifiedName = name[1]
                        self.element.setAttributeNS(name[2], qualifiedName, value)
                    else:
                        self.element.setAttribute(name, value)

        attributes = property(getAttributes, setAttributes)

        def cloneNode(self):
            """
            Perform the cloneNode utility operation under explicit compatibility rules.

            Example:
                Exercise getDomBuilder.NodeBuilder.cloneNode through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return NodeBuilder(self.element.cloneNode(False))

        def hasContent(self):
            """
            Perform the hasContent utility operation under explicit compatibility rules.

            Example:
                Exercise getDomBuilder.NodeBuilder.hasContent through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.element.hasChildNodes()

        def getNameTuple(self):
            """
            Perform the getNameTuple utility operation under explicit compatibility rules.

            Example:
                Exercise getDomBuilder.NodeBuilder.getNameTuple through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if self.namespace is None:
                return namespaces["html"], self.name
            else:
                return self.namespace, self.name

        nameTuple = property(getNameTuple)

    class TreeBuilder(_base.TreeBuilder):
        """
        Provide the TreeBuilder utility contract with explicit state and cleanup behavior.

        Example:
            Exercise getDomBuilder.TreeBuilder through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def documentClass(self):
            """
            Perform the documentClass utility operation under explicit compatibility rules.

            Example:
                Exercise getDomBuilder.TreeBuilder.documentClass through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.dom = Dom.getDOMImplementation().createDocument(None, None, None)
            return weakref.proxy(self)

        def insertDoctype(self, token):
            """
            Perform the insertDoctype utility operation under explicit compatibility rules.

            Example:
                Exercise getDomBuilder.TreeBuilder.insertDoctype through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            name = token["name"]
            publicId = token["publicId"]
            systemId = token["systemId"]

            domimpl = Dom.getDOMImplementation()
            doctype = domimpl.createDocumentType(name, publicId, systemId)
            self.document.appendChild(NodeBuilder(doctype))
            if Dom == minidom:
                doctype.ownerDocument = self.dom

        def elementClass(self, name, namespace=None):
            """
            Perform the elementClass utility operation under explicit compatibility rules.

            Example:
                Exercise getDomBuilder.TreeBuilder.elementClass through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param name: Field, file, function or resource name addressed by the operation.
            :param namespace: Value supplied for namespace under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if namespace is None and self.defaultNamespace is None:
                node = self.dom.createElement(name)
            else:
                node = self.dom.createElementNS(namespace, name)

            return NodeBuilder(node)

        def commentClass(self, data):
            """
            Perform the commentClass utility operation under explicit compatibility rules.

            Example:
                Exercise getDomBuilder.TreeBuilder.commentClass through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param data: Value supplied for data under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return NodeBuilder(self.dom.createComment(data))

        def fragmentClass(self):
            """
            Perform the fragmentClass utility operation under explicit compatibility rules.

            Example:
                Exercise getDomBuilder.TreeBuilder.fragmentClass through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return NodeBuilder(self.dom.createDocumentFragment())

        def appendChild(self, node):
            """
            Perform the appendChild utility operation under explicit compatibility rules.

            Example:
                Exercise getDomBuilder.TreeBuilder.appendChild through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param node: Value supplied for node under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.dom.appendChild(node.element)

        def testSerializer(self, element):
            """
            Perform the testSerializer utility operation under explicit compatibility rules.

            Example:
                Exercise getDomBuilder.TreeBuilder.testSerializer through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param element: Value supplied for element under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return testSerializer(element)

        def getDocument(self):
            """
            Perform the getDocument utility operation under explicit compatibility rules.

            Example:
                Exercise getDomBuilder.TreeBuilder.getDocument through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.dom

        def getFragment(self):
            """
            Perform the getFragment utility operation under explicit compatibility rules.

            Example:
                Exercise getDomBuilder.TreeBuilder.getFragment through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return _base.TreeBuilder.getFragment(self).element

        def insertText(self, data, parent=None):
            """
            Perform the insertText utility operation under explicit compatibility rules.

            Example:
                Exercise getDomBuilder.TreeBuilder.insertText through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param data: Value supplied for data under the utility contract.
            :param parent: Value supplied for parent under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            data = data
            if parent != self:
                _base.TreeBuilder.insertText(self, data, parent)
            else:
                # HACK: allow text nodes as children of the document node
                if hasattr(self.dom, "_child_node_types"):
                    if not Node.TEXT_NODE in self.dom._child_node_types:
                        self.dom._child_node_types = list(self.dom._child_node_types)
                        self.dom._child_node_types.append(Node.TEXT_NODE)
                self.dom.appendChild(self.dom.createTextNode(data))

        implementation = DomImplementation
        name = None

    def testSerializer(element):
        """
        Perform the testSerializer utility operation under explicit compatibility rules.

        Example:
            Exercise getDomBuilder.testSerializer through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param element: Value supplied for element under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        element.normalize()
        rv = []

        def serializeElement(element, indent=0):
            """
            Perform the serializeElement utility operation under explicit compatibility rules.

            Example:
                Exercise getDomBuilder.testSerializer.serializeElement through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param element: Value supplied for element under the utility contract.
            :param indent: Value supplied for indent under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if element.nodeType == Node.DOCUMENT_TYPE_NODE:
                if element.name:
                    if element.publicId or element.systemId:
                        publicId = element.publicId or ""
                        systemId = element.systemId or ""
                        rv.append("""|%s<!DOCTYPE %s "%s" "%s">""" % (" " * indent, element.name, publicId, systemId))
                    else:
                        rv.append("|%s<!DOCTYPE %s>" % (" " * indent, element.name))
                else:
                    rv.append("|%s<!DOCTYPE >" % (" " * indent,))
            elif element.nodeType == Node.DOCUMENT_NODE:
                rv.append("#document")
            elif element.nodeType == Node.DOCUMENT_FRAGMENT_NODE:
                rv.append("#document-fragment")
            elif element.nodeType == Node.COMMENT_NODE:
                rv.append("|%s<!-- %s -->" % (" " * indent, element.nodeValue))
            elif element.nodeType == Node.TEXT_NODE:
                rv.append('|%s"%s"' % (" " * indent, element.nodeValue))
            else:
                if hasattr(element, "namespaceURI") and element.namespaceURI is not None:
                    name = "%s %s" % (
                        constants.prefixes[element.namespaceURI],
                        element.nodeName,
                    )
                else:
                    name = element.nodeName
                rv.append("|%s<%s>" % (" " * indent, name))
                if element.hasAttributes():
                    attributes = []
                    for i in range(len(element.attributes)):
                        attr = element.attributes.item(i)
                        name = attr.nodeName
                        value = attr.value
                        ns = attr.namespaceURI
                        if ns:
                            name = "%s %s" % (constants.prefixes[ns], attr.localName)
                        else:
                            name = attr.nodeName
                        attributes.append((name, value))

                    for name, value in sorted(attributes):
                        rv.append('|%s%s="%s"' % (" " * (indent + 2), name, value))
            indent += 2
            for child in element.childNodes:
                serializeElement(child, indent)

        serializeElement(element, 0)

        return "\n".join(rv)

    return locals()


# The actual means to get a module!
getDomModule = moduleFactoryFactory(getDomBuilder)

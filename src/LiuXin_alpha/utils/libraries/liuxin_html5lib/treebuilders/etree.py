"""
Build or walk HTML5 trees using the configured ElementTree implementation.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise etree through a consuming regression::

        python -m pytest -q tests/file_formats/html/test_html_modernized.py
"""
from __future__ import absolute_import, division, unicode_literals

try:
    text_type = unicode
except NameError:
    text_type = str

import re

from LiuXin_alpha.utils.libraries.liuxin_html5lib.treebuilders import _base
from LiuXin_alpha.utils.libraries.liuxin_html5lib import ihatexml
from LiuXin_alpha.utils.libraries.liuxin_html5lib import constants
from LiuXin_alpha.utils.libraries.liuxin_html5lib.constants import namespaces
from LiuXin_alpha.utils.libraries.liuxin_html5lib.utils import moduleFactoryFactory

tag_regexp = re.compile("{([^}]*)}(.*)")


def getETreeBuilder(ElementTreeImplementation, fullTree=False):
    """
    Perform the getETreeBuilder utility operation under explicit compatibility rules.

    Example:
        Exercise getETreeBuilder through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param ElementTreeImplementation: Value supplied for ElementTreeImplementation under
        the utility contract.
    :param fullTree: Value supplied for fullTree under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    ElementTree = ElementTreeImplementation
    ElementTreeCommentType = ElementTree.Comment("asd").tag

    class Element(_base.Node):
        """
        Provide the Element utility contract with explicit state and cleanup behavior.

        Example:
            Exercise getETreeBuilder.Element through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def __init__(self, name, namespace=None):
            """
            Initialize and validate the Element state.

            Example:
                Exercise getETreeBuilder.Element.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param name: Field, file, function or resource name addressed by the operation.
            :param namespace: Value supplied for namespace under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            self._name = name
            self._namespace = namespace
            self._element = ElementTree.Element(self._getETreeTag(name, namespace))
            if namespace is None:
                self.nameTuple = namespaces["html"], self._name
            else:
                self.nameTuple = self._namespace, self._name
            self.parent = None
            self._childNodes = []
            self._flags = []

        def _getETreeTag(self, name, namespace):
            """
            Perform the getETreeTag utility operation under explicit compatibility rules.

            Example:
                Exercise getETreeBuilder.Element. getETreeTag through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param name: Field, file, function or resource name addressed by the operation.
            :param namespace: Value supplied for namespace under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if namespace is None:
                etree_tag = name
            else:
                etree_tag = "{%s}%s" % (namespace, name)
            return etree_tag

        def _setName(self, name):
            """
            Perform the setName utility operation under explicit compatibility rules.

            Example:
                Exercise getETreeBuilder.Element. setName through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param name: Field, file, function or resource name addressed by the operation.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self._name = name
            self._element.tag = self._getETreeTag(self._name, self._namespace)

        def _getName(self):
            """
            Perform the getName utility operation under explicit compatibility rules.

            Example:
                Exercise getETreeBuilder.Element. getName through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self._name

        name = property(_getName, _setName)

        def _setNamespace(self, namespace):
            """
            Perform the setNamespace utility operation under explicit compatibility rules.

            Example:
                Exercise getETreeBuilder.Element. setNamespace through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param namespace: Value supplied for namespace under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self._namespace = namespace
            self._element.tag = self._getETreeTag(self._name, self._namespace)

        def _getNamespace(self):
            """
            Perform the getNamespace utility operation under explicit compatibility rules.

            Example:
                Exercise getETreeBuilder.Element. getNamespace through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self._namespace

        namespace = property(_getNamespace, _setNamespace)

        def _getAttributes(self):
            """
            Perform the getAttributes utility operation under explicit compatibility rules.

            Example:
                Exercise getETreeBuilder.Element. getAttributes through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self._element.attrib

        def _setAttributes(self, attributes):
            # Delete existing attributes first
            # XXX - there may be a better way to do this...
            """
            Perform the setAttributes utility operation under explicit compatibility rules.

            Example:
                Exercise getETreeBuilder.Element. setAttributes through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param attributes: Value supplied for attributes under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            for key in list(self._element.attrib.keys()):
                del self._element.attrib[key]
            for key, value in attributes.items():
                if isinstance(key, tuple):
                    name = "{%s}%s" % (key[2], key[1])
                else:
                    name = key
                self._element.set(name, value)

        attributes = property(_getAttributes, _setAttributes)

        def _getChildNodes(self):
            """
            Perform the getChildNodes utility operation under explicit compatibility rules.

            Example:
                Exercise getETreeBuilder.Element. getChildNodes through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self._childNodes

        def _setChildNodes(self, value):
            """
            Perform the setChildNodes utility operation under explicit compatibility rules.

            Example:
                Exercise getETreeBuilder.Element. setChildNodes through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param value: Value normalized, stored, formatted or returned.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            del self._element[:]
            self._childNodes = []
            for element in value:
                self.insertChild(element)

        childNodes = property(_getChildNodes, _setChildNodes)

        def hasContent(self):
            """
            Return true if the node has children or text

            Example:
                Exercise getETreeBuilder.Element.hasContent through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return bool(self._element.text or len(self._element))

        def appendChild(self, node):
            """
            Perform the appendChild utility operation under explicit compatibility rules.

            Example:
                Exercise getETreeBuilder.Element.appendChild through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param node: Value supplied for node under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self._childNodes.append(node)
            self._element.append(node._element)
            node.parent = self

        def insertBefore(self, node, refNode):
            """
            Perform the insertBefore utility operation under explicit compatibility rules.

            Example:
                Exercise getETreeBuilder.Element.insertBefore through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param node: Value supplied for node under the utility contract.
            :param refNode: Value supplied for refNode under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            index = list(self._element).index(refNode._element)
            self._element.insert(index, node._element)
            node.parent = self

        def removeChild(self, node):
            """
            Perform the removeChild utility operation under explicit compatibility rules.

            Example:
                Exercise getETreeBuilder.Element.removeChild through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param node: Value supplied for node under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self._element.remove(node._element)
            node.parent = None

        def insertText(self, data, insertBefore=None):
            """
            Perform the insertText utility operation under explicit compatibility rules.

            Example:
                Exercise getETreeBuilder.Element.insertText through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param data: Value supplied for data under the utility contract.
            :param insertBefore: Value supplied for insertBefore under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if not (len(self._element)):
                if not self._element.text:
                    self._element.text = ""
                self._element.text += data
            elif insertBefore is None:
                # Insert the text as the tail of the last child element
                if not self._element[-1].tail:
                    self._element[-1].tail = ""
                self._element[-1].tail += data
            else:
                # Insert the text before the specified node
                children = list(self._element)
                index = children.index(insertBefore._element)
                if index > 0:
                    if not self._element[index - 1].tail:
                        self._element[index - 1].tail = ""
                    self._element[index - 1].tail += data
                else:
                    if not self._element.text:
                        self._element.text = ""
                    self._element.text += data

        def cloneNode(self):
            """
            Perform the cloneNode utility operation under explicit compatibility rules.

            Example:
                Exercise getETreeBuilder.Element.cloneNode through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            element = type(self)(self.name, self.namespace)
            for name, value in self.attributes.items():
                element.attributes[name] = value
            return element

        def reparentChildren(self, newParent):
            """
            Perform the reparentChildren utility operation under explicit compatibility rules.

            Example:
                Exercise getETreeBuilder.Element.reparentChildren through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param newParent: Value supplied for newParent under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if newParent.childNodes:
                newParent.childNodes[-1]._element.tail += self._element.text
            else:
                if not newParent._element.text:
                    newParent._element.text = ""
                if self._element.text is not None:
                    newParent._element.text += self._element.text
            self._element.text = ""
            _base.Node.reparentChildren(self, newParent)

    class Comment(Element):
        """
        Provide the Comment utility contract with explicit state and cleanup behavior.

        Example:
            Exercise getETreeBuilder.Comment through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def __init__(self, data):
            # Use the superclass constructor to set all properties on the
            # wrapper element
            """
            Initialize and validate the Comment state.

            Example:
                Exercise getETreeBuilder.Comment.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param data: Value supplied for data under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            self._element = ElementTree.Comment(data)
            self.parent = None
            self._childNodes = []
            self._flags = []

        def _getData(self):
            """
            Perform the getData utility operation under explicit compatibility rules.

            Example:
                Exercise getETreeBuilder.Comment. getData through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self._element.text

        def _setData(self, value):
            """
            Perform the setData utility operation under explicit compatibility rules.

            Example:
                Exercise getETreeBuilder.Comment. setData through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param value: Value normalized, stored, formatted or returned.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self._element.text = value

        data = property(_getData, _setData)

    class DocumentType(Element):
        """
        Provide the DocumentType utility contract with explicit state and cleanup behavior.

        Example:
            Exercise getETreeBuilder.DocumentType through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def __init__(self, name, publicId, systemId):
            """
            Initialize and validate the DocumentType state.

            Example:
                Exercise getETreeBuilder.DocumentType.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param name: Field, file, function or resource name addressed by the operation.
            :param publicId: Value supplied for publicId under the utility contract.
            :param systemId: Value supplied for systemId under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            Element.__init__(self, "<!DOCTYPE>")
            self._element.text = name
            self.publicId = publicId
            self.systemId = systemId

        def _getPublicId(self):
            """
            Perform the getPublicId utility operation under explicit compatibility rules.

            Example:
                Exercise getETreeBuilder.DocumentType. getPublicId through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self._element.get("publicId", "")

        def _setPublicId(self, value):
            """
            Perform the setPublicId utility operation under explicit compatibility rules.

            Example:
                Exercise getETreeBuilder.DocumentType. setPublicId through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param value: Value normalized, stored, formatted or returned.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if value is not None:
                self._element.set("publicId", value)

        publicId = property(_getPublicId, _setPublicId)

        def _getSystemId(self):
            """
            Perform the getSystemId utility operation under explicit compatibility rules.

            Example:
                Exercise getETreeBuilder.DocumentType. getSystemId through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self._element.get("systemId", "")

        def _setSystemId(self, value):
            """
            Perform the setSystemId utility operation under explicit compatibility rules.

            Example:
                Exercise getETreeBuilder.DocumentType. setSystemId through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param value: Value normalized, stored, formatted or returned.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if value is not None:
                self._element.set("systemId", value)

        systemId = property(_getSystemId, _setSystemId)

    class Document(Element):
        """
        Provide the Document utility contract with explicit state and cleanup behavior.

        Example:
            Exercise getETreeBuilder.Document through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def __init__(self):
            """
            Initialize and validate the Document state.

            Example:
                Exercise getETreeBuilder.Document.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: None; validated state is stored on the receiving object.
            """
            Element.__init__(self, "DOCUMENT_ROOT")

    class DocumentFragment(Element):
        """
        Provide the DocumentFragment utility contract with explicit state and cleanup behavior.

        Example:
            Exercise getETreeBuilder.DocumentFragment through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def __init__(self):
            """
            Initialize and validate the DocumentFragment state.

            Example:
                Exercise getETreeBuilder.DocumentFragment.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: None; validated state is stored on the receiving object.
            """
            Element.__init__(self, "DOCUMENT_FRAGMENT")

    def testSerializer(element):
        """
        Perform the testSerializer utility operation under explicit compatibility rules.

        Example:
            Exercise getETreeBuilder.testSerializer through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param element: Value supplied for element under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        rv = []

        def serializeElement(element, indent=0):
            """
            Perform the serializeElement utility operation under explicit compatibility rules.

            Example:
                Exercise getETreeBuilder.testSerializer.serializeElement through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param element: Value supplied for element under the utility contract.
            :param indent: Value supplied for indent under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if not (hasattr(element, "tag")):
                element = element.getroot()
            if element.tag == "<!DOCTYPE>":
                if element.get("publicId") or element.get("systemId"):
                    publicId = element.get("publicId") or ""
                    systemId = element.get("systemId") or ""
                    rv.append("""<!DOCTYPE %s "%s" "%s">""" % (element.text, publicId, systemId))
                else:
                    rv.append("<!DOCTYPE %s>" % (element.text,))
            elif element.tag == "DOCUMENT_ROOT":
                rv.append("#document")
                if element.text is not None:
                    rv.append('|%s"%s"' % (" " * (indent + 2), element.text))
                if element.tail is not None:
                    raise TypeError("Document node cannot have tail")
                if hasattr(element, "attrib") and len(element.attrib):
                    raise TypeError("Document node cannot have attributes")
            elif element.tag == ElementTreeCommentType:
                rv.append("|%s<!-- %s -->" % (" " * indent, element.text))
            else:
                assert isinstance(element.tag, text_type), "Expected unicode, got %s, %s" % (
                    type(element.tag),
                    element.tag,
                )
                nsmatch = tag_regexp.match(element.tag)

                if nsmatch is None:
                    name = element.tag
                else:
                    ns, name = nsmatch.groups()
                    prefix = constants.prefixes[ns]
                    name = "%s %s" % (prefix, name)
                rv.append("|%s<%s>" % (" " * indent, name))

                if hasattr(element, "attrib"):
                    attributes = []
                    for name, value in element.attrib.items():
                        nsmatch = tag_regexp.match(name)
                        if nsmatch is not None:
                            ns, name = nsmatch.groups()
                            prefix = constants.prefixes[ns]
                            attr_string = "%s %s" % (prefix, name)
                        else:
                            attr_string = name
                        attributes.append((attr_string, value))

                    for name, value in sorted(attributes):
                        rv.append('|%s%s="%s"' % (" " * (indent + 2), name, value))
                if element.text:
                    rv.append('|%s"%s"' % (" " * (indent + 2), element.text))
            indent += 2
            for child in element:
                serializeElement(child, indent)
            if element.tail:
                rv.append('|%s"%s"' % (" " * (indent - 2), element.tail))

        serializeElement(element, 0)

        return "\n".join(rv)

    def tostring(element):
        """
        Serialize an element and its child nodes to a string

        Example:
            Exercise getETreeBuilder.tostring through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param element: Value supplied for element under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        rv = []
        filter = ihatexml.InfosetFilter()

        def serializeElement(element):
            """
            Perform the serializeElement utility operation under explicit compatibility rules.

            Example:
                Exercise getETreeBuilder.tostring.serializeElement through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param element: Value supplied for element under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if isinstance(element, ElementTree.ElementTree):
                element = element.getroot()

            if element.tag == "<!DOCTYPE>":
                if element.get("publicId") or element.get("systemId"):
                    publicId = element.get("publicId") or ""
                    systemId = element.get("systemId") or ""
                    rv.append("""<!DOCTYPE %s PUBLIC "%s" "%s">""" % (element.text, publicId, systemId))
                else:
                    rv.append("<!DOCTYPE %s>" % (element.text,))
            elif element.tag == "DOCUMENT_ROOT":
                if element.text is not None:
                    rv.append(element.text)
                if element.tail is not None:
                    raise TypeError("Document node cannot have tail")
                if hasattr(element, "attrib") and len(element.attrib):
                    raise TypeError("Document node cannot have attributes")

                for child in element:
                    serializeElement(child)

            elif element.tag == ElementTreeCommentType:
                rv.append("<!--%s-->" % (element.text,))
            else:
                # This is assumed to be an ordinary element
                if not element.attrib:
                    rv.append("<%s>" % (filter.fromXmlName(element.tag),))
                else:
                    attr = " ".join(
                        ['%s="%s"' % (filter.fromXmlName(name), value) for name, value in element.attrib.items()]
                    )
                    rv.append("<%s %s>" % (element.tag, attr))
                if element.text:
                    rv.append(element.text)

                for child in element:
                    serializeElement(child)

                rv.append("</%s>" % (element.tag,))

            if element.tail:
                rv.append(element.tail)

        serializeElement(element)

        return "".join(rv)

    class TreeBuilder(_base.TreeBuilder):
        """
        Provide the TreeBuilder utility contract with explicit state and cleanup behavior.

        Example:
            Exercise getETreeBuilder.TreeBuilder through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        documentClass = Document
        doctypeClass = DocumentType
        elementClass = Element
        commentClass = Comment
        fragmentClass = DocumentFragment
        implementation = ElementTreeImplementation

        def testSerializer(self, element):
            """
            Perform the testSerializer utility operation under explicit compatibility rules.

            Example:
                Exercise getETreeBuilder.TreeBuilder.testSerializer through a consuming regression::

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
                Exercise getETreeBuilder.TreeBuilder.getDocument through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if fullTree:
                return self.document._element
            else:
                if self.defaultNamespace is not None:
                    return self.document._element.find("{%s}html" % self.defaultNamespace)
                else:
                    return self.document._element.find("html")

        def getFragment(self):
            """
            Perform the getFragment utility operation under explicit compatibility rules.

            Example:
                Exercise getETreeBuilder.TreeBuilder.getFragment through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return _base.TreeBuilder.getFragment(self)._element

    return locals()


getETreeModule = moduleFactoryFactory(getETreeBuilder)

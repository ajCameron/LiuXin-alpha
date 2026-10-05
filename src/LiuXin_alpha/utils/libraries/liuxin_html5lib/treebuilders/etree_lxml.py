"""
Build HTML5 trees through lxml while preserving namespace and fragment semantics.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise etree lxml through a consuming regression::

        python -m pytest -q tests/file_formats/html/test_html_modernized.py
"""

from __future__ import absolute_import, division, unicode_literals

import warnings
import re
import sys

from LiuXin_alpha.utils.libraries.liuxin_html5lib.treebuilders import _base
from LiuXin_alpha.utils.libraries.liuxin_html5lib.constants import DataLossWarning
from LiuXin_alpha.utils.libraries.liuxin_html5lib import constants
from LiuXin_alpha.utils.libraries.liuxin_html5lib.treebuilders import etree as etree_builders
from LiuXin_alpha.utils.libraries.liuxin_html5lib import ihatexml

import lxml.etree as etree


fullTree = True
tag_regexp = re.compile("{([^}]*)}(.*)")

comment_type = etree.Comment("asd").tag


class DocumentType(object):
    """
    Provide the DocumentType utility contract with explicit state and cleanup behavior.

    Example:
        Exercise DocumentType through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """
    def __init__(self, name, publicId, systemId):
        """
        Initialize and validate the DocumentType state.

        Example:
            Exercise DocumentType.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param publicId: Value supplied for publicId under the utility contract.
        :param systemId: Value supplied for systemId under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.name = name
        self.publicId = publicId
        self.systemId = systemId


class Document(object):
    """
    Provide the Document utility contract with explicit state and cleanup behavior.

    Example:
        Exercise Document through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """
    def __init__(self):
        """
        Initialize and validate the Document state.

        Example:
            Exercise Document.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: None; validated state is stored on the receiving object.
        """
        self._elementTree = None
        self._childNodes = []

    def appendChild(self, element):
        """
        Perform the appendChild utility operation under explicit compatibility rules.

        Example:
            Exercise Document.appendChild through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param element: Value supplied for element under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._elementTree.getroot().addnext(element._element)

    def _getChildNodes(self):
        """
        Perform the getChildNodes utility operation under explicit compatibility rules.

        Example:
            Exercise Document. getChildNodes through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._childNodes

    childNodes = property(_getChildNodes)


def testSerializer(element):
    """
    Perform the testSerializer utility operation under explicit compatibility rules.

    Example:
        Exercise testSerializer through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param element: Value supplied for element under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    rv = []
    finalText = None
    infosetFilter = ihatexml.InfosetFilter()

    def serializeElement(element, indent=0):
        """
        Perform the serializeElement utility operation under explicit compatibility rules.

        Example:
            Exercise testSerializer.serializeElement through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param element: Value supplied for element under the utility contract.
        :param indent: Value supplied for indent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not hasattr(element, "tag"):
            if hasattr(element, "getroot"):
                # Full tree case
                rv.append("#document")
                if element.docinfo.internalDTD:
                    if not (element.docinfo.public_id or element.docinfo.system_url):
                        dtd_str = "<!DOCTYPE %s>" % element.docinfo.root_name
                    else:
                        dtd_str = """<!DOCTYPE %s "%s" "%s">""" % (
                            element.docinfo.root_name,
                            element.docinfo.public_id,
                            element.docinfo.system_url,
                        )
                    rv.append("|%s%s" % (" " * (indent + 2), dtd_str))
                next_element = element.getroot()
                while next_element.getprevious() is not None:
                    next_element = next_element.getprevious()
                while next_element is not None:
                    serializeElement(next_element, indent + 2)
                    next_element = next_element.getnext()
            elif isinstance(element, str) or isinstance(element, bytes):
                # Text in a fragment
                assert isinstance(element, str) or sys.version_info.major == 2
                rv.append('|%s"%s"' % (" " * indent, element))
            else:
                # Fragment case
                rv.append("#document-fragment")
                for next_element in element:
                    serializeElement(next_element, indent + 2)
        elif element.tag == comment_type:
            rv.append("|%s<!-- %s -->" % (" " * indent, element.text))
            if hasattr(element, "tail") and element.tail:
                rv.append('|%s"%s"' % (" " * indent, element.tail))
        else:
            assert isinstance(element, etree._Element)
            nsmatch = etree_builders.tag_regexp.match(element.tag)
            if nsmatch is not None:
                ns = nsmatch.group(1)
                tag = nsmatch.group(2)
                prefix = constants.prefixes[ns]
                rv.append("|%s<%s %s>" % (" " * indent, prefix, infosetFilter.fromXmlName(tag)))
            else:
                rv.append("|%s<%s>" % (" " * indent, infosetFilter.fromXmlName(element.tag)))

            if hasattr(element, "attrib"):
                attributes = []
                for name, value in element.attrib.items():
                    nsmatch = tag_regexp.match(name)
                    if nsmatch is not None:
                        ns, name = nsmatch.groups()
                        name = infosetFilter.fromXmlName(name)
                        prefix = constants.prefixes[ns]
                        attr_string = "%s %s" % (prefix, name)
                    else:
                        attr_string = infosetFilter.fromXmlName(name)
                    attributes.append((attr_string, value))

                for name, value in sorted(attributes):
                    rv.append('|%s%s="%s"' % (" " * (indent + 2), name, value))

            if element.text:
                rv.append('|%s"%s"' % (" " * (indent + 2), element.text))
            indent += 2
            for child in element:
                serializeElement(child, indent)
            if hasattr(element, "tail") and element.tail:
                rv.append('|%s"%s"' % (" " * (indent - 2), element.tail))

    serializeElement(element, 0)

    if finalText is not None:
        rv.append('|%s"%s"' % (" " * 2, finalText))

    return "\n".join(rv)


def tostring(element):
    """
    Serialize an element and its child nodes to a string

    Example:
        Exercise tostring through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param element: Value supplied for element under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    rv = []
    finalText = None

    def serializeElement(element):
        """
        Perform the serializeElement utility operation under explicit compatibility rules.

        Example:
            Exercise tostring.serializeElement through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param element: Value supplied for element under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not hasattr(element, "tag"):
            if element.docinfo.internalDTD:
                if element.docinfo.doctype:
                    dtd_str = element.docinfo.doctype
                else:
                    dtd_str = "<!DOCTYPE %s>" % element.docinfo.root_name
                rv.append(dtd_str)
            serializeElement(element.getroot())

        elif element.tag == comment_type:
            rv.append("<!--%s-->" % (element.text,))

        else:
            # This is assumed to be an ordinary element
            if not element.attrib:
                rv.append("<%s>" % (element.tag,))
            else:
                attr = " ".join(['%s="%s"' % (name, value) for name, value in element.attrib.items()])
                rv.append("<%s %s>" % (element.tag, attr))
            if element.text:
                rv.append(element.text)

            for child in element:
                serializeElement(child)

            rv.append("</%s>" % (element.tag,))

        if hasattr(element, "tail") and element.tail:
            rv.append(element.tail)

    serializeElement(element)

    if finalText is not None:
        rv.append('%s"' % (" " * 2, finalText))

    return "".join(rv)


class TreeBuilder(_base.TreeBuilder):
    """
    Provide the TreeBuilder utility contract with explicit state and cleanup behavior.

    Example:
        Exercise TreeBuilder through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """
    documentClass = Document
    doctypeClass = DocumentType
    elementClass = None
    commentClass = None
    fragmentClass = Document
    implementation = etree

    def __init__(self, namespaceHTMLElements, fullTree=False):
        """
        Initialize and validate the TreeBuilder state.

        Example:
            Exercise TreeBuilder.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param namespaceHTMLElements: Value supplied for namespaceHTMLElements under the
            utility contract.
        :param fullTree: Value supplied for fullTree under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        builder = etree_builders.getETreeModule(etree, fullTree=fullTree)
        infosetFilter = self.infosetFilter = ihatexml.InfosetFilter()
        self.namespaceHTMLElements = namespaceHTMLElements

        class Attributes(dict):
            """
            Provide the Attributes utility contract with explicit state and cleanup behavior.

            Example:
                Exercise TreeBuilder.  init  .Attributes through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py
            """
            def __init__(self, element, value={}):
                """
                Initialize and validate the Attributes state.

                Example:
                    Exercise TreeBuilder.  init  .Attributes.  init   through a consuming regression::

                        python -m pytest -q tests/file_formats/html/test_html_modernized.py


                :param element: Value supplied for element under the utility contract.
                :param value: Value normalized, stored, formatted or returned.
                :return: None; validated state is stored on the receiving object.
                """
                self._element = element
                dict.__init__(self, value)
                for key, value in self.items():
                    if isinstance(key, tuple):
                        name = "{%s}%s" % (
                            key[2],
                            infosetFilter.coerceAttribute(key[1]),
                        )
                    else:
                        name = infosetFilter.coerceAttribute(key)
                    self._element._element.attrib[name] = value

            def __setitem__(self, key, value):
                """
                Perform the setitem utility operation under explicit compatibility rules.

                Example:
                    Exercise TreeBuilder.  init  .Attributes.  setitem   through a consuming regression::

                        python -m pytest -q tests/file_formats/html/test_html_modernized.py


                :param key: Metadata, identifier or local-variable key.
                :param value: Value normalized, stored, formatted or returned.
                :return: None; the operation mutates state, writes output or performs cleanup in
                    place.
                """
                dict.__setitem__(self, key, value)
                if isinstance(key, tuple):
                    name = "{%s}%s" % (key[2], infosetFilter.coerceAttribute(key[1]))
                else:
                    name = infosetFilter.coerceAttribute(key)
                self._element._element.attrib[name] = value

        class Element(builder.Element):
            """
            Provide the Element utility contract with explicit state and cleanup behavior.

            Example:
                Exercise TreeBuilder.  init  .Element through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py
            """
            def __init__(self, name, namespace):
                """
                Initialize and validate the Element state.

                Example:
                    Exercise TreeBuilder.  init  .Element.  init   through a consuming regression::

                        python -m pytest -q tests/file_formats/html/test_html_modernized.py


                :param name: Field, file, function or resource name addressed by the operation.
                :param namespace: Value supplied for namespace under the utility contract.
                :return: None; validated state is stored on the receiving object.
                """
                name = infosetFilter.coerceElement(name)
                builder.Element.__init__(self, name, namespace=namespace)
                self._attributes = Attributes(self)

            def _setName(self, name):
                """
                Perform the setName utility operation under explicit compatibility rules.

                Example:
                    Exercise TreeBuilder.  init  .Element. setName through a consuming regression::

                        python -m pytest -q tests/file_formats/html/test_html_modernized.py


                :param name: Field, file, function or resource name addressed by the operation.
                :return: None; the operation mutates state, writes output or performs cleanup in
                    place.
                """
                self._name = infosetFilter.coerceElement(name)
                self._element.tag = self._getETreeTag(self._name, self._namespace)

            def _getName(self):
                """
                Perform the getName utility operation under explicit compatibility rules.

                Example:
                    Exercise TreeBuilder.  init  .Element. getName through a consuming regression::

                        python -m pytest -q tests/file_formats/html/test_html_modernized.py


                :return: The normalized value, metadata record, path, stream result or collection
                    described above.
                """
                return infosetFilter.fromXmlName(self._name)

            name = property(_getName, _setName)

            def _getAttributes(self):
                """
                Perform the getAttributes utility operation under explicit compatibility rules.

                Example:
                    Exercise TreeBuilder.  init  .Element. getAttributes through a consuming regression::

                        python -m pytest -q tests/file_formats/html/test_html_modernized.py


                :return: The normalized value, metadata record, path, stream result or collection
                    described above.
                """
                return self._attributes

            def _setAttributes(self, attributes):
                """
                Perform the setAttributes utility operation under explicit compatibility rules.

                Example:
                    Exercise TreeBuilder.  init  .Element. setAttributes through a consuming regression::

                        python -m pytest -q tests/file_formats/html/test_html_modernized.py


                :param attributes: Value supplied for attributes under the utility contract.
                :return: None; the operation mutates state, writes output or performs cleanup in
                    place.
                """
                self._attributes = Attributes(self, attributes)

            attributes = property(_getAttributes, _setAttributes)

            def insertText(self, data, insertBefore=None):
                """
                Perform the insertText utility operation under explicit compatibility rules.

                Example:
                    Exercise TreeBuilder.  init  .Element.insertText through a consuming regression::

                        python -m pytest -q tests/file_formats/html/test_html_modernized.py


                :param data: Value supplied for data under the utility contract.
                :param insertBefore: Value supplied for insertBefore under the utility contract.
                :return: None; the operation mutates state, writes output or performs cleanup in
                    place.
                """
                data = infosetFilter.coerceCharacters(data)
                builder.Element.insertText(self, data, insertBefore)

            def appendChild(self, child):
                """
                Perform the appendChild utility operation under explicit compatibility rules.

                Example:
                    Exercise TreeBuilder.  init  .Element.appendChild through a consuming regression::

                        python -m pytest -q tests/file_formats/html/test_html_modernized.py


                :param child: Value supplied for child under the utility contract.
                :return: None; the operation mutates state, writes output or performs cleanup in
                    place.
                """
                builder.Element.appendChild(self, child)

        class Comment(builder.Comment):
            """
            Provide the Comment utility contract with explicit state and cleanup behavior.

            Example:
                Exercise TreeBuilder.  init  .Comment through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py
            """
            def __init__(self, data):
                """
                Initialize and validate the Comment state.

                Example:
                    Exercise TreeBuilder.  init  .Comment.  init   through a consuming regression::

                        python -m pytest -q tests/file_formats/html/test_html_modernized.py


                :param data: Value supplied for data under the utility contract.
                :return: None; validated state is stored on the receiving object.
                """
                data = infosetFilter.coerceComment(data)
                builder.Comment.__init__(self, data)

            def _setData(self, data):
                """
                Perform the setData utility operation under explicit compatibility rules.

                Example:
                    Exercise TreeBuilder.  init  .Comment. setData through a consuming regression::

                        python -m pytest -q tests/file_formats/html/test_html_modernized.py


                :param data: Value supplied for data under the utility contract.
                :return: None; the operation mutates state, writes output or performs cleanup in
                    place.
                """
                data = infosetFilter.coerceComment(data)
                self._element.text = data

            def _getData(self):
                """
                Perform the getData utility operation under explicit compatibility rules.

                Example:
                    Exercise TreeBuilder.  init  .Comment. getData through a consuming regression::

                        python -m pytest -q tests/file_formats/html/test_html_modernized.py


                :return: The normalized value, metadata record, path, stream result or collection
                    described above.
                """
                return self._element.text

            data = property(_getData, _setData)

        self.elementClass = Element
        self.commentClass = builder.Comment
        # self.fragmentClass = builder.DocumentFragment
        _base.TreeBuilder.__init__(self, namespaceHTMLElements)

    def reset(self):
        """
        Perform the reset utility operation under explicit compatibility rules.

        Example:
            Exercise TreeBuilder.reset through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        _base.TreeBuilder.reset(self)
        self.insertComment = self.insertCommentInitial
        self.initial_comments = []
        self.doctype = None

    def testSerializer(self, element):
        """
        Perform the testSerializer utility operation under explicit compatibility rules.

        Example:
            Exercise TreeBuilder.testSerializer through a consuming regression::

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
            Exercise TreeBuilder.getDocument through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if fullTree:
            return self.document._elementTree
        else:
            return self.document._elementTree.getroot()

    def getFragment(self):
        """
        Perform the getFragment utility operation under explicit compatibility rules.

        Example:
            Exercise TreeBuilder.getFragment through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        fragment = []
        element = self.openElements[0]._element
        if element.text:
            fragment.append(element.text)
        fragment.extend(list(element))
        if element.tail:
            fragment.append(element.tail)
        return fragment

    def insertDoctype(self, token):
        """
        Perform the insertDoctype utility operation under explicit compatibility rules.

        Example:
            Exercise TreeBuilder.insertDoctype through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param token: Value supplied for token under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        name = token["name"]
        publicId = token["publicId"]
        systemId = token["systemId"]

        if not name:
            warnings.warn("lxml cannot represent empty doctype", DataLossWarning)
            self.doctype = None
        else:
            coercedName = self.infosetFilter.coerceElement(name)
            if coercedName != name:
                warnings.warn("lxml cannot represent non-xml doctype", DataLossWarning)

            doctype = self.doctypeClass(coercedName, publicId, systemId)
            self.doctype = doctype

    def insertCommentInitial(self, data, parent=None):
        """
        Perform the insertCommentInitial utility operation under explicit compatibility rules.

        Example:
            Exercise TreeBuilder.insertCommentInitial through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param data: Value supplied for data under the utility contract.
        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.initial_comments.append(data)

    def insertCommentMain(self, data, parent=None):
        """
        Perform the insertCommentMain utility operation under explicit compatibility rules.

        Example:
            Exercise TreeBuilder.insertCommentMain through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param data: Value supplied for data under the utility contract.
        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if parent == self.document and self.document._elementTree.getroot()[-1].tag == comment_type:
            warnings.warn(
                "lxml cannot represent adjacent comments beyond the root elements",
                DataLossWarning,
            )
        if data["data"]:
            # lxml cannot handle comment text that contains -- or endswith -
            # Should really check if changes happened and issue a data loss
            # warning, but that's a fairly big performance hit.
            data["data"] = data["data"].replace("--", "\u2010\u2010").rstrip("-")
        super(TreeBuilder, self).insertComment(data, parent)

    def insertRoot(self, token):
        """
        Create the document root

        Example:
            Exercise TreeBuilder.insertRoot through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param token: Value supplied for token under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        # Because of the way libxml2 works, it doesn't seem to be possible to
        # alter information like the doctype after the tree has been parsed.
        # Therefore we need to use the built-in parser to create our iniial
        # tree, after which we can add elements like normal
        docStr = ""
        if self.doctype:
            assert self.doctype.name
            docStr += "<!DOCTYPE %s" % self.doctype.name
            if self.doctype.publicId is not None or self.doctype.systemId is not None:
                docStr += ' PUBLIC "%s" ' % (self.infosetFilter.coercePubid(self.doctype.publicId or ""))
                if self.doctype.systemId:
                    sysid = self.doctype.systemId
                    if sysid.find("'") >= 0 and sysid.find('"') >= 0:
                        warnings.warn(
                            "DOCTYPE system cannot contain single and double quotes",
                            DataLossWarning,
                        )
                        sysid = sysid.replace("'", "U00027")
                    if sysid.find("'") >= 0:
                        docStr += '"%s"' % sysid
                    else:
                        docStr += "'%s'" % sysid
                else:
                    docStr += "''"
            docStr += ">"
            if self.doctype.name != token["name"]:
                warnings.warn(
                    "lxml cannot represent doctype with a different name to the root element",
                    DataLossWarning,
                )
        docStr += "<THIS_SHOULD_NEVER_APPEAR_PUBLICLY/>"
        root = etree.fromstring(docStr)

        # Append the initial comments:
        for comment_token in self.initial_comments:
            root.addprevious(etree.Comment(comment_token["data"]))

        # Create the root document and add the ElementTree to it
        self.document = self.documentClass()
        self.document._elementTree = root.getroottree()

        # Give the root element the right name
        name = token["name"]
        namespace = token.get("namespace", self.defaultNamespace)
        if namespace is None:
            etree_tag = name
        else:
            etree_tag = "{%s}%s" % (namespace, name)
        root.tag = etree_tag

        # Add the root element to the internal child/open data structures
        root_element = self.elementClass(name, namespace)
        root_element._element = root
        self.document._childNodes.append(root_element)
        self.openElements.append(root_element)

        # Reset to the default insert comment function
        self.insertComment = self.insertCommentMain

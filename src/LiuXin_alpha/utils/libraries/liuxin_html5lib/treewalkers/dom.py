"""
Build or walk HTML5 trees using the DOM compatibility interface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise dom through a consuming regression::

        python -m pytest -q tests/file_formats/html/test_html_modernized.py
"""
from __future__ import absolute_import, division, unicode_literals

from xml.dom import Node

from . import _base


class TreeWalker(_base.NonRecursiveTreeWalker):
    """
    Provide the TreeWalker utility contract with explicit state and cleanup behavior.

    Example:
        Exercise TreeWalker through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """
    def getNodeDetails(self, node):
        """
        Perform the getNodeDetails utility operation under explicit compatibility rules.

        Example:
            Exercise TreeWalker.getNodeDetails through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param node: Value supplied for node under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if node.nodeType == Node.DOCUMENT_TYPE_NODE:
            return _base.DOCTYPE, node.name, node.publicId, node.systemId

        elif node.nodeType in (Node.TEXT_NODE, Node.CDATA_SECTION_NODE):
            return _base.TEXT, node.nodeValue

        elif node.nodeType == Node.ELEMENT_NODE:
            attrs = {}
            for attr in list(node.attributes.keys()):
                attr = node.getAttributeNode(attr)
                if attr.namespaceURI:
                    attrs[(attr.namespaceURI, attr.localName)] = attr.value
                else:
                    attrs[(None, attr.name)] = attr.value
            return (
                _base.ELEMENT,
                node.namespaceURI,
                node.nodeName,
                attrs,
                node.hasChildNodes(),
            )

        elif node.nodeType == Node.COMMENT_NODE:
            return _base.COMMENT, node.nodeValue

        elif node.nodeType in (Node.DOCUMENT_NODE, Node.DOCUMENT_FRAGMENT_NODE):
            return (_base.DOCUMENT,)

        else:
            return _base.UNKNOWN, node.nodeType

    def getFirstChild(self, node):
        """
        Perform the getFirstChild utility operation under explicit compatibility rules.

        Example:
            Exercise TreeWalker.getFirstChild through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param node: Value supplied for node under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return node.firstChild

    def getNextSibling(self, node):
        """
        Perform the getNextSibling utility operation under explicit compatibility rules.

        Example:
            Exercise TreeWalker.getNextSibling through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param node: Value supplied for node under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return node.nextSibling

    def getParentNode(self, node):
        """
        Perform the getParentNode utility operation under explicit compatibility rules.

        Example:
            Exercise TreeWalker.getParentNode through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param node: Value supplied for node under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return node.parentNode

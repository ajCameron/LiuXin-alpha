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
    from collections import OrderedDict
except ImportError:
    try:
        from ordereddict import OrderedDict
    except ImportError:
        OrderedDict = dict

import re

try:
    unicode
    string_types = (basestring,)
except NameError:
    string_types = (str,)

from . import _base
from ..utils import moduleFactoryFactory

tag_regexp = re.compile("{([^}]*)}(.*)")


def getETreeBuilder(ElementTreeImplementation):
    """
    Perform the getETreeBuilder utility operation under explicit compatibility rules.

    Example:
        Exercise getETreeBuilder through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param ElementTreeImplementation: Value supplied for ElementTreeImplementation under
        the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    ElementTree = ElementTreeImplementation
    ElementTreeCommentType = ElementTree.Comment("asd").tag

    class TreeWalker(_base.NonRecursiveTreeWalker):
        """
        Given the particular ElementTree representation, this implementation, to avoid using recursion, returns "nodes" as tuples with the following content:

        Example:
            Exercise getETreeBuilder.TreeWalker through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """

        def getNodeDetails(self, node):
            """
            Perform the getNodeDetails utility operation under explicit compatibility rules.

            Example:
                Exercise getETreeBuilder.TreeWalker.getNodeDetails through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param node: Value supplied for node under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if isinstance(node, tuple):  # It might be the root Element
                elt, key, parents, flag = node
                if flag in ("text", "tail"):
                    return _base.TEXT, getattr(elt, flag)
                else:
                    node = elt

            if not (hasattr(node, "tag")):
                node = node.getroot()

            if node.tag in ("DOCUMENT_ROOT", "DOCUMENT_FRAGMENT"):
                return (_base.DOCUMENT,)

            elif node.tag == "<!DOCTYPE>":
                return (
                    _base.DOCTYPE,
                    node.text,
                    node.get("publicId"),
                    node.get("systemId"),
                )

            elif node.tag == ElementTreeCommentType:
                return _base.COMMENT, node.text

            else:
                assert isinstance(node.tag, string_types), type(node.tag)
                # This is assumed to be an ordinary element
                match = tag_regexp.match(node.tag)
                if match:
                    namespace, tag = match.groups()
                else:
                    namespace = None
                    tag = node.tag
                attrs = OrderedDict()
                for name, value in list(node.attrib.items()):
                    match = tag_regexp.match(name)
                    if match:
                        attrs[(match.group(1), match.group(2))] = value
                    else:
                        attrs[(None, name)] = value
                return (_base.ELEMENT, namespace, tag, attrs, len(node) or node.text)

        def getFirstChild(self, node):
            """
            Perform the getFirstChild utility operation under explicit compatibility rules.

            Example:
                Exercise getETreeBuilder.TreeWalker.getFirstChild through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param node: Value supplied for node under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if isinstance(node, tuple):
                element, key, parents, flag = node
            else:
                element, key, parents, flag = node, None, [], None

            if flag in ("text", "tail"):
                return None
            else:
                if element.text:
                    return element, key, parents, "text"
                elif len(element):
                    parents.append(element)
                    return element[0], 0, parents, None
                else:
                    return None

        def getNextSibling(self, node):
            """
            Perform the getNextSibling utility operation under explicit compatibility rules.

            Example:
                Exercise getETreeBuilder.TreeWalker.getNextSibling through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param node: Value supplied for node under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if isinstance(node, tuple):
                element, key, parents, flag = node
            else:
                return None

            if flag == "text":
                if len(element):
                    parents.append(element)
                    return element[0], 0, parents, None
                else:
                    return None
            else:
                if element.tail and flag != "tail":
                    return element, key, parents, "tail"
                elif key < len(parents[-1]) - 1:
                    return parents[-1][key + 1], key + 1, parents, None
                else:
                    return None

        def getParentNode(self, node):
            """
            Perform the getParentNode utility operation under explicit compatibility rules.

            Example:
                Exercise getETreeBuilder.TreeWalker.getParentNode through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param node: Value supplied for node under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if isinstance(node, tuple):
                element, key, parents, flag = node
            else:
                return None

            if flag == "text":
                if not parents:
                    return element
                else:
                    return element, key, parents, None
            else:
                parent = parents.pop()
                if not parents:
                    return parent
                else:
                    return parent, list(parents[-1]).index(parent), parents, None

    return locals()


getETreeModule = moduleFactoryFactory(getETreeBuilder)

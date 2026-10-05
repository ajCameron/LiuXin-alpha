"""
Provide base utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise  base through a consuming regression::

        python -m pytest -q tests/file_formats/html/test_html_modernized.py
"""
from __future__ import absolute_import, division, unicode_literals

try:
    text_type = unicode
    string_types = (basestring,)
except NameError:
    text_type = str
    string_types = (str,)

__all__ = [
    "DOCUMENT",
    "DOCTYPE",
    "TEXT",
    "ELEMENT",
    "COMMENT",
    "ENTITY",
    "UNKNOWN",
    "TreeWalker",
    "NonRecursiveTreeWalker",
]

from xml.dom import Node

DOCUMENT = Node.DOCUMENT_NODE
DOCTYPE = Node.DOCUMENT_TYPE_NODE
TEXT = Node.TEXT_NODE
ELEMENT = Node.ELEMENT_NODE
COMMENT = Node.COMMENT_NODE
ENTITY = Node.ENTITY_NODE
UNKNOWN = "<#UNKNOWN#>"

from ..constants import voidElements, spaceCharacters

spaceCharacters = "".join(spaceCharacters)


def to_text(s, blank_if_none=True):
    """
    Wrapper around six.text_type to convert None to empty string

    Example:
        Exercise to text through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param s: Value supplied for s under the utility contract.
    :param blank_if_none: Value supplied for blank if none under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if s is None:
        if blank_if_none:
            return ""
        else:
            return None
    elif isinstance(s, text_type):
        return s
    else:
        return text_type(s)


def is_text_or_none(string):
    """
    Wrapper around isinstance(string_types) or is None

    Example:
        Exercise is text or none through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param string: Value supplied for string under the utility contract.
    :return: True when the documented condition holds; otherwise False.
    """
    return string is None or isinstance(string, string_types)


class TreeWalker(object):
    """
    Provide the TreeWalker utility contract with explicit state and cleanup behavior.

    Example:
        Exercise TreeWalker through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """
    def __init__(self, tree):
        """
        Initialize and validate the TreeWalker state.

        Example:
            Exercise TreeWalker.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param tree: Value supplied for tree under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.tree = tree

    def __iter__(self):
        """
        Expose iter behavior for the compatibility container.

        Example:
            Exercise TreeWalker.  iter   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def error(self, msg):
        """
        Perform the error utility operation under explicit compatibility rules.

        Example:
            Exercise TreeWalker.error through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param msg: Value supplied for msg under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return {"type": "SerializeError", "data": msg}

    def emptyTag(self, namespace, name, attrs, hasChildren=False):
        """
        Perform the emptyTag utility operation under explicit compatibility rules.

        Example:
            Exercise TreeWalker.emptyTag through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param namespace: Value supplied for namespace under the utility contract.
        :param name: Field, file, function or resource name addressed by the operation.
        :param attrs: Value supplied for attrs under the utility contract.
        :param hasChildren: Value supplied for hasChildren under the utility contract.
        :return: An iterator yielding the normalized values described above.
        """
        assert namespace is None or isinstance(namespace, string_types), type(namespace)
        assert isinstance(name, string_types), type(name)
        assert all(
            (namespace is None or isinstance(namespace, string_types))
            and isinstance(name, string_types)
            and isinstance(value, string_types)
            for (namespace, name), value in attrs.items()
        )

        yield {
            "type": "EmptyTag",
            "name": to_text(name, False),
            "namespace": to_text(namespace),
            "data": attrs,
        }
        if hasChildren:
            yield self.error("Void element has children")

    def startTag(self, namespace, name, attrs):
        """
        Perform the startTag utility operation under explicit compatibility rules.

        Example:
            Exercise TreeWalker.startTag through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param namespace: Value supplied for namespace under the utility contract.
        :param name: Field, file, function or resource name addressed by the operation.
        :param attrs: Value supplied for attrs under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        assert namespace is None or isinstance(namespace, string_types), type(namespace)
        assert isinstance(name, string_types), type(name)
        assert all(
            (namespace is None or isinstance(namespace, string_types))
            and isinstance(name, string_types)
            and isinstance(value, string_types)
            for (namespace, name), value in attrs.items()
        )

        return {
            "type": "StartTag",
            "name": text_type(name),
            "namespace": to_text(namespace),
            "data": dict(
                ((to_text(namespace, False), to_text(name)), to_text(value, False))
                for (namespace, name), value in attrs.items()
            ),
        }

    def endTag(self, namespace, name):
        """
        Perform the endTag utility operation under explicit compatibility rules.

        Example:
            Exercise TreeWalker.endTag through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param namespace: Value supplied for namespace under the utility contract.
        :param name: Field, file, function or resource name addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        assert namespace is None or isinstance(namespace, string_types), type(namespace)
        assert isinstance(name, string_types), type(namespace)

        return {
            "type": "EndTag",
            "name": to_text(name, False),
            "namespace": to_text(namespace),
            "data": {},
        }

    def text(self, data):
        """
        Perform the text utility operation under explicit compatibility rules.

        Example:
            Exercise TreeWalker.text through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param data: Value supplied for data under the utility contract.
        :return: An iterator yielding the normalized values described above.
        """
        assert isinstance(data, string_types), type(data)

        data = to_text(data)
        middle = data.lstrip(spaceCharacters)
        left = data[: len(data) - len(middle)]
        if left:
            yield {"type": "SpaceCharacters", "data": left}
        data = middle
        middle = data.rstrip(spaceCharacters)
        right = data[len(middle) :]
        if middle:
            yield {"type": "Characters", "data": middle}
        if right:
            yield {"type": "SpaceCharacters", "data": right}

    def comment(self, data):
        """
        Perform the comment utility operation under explicit compatibility rules.

        Example:
            Exercise TreeWalker.comment through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param data: Value supplied for data under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        assert isinstance(data, string_types), type(data)

        return {"type": "Comment", "data": text_type(data)}

    def doctype(self, name, publicId=None, systemId=None, correct=True):
        """
        Perform the doctype utility operation under explicit compatibility rules.

        Example:
            Exercise TreeWalker.doctype through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param publicId: Value supplied for publicId under the utility contract.
        :param systemId: Value supplied for systemId under the utility contract.
        :param correct: Value supplied for correct under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        assert is_text_or_none(name), type(name)
        assert is_text_or_none(publicId), type(publicId)
        assert is_text_or_none(systemId), type(systemId)

        return {
            "type": "Doctype",
            "name": to_text(name),
            "publicId": to_text(publicId),
            "systemId": to_text(systemId),
            "correct": to_text(correct),
        }

    def entity(self, name):
        """
        Perform the entity utility operation under explicit compatibility rules.

        Example:
            Exercise TreeWalker.entity through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        assert isinstance(name, string_types), type(name)

        return {"type": "Entity", "name": text_type(name)}

    def unknown(self, nodeType):
        """
        Perform the unknown utility operation under explicit compatibility rules.

        Example:
            Exercise TreeWalker.unknown through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param nodeType: Value supplied for nodeType under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.error("Unknown node type: " + nodeType)


class NonRecursiveTreeWalker(TreeWalker):
    """
    Provide the NonRecursiveTreeWalker utility contract with explicit state and cleanup behavior.

    Example:
        Exercise NonRecursiveTreeWalker through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """
    def getNodeDetails(self, node):
        """
        Perform the getNodeDetails utility operation under explicit compatibility rules.

        Example:
            Exercise NonRecursiveTreeWalker.getNodeDetails through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param node: Value supplied for node under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def getFirstChild(self, node):
        """
        Perform the getFirstChild utility operation under explicit compatibility rules.

        Example:
            Exercise NonRecursiveTreeWalker.getFirstChild through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param node: Value supplied for node under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def getNextSibling(self, node):
        """
        Perform the getNextSibling utility operation under explicit compatibility rules.

        Example:
            Exercise NonRecursiveTreeWalker.getNextSibling through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param node: Value supplied for node under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def getParentNode(self, node):
        """
        Perform the getParentNode utility operation under explicit compatibility rules.

        Example:
            Exercise NonRecursiveTreeWalker.getParentNode through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param node: Value supplied for node under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def __iter__(self):
        """
        Expose iter behavior for the compatibility container.

        Example:
            Exercise NonRecursiveTreeWalker.  iter   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: An iterator yielding the normalized values described above.
        """
        currentNode = self.tree
        while currentNode is not None:
            details = self.getNodeDetails(currentNode)
            type, details = details[0], details[1:]
            hasChildren = False

            if type == DOCTYPE:
                yield self.doctype(*details)

            elif type == TEXT:
                for token in self.text(*details):
                    yield token

            elif type == ELEMENT:
                namespace, name, attributes, hasChildren = details
                if name in voidElements:
                    for token in self.emptyTag(namespace, name, attributes, hasChildren):
                        yield token
                    hasChildren = False
                else:
                    yield self.startTag(namespace, name, attributes)

            elif type == COMMENT:
                yield self.comment(details[0])

            elif type == ENTITY:
                yield self.entity(details[0])

            elif type == DOCUMENT:
                hasChildren = True

            else:
                yield self.unknown(details[0])

            if hasChildren:
                firstChild = self.getFirstChild(currentNode)
            else:
                firstChild = None

            if firstChild is not None:
                currentNode = firstChild
            else:
                while currentNode is not None:
                    details = self.getNodeDetails(currentNode)
                    type, details = details[0], details[1:]
                    if type == ELEMENT:
                        namespace, name, attributes, hasChildren = details
                        if name not in voidElements:
                            yield self.endTag(namespace, name)
                    if self.tree is currentNode:
                        currentNode = None
                        break
                    nextSibling = self.getNextSibling(currentNode)
                    if nextSibling is not None:
                        currentNode = nextSibling
                        break
                    else:
                        currentNode = self.getParentNode(currentNode)

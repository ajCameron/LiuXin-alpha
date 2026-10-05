"""
Walk PullDOM nodes through normalized HTML5 token events.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise pulldom through a consuming regression::

        python -m pytest -q tests/file_formats/html/test_html_modernized.py
"""
from __future__ import absolute_import, division, unicode_literals

from xml.dom.pulldom import (
    START_ELEMENT,
    END_ELEMENT,
    COMMENT,
    IGNORABLE_WHITESPACE,
    CHARACTERS,
)

from LiuXin_alpha.utils.libraries.liuxin_html5lib.treewalkers import _base
from LiuXin_alpha.utils.libraries.liuxin_html5lib.constants import voidElements


class TreeWalker(_base.TreeWalker):
    """
    Provide the TreeWalker utility contract with explicit state and cleanup behavior.

    Example:
        Exercise TreeWalker through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """
    def __iter__(self):
        """
        Expose iter behavior for the compatibility container.

        Example:
            Exercise TreeWalker.  iter   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: An iterator yielding the normalized values described above.
        """
        ignore_until = None
        previous = None
        for event in self.tree:
            if previous is not None and (ignore_until is None or previous[1] is ignore_until):
                if previous[1] is ignore_until:
                    ignore_until = None
                for token in self.tokens(previous, event):
                    yield token
                    if token["type"] == "EmptyTag":
                        ignore_until = previous[1]
            previous = event
        if ignore_until is None or previous[1] is ignore_until:
            for token in self.tokens(previous, None):
                yield token
        elif ignore_until is not None:
            raise ValueError("Illformed DOM event stream: void element without END_ELEMENT")

    def tokens(self, event, next):
        """
        Perform the tokens utility operation under explicit compatibility rules.

        Example:
            Exercise TreeWalker.tokens through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param event: Value supplied for event under the utility contract.
        :param next: Value supplied for next under the utility contract.
        :return: An iterator yielding the normalized values described above.
        """
        type, node = event
        if type == START_ELEMENT:
            name = node.nodeName
            namespace = node.namespaceURI
            attrs = {}
            for attr in list(node.attributes.keys()):
                attr = node.getAttributeNode(attr)
                attrs[(attr.namespaceURI, attr.localName)] = attr.value
            if name in voidElements:
                for token in self.emptyTag(namespace, name, attrs, not next or next[1] is not node):
                    yield token
            else:
                yield self.startTag(namespace, name, attrs)

        elif type == END_ELEMENT:
            name = node.nodeName
            namespace = node.namespaceURI
            if name not in voidElements:
                yield self.endTag(namespace, name)

        elif type == COMMENT:
            yield self.comment(node.nodeValue)

        elif type in (IGNORABLE_WHITESPACE, CHARACTERS):
            for token in self.text(node.nodeValue):
                yield token

        else:
            yield self.unknown(type)

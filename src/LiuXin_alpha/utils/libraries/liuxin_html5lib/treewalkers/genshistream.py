"""
Walk Genshi streams through normalized HTML5 token events.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise genshistream through a consuming regression::

        python -m pytest -q tests/file_formats/html/test_html_modernized.py
"""
from __future__ import absolute_import, division, unicode_literals

from genshi.core import QName
from genshi.core import START, END, XML_NAMESPACE, DOCTYPE, TEXT
from genshi.core import START_NS, END_NS, START_CDATA, END_CDATA, PI, COMMENT

from . import _base

from ..constants import voidElements, namespaces


class TreeWalker(_base.TreeWalker):
    """
    Provide the TreeWalker utility contract with explicit state and cleanup behavior.

    Example:
        Exercise TreeWalker through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """
    def __iter__(self):
        # Buffer the events so we can pass in the following one
        """
        Expose iter behavior for the compatibility container.

        Example:
            Exercise TreeWalker.  iter   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: An iterator yielding the normalized values described above.
        """
        previous = None
        for event in self.tree:
            if previous is not None:
                for token in self.tokens(previous, event):
                    yield token
            previous = event

        # Don't forget the final event!
        if previous is not None:
            for token in self.tokens(previous, None):
                yield token

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
        kind, data, pos = event
        if kind == START:
            tag, attribs = data
            name = tag.localname
            namespace = tag.namespace
            converted_attribs = {}
            for k, v in attribs:
                if isinstance(k, QName):
                    converted_attribs[(k.namespace, k.localname)] = v
                else:
                    converted_attribs[(None, k)] = v

            if namespace == namespaces["html"] and name in voidElements:
                for token in self.emptyTag(
                    namespace,
                    name,
                    converted_attribs,
                    not next or next[0] != END or next[1] != tag,
                ):
                    yield token
            else:
                yield self.startTag(namespace, name, converted_attribs)

        elif kind == END:
            name = data.localname
            namespace = data.namespace
            if name not in voidElements:
                yield self.endTag(namespace, name)

        elif kind == COMMENT:
            yield self.comment(data)

        elif kind == TEXT:
            for token in self.text(data):
                yield token

        elif kind == DOCTYPE:
            yield self.doctype(*data)

        elif kind in (
            XML_NAMESPACE,
            DOCTYPE,
            START_NS,
            END_NS,
            START_CDATA,
            END_CDATA,
            PI,
        ):
            pass

        else:
            yield self.unknown(kind)

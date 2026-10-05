"""
Coordinate Markdown block parsing and processor state.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise blockparser through a consuming regression::

        python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
"""
from __future__ import absolute_import
from __future__ import unicode_literals
from __future__ import annotations

import typing as _typing

from . import util
from . import odict


class State(list):
    """
    Track the current and nested state of the parser.

    Example:
        Exercise State through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def set(self: _typing.Self, state: _typing.Any) -> None:
        """
        Set a new state.

        Example:
            Exercise State.set through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param state: Value supplied for state under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.append(state)

    def reset(self: _typing.Self) -> None:
        """
        Step back one step in nested state.

        Example:
            Exercise State.reset through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.pop()

    def isstate(self: _typing.Self, state: _typing.Any) -> bool:
        """
        Test that top (current) level is of given state.

        Example:
            Exercise State.isstate through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param state: Value supplied for state under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if len(self):
            return self[-1] == state
        else:
            return False


class BlockParser:
    """
    Parse Markdown blocks into an ElementTree object.

    Example:
        Exercise BlockParser through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def __init__(self: _typing.Self, markdown: _typing.Any) -> None:
        """
        Initialize and validate the blockparser state.

        Example:
            Exercise BlockParser.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param markdown: Value supplied for markdown under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.blockprocessors = odict.OrderedDict()
        self.state = State()
        self.markdown = markdown

    def parseDocument(self: _typing.Self, lines: _typing.Any) -> _typing.Any:
        """
        Parse a markdown document into an ElementTree.

        Example:
            Exercise BlockParser.parseDocument through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param lines: Value supplied for lines under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        # Create a ElementTree from the lines
        self.root = util.etree.Element(self.markdown.doc_tag)
        self.parseChunk(self.root, "\n".join(lines))
        return util.etree.ElementTree(self.root)

    def parseChunk(self: _typing.Self, parent: _typing.Any, text: _typing.Any) -> None:
        """
        Parse a chunk of markdown text and attach to given etree node.

        Example:
            Exercise BlockParser.parseChunk through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param parent: Value supplied for parent under the utility contract.
        :param text: Text parsed, normalized or rendered.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.parseBlocks(parent, text.split("\n\n"))

    def parseBlocks(self: _typing.Self, parent: _typing.Any, blocks: _typing.Any) -> None:
        """
        Process blocks of markdown text and attach to given etree node.

        Example:
            Exercise BlockParser.parseBlocks through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param parent: Value supplied for parent under the utility contract.
        :param blocks: Value supplied for blocks under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        while blocks:
            for processor in self.blockprocessors.values():
                if processor.test(parent, blocks[0]):
                    if processor.run(parent, blocks) is not False:
                        # run returns True or None
                        break

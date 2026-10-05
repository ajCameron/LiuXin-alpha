"""
Implement Markdown definition-list block parsing.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise def list through a consuming regression::

        python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
"""
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import annotations

import typing as _typing

"""
Definition List Extension for Python-Markdown
=============================================

Added parsing of Definition Lists to Python-Markdown.

A simple example:

    Apple
    :   Pomaceous fruit of plants of the genus Malus in 
        the family Rosaceae.
    :   An american computer company.

    Orange
    :   The fruit of an evergreen tree of the genus Citrus.

Copyright 2008 - [Waylan Limberg](http://achinghead.com)

"""

from . import Extension
from ..blockprocessors import BlockProcessor, ListIndentProcessor
from ..util import etree

import re


class DefListProcessor(BlockProcessor):
    """
    Process Definition Lists.

    Example:
        Exercise DefListProcessor through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    RE = re.compile(r"(^|\n)[ ]{0,3}:[ ]{1,3}(.*?)(\n|$)")
    NO_INDENT_RE = re.compile(r"^[ ]{0,3}[^ :]")

    def test(self: _typing.Self, parent: _typing.Any, block: _typing.Any) -> _typing.Any:
        """
        Perform the test operation under explicit file-format and conversion rules.

        Example:
            Exercise DefListProcessor.test through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param parent: Value supplied for parent under the utility contract.
        :param block: Value supplied for block under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return bool(self.RE.search(block))

    def run(self: _typing.Self, parent: _typing.Any, blocks: _typing.Any) -> bool:

        """
        Execute the configured conversion stage and return its primary result.

        Example:
            Exercise DefListProcessor.run through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param parent: Value supplied for parent under the utility contract.
        :param blocks: Value supplied for blocks under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        raw_block = blocks.pop(0)
        m = self.RE.search(raw_block)
        terms = [l.strip() for l in raw_block[: m.start()].split("\n") if l.strip()]
        block = raw_block[m.end() :]
        no_indent = self.NO_INDENT_RE.match(block)
        if no_indent:
            d, theRest = (block, None)
        else:
            d, theRest = self.detab(block)
        if d:
            d = "%s\n%s" % (m.group(2), d)
        else:
            d = m.group(2)
        sibling = self.lastChild(parent)
        if not terms and sibling is None:
            # This is not a definition item. Most likely a paragraph that
            # starts with a colon at the begining of a document or list.
            blocks.insert(0, raw_block)
            return False
        if not terms and sibling.tag == "p":
            # The previous paragraph contains the terms
            state = "looselist"
            terms = sibling.text.split("\n")
            parent.remove(sibling)
            # Aquire new sibling
            sibling = self.lastChild(parent)
        else:
            state = "list"

        if sibling and sibling.tag == "dl":
            # This is another item on an existing list
            dl = sibling
            if len(dl) and dl[-1].tag == "dd" and len(dl[-1]):
                state = "looselist"
        else:
            # This is a new list
            dl = etree.SubElement(parent, "dl")
        # Add terms
        for term in terms:
            dt = etree.SubElement(dl, "dt")
            dt.text = term
        # Add definition
        self.parser.state.set(state)
        dd = etree.SubElement(dl, "dd")
        self.parser.parseBlocks(dd, [d])
        self.parser.state.reset()

        if theRest:
            blocks.insert(0, theRest)


class DefListIndentProcessor(ListIndentProcessor):
    """
    Process indented children of definition list items.

    Example:
        Exercise DefListIndentProcessor through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    ITEM_TYPES = ["dd"]
    LIST_TYPES = ["dl"]

    def create_item(self: _typing.Self, parent: _typing.Any, block: _typing.Any) -> None:
        """
        Create a new dd and parse the block with it as the parent.

        Example:
            Exercise DefListIndentProcessor.create item through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param parent: Value supplied for parent under the utility contract.
        :param block: Value supplied for block under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        dd = etree.SubElement(parent, "dd")
        self.parser.parseBlocks(dd, [block])


class DefListExtension(Extension):
    """
    Add definition lists to Markdown.

    Example:
        Exercise DefListExtension through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def extendMarkdown(self: _typing.Self, md: _typing.Any, md_globals: _typing.Any) -> None:
        """
        Add an instance of DefListProcessor to BlockParser.

        Example:
            Exercise DefListExtension.extendMarkdown through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param md: Value supplied for md under the utility contract.
        :param md_globals: Value supplied for md globals under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        md.parser.blockprocessors.add("defindent", DefListIndentProcessor(md.parser), ">indent")
        md.parser.blockprocessors.add("deflist", DefListProcessor(md.parser), ">ulist")


def makeExtension(configs: _typing.Any = None) -> _typing.Any:
    """
    Perform the makeExtension operation under explicit file-format and conversion rules.

    Example:
        Exercise makeExtension through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


    :param configs: Value supplied for configs under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return DefListExtension(configs=configs)

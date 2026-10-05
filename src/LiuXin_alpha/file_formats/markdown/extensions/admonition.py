"""
Implement titled Markdown admonition blocks.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise admonition through a consuming regression::

        python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
"""
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import annotations

import typing as _typing

"""
Admonition extension for Python-Markdown
========================================

Adds rST-style admonitions. Inspired by [rST][] feature with the same name.

The syntax is (followed by an indented block with the contents):
    !!! [type] [optional explicit title]

Where `type` is used as a CSS class name of the div. If not present, `title`
defaults to the capitalized `type`, so "note" -> "Note".

rST suggests the following `types`, but you're free to use whatever you want:
    attention, caution, danger, error, hint, important, note, tip, warning


A simple example:
    !!! note
        This is the first line inside the box.

Outputs:
    <div class="admonition note">
    <p class="admonition-title">Note</p>
    <p>This is the first line inside the box</p>
    </div>

You can also specify the title and CSS class of the admonition:
    !!! custom "Did you know?"
        Another line here.

Outputs:
    <div class="admonition custom">
    <p class="admonition-title">Did you know?</p>
    <p>Another line here.</p>
    </div>

[rST]: http://docutils.sourceforge.net/docs/ref/rst/directives.html#specific-admonitions

By [Tiago Serafim](http://www.tiagoserafim.com/).

"""

from . import Extension
from ..blockprocessors import BlockProcessor
from ..util import etree
import re


class AdmonitionExtension(Extension):
    """
    Admonition extension for Python-Markdown.

    Example:
        Exercise AdmonitionExtension through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def extendMarkdown(self: _typing.Self, md: _typing.Any, md_globals: _typing.Any) -> None:
        """
        Add Admonition to Markdown instance.

        Example:
            Exercise AdmonitionExtension.extendMarkdown through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param md: Value supplied for md under the utility contract.
        :param md_globals: Value supplied for md globals under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        md.registerExtension(self)

        md.parser.blockprocessors.add("admonition", AdmonitionProcessor(md.parser), "_begin")


class AdmonitionProcessor(BlockProcessor):

    """
    Provide the admonitionprocessor contract for validated ebook processing.

    Example:
        Exercise AdmonitionProcessor through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """
    CLASSNAME = "admonition"
    CLASSNAME_TITLE = "admonition-title"
    RE = re.compile(r'(?:^|\n)!!!\ ?([\w\-]+)(?:\ "(.*?)")?')

    def test(self: _typing.Self, parent: _typing.Any, block: _typing.Any) -> bool:
        """
        Perform the test operation under explicit file-format and conversion rules.

        Example:
            Exercise AdmonitionProcessor.test through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param parent: Value supplied for parent under the utility contract.
        :param block: Value supplied for block under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        sibling = self.lastChild(parent)
        return self.RE.search(block) or (
            block.startswith(" " * self.tab_length) and sibling and sibling.get("class", "").find(self.CLASSNAME) != -1
        )

    def run(self: _typing.Self, parent: _typing.Any, blocks: _typing.Any) -> None:
        """
        Execute the configured conversion stage and return its primary result.

        Example:
            Exercise AdmonitionProcessor.run through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param parent: Value supplied for parent under the utility contract.
        :param blocks: Value supplied for blocks under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        sibling = self.lastChild(parent)
        block = blocks.pop(0)
        m = self.RE.search(block)

        if m:
            block = block[m.end() + 1 :]  # removes the first line

        block, theRest = self.detab(block)

        if m:
            klass, title = self.get_class_and_title(m)
            div = etree.SubElement(parent, "div")
            div.set("class", "%s %s" % (self.CLASSNAME, klass))
            if title:
                p = etree.SubElement(div, "p")
                p.text = title
                p.set("class", self.CLASSNAME_TITLE)
        else:
            div = sibling

        self.parser.parseChunk(div, block)

        if theRest:
            # This block contained unindented line(s) after the first indented
            # line. Insert these lines as the first block of the master blocks
            # list for future processing.
            blocks.insert(0, theRest)

    def get_class_and_title(self: _typing.Self, match: _typing.Any) -> tuple[_typing.Any, ...]:
        """
        Return class and title under the format's safety and compatibility rules.

        Example:
            Exercise AdmonitionProcessor.get class and title through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param match: Value supplied for match under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        klass, title = match.group(1).lower(), match.group(2)
        if title is None:
            # no title was provided, use the capitalized classname as title
            # e.g.: `!!! note` will render `<p class="admonition-title">Note</p>`
            title = klass.capitalize()
        elif title == "":
            # an explicit blank title should not be rendered
            # e.g.: `!!! warning ""` will *not* render `p` with a title
            title = None
        return klass, title


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
    return AdmonitionExtension(configs=configs)

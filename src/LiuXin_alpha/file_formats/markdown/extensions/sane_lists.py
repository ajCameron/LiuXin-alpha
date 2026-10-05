"""
Apply predictable ordered and unordered Markdown list rules.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise sane lists through a consuming regression::

        python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
"""
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import annotations

import typing as _typing

"""
Sane List Extension for Python-Markdown
=======================================

Modify the behavior of Lists in Python-Markdown t act in a sane manor.

In standard Markdown sytex, the following would constitute a single 
ordered list. However, with this extension, the output would include 
two lists, the first an ordered list and the second and unordered list.

    1. ordered
    2. list

    * unordered
    * list

Copyright 2011 - [Waylan Limberg](http://achinghead.com)

"""

import re

from . import Extension
from ..blockprocessors import OListProcessor, UListProcessor


class SaneOListProcessor(OListProcessor):

    """
    Provide the saneolistprocessor contract for validated ebook processing.

    Example:
        Exercise SaneOListProcessor through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """
    CHILD_RE = re.compile(r"^[ ]{0,3}((\d+\.))[ ]+(.*)")
    SIBLING_TAGS = ["ol"]


class SaneUListProcessor(UListProcessor):

    """
    Provide the saneulistprocessor contract for validated ebook processing.

    Example:
        Exercise SaneUListProcessor through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """
    CHILD_RE = re.compile(r"^[ ]{0,3}(([*+-]))[ ]+(.*)")
    SIBLING_TAGS = ["ul"]


class SaneListExtension(Extension):
    """
    Add sane lists to Markdown.

    Example:
        Exercise SaneListExtension through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def extendMarkdown(self: _typing.Self, md: _typing.Any, md_globals: _typing.Any) -> None:
        """
        Override existing Processors.

        Example:
            Exercise SaneListExtension.extendMarkdown through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param md: Value supplied for md under the utility contract.
        :param md_globals: Value supplied for md globals under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        md.parser.blockprocessors["olist"] = SaneOListProcessor(md.parser)
        md.parser.blockprocessors["ulist"] = SaneUListProcessor(md.parser)


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
    return SaneListExtension(configs=configs)

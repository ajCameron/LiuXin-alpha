"""
Apply unambiguous Markdown strong-emphasis matching.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise smart strong through a consuming regression::

        python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
"""
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import annotations

import typing as _typing

"""
Smart_Strong Extension for Python-Markdown
==========================================

This extention adds smarter handling of double underscores within words.

Simple Usage:

    >>> import markdown
    >>> print markdown.markdown('Text with double__underscore__words.',
    ...                   extensions=['smart_strong'])
    <p>Text with double__underscore__words.</p>
    >>> print markdown.markdown('__Strong__ still works.',
    ...                   extensions=['smart_strong'])
    <p><strong>Strong</strong> still works.</p>
    >>> print markdown.markdown('__this__works__too__.',
    ...                   extensions=['smart_strong'])
    <p><strong>this__works__too</strong>.</p>

Copyright 2011
[Waylan Limberg](http://achinghead.com)

"""

from . import Extension
from ..inlinepatterns import SimpleTagPattern

SMART_STRONG_RE = r"(?<!\w)(_{2})(?!_)(.+?)(?<!_)\2(?!\w)"
STRONG_RE = r"(\*{2})(.+?)\2"


class SmartEmphasisExtension(Extension):
    """
    Add smart_emphasis extension to Markdown class.

    Example:
        Exercise SmartEmphasisExtension through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def extendMarkdown(self: _typing.Self, md: _typing.Any, md_globals: _typing.Any) -> None:
        """
        Modify inline patterns.

        Example:
            Exercise SmartEmphasisExtension.extendMarkdown through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param md: Value supplied for md under the utility contract.
        :param md_globals: Value supplied for md globals under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        md.inlinePatterns["strong"] = SimpleTagPattern(STRONG_RE, "strong")
        md.inlinePatterns.add("strong2", SimpleTagPattern(SMART_STRONG_RE, "strong"), ">emphasis2")


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
    return SmartEmphasisExtension(configs=dict(configs or {}))

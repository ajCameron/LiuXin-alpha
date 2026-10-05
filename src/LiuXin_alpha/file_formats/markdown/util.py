"""
Provide Markdown registries, atomic strings, HTML placeholders and helpers.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise util through a consuming regression::

        python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
"""
from __future__ import unicode_literals
from __future__ import annotations

import typing as _typing

# -*- coding: utf-8 -*-

import re
import sys


"""
Python 3 Stuff
=============================================================================
"""
PY3 = sys.version_info[0] == 3

if PY3:
    string_type = str
    text_type = str
    int2str = chr
else:
    string_type = basestring
    text_type = unicode
    int2str = unichr


"""
Constants you might want to modify
-----------------------------------------------------------------------------
"""

BLOCK_LEVEL_ELEMENTS = re.compile(
    "^(p|div|h[1-6]|blockquote|pre|table|dl|ol|ul"
    "|script|noscript|form|fieldset|iframe|math"
    "|hr|hr/|style|li|dt|dd|thead|tbody"
    "|tr|th|td|section|footer|header|group|figure"
    "|figcaption|aside|article|canvas|output"
    "|progress|video)$",
    re.IGNORECASE,
)
# Placeholders
STX = "\u0002"  # Use STX ("Start of text") for start-of-placeholder
ETX = "\u0003"  # Use ETX ("End of text") for end-of-placeholder
INLINE_PLACEHOLDER_PREFIX = STX + "klzzwxh:"
INLINE_PLACEHOLDER = INLINE_PLACEHOLDER_PREFIX + "%s" + ETX
INLINE_PLACEHOLDER_RE = re.compile(INLINE_PLACEHOLDER % r"([0-9]+)")
AMP_SUBSTITUTE = STX + "amp" + ETX

"""
Constants you probably do not need to change
-----------------------------------------------------------------------------
"""

RTL_BIDI_RANGES = (
    ("\u0590", "\u07FF"),
    # Hebrew (0590-05FF), Arabic (0600-06FF),
    # Syriac (0700-074F), Arabic supplement (0750-077F),
    # Thaana (0780-07BF), Nko (07C0-07FF).
    ("\u2D30", "\u2D7F"),  # Tifinagh
)

# Extensions should use "markdown.util.etree" instead of "etree" (or do `from
# markdown.util import etree`).  Do not import it by yourself.

try:  # Is the C implemenation of ElementTree available?
    import xml.etree.cElementTree as etree
    from xml.etree.ElementTree import Comment

    # Serializers (including ours) test with non-c Comment
    etree.test_comment = Comment
    if etree.VERSION < "1.0.5":
        raise RuntimeError("cElementTree version 1.0.5 or higher is required.")
except (ImportError, RuntimeError):
    # Use the Python implementation of ElementTree?
    import xml.etree.ElementTree as etree

    if etree.VERSION < "1.1":
        raise RuntimeError("ElementTree version 1.1 or higher is required")


"""
AUXILIARY GLOBAL FUNCTIONS
=============================================================================
"""


def isBlockLevel(tag: _typing.Any) -> _typing.Any:
    """
    Check if the tag is a block level HTML tag.

    Example:
        Exercise isBlockLevel through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


    :param tag: Value supplied for tag under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(tag, string_type):
        return BLOCK_LEVEL_ELEMENTS.match(tag)
    # Some ElementTree tags are not strings, so return False.
    return False


"""
MISC AUXILIARY CLASSES
=============================================================================
"""


class AtomicString(text_type):
    """
    A string which should not be further processed.

    Example:
        Exercise AtomicString through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    pass


class Processor(object):
    """
    Provide the processor contract for validated ebook processing.

    Example:
        Exercise Processor through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """
    def __init__(self: _typing.Self, markdown_instance: _typing.Any = None) -> None:
        """
        Initialize and validate the processor state.

        Example:
            Exercise Processor.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param markdown_instance: Value supplied for markdown instance under the utility
            contract.
        :return: None; validated state is stored on the receiving object.
        """
        if markdown_instance:
            self.markdown = markdown_instance


class HtmlStash(object):
    """
    This class is used for stashing HTML objects that we extract in the beginning and replace with place-holders.

    Example:
        Exercise HtmlStash through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def __init__(self: _typing.Self) -> None:
        """
        Create a HtmlStash.

        Example:
            Exercise HtmlStash.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :return: None; validated state is stored on the receiving object.
        """
        self.html_counter = 0  # for counting inline html segments
        self.rawHtmlBlocks = []

    def store(self: _typing.Self, html: _typing.Any, safe: bool = False) -> _typing.Any:
        """
        Saves an HTML segment for later reinsertion. Returns a placeholder string that needs to be inserted into the document.

        Example:
            Exercise HtmlStash.store through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param html: Value supplied for html under the utility contract.
        :param safe: Value supplied for safe under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.rawHtmlBlocks.append((html, safe))
        placeholder = self.get_placeholder(self.html_counter)
        self.html_counter += 1
        return placeholder

    def reset(self: _typing.Self) -> None:
        """
        Perform the reset operation under explicit file-format and conversion rules.

        Example:
            Exercise HtmlStash.reset through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.html_counter = 0
        self.rawHtmlBlocks = []

    def get_placeholder(self: _typing.Self, key: _typing.Any) -> _typing.Any:
        """
        Return placeholder under the format's safety and compatibility rules.

        Example:
            Exercise HtmlStash.get placeholder through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param key: Metadata, identifier or local-variable key.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "%swzxhzdk:%d%s" % (STX, key, ETX)

"""
Parse document metadata from Markdown source headers.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise meta through a consuming regression::

        python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
"""
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import annotations

import typing as _typing

"""
Meta Data Extension for Python-Markdown
=======================================

This extension adds Meta Data handling to markdown.

Basic Usage:

    >>> import markdown
    >>> text = '''Title: A Test Doc.
    ... Author: Waylan Limberg
    ...         John Doe
    ... Blank_Data:
    ...
    ... The body. This is paragraph one.
    ... '''
    >>> md = markdown.Markdown(['meta'])
    >>> print md.convert(text)
    <p>The body. This is paragraph one.</p>
    >>> print md.Meta
    {u'blank_data': [u''], u'author': [u'Waylan Limberg', u'John Doe'], u'title': [u'A Test Doc.']}

Make sure text without Meta Data still works (markdown < 1.6b returns a <p>).

    >>> text = '    Some Code - not extra lines of meta data.'
    >>> md = markdown.Markdown(['meta'])
    >>> print md.convert(text)
    <pre><code>Some Code - not extra lines of meta data.
    </code></pre>
    >>> md.Meta
    {}

Copyright 2007-2008 [Waylan Limberg](http://achinghead.com).

Project website: <http://packages.python.org/Markdown/meta_data.html>
Contact: markdown@freewisdom.org

License: BSD (see ../LICENSE.md for details)

"""

from . import Extension
from ..preprocessors import Preprocessor

import re

# Global Vars
META_RE = re.compile(r"^[ ]{0,3}(?P<key>[A-Za-z0-9_-]+):\s*(?P<value>.*)")
META_MORE_RE = re.compile(r"^[ ]{4,}(?P<value>.*)")


class MetaExtension(Extension):
    """
    Meta-Data extension for Python-Markdown.

    Example:
        Exercise MetaExtension through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def extendMarkdown(self: _typing.Self, md: _typing.Any, md_globals: _typing.Any) -> None:
        """
        Add MetaPreprocessor to Markdown instance.

        Example:
            Exercise MetaExtension.extendMarkdown through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param md: Value supplied for md under the utility contract.
        :param md_globals: Value supplied for md globals under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

        md.preprocessors.add("meta", MetaPreprocessor(md), "_begin")


class MetaPreprocessor(Preprocessor):
    """
    Get Meta-Data.

    Example:
        Exercise MetaPreprocessor through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def run(self: _typing.Self, lines: _typing.Any) -> _typing.Any:
        """
        Parse Meta-Data and store in Markdown.Meta.

        Example:
            Exercise MetaPreprocessor.run through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param lines: Value supplied for lines under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        meta = {}
        key = None
        while 1:
            line = lines.pop(0)
            if line.strip() == "":
                break  # blank line - done
            m1 = META_RE.match(line)
            if m1:
                key = m1.group("key").lower().strip()
                value = m1.group("value").strip()
                try:
                    meta[key].append(value)
                except KeyError:
                    meta[key] = [value]
            else:
                m2 = META_MORE_RE.match(line)
                if m2 and key:
                    # Add another line to existing key
                    meta[key].append(m2.group("value").strip())
                else:
                    lines.insert(0, line)
                    break  # no meta data - done
        self.markdown.Meta = meta
        return lines


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
    return MetaExtension(configs=configs)

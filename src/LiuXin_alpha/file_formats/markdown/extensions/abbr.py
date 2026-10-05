"""
Implement Markdown abbreviation definitions and inline expansion.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise abbr through a consuming regression::

        python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
"""
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import annotations

import typing as _typing

'''
Abbreviation Extension for Python-Markdown
==========================================

This extension adds abbreviation handling to Python-Markdown.

Simple Usage:

    >>> import markdown
    >>> text = """
    ... Some text with an ABBR and a REF. Ignore REFERENCE and ref.
    ...
    ... *[ABBR]: Abbreviation
    ... *[REF]: Abbreviation Reference
    ... """
    >>> print markdown.markdown(text, ['abbr'])
    <p>Some text with an <abbr title="Abbreviation">ABBR</abbr> and a <abbr title="Abbreviation Reference">REF</abbr>. Ignore REFERENCE and ref.</p>

Copyright 2007-2008
* [Waylan Limberg](http://achinghead.com/)
* [Seemant Kulleen](http://www.kulleen.org/)
	

'''

from . import Extension
from ..preprocessors import Preprocessor
from ..inlinepatterns import Pattern
from ..util import etree
import re

# Global Vars
ABBR_REF_RE = re.compile(r"[*]\[(?P<abbr>[^\]]*)\][ ]?:\s*(?P<title>.*)")


class AbbrExtension(Extension):
    """
    Abbreviation Extension for Python-Markdown.

    Example:
        Exercise AbbrExtension through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def extendMarkdown(self: _typing.Self, md: _typing.Any, md_globals: _typing.Any) -> None:
        """
        Insert AbbrPreprocessor before ReferencePreprocessor.

        Example:
            Exercise AbbrExtension.extendMarkdown through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param md: Value supplied for md under the utility contract.
        :param md_globals: Value supplied for md globals under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        md.preprocessors.add("abbr", AbbrPreprocessor(md), "<reference")


class AbbrPreprocessor(Preprocessor):
    """
    Abbreviation Preprocessor - parse text for abbr references.

    Example:
        Exercise AbbrPreprocessor through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def run(self: _typing.Self, lines: _typing.Any) -> _typing.Any:
        """
        Find and remove all Abbreviation references from the text. Each reference is set as a new AbbrPattern in the markdown instance.

        Example:
            Exercise AbbrPreprocessor.run through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param lines: Value supplied for lines under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        new_text = []
        for line in lines:
            m = ABBR_REF_RE.match(line)
            if m:
                abbr = m.group("abbr").strip()
                title = m.group("title").strip()
                self.markdown.inlinePatterns["abbr-%s" % abbr] = AbbrPattern(self._generate_pattern(abbr), title)
            else:
                new_text.append(line)
        return new_text

    def _generate_pattern(self: _typing.Self, text: _typing.Any) -> _typing.Any:
        """
        Given a string, returns an regex pattern to match that string.

        Example:
            Exercise AbbrPreprocessor. generate pattern through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param text: Text parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        chars = list(text)
        for i in range(len(chars)):
            chars[i] = r"[%s]" % chars[i]
        return r"(?P<abbr>\b%s\b)" % (r"".join(chars))


class AbbrPattern(Pattern):
    """
    Abbreviation inline pattern.

    Example:
        Exercise AbbrPattern through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def __init__(self: _typing.Self, pattern: _typing.Any, title: _typing.Any) -> None:
        """
        Initialize and validate the abbrpattern state.

        Example:
            Exercise AbbrPattern.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param pattern: Value supplied for pattern under the utility contract.
        :param title: Value supplied for title under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        super(AbbrPattern, self).__init__(pattern)
        self.title = title

    def handleMatch(self: _typing.Self, m: _typing.Any) -> _typing.Any:
        """
        Perform the handleMatch operation under explicit file-format and conversion rules.

        Example:
            Exercise AbbrPattern.handleMatch through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param m: Value supplied for m under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        abbr = etree.Element("abbr")
        abbr.text = m.group("abbr")
        abbr.set("title", self.title)
        return abbr


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
    return AbbrExtension(configs=configs)

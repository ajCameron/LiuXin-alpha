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
    """Abbreviation Extension for Python-Markdown."""

    def extendMarkdown(self: _typing.Self, md: _typing.Any, md_globals: _typing.Any) -> None:
        """Insert AbbrPreprocessor before ReferencePreprocessor."""
        md.preprocessors.add("abbr", AbbrPreprocessor(md), "<reference")


class AbbrPreprocessor(Preprocessor):
    """Abbreviation Preprocessor - parse text for abbr references."""

    def run(self: _typing.Self, lines: _typing.Any) -> _typing.Any:
        """
        Find and remove all Abbreviation references from the text.
        Each reference is set as a new AbbrPattern in the markdown instance.

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

        'HTML' -> r'(?P<abbr>[H][T][M][L])'

        Note: we force each char as a literal match (in brackets) as we don't
        know what they will be beforehand.

        """
        chars = list(text)
        for i in range(len(chars)):
            chars[i] = r"[%s]" % chars[i]
        return r"(?P<abbr>\b%s\b)" % (r"".join(chars))


class AbbrPattern(Pattern):
    """Abbreviation inline pattern."""

    def __init__(self: _typing.Self, pattern: _typing.Any, title: _typing.Any) -> None:
        super(AbbrPattern, self).__init__(pattern)
        self.title = title

    def handleMatch(self: _typing.Self, m: _typing.Any) -> _typing.Any:
        abbr = etree.Element("abbr")
        abbr.text = m.group("abbr")
        abbr.set("title", self.title)
        return abbr


def makeExtension(configs: _typing.Any = None) -> _typing.Any:
    return AbbrExtension(configs=configs)

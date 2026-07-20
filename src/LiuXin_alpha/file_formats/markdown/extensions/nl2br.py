from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import annotations

import typing as _typing

"""
NL2BR Extension
===============

A Python-Markdown extension to treat newlines as hard breaks; like
GitHub-flavored Markdown does.

Usage:

    >>> import markdown
    >>> print markdown.markdown('line 1\\nline 2', extensions=['nl2br'])
    <p>line 1<br />
    line 2</p>

Copyright 2011 [Brian Neal](http://deathofagremmie.com/)

Dependencies:
* [Python 2.4+](http://python.org)
* [Markdown 2.1+](http://packages.python.org/Markdown/)

"""

from . import Extension
from ..inlinepatterns import SubstituteTagPattern

BR_RE = r"\n"


class Nl2BrExtension(Extension):
    def extendMarkdown(self: _typing.Self, md: _typing.Any, md_globals: _typing.Any) -> None:
        br_tag = SubstituteTagPattern(BR_RE, "br")
        md.inlinePatterns.add("nl", br_tag, "_end")


def makeExtension(configs: _typing.Any = None) -> _typing.Any:
    return Nl2BrExtension(configs)

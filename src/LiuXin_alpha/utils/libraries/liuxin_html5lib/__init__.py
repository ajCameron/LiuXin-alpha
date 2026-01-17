"""
HTML parsing library based on the WHATWG "HTML5"
specification. The parser is designed to be compatible with existing
HTML found in the wild and implements well-defined error recovery that
is largely compatible with modern desktop web browsers.

Example usage:

import html5lib
f = open("my_document.html")
tree = html5lib.parse(f)
"""

from __future__ import absolute_import, division, unicode_literals

from LiuXin_alpha.utils.libraries.liuxin_html5lib.html5parser import HTMLParser, parse, parseFragment
from LiuXin_alpha.utils.libraries.liuxin_html5lib.treebuilders import getTreeBuilder
from LiuXin_alpha.utils.libraries.liuxin_html5lib.treewalkers import getTreeWalker
from LiuXin_alpha.utils.libraries.liuxin_html5lib.serializer import serialize

__all__ = [
    "HTMLParser",
    "parse",
    "parseFragment",
    "getTreeBuilder",
    "getTreeWalker",
    "serialize",
]
__version__ = "0.999999-dev"

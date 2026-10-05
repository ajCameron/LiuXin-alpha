"""
Expose the supported liuxin html5lib compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/html/test_html_modernized.py
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

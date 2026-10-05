"""
Expose the supported trie compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/html/test_html_modernized.py
"""
from __future__ import absolute_import, division, unicode_literals

from LiuXin_alpha.utils.libraries.liuxin_html5lib.trie.py import Trie as PyTrie

Trie = PyTrie

try:
    from .datrie import Trie as DATrie
except ImportError:
    pass
else:
    Trie = DATrie

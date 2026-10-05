"""
Expose the supported cfi compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/epub/test_epub_modernized.py
"""
from __future__ import annotations

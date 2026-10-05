"""
Expose the supported readability compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/readability/test_readability_modernized.py
"""
from __future__ import annotations
from LiuXin_alpha.file_formats.readability.readability import Document, Unparseable

__all__ = ["Document", "Unparseable"]

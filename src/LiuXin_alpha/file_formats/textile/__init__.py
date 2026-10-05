"""
Expose the supported textile compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/textile/test_textile_modernized.py
"""
from __future__ import annotations
from LiuXin_alpha.file_formats.textile.functions import textile, textile_restricted, Textile


__all__ = ["textile", "textile_restricted", "Textile"]

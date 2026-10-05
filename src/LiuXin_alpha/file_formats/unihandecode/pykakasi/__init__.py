"""
Expose the supported pykakasi compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/txt/test_txt_modernized.py
"""
from __future__ import annotations
from LiuXin_alpha.file_formats.unihandecode.pykakasi.kakasi import kakasi

kakasi

__all__ = ["pykakasi"]

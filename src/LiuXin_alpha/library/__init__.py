"""
Expose the supported library compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/library/test_unified_library.py
"""

from __future__ import annotations

from LiuXin_alpha.library.library import Library

__all__ = ["Library"]

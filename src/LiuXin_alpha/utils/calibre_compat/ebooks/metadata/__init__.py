"""
Expose the supported metadata compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
"""

from __future__ import annotations

from LiuXin_alpha.metadata.utils import authors_to_string, string_to_authors
from LiuXin_alpha.utils.calibre_compat.ebooks.metadata.book.base import MetaInformation

__all__ = ["string_to_authors", "authors_to_string", "MetaInformation"]

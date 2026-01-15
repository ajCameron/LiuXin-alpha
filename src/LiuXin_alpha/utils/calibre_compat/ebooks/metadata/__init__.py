"""Tiny helpers that some calibre code expects.

Wherever it makes sense, these functions delegate to existing LiuXin
implementations (LiuXin began life as a calibre fork).
"""

from __future__ import annotations

from LiuXin_alpha.metadata.utils import authors_to_string, string_to_authors
from LiuXin_alpha.utils.calibre_compat.ebooks.metadata.book.base import MetaInformation

__all__ = ["string_to_authors", "authors_to_string", "MetaInformation"]

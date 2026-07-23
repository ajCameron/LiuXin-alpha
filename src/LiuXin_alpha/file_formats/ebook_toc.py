"""Compatibility facade for legacy imports.

Historically TOC support lived in this module. The maintained
implementation is now `LiuXin_alpha.file_formats.toc`.
"""
from __future__ import annotations

from LiuXin_alpha.file_formats.toc import CALIBRE_NS, C, E, NCX_NS, NSMAP, TOC

__all__ = ["TOC", "NCX_NS", "CALIBRE_NS", "NSMAP", "E", "C"]


"""Calibre-compatibility shims for LiuXin.

This package hosts import-layer and lightweight compatibility objects so that
third-party calibre plugins can run inside LiuXin without the calibre runtime.

Entry point: :func:`install_calibre_shims`.
"""

from __future__ import annotations

from .install import install_calibre_shims

__all__ = ["install_calibre_shims"]

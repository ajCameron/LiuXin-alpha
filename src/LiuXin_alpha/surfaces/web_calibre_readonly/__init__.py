"""Calibre-compatible read-only web surface package."""

from __future__ import annotations

from .app import CalibreReadOnlyWebApplication, CalibreReadOnlyWebConfig, build_arg_parser, main

__all__ = [
    "CalibreReadOnlyWebApplication",
    "CalibreReadOnlyWebConfig",
    "build_arg_parser",
    "main",
]

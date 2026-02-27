#!/usr/bin/env python

"""
Compatibility exports for legacy OPF utility imports.

Use `LiuXin_alpha.metadata.utils` directly for new code.
"""

from __future__ import annotations

from LiuXin_alpha.metadata.utils import (
    OPFVersion,
    PARSER,
    create_manifest_item,
    ensure_unique,
    normalize_languages,
    parse_opf,
    parse_opf_version,
    pretty_print_opf,
)

__all__ = [
    "OPFVersion",
    "PARSER",
    "create_manifest_item",
    "ensure_unique",
    "normalize_languages",
    "parse_opf",
    "parse_opf_version",
    "pretty_print_opf",
]


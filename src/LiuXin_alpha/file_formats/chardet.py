#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""Compatibility shim for Calibre-derived encoding helpers.

This module historically contained a copy of Calibre's encoding utilities.
In LiuXin-alpha we keep the maintained implementation in
``LiuXin_alpha.utils.libraries.calibre_chardet``.

Keep this module (for now) to avoid churn in import paths across the
``file_formats`` stack.
"""

from __future__ import annotations

from LiuXin_alpha.utils.libraries.calibre_chardet import (  # noqa: F401
    ENCODING_PATS,
    ENTITY_PATTERN,
    detect,
    detect_xml_encoding,
    find_declared_encoding,
    force_encoding,
    recode_to_utf8,
    replace_encoding_declarations,
    strip_encoding_declarations,
    substitute_entites,
    substitute_entities,
    unicode,
    xml_to_unicode,
)

__license__ = "GPL v3"
__copyright__ = "2009, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"

__all__ = [
    "ENCODING_PATS",
    "ENTITY_PATTERN",
    "detect",
    "detect_xml_encoding",
    "find_declared_encoding",
    "force_encoding",
    "recode_to_utf8",
    "replace_encoding_declarations",
    "strip_encoding_declarations",
    "substitute_entites",
    "substitute_entities",
    "unicode",
    "xml_to_unicode",
]

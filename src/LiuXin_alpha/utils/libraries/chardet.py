#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""Compatibility shim for Calibre-derived encoding helpers.

Historically Calibre shipped these helpers in multiple places, and older
LiuXin-alpha code ended up with copies. The canonical implementation now lives
in ``LiuXin_alpha.utils.libraries.calibre_chardet``.

This module remains as a stable import target for legacy call-sites.
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

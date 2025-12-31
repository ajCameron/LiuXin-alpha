# -*- coding: utf-8 -*-
"""
Pure-python fallback for the compiled ``freetype`` extension.

The compiled extension is used for fast glyph coverage checks.
Fallback behavior:
- load_font(font_bytes, index=0) -> Face
- Face.supports_text(text) -> bool
- Face.glyph_id(ch) -> int

This fallback does not parse font data; it simply assumes basic coverage.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Face:
    _font_bytes: bytes
    _index: int = 0

    def supports_text(self, text: str) -> bool:
        # Best-effort: assume coverage.
        return True

    def glyph_id(self, ch: str) -> int:
        if not ch:
            return 0
        return ord(ch[0])


def load_font(font_data: bytes, index: int = 0) -> Face:
    if not isinstance(font_data, (bytes, bytearray, memoryview)):
        raise TypeError("load_font expects font bytes")
    return Face(bytes(font_data), int(index))

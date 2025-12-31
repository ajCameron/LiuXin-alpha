# -*- coding: utf-8 -*-
"""
Pure-python fallback for the compiled ``icu`` extension.

This is a compatibility layer. It is *not* a full ICU replacement, but it aims to
provide enough of the API surface for non-GUI / non-locale-critical code paths.

Where ICU-specific behavior is expected (true locale collation, Unicode word
break rules, transliteration), this fallback provides reasonable approximations.
"""

from __future__ import annotations

import builtins
import locale as _locale
import unicodedata
import re
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Tuple


UPPER_CASE = 0
LOWER_CASE = 1


def set_default_encoding(enc: str) -> None:
    # ICU sets internal encodings; Python already uses Unicode.
    return None


def set_filesystem_encoding(enc: str) -> None:
    return None


def change_case(s, which: int = UPPER_CASE, locale: Optional[str] = None):
    if locale is None:
        # Mirror the extension's explicit NotImplementedError for missing locale
        raise NotImplementedError("You must specify a locale")
    txt = s if isinstance(s, str) else str(s)
    if which == UPPER_CASE:
        return txt.upper()
    return txt.lower()


def swap_case(s):
    txt = s if isinstance(s, str) else str(s)
    return txt.swapcase()


def normalize(s, form: str = "NFC"):
    txt = s if isinstance(s, str) else str(s)
    try:
        return unicodedata.normalize(form, txt)
    except Exception:
        return txt


def chr(code: int) -> str:
    return builtins.chr(code)  # type: ignore[name-defined]


def character_name(ch: str) -> str:
    if not ch:
        return ""
    return unicodedata.name(ch[0], "")


def character_name_from_code(code: int) -> str:
    try:
        return unicodedata.name(builtins.chr(code), "")  # type: ignore[name-defined]
    except Exception:
        return ""


def string_length(s) -> int:
    txt = s if isinstance(s, str) else str(s)
    return len(txt)


def utf16_length(s) -> int:
    txt = s if isinstance(s, str) else str(s)
    # length in UTF-16 code units
    return len(txt.encode("utf-16-le")) // 2


def roundtrip(s, enc: str = "utf-8"):
    # Best-effort: encode+decode
    txt = s if isinstance(s, str) else str(s)
    return txt.encode(enc, "replace").decode(enc, "replace")


def get_available_transliterators() -> List[str]:
    return []


def available_locales_for_break_iterator() -> List[str]:
    return []


@dataclass
class Collator:
    locale: str

    def __post_init__(self) -> None:
        try:
            _locale.setlocale(_locale.LC_COLLATE, self.locale)
        except Exception:
            # Ignore invalid locales; continue with default collation
            pass

    def sort_key(self, s) -> bytes:
        txt = s if isinstance(s, str) else str(s)
        try:
            k = _locale.strxfrm(txt)
        except Exception:
            k = txt
        return k.encode("utf-8", "surrogatepass")

    def strcmp(self, a, b) -> int:
        ka = self.sort_key(a)
        kb = self.sort_key(b)
        return -1 if ka < kb else 1 if ka > kb else 0

    def find(self, haystack, needle, start: int = 0) -> int:
        h = haystack if isinstance(haystack, str) else str(haystack)
        n = needle if isinstance(needle, str) else str(needle)
        return h.find(n, start)

    def contains(self, haystack, needle) -> bool:
        return self.find(haystack, needle) != -1

    def contractions(self):
        # True ICU returns contraction expansions; we don't implement it.
        return ()

    def clone(self):
        return Collator(self.locale)

    def startswith(self, s, prefix) -> bool:
        txt = s if isinstance(s, str) else str(s)
        p = prefix if isinstance(prefix, str) else str(prefix)
        return txt.startswith(p)

    def collation_order(self, s) -> List[int]:
        # Compatibility: return a list of integer "orders" from the sort key bytes.
        return list(self.sort_key(s))


# Break iterator type constants are not mirrored here; users can pass any int.
class BreakIterator:
    def __init__(self, break_iterator_type: int, locale: str):
        self.type = int(break_iterator_type)
        self.locale = locale
        self._text = ""

    def set_text(self, s) -> None:
        self._text = s if isinstance(s, str) else str(s)

    def split2(self) -> List[Tuple[int, int]]:
        # Approximation: return spans of "words" (letters/digits/underscore), allowing hyphens inside.
        txt = self._text
        spans: List[Tuple[int, int]] = []
        for m in re.finditer(r"[A-Za-z0-9_]+(?:-[A-Za-z0-9_]+)*", txt):
            spans.append((m.start(), m.end() - m.start()))
        return spans

    def index(self, token) -> int:
        t = token if isinstance(token, str) else str(token)
        if not t:
            return -1
        return self._text.find(t)

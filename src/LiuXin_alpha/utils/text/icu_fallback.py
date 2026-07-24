"""Pure-Python compatibility fallback for the compiled ICU extension.

The implementation intentionally provides approximate, locale-neutral
semantics. Its public surface matches the subset consumed by
``LiuXin_alpha.utils.text.icu`` so source checkouts remain functional when the
compiled extension is unavailable.
"""

from __future__ import annotations

import builtins
import re
import unicodedata
from typing import Any, Literal, cast


UPPER_CASE = 0
LOWER_CASE = 1
TITLE_CASE = 2

UCOL_PRIMARY = 0
UCOL_SECONDARY = 1

UNORM_NFC = "NFC"
UNORM_NFD = "NFD"
UNORM_NFKC = "NFKC"
UNORM_NFKD = "NFKD"
UNORM_NONE = "NFC"
UNORM_DEFAULT = "NFC"
UNORM_FCD = "NFC"

unicode_version = unicodedata.unidata_version


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return value if isinstance(value, str) else str(value)


def _without_accents(value: str) -> str:
    return "".join(
        character
        for character in unicodedata.normalize("NFD", value)
        if unicodedata.category(character) != "Mn"
    )


def set_default_encoding(_encoding: str | bytes) -> None:
    """Retain the extension API; Python strings need no global encoding."""
    return None


def set_filesystem_encoding(_encoding: str | bytes) -> None:
    """Retain the extension API; Python owns the filesystem encoding."""
    return None


def change_case(value: Any, operation: int, _locale: str | None = None) -> str:
    text = _text(value)
    if operation == UPPER_CASE:
        return text.upper()
    if operation == TITLE_CASE:
        return text.title()
    return text.lower()


def swap_case(value: Any) -> str:
    return _text(value).swapcase()


def normalize(mode: str | None, text: Any) -> str:
    form = cast(
        Literal["NFC", "NFD", "NFKC", "NFKD"],
        mode if mode in {"NFC", "NFD", "NFKC", "NFKD"} else "NFC",
    )
    return unicodedata.normalize(form, _text(text))


def chr(character: int) -> str:
    return builtins.chr(int(character))


def character_name(character: Any) -> str:
    text = _text(character)
    return unicodedata.name(text[0], "") if text else ""


def character_name_from_code(character_code: int) -> str:
    try:
        return unicodedata.name(builtins.chr(int(character_code)), "")
    except (TypeError, ValueError):
        return ""


def string_length(value: Any) -> int:
    return len(_text(value))


def utf16_length(value: Any) -> int:
    return len(_text(value).encode("utf-16-le")) // 2


class Collator:
    """Small collation adapter with the compiled extension's call conventions."""

    def __init__(self, locale: str) -> None:
        self.locale = locale
        self.strength = UCOL_SECONDARY
        self.numeric = False
        self.upper_first = False

    def clone(self) -> "Collator":
        clone = type(self)(self.locale)
        clone.strength = self.strength
        clone.numeric = self.numeric
        clone.upper_first = self.upper_first
        return clone

    def _comparison_text(self, value: Any) -> str:
        text = _text(value)
        if self.strength in {UCOL_PRIMARY, UCOL_SECONDARY}:
            text = text.casefold()
        if self.strength == UCOL_PRIMARY:
            text = _without_accents(text)
        if self.numeric:
            text = re.sub(
                r"\d+",
                lambda match: f"{int(match.group()):020d}",
                text,
            )
        return text

    def sort_key(self, value: Any) -> bytes:
        text = self._comparison_text(value)
        if self.upper_first:
            original = _text(value)
            case_key = "".join("0" if char.isupper() else "1" for char in original)
            text = case_key + "\0" + text
        return text.encode("utf-8", "surrogatepass")

    def strcmp(self, first: Any, second: Any) -> int:
        first_key = self.sort_key(first)
        second_key = self.sort_key(second)
        return (first_key > second_key) - (first_key < second_key)

    def find(self, needle: Any, haystack: Any) -> int:
        return self._comparison_text(haystack).find(
            self._comparison_text(needle)
        )

    def contains(self, needle: Any, haystack: Any) -> bool:
        return self.find(needle, haystack) >= 0

    def startswith(self, prefix: Any, text: Any) -> bool:
        return self._comparison_text(text).startswith(
            self._comparison_text(prefix)
        )

    def collation_order(self, value: Any) -> tuple[int, int]:
        text = self._comparison_text(value)
        return (ord(text[0]), 1) if text else (0, 0)

    def contractions(self) -> tuple[()]:
        return ()

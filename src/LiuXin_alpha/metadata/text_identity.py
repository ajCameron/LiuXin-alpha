"""Pure text-key policies used by schema-selected bibliographic identities.

The full-text policy preserves punctuation and every word. Historical search
profiles remain available for existing databases and explicit migrations.
Approximate matching and title sorting are separate operations.
"""

from __future__ import annotations

import unicodedata
import re
from copy import deepcopy


LEGACY_LITTLE_WORDS = ("on", "the", "a", "at", "of", "and")
LEGACY_DROP_CHARACTERS = (
    ".",
    ",",
    '"',
    "'",
    "?",
    "!",
    "$",
    "%",
    "^",
    "&",
    "*",
    "#",
    ":",
    ";",
    "-",
)


def legacy_tag_search_term(value: str) -> str:
    """Preserve the historical whitespace-removing, lowercase tag-key contract."""
    return re.sub(r"\s+", "", deepcopy(value)).lower()


def legacy_title_search_term(value: str) -> str:
    """Preserve the historical lossy title key, including first-hyphen truncation."""
    value = deepcopy(value).split("-")[0]
    text = "".join(
        character for character in value if character not in LEGACY_DROP_CHARACTERS
    )
    return "_".join(
        word for word in text.lower().split() if word not in LEGACY_LITTLE_WORDS
    )


def normalize_identity_text(value: str, profile: str) -> str:
    """Derive a key without consulting settings, storage or schema metadata."""
    if profile == "none":
        return value
    if profile == "unicode_nfc":
        return unicodedata.normalize("NFC", value)
    if profile == "unicode_nfc_trim_casefold":
        return unicodedata.normalize("NFC", value).strip().casefold()
    if profile == "unicode_nfkc_whitespace_casefold":
        return " ".join(unicodedata.normalize("NFKC", value).casefold().split())
    if profile == "tag_search_term":
        return legacy_tag_search_term(unicodedata.normalize("NFC", value))
    if profile == "title_search_term":
        return legacy_title_search_term(unicodedata.normalize("NFC", value))
    raise ValueError(f"Unsupported text identity profile: {profile!r}")


__all__ = ["normalize_identity_text"]

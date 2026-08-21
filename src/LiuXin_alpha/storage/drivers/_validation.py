"""Validation shared by remote storage-driver address spaces."""

from __future__ import annotations

import string

from LiuXin_alpha.storage.api import StorageInvalidAddress


_HEXADECIMAL = frozenset(string.hexdigits)


def reject_malformed_unicode(value: str, *, label: str) -> None:
    """Reject lone surrogates which cannot cross Unicode remote protocols."""

    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as error:
        raise StorageInvalidAddress(
            f"{label} contains malformed Unicode (an unpaired surrogate)."
        ) from error


def reject_malformed_percent_escapes(value: str, *, label: str) -> None:
    """Require every percent sign in a URL component to encode two hex digits."""

    position = 0
    while True:
        position = value.find("%", position)
        if position < 0:
            return
        escape = value[position + 1 : position + 3]
        if len(escape) != 2 or any(char not in _HEXADECIMAL for char in escape):
            raise StorageInvalidAddress(
                f"{label} contains a malformed percent escape."
            )
        position += 3


__all__ = [
    "reject_malformed_percent_escapes",
    "reject_malformed_unicode",
]

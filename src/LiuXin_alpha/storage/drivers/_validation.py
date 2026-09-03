"""
Validation shared by remote storage-driver address spaces.
"""

from __future__ import annotations

import string

from LiuXin_alpha.storage.api import StorageInvalidAddress


_HEXADECIMAL = frozenset(string.hexdigits)


def best_effort_close(value: object) -> None:
    """
    Close an untrusted remote adapter without masking the real outcome.

    Example:
        >>> class Adapter:
        ...     def close(self):
        ...         raise RuntimeError("ignored during cleanup")
        >>> best_effort_close(Adapter())


    :param value:
    :return:
    """

    close = getattr(value, "close", None)
    if callable(close):
        try:
            close()
        except Exception:
            pass


def reject_malformed_unicode(value: str, *, label: str) -> None:
    """
    Reject lone surrogates which cannot cross Unicode remote protocols.

    Example:
        >>> reject_malformed_unicode("Café", label="object key")


    :param value:
    :param label:
    :return:
    """

    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as error:
        raise StorageInvalidAddress(
            f"{label} contains malformed Unicode (an unpaired surrogate)."
        ) from error


def reject_malformed_percent_escapes(value: str, *, label: str) -> None:
    """
    Require every percent sign in a URL component to encode two hex digits.

    Example:
        >>> reject_malformed_percent_escapes("books/Caf%C3%A9.epub", label="URL path")


    :param value:
    :param label:
    :return:
    """

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
    "best_effort_close",
    "reject_malformed_percent_escapes",
    "reject_malformed_unicode",
]

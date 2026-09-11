"""
Provide narrow cleanup and text-validation helpers for remote storage adapters.

Unicode checking rejects strings that cannot be encoded as strict UTF-8. Percent
checking validates escape syntax without decoding or interpreting the address.
Cleanup suppresses ordinary exceptions from a callable close method, while
attribute lookup and BaseException subclasses remain outside that suppression.
"""

from __future__ import annotations

import string

from LiuXin_alpha.storage.api import StorageInvalidAddress


_HEXADECIMAL = frozenset(string.hexdigits)


def best_effort_close(value: object) -> None:
    """
    Call an available close method and suppress ordinary exceptions from that call.

    A missing or noncallable attribute does nothing. Attribute lookup itself occurs outside the try
    block, and BaseException subclasses are not suppressed. The caller must not infer that a
    resource closed successfully from this return.

    Example:
        >>> class Adapter:
        ...     def close(self):
        ...         raise RuntimeError("ignored during cleanup")
        >>> best_effort_close(Adapter())


    :param value: Adapter-like object whose optional close attribute is inspected and called.
    :return: None after a skipped, successful, or ordinarily failing close call.
    """

    close = getattr(value, "close", None)
    if callable(close):
        try:
            close()
        except Exception:
            pass


def reject_malformed_unicode(value: str, *, label: str) -> None:
    """
    Require a string to encode as strict UTF-8, translating unpaired surrogates.

    UnicodeEncodeError becomes StorageInvalidAddress with label context. This does not normalize
    text, reject control characters, or validate protocol/key syntax.

    Example:
        >>> reject_malformed_unicode("Café", label="object key")


    :param value: Text to encode for validation only; the encoded bytes are discarded.
    :param label: Human-readable field description used in the malformed-Unicode error.
    :return: None for encodable text; StorageInvalidAddress chained from a Unicode encoding failure otherwise.
    """

    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as error:
        raise StorageInvalidAddress(
            f"{label} contains malformed Unicode (an unpaired surrogate)."
        ) from error


def reject_malformed_percent_escapes(value: str, *, label: str) -> None:
    """
    Require two hexadecimal digits after every percent sign without decoding the text.

    Uppercase and lowercase hex digits are accepted. A trailing percent sign, incomplete pair, or
    nonhex pair raises StorageInvalidAddress. Valid escape syntax does not establish decoded Unicode
    validity or address safety.

    Example:
        >>> reject_malformed_percent_escapes("books/Caf%C3%A9.epub", label="URL path")


    :param value: URL-component text to scan; literal percent signs must also be escaped.
    :param label: Field description included in the malformed-escape error message.
    :return: None after every percent escape passes the syntax check.
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

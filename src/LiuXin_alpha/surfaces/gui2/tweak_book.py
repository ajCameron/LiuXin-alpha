"""Compatibility shim for optional tweak-book helpers used by file-format code."""

from __future__ import annotations

from types import SimpleNamespace


dictionaries = SimpleNamespace(default_locale="en")


def set_book_locale(locale: str | None) -> None:
    """Compatibility no-op used by headless processing.

    The real GUI-backed implementation mutates environment or locale state. In a
    non-GUI run, we intentionally do nothing rather than failing on import.
    """
    return None


__all__ = ["dictionaries", "set_book_locale"]

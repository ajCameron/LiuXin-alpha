"""Compatibility layer for GUI helpers used by file-format code.

This project's headless validation paths rely on optional Qt integrations that may
not be available in a server or CI environment. The historical calibre-style API
is kept intentionally tiny here so import-time compatibility is preserved without
trying to boot a GUI.
"""

from __future__ import annotations

from typing import Any

config: dict[str, Any] = {"use_roman_numerals_for_series_number": False}


def is_ok_to_use_qt() -> bool:
    """Return whether the GUI path is available in this process."""
    return False


def must_use_qt() -> bool:
    """Compatibility hook for callers that require Qt to be used."""
    return False


def ensure_app() -> None:
    """No-op placeholder when no Qt application bootstrap is available."""
    return None


def load_builtin_fonts() -> None:
    """No-op placeholder for font loading in headless runs."""
    return None


def pixmap_to_data(pixmap: Any) -> bytes:
    """Convert a Qt pixmap-like object to bytes.

    In the headless fallback, a real Qt object is unavailable, so callers should
    treat this as a strict no-op and handle the absence of a pixmap before use.
    """
    if pixmap is None:
        return b""
    return b""


__all__ = [
    "config",
    "is_ok_to_use_qt",
    "must_use_qt",
    "ensure_app",
    "load_builtin_fonts",
    "pixmap_to_data",
]

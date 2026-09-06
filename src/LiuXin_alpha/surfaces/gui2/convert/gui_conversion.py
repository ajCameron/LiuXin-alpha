"""Compatibility shim for GUI conversion helpers.

The real project expects a Qt-backed conversion helper here, but the current
headless environment intentionally avoids GUI initialization. The compatibility
module preserves import paths while failing clearly if a caller actually asks for
GUI conversion in a non-GUI environment.
"""

from __future__ import annotations

from typing import Any


def gui_convert(*args: Any, **kwargs: Any) -> Any:
    """Fail clearly when GUI-only conversion is requested headlessly."""

    raise RuntimeError("GUI conversion is unavailable in a headless environment.")


__all__ = ["gui_convert"]

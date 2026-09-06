"""Shared text and row presentation without a dependency on a web application.

Missing columns have an explicit fallback; unexpected row-access failures
remain visible to callers instead of being rendered as absent metadata.
"""

from __future__ import annotations

import html
from typing import Protocol


class RowLookup(Protocol):
    """Column lookup shared by mapping rows and Core's row projection."""

    def __getitem__(self, column: str, /) -> object: ...


def escape(value: object) -> str:
    """Escape displayed text and quoted HTML attribute values alike."""
    return html.escape("" if value is None else str(value), quote=True)


def short_text(value: object, *, width: int = 120) -> str:
    """Normalize line endings and abbreviate long values for surface summaries."""
    text = "" if value is None else str(value)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def coerce_int(
    raw: str | None,
    *,
    default: int,
    minimum: int = 0,
    maximum: int | None = None,
) -> int:
    """Parse a surface option, falling back and clamping to its declared bounds."""
    try:
        value = int(str(raw).strip())
    except Exception:
        value = int(default)
    value = max(int(minimum), int(value))
    if maximum is not None:
        value = min(int(maximum), value)
    return value


def row_value(row: RowLookup, column: str) -> object:
    """Return None for a missing column, but propagate failed row access."""
    try:
        return row[column]
    except KeyError:
        return None

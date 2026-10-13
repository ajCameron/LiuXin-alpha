"""Structural helpers for consuming row-shaped relation targets without database imports."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def row_mapping(value: Any) -> Mapping[str, Any] | None:
    """Return a mapping directly or from a structural ``row_dict`` attribute."""
    if isinstance(value, Mapping):
        return value
    mapping = getattr(value, "row_dict", None)
    return mapping if isinstance(mapping, Mapping) else None


def row_id(value: Any) -> Any:
    """Return a structural row identifier when present."""
    return getattr(value, "row_id", None)


def row_table(value: Any) -> Any:
    """Return a structural row table name when present."""
    return getattr(value, "table", None)


__all__ = ["row_id", "row_mapping", "row_table"]

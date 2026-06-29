"""Shared helpers for keeping surface read sources fresh after writes."""

from __future__ import annotations

from typing import Any


def _try_refresh(candidate: Any) -> bool:
    if candidate is None:
        return False
    refresh = getattr(candidate, "refresh_read_source", None)
    if callable(refresh):
        return bool(refresh())
    refresh = getattr(candidate, "refresh", None)
    if callable(refresh):
        return bool(refresh())
    reload_source = getattr(candidate, "reload", None)
    if callable(reload_source):
        reload_source()
        return True
    return False


def refresh_metadata_read_source_after_write(owner: Any) -> bool:
    """
    Refresh the metadata/read source attached to a surface object, if any.

    The helper deliberately treats refresh as best-effort: write callers should
    not report a failed write just because a read cache was not present.
    """
    candidates = (
        getattr(owner, "read_model", None),
        getattr(owner, "metadata_read_source", None),
        getattr(owner, "read_source", None),
    )
    for candidate in candidates:
        try:
            if _try_refresh(candidate):
                return True
        except Exception:
            continue
    return False


__all__ = [
    "refresh_metadata_read_source_after_write",
]

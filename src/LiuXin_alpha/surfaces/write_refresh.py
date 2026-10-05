"""
Refresh optional surface read sources after a successful metadata write.

Write completion remains distinct from cache-refresh success. These helpers try
the attached read models in a fixed order and contain refresh-method failures,
allowing callers to report the completed write even when no cache can be refreshed.
"""

from __future__ import annotations

from typing import Any


def _try_refresh(candidate: Any) -> bool:
    """
    Invoke the first refresh capability exposed by one candidate read source.

    Prefer ``refresh_read_source``, then ``refresh``, then ``reload``. A false
    result from a supported refresh method does not fall through to another
    method on the same object. A completed ``reload`` counts as success regardless
    of its return value. Method failures propagate to the outer candidate loop.

    Example:
        >>> from types import SimpleNamespace
        >>> _try_refresh(SimpleNamespace(reload=lambda: None))
        True
        >>> _try_refresh(None)
        False


    :param candidate: Optional read-source object whose capabilities are probed.
    :return: Whether a supported method reports success or a reload completes.
    """
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
    Refresh the first attached metadata view that can complete a refresh.

    Try ``read_model``, ``metadata_read_source``, and ``read_source`` in that
    order. Absent capabilities, false results, and refresh-method exceptions
    advance to the next candidate. Attribute errors raised while discovering
    these owner properties are not caught by the refresh-method boundary.

    Example:
        >>> from types import SimpleNamespace
        >>> source = SimpleNamespace(refresh=lambda: True)
        >>> refresh_metadata_read_source_after_write(SimpleNamespace(read_source=source))
        True


    :param owner: Surface object optionally exposing one or more read-source attributes.
    :return: Whether any candidate completed a successful refresh; otherwise ``False``.
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

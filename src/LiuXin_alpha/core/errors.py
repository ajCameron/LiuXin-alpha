"""Core runtime error taxonomy."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


class CoreError(RuntimeError):
    """Base class for core/runtime failures."""

    code = "core_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        if code is not None:
            self.code = str(code)
        self.details = dict(details or {})


class CoreShutdownError(CoreError):
    """Raised when an operation is requested after core shutdown."""

    code = "core_shutdown"


class CoreDispatchError(CoreError):
    """Raised when a command/query cannot be dispatched."""

    code = "dispatch_error"


class CoreHandlerError(CoreError):
    """Raised when a registered command/query handler fails."""

    code = "handler_error"


def core_error_details(exc: BaseException) -> tuple[str, dict[str, Any]]:
    """Return the stable error code/details for a handler failure."""

    from LiuXin_alpha.core.services import (
        CoreServiceReconciliationError,
    )

    if isinstance(exc, CoreServiceReconciliationError):
        return (
            "cache_reconciliation_failed",
            {
                "receipt": dict(exc.receipt),
                "canonical_write_committed": True,
            },
        )
    try:
        from LiuXin_alpha.caches import CacheReconciliationError
    except Exception:
        CacheReconciliationError = ()  # type: ignore[assignment,misc]
    if CacheReconciliationError and isinstance(
        exc,
        CacheReconciliationError,
    ):
        return (
            "cache_reconciliation_failed",
            {
                "receipt": dict(exc.receipt),
                "dependencies": sorted(exc.dependencies),
                "canonical_write_committed": True,
            },
        )
    if isinstance(exc, CoreError):
        return exc.code, dict(exc.details)
    return (
        "handler_error",
        {
            "exception_type": type(exc).__name__,
        },
    )


__all__ = [
    "CoreDispatchError",
    "CoreError",
    "CoreHandlerError",
    "CoreShutdownError",
    "core_error_details",
]

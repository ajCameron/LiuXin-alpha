"""
Classify Core lifecycle/dispatch failures and preserve committed-write reconciliation receipts.

Stable codes/details are separate from human-readable exception messages. The
extraction helper does not itself wire-encode details or perform recovery.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


class CoreError(RuntimeError):
    """
    Carry a runtime failure message, stable code, and shallow-copied detail mapping.

    Subclasses supply default codes; individual instances may override them.
    Details remain mutable and are not validated for transport compatibility.

    Example:
        >>> error = CoreError("Unavailable", details={"retryable": True})
        >>> str(error), error.code, error.details
        ('Unavailable', 'core_error', {'retryable': True})
    """

    code = "core_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        """
        Initialize the exception message and copy details, optionally overriding the class code.

        Example:
            >>> CoreError("Stopped", code="stopped").code
            'stopped'


        :param message: Human-readable message passed to ``RuntimeError``.
        :param code: Optional code override stringified when supplied; ``None`` retains the subclass default.
        :param details: Optional mapping copied at the top level, with nested objects retained by reference.
        :return: ``None`` after initializing the exception and its details.
        """
        super().__init__(message)
        if code is not None:
            self.code = str(code)
        self.details = dict(details or {})


class CoreShutdownError(CoreError):
    """
    Report an operation rejected because its Core runtime has shut down.

    Example:
        >>> CoreShutdownError("Core is closed").code
        'core_shutdown'
    """

    code = "core_shutdown"


class CoreDispatchError(CoreError):
    """
    Report a command/query that cannot be resolved, validated, or routed for execution.

    Example:
        >>> CoreDispatchError("Unknown command").code
        'dispatch_error'
    """

    code = "dispatch_error"


class CoreHandlerError(CoreError):
    """
    Represent a failure from a registered handler, retaining optional structured details.

    Example:
        >>> CoreHandlerError("Read failed", details={"exception_type": "OSError"}).code
        'handler_error'
    """

    code = "handler_error"


def core_error_details(exc: BaseException) -> tuple[str, dict[str, Any]]:
    """
    Extract code/details while distinguishing cache-refresh failure from an uncommitted write.

    Known service/cache reconciliation exceptions expose copied receipts and
    ``canonical_write_committed=True``; cache exceptions also expose sorted
    dependency names. Callers must not interpret those errors as permission to
    retry the canonical write blindly. Other Core errors retain their code and
    copied details; arbitrary exceptions expose only their class name here.

    Cache-error import failures are tolerated, but service import and receipt
    conversion failures propagate. Returned nested values are not copied deeply
    or made wire-safe by this helper.

    Example:
        >>> core_error_details(ValueError("bad input"))
        ('handler_error', {'exception_type': 'ValueError'})
        >>> core_error_details(CoreDispatchError("Unknown command"))
        ('dispatch_error', {})


    :param exc: Failure to classify, normally caught at the Core handler boundary.
    :return: Stable code and a new details dictionary, without the human-readable exception message.
    """

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

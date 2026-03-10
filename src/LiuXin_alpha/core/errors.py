"""Core runtime error taxonomy."""

from __future__ import annotations


class CoreError(RuntimeError):
    """Base class for core/runtime failures."""


class CoreShutdownError(CoreError):
    """Raised when an operation is requested after core shutdown."""


class CoreDispatchError(CoreError):
    """Raised when a command/query cannot be dispatched."""


class CoreHandlerError(CoreError):
    """Raised when a registered command/query handler fails."""

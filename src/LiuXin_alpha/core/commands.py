"""
Represent Core write-path requests and responses without executing or validating them.

These frozen dataclasses carry routing/correlation information. Nested payloads
and results can remain mutable; transport conversion is a separate operation.
"""

from __future__ import annotations

import dataclasses
import uuid
from collections.abc import Mapping
from typing import Any


@dataclasses.dataclass(frozen=True)
class CoreCommand:
    """
    Carry a named operation, payload, request identity, and optional caller correlation token.

    ``name`` selects a handler; ``command_id`` defaults to a fresh UUID4 string.
    The supplied payload is retained, not copied or checked for wire safety.
    Frozen fields do not make nested values immutable or dispatch the command.

    Example:
        >>> command = CoreCommand("jobs.cancel", {"job_id": "job-1"}, command_id="request-1")
        >>> command.name, command.command_id, command.correlation_id
        ('jobs.cancel', 'request-1', None)
    """

    name: str
    payload: Mapping[str, Any] = dataclasses.field(default_factory=dict[str, Any])
    command_id: str = dataclasses.field(default_factory=lambda: str(uuid.uuid4()))
    correlation_id: str | None = None


@dataclasses.dataclass(frozen=True)
class CoreCommandResult:
    """
    Associate an execution outcome with its command ID and optional correlation token.

    ``ok`` reports success, ``result`` carries the handler value, and ``error``
    carries optional failure text. Construction does not enforce consistency
    between them or imply rollback when ``ok`` is false.

    Example:
        >>> result = CoreCommandResult(False, "request-1", error="Unknown command")
        >>> result.ok, result.error
        (False, 'Unknown command')
    """

    ok: bool
    command_id: str
    result: Any = None
    error: str | None = None
    correlation_id: str | None = None


__all__ = [
    "CoreCommand",
    "CoreCommandResult",
]

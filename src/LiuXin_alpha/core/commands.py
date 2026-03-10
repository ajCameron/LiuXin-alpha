"""Command envelopes and results for core write-path operations."""

from __future__ import annotations

import dataclasses
import uuid

from typing import Any, Mapping


@dataclasses.dataclass(frozen=True)
class CoreCommand:
    """Command request envelope."""

    name: str
    payload: Mapping[str, Any] = dataclasses.field(default_factory=dict)
    command_id: str = dataclasses.field(default_factory=lambda: str(uuid.uuid4()))
    correlation_id: str | None = None


@dataclasses.dataclass(frozen=True)
class CoreCommandResult:
    """Command response envelope."""

    ok: bool
    command_id: str
    result: Any = None
    error: str | None = None
    correlation_id: str | None = None


__all__ = [
    "CoreCommand",
    "CoreCommandResult",
]

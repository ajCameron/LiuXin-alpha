"""Query envelopes and results for core read-path operations."""

from __future__ import annotations

import dataclasses
import uuid
from collections.abc import Mapping
from typing import Any


@dataclasses.dataclass(frozen=True)
class CoreQuery:
    """Query request envelope."""

    name: str
    payload: Mapping[str, Any] = dataclasses.field(default_factory=dict[str, Any])
    query_id: str = dataclasses.field(default_factory=lambda: str(uuid.uuid4()))
    correlation_id: str | None = None


@dataclasses.dataclass(frozen=True)
class CoreQueryResult:
    """Query response envelope."""

    ok: bool
    query_id: str
    result: Any = None
    error: str | None = None
    correlation_id: str | None = None


__all__ = [
    "CoreQuery",
    "CoreQueryResult",
]

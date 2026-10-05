"""
Represent Core read-path requests and responses without performing dispatch or enforcing read-only behavior.

Frozen envelopes retain nested objects as supplied. Handler selection, failure
policy, and transport normalization belong to the runtime and its clients.
"""

from __future__ import annotations

import dataclasses
import uuid
from collections.abc import Mapping
from typing import Any


@dataclasses.dataclass(frozen=True)
class CoreQuery:
    """
    Carry a named read request, payload, generated or supplied query ID, and optional correlation token.

    ``name`` selects a handler; omitted ``query_id`` receives a UUID4 string.
    Supplied payload mappings are retained by reference without validation.
    The dataclass does not itself guarantee a side-effect-free handler.

    Example:
        >>> query = CoreQuery("jobs.list", query_id="request-1", correlation_id="view-1")
        >>> query.name, query.query_id, query.payload
        ('jobs.list', 'request-1', {})
    """

    name: str
    payload: Mapping[str, Any] = dataclasses.field(default_factory=dict[str, Any])
    query_id: str = dataclasses.field(default_factory=lambda: str(uuid.uuid4()))
    correlation_id: str | None = None


@dataclasses.dataclass(frozen=True)
class CoreQueryResult:
    """
    Pair a query's success flag, optional result/error, and correlation token with its request ID.

    ``ok`` and ``error`` describe the reported outcome; their consistency is not
    checked at construction. Nested result objects remain mutable and need
    separate transport conversion when crossing a serialization boundary.

    Example:
        >>> result = CoreQueryResult(True, "request-1", result={"count": 2})
        >>> result.ok, result.result
        (True, {'count': 2})
    """

    ok: bool
    query_id: str
    result: Any = None
    error: str | None = None
    correlation_id: str | None = None


__all__ = [
    "CoreQuery",
    "CoreQueryResult",
]

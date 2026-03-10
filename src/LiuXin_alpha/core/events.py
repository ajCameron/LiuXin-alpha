"""Event models used by the core runtime and client proxies."""

from __future__ import annotations

import dataclasses
import datetime
import uuid

from typing import Any, Mapping


@dataclasses.dataclass(frozen=True)
class CoreEvent:
    """Immutable event emitted by the core runtime."""

    event_id: str
    core_uuid: str
    event_type: str
    timestamp_utc: str
    payload: Mapping[str, Any] = dataclasses.field(default_factory=dict)


def utc_now_iso() -> str:
    """Return an RFC3339-ish UTC timestamp with trailing `Z`."""
    return datetime.datetime.now(tz=datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def make_core_event(*, core_uuid: str, event_type: str, payload: Mapping[str, Any] | None = None) -> CoreEvent:
    """Factory for `CoreEvent` with generated id/timestamp."""
    return CoreEvent(
        event_id=str(uuid.uuid4()),
        core_uuid=str(core_uuid),
        event_type=str(event_type),
        timestamp_utc=utc_now_iso(),
        payload=dict(payload or {}),
    )


__all__ = [
    "CoreEvent",
    "make_core_event",
    "utc_now_iso",
]

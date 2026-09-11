"""
Construct Core event envelopes with UUID identifiers and UTC wall-clock timestamps.

Creation does not publish, queue, or persist an event. Delivery and filtering
belong to the runtime/proxies; payload serialization belongs to their boundaries.
"""

from __future__ import annotations

import dataclasses
import datetime
import uuid

from typing import Any, Mapping


@dataclasses.dataclass(frozen=True)
class CoreEvent:
    """
    Describe one runtime event with identity, producer, type, timestamp, and payload.

    ``event_id`` identifies the event and ``core_uuid`` its producer; ``event_type``
    is the routing label and ``timestamp_utc`` is timestamp text. Construction
    validates none of these fields. Freezing prevents field reassignment, not
    mutation of the payload mapping or objects inside it.

    Example:
        >>> event = CoreEvent("event-1", "core-1", "ready", "2026-09-08T00:00:00Z")
        >>> event.event_type, dict(event.payload)
        ('ready', {})
    """

    event_id: str
    core_uuid: str
    event_type: str
    timestamp_utc: str
    payload: Mapping[str, Any] = dataclasses.field(default_factory=dict)


def utc_now_iso() -> str:
    """
    Format the current UTC wall-clock time as ISO text with a trailing ``Z``.

    Fractional seconds follow ``datetime.isoformat`` defaults. The value is
    neither monotonic nor guaranteed unique across calls.

    Example:
        >>> utc_now_iso().endswith("Z")
        True


    :return: Current UTC date/time string with ``Z`` replacing the numeric UTC offset.
    """
    return (
        datetime.datetime.now(tz=datetime.timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def make_core_event(
    *, core_uuid: str, event_type: str, payload: Mapping[str, Any] | None = None
) -> CoreEvent:
    """
    Generate an event ID and timestamp, stringify routing labels, and shallow-copy the payload.

    Example:
        >>> event = make_core_event(core_uuid="core-1", event_type="ready", payload={"count": 2})
        >>> event.core_uuid, event.event_type, event.payload["count"]
        ('core-1', 'ready', 2)


    :param core_uuid: Producer identity stringified without UUID validation.
    :param event_type: Event routing label stringified without vocabulary validation.
    :param payload: Optional top-level data mapping copied into a new dictionary.
    :return: Undelivered CoreEvent with a random UUID4 string and current UTC timestamp.
    """
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

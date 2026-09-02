"""
Typed maintenance events for the database maintenance engine.
"""

from __future__ import annotations

import dataclasses
import time
import uuid

from typing import Literal

MaintenanceEventKind = Literal[
    "dirty_row",
    "new_dirty_row",
    "dirty_interlink",
    "rename_request",
    "tick",
    "shutdown",
]


@dataclasses.dataclass(frozen=True, slots=True)
class MaintenanceEvent:
    """
    Records a maintenance event.
    """
    kind: MaintenanceEventKind
    created_at: float = dataclasses.field(default_factory=time.monotonic)
    event_id: str = dataclasses.field(default_factory=lambda: str(uuid.uuid4()))


@dataclasses.dataclass(frozen=True, slots=True)
class DirtyRowEvent(MaintenanceEvent):
    """
    Records that a row has been dirtied.
    """
    table: str = ""
    row_id: int = 0

    def __init__(self, table: str, row_id: int, *, kind: MaintenanceEventKind = "dirty_row") -> None:
        """
        Constructor.

        :param table:
        :param row_id:
        :param kind:
        """
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "created_at", time.monotonic())
        object.__setattr__(self, "event_id", str(uuid.uuid4()))
        object.__setattr__(self, "table", str(table or ""))
        object.__setattr__(self, "row_id", int(row_id))


@dataclasses.dataclass(frozen=True, slots=True)
class DirtyInterlinkEvent(MaintenanceEvent):
    """
    Records that an interlink row has been dirtied.
    """
    update_type: str = ""
    table1: str = ""
    table2: str = ""
    table1_id: int = 0
    table2_id: int = 0

    def __init__(self, update_type: str, table1: str, table2: str, table1_id: int, table2_id: int) -> None:
        object.__setattr__(self, "kind", "dirty_interlink")
        object.__setattr__(self, "created_at", time.monotonic())
        object.__setattr__(self, "event_id", str(uuid.uuid4()))
        object.__setattr__(self, "update_type", str(update_type or ""))
        object.__setattr__(self, "table1", str(table1 or ""))
        object.__setattr__(self, "table2", str(table2 or ""))
        object.__setattr__(self, "table1_id", int(table1_id))
        object.__setattr__(self, "table2_id", int(table2_id))


@dataclasses.dataclass(frozen=True, slots=True)
class RenameRequestEvent(MaintenanceEvent):
    """
    Register that a rename has been requested by the system.
    """
    item_id: int = 0
    table: str = ""
    value: str = ""

    def __init__(self, item_id: int, table: str, value: str) -> None:
        """
        Constructor.

        :param item_id:
        :param table:
        :param value:
        """
        object.__setattr__(self, "kind", "rename_request")
        object.__setattr__(self, "created_at", time.monotonic())
        object.__setattr__(self, "event_id", str(uuid.uuid4()))
        object.__setattr__(self, "item_id", int(item_id))
        object.__setattr__(self, "table", str(table or ""))
        object.__setattr__(self, "value", str(value or ""))


@dataclasses.dataclass(frozen=True, slots=True)
class TickEvent(MaintenanceEvent):
    """Request one periodic maintenance scheduling pass."""

    def __init__(self) -> None:
        object.__setattr__(self, "kind", "tick")
        object.__setattr__(self, "created_at", time.monotonic())
        object.__setattr__(self, "event_id", str(uuid.uuid4()))


@dataclasses.dataclass(frozen=True, slots=True)
class ShutdownEvent(MaintenanceEvent):
    """Request an orderly maintenance-engine shutdown with optional context."""

    reason: str = ""

    def __init__(self, reason: str = "") -> None:
        object.__setattr__(self, "kind", "shutdown")
        object.__setattr__(self, "created_at", time.monotonic())
        object.__setattr__(self, "event_id", str(uuid.uuid4()))
        object.__setattr__(self, "reason", str(reason or ""))

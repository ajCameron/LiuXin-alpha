"""
Represent maintenance requests as immutable, slotted dataclasses.

Each custom initializer assigns a fresh monotonic timestamp and UUID string. These
records validate integer conversions but do not verify schema names, supported kinds
or row existence.
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
    Store an event kind, monotonic creation time and UUID identity.

    The frozen, slotted record uses generated defaults; annotations do not validate kind
    membership.

    Example:
        >>> event = MaintenanceEvent("tick")
        >>> event.kind, bool(event.event_id)
        ('tick', True)
    """
    kind: MaintenanceEventKind
    created_at: float = dataclasses.field(default_factory=time.monotonic)
    event_id: str = dataclasses.field(default_factory=lambda: str(uuid.uuid4()))


@dataclasses.dataclass(frozen=True, slots=True)
class DirtyRowEvent(MaintenanceEvent):
    """
    Identify a changed row by table and integer ID.

    kind can distinguish new_dirty_row from dirty_row; it is stored as supplied.

    Example:
        >>> event = DirtyRowEvent("creators", "7")
        >>> event.kind, event.row_id
        ('dirty_row', 7)
    """
    table: str = ""
    row_id: int = 0

    def __init__(self, table: str, row_id: int, *, kind: MaintenanceEventKind = "dirty_row") -> None:
        """
        Initialize immutable event fields with fresh timing and identity metadata.

        Integer fields are coerced with int(); table/name/reason text uses str(value or "").
        Conversion errors propagate. Tick and shutdown kinds are fixed; DirtyRowEvent
        accepts its supplied kind.

        Example:
            >>> event = DirtyRowEvent("creators", "7")
            >>> event.kind, event.row_id
            ('dirty_row', 7)


        :param table: Table associated with the row or operation.
        :param row_id: Row identifier converted to int by event/callback construction.
        :param kind: Event kind stored without runtime membership validation; defaults to
            dirty_row.
        :return: None.
        """
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "created_at", time.monotonic())
        object.__setattr__(self, "event_id", str(uuid.uuid4()))
        object.__setattr__(self, "table", str(table or ""))
        object.__setattr__(self, "row_id", int(row_id))


@dataclasses.dataclass(frozen=True, slots=True)
class DirtyInterlinkEvent(MaintenanceEvent):
    """
    Describe a changed relationship with two endpoint tables and IDs.

    Example:
        >>> event = DirtyInterlinkEvent("UPDATE", "creators", "titles", 1, 2)
        >>> event.kind, event.table2_id
        ('dirty_interlink', 2)
    """
    update_type: str = ""
    table1: str = ""
    table2: str = ""
    table1_id: int = 0
    table2_id: int = 0

    def __init__(self, update_type: str, table1: str, table2: str, table1_id: int, table2_id: int) -> None:
        """
        Initialize immutable event fields with fresh timing and identity metadata.

        Integer fields are coerced with int(); table/name/reason text uses str(value or "").
        Conversion errors propagate. Tick and shutdown kinds are fixed; DirtyRowEvent
        accepts its supplied kind.

        Example:
            >>> event = DirtyInterlinkEvent("UPDATE", "creators", "titles", 1, 2)
            >>> event.kind, event.table2_id
            ('dirty_interlink', 2)


        :param update_type: Relationship change label; false values become empty text.
        :param table1: First relationship endpoint table.
        :param table2: Second relationship endpoint table.
        :param table1_id: First endpoint identifier, converted to int.
        :param table2_id: Second endpoint identifier, converted to int.
        :return: None.
        """
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
    Store a requested name change with an integer item ID.

    Example:
        >>> RenameRequestEvent(1, "creators", None).value
        ''
    """
    item_id: int = 0
    table: str = ""
    value: str = ""

    def __init__(self, item_id: int, table: str, value: str) -> None:
        """
        Initialize immutable event fields with fresh timing and identity metadata.

        Integer fields are coerced with int(); table/name/reason text uses str(value or "").
        Conversion errors propagate. Tick and shutdown kinds are fixed; DirtyRowEvent
        accepts its supplied kind.

        Example:
            >>> RenameRequestEvent(1, "creators", None).value
            ''


        :param item_id: Identifier of the row to rename.
        :param table: Table associated with the row or operation.
        :param value: Requested new name; event construction converts false values to empty
            text.
        :return: None.
        """
        object.__setattr__(self, "kind", "rename_request")
        object.__setattr__(self, "created_at", time.monotonic())
        object.__setattr__(self, "event_id", str(uuid.uuid4()))
        object.__setattr__(self, "item_id", int(item_id))
        object.__setattr__(self, "table", str(table or ""))
        object.__setattr__(self, "value", str(value or ""))


@dataclasses.dataclass(frozen=True, slots=True)
class TickEvent(MaintenanceEvent):
    """
    Request a scheduling pass when no queued work is available.

    Example:
        >>> TickEvent().kind
        'tick'
    """

    def __init__(self) -> None:
        """
        Initialize immutable event fields with fresh timing and identity metadata.

        Integer fields are coerced with int(); table/name/reason text uses str(value or "").
        Conversion errors propagate. Tick and shutdown kinds are fixed; DirtyRowEvent
        accepts its supplied kind.

        Example:
            >>> TickEvent().kind
            'tick'


        :return: None.
        """
        object.__setattr__(self, "kind", "tick")
        object.__setattr__(self, "created_at", time.monotonic())
        object.__setattr__(self, "event_id", str(uuid.uuid4()))


@dataclasses.dataclass(frozen=True, slots=True)
class ShutdownEvent(MaintenanceEvent):
    """
    Carry shutdown context as an ordinary maintenance event.

    The engine stop flag controls thread exit; enqueuing this record alone does not stop
    the worker.

    Example:
        >>> ShutdownEvent("done").reason
        'done'
    """

    reason: str = ""

    def __init__(self, reason: str = "") -> None:
        """
        Initialize immutable event fields with fresh timing and identity metadata.

        Integer fields are coerced with int(); table/name/reason text uses str(value or "").
        Conversion errors propagate. Tick and shutdown kinds are fixed; DirtyRowEvent
        accepts its supplied kind.

        Example:
            >>> ShutdownEvent("done").reason
            'done'


        :param reason: Shutdown context, converted to text with false values becoming empty
            text.
        :return: None.
        """
        object.__setattr__(self, "kind", "shutdown")
        object.__setattr__(self, "created_at", time.monotonic())
        object.__setattr__(self, "event_id", str(uuid.uuid4()))
        object.__setattr__(self, "reason", str(reason or ""))

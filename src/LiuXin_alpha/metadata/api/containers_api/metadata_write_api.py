"""Shared metadata write-back API contracts."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol, TypeAlias, TypedDict, runtime_checkable


MetadataWriteScalar: TypeAlias = str | int | float | bool | None
MetadataWriteValue: TypeAlias = (
    MetadataWriteScalar
    | list["MetadataWriteValue"]
    | tuple["MetadataWriteValue", ...]
    | Mapping[str, "MetadataWriteValue"]
)
MetadataWriteRecord: TypeAlias = Mapping[str, MetadataWriteValue]


class MetadataWriteReportMapping(TypedDict):
    """Mapping payload returned by metadata write-back reports."""

    item_id: int | None
    target_level: str
    target_table: str | None
    target_id: int | None
    fields_checked: list[str]
    rows_added: list[MetadataWriteRecord]
    rows_updated: list[MetadataWriteRecord]
    rows_removed: list[MetadataWriteRecord]
    links_added: list[MetadataWriteRecord]
    links_removed: list[MetadataWriteRecord]
    skipped: list[str]
    errors: list[str]
    changed: bool


class MetadataWriteTargetRowAPI(Protocol):
    """Row-like target accepted by metadata write-back adapters."""

    @property
    def row_dict(self) -> MetadataWriteRecord:
        """Return the row payload as a mapping."""


MetadataWriteTargetRow: TypeAlias = MetadataWriteRecord | MetadataWriteTargetRowAPI


class MetadataWriteDatabaseAPI(Protocol):
    """Database handle accepted by metadata write-back adapters."""


@runtime_checkable
class MetadataWriteReportAPI(Protocol):
    """Summary returned by metadata write-back operations."""

    item_id: int | None
    target_level: str
    target_table: str | None
    target_id: int | None
    fields_checked: list[str]
    rows_added: list[MetadataWriteRecord]
    rows_updated: list[MetadataWriteRecord]
    rows_removed: list[MetadataWriteRecord]
    links_added: list[MetadataWriteRecord]
    links_removed: list[MetadataWriteRecord]
    skipped: list[str]
    errors: list[str]

    @property
    def changed(self) -> bool:
        """Return true when the write changed rows or links."""

    def to_mapping(self) -> MetadataWriteReportMapping:
        """Return a mapping representation of the report."""


__all__ = [
    "MetadataWriteDatabaseAPI",
    "MetadataWriteRecord",
    "MetadataWriteReportMapping",
    "MetadataWriteReportAPI",
    "MetadataWriteScalar",
    "MetadataWriteTargetRow",
    "MetadataWriteTargetRowAPI",
    "MetadataWriteValue",
]

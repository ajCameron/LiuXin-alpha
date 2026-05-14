"""Shared metadata payload and relation-target API types."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol, TypeAlias, runtime_checkable

MetadataScalar: TypeAlias = str | int | float | bool | None
MetadataValue: TypeAlias = (
    MetadataScalar
    | list["MetadataValue"]
    | tuple["MetadataValue", ...]
    | Mapping[str, "MetadataValue"]
)
MetadataRecord: TypeAlias = Mapping[str, MetadataValue]
MutableMetadataRecord: TypeAlias = dict[str, MetadataValue]
RelationLinkType: TypeAlias = str


@runtime_checkable
class SupportsMetadataMapping(Protocol):
    """Implemented by metadata identities/containers that can serialize themselves."""

    def to_mapping(self) -> MetadataRecord:
        """Return a metadata payload representation."""


@runtime_checkable
class SupportsRowMapping(Protocol):
    """Structural contract for database-row objects exposed by hydrators."""

    @property
    def table(self) -> str:
        """Return the source table name for this row."""

    @property
    def row_id(self) -> int | None:
        """Return the row id when the row has one."""

    @property
    def row_dict(self) -> MetadataRecord:
        """Return the row payload as a metadata record."""


RelationTarget: TypeAlias = (
    MetadataScalar
    | MetadataRecord
    | SupportsMetadataMapping
    | SupportsRowMapping
)

__all__ = [
    "MetadataScalar",
    "MetadataValue",
    "MetadataRecord",
    "MutableMetadataRecord",
    "RelationLinkType",
    "RelationTarget",
    "SupportsMetadataMapping",
    "SupportsRowMapping",
]

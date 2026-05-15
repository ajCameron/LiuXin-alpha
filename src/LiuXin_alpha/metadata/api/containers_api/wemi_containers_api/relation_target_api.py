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


def relation_target_id(target: RelationTarget | None, id_column: str) -> int | None:
    """Return an integer id from a relation target when one can be found."""

    value = None
    if isinstance(target, Mapping):
        value = _first_present_mapping_value(target, id_column, "id", "row_id")
    else:
        value = getattr(target, id_column, None)
        row_dict = getattr(target, "row_dict", None)
        if value in (None, "") and isinstance(row_dict, Mapping):
            value = _first_present_mapping_value(row_dict, id_column)
        if value in (None, ""):
            value = getattr(target, "row_id", None)
        if value in (None, ""):
            value = getattr(target, "id", None)

    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _first_present_mapping_value(
    mapping: Mapping[str, MetadataValue],
    *keys: str,
) -> MetadataValue:
    for key in keys:
        value = mapping.get(key)
        if value not in (None, ""):
            return value
    return None


__all__ = [
    "MetadataScalar",
    "MetadataValue",
    "MetadataRecord",
    "MutableMetadataRecord",
    "relation_target_id",
    "RelationLinkType",
    "RelationTarget",
    "SupportsMetadataMapping",
    "SupportsRowMapping",
]

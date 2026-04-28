"""Shared base for concrete non-WEMI main-table row containers."""

from __future__ import annotations

from dataclasses import fields
from typing import ClassVar, Mapping, Self


MetadataRowValue = str | int | float | bool | None
MetadataRowMapping = Mapping[str, MetadataRowValue]


class MetadataTableRow:
    """Small concrete container for one database main-table row."""

    TABLE_NAME: ClassVar[str]
    ID_COLUMN: ClassVar[str]

    @classmethod
    def from_mapping(cls, row: Mapping[str, MetadataRowValue]) -> Self:
        keys_method = getattr(row, "keys", None)
        row_keys = set(keys_method()) if callable(keys_method) else set(row)
        kwargs = {
            field.name: row[field.name]
            for field in fields(cls)
            if field.init and field.name in row_keys
        }
        return cls(**kwargs)

    @property
    def primary_id(self) -> int | None:
        value = getattr(self, self.ID_COLUMN)
        return value if type(value) is int else None

    def to_mapping(self) -> dict[str, MetadataRowValue]:
        return {
            field.name: getattr(self, field.name)
            for field in fields(self)
            if field.init
        }


__all__ = [
    "MetadataRowMapping",
    "MetadataRowValue",
    "MetadataTableRow",
]

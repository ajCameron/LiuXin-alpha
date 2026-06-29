"""Shared base for concrete non-WEMI main-table row containers."""

from __future__ import annotations

from dataclasses import fields
from typing import ClassVar, Self

from LiuXin_alpha.metadata.api.containers_api.main_table_containers_api import (
    MetadataRowMapping,
    MetadataRowValue,
    MetadataTableRowAPI,
)
from LiuXin_alpha.metadata.containers.metadata_containers._string_formatting import (
    compact_mapping_string,
)


class MetadataTableRow(MetadataTableRowAPI):
    """Small concrete container for one database main-table row."""

    TABLE_NAME: ClassVar[str]
    ID_COLUMN: ClassVar[str]

    @classmethod
    def from_mapping(cls, row: MetadataRowMapping) -> Self:
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

    def __str__(self) -> str:
        return compact_mapping_string(
            self,
            self.to_mapping(),
            id_keys=(self.ID_COLUMN,),
        )


__all__ = [
    "MetadataRowMapping",
    "MetadataRowValue",
    "MetadataTableRow",
]

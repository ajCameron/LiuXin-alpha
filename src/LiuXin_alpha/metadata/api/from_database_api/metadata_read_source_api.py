"""Read-source contract used by metadata hydrators."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.relation_target_api import (
    MetadataValue,
    SupportsRowMapping,
)


class MetadataDriverWrapperAPI(Protocol):
    """Opaque schema helper exposed by database-like metadata read sources."""


class MetadataReadSourceAPI(Protocol):
    """Small database-like read surface required by metadata hydrators."""

    driver_wrapper: MetadataDriverWrapperAPI

    def get_tables(self, force_refresh: bool = False) -> Sequence[str]: ...

    def get_tables_and_columns(self) -> Mapping[str, Sequence[str]]: ...

    def get_column_headings(self, table: str) -> set[str]: ...

    def get_row_from_id(self, table: str, row_id: int) -> SupportsRowMapping | None: ...

    def search(
        self,
        table: str,
        column: str,
        search_term: MetadataValue,
    ) -> Sequence[SupportsRowMapping]: ...

    def get_interlink_rows(
        self,
        primary_row: SupportsRowMapping,
        secondary_table: str,
    ) -> Sequence[SupportsRowMapping]: ...


__all__ = [
    "MetadataDriverWrapperAPI",
    "MetadataReadSourceAPI",
]

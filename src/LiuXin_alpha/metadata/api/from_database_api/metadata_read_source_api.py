"""Read-source contract used by metadata hydrators."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol, TypeAlias, runtime_checkable

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.relation_target_api import (
    MetadataRecord,
    MetadataValue,
    SupportsRowMapping,
)


MetadataSearchTerm: TypeAlias = MetadataValue
MetadataTableColumns: TypeAlias = Mapping[str, Sequence[str]]
MetadataRowSequence: TypeAlias = Sequence[SupportsRowMapping]
MetadataLinkRow: TypeAlias = MetadataRecord | SupportsRowMapping
MetadataLinkRowSequence: TypeAlias = Sequence[MetadataLinkRow]


@runtime_checkable
class MetadataDriverWrapperAPI(Protocol):
    """Schema helper exposed by database-like metadata read sources."""

    def get_allowed_tables_snapshot(self) -> Sequence[str]: ...

    def identify_table_from_row_dict(self, row_dict: MetadataRecord) -> str: ...

    def get_id_column(self, table: str) -> str: ...

    def check_for_intralink_table(self, table: str) -> bool: ...

    def get_interlinked_tables(self, table: str) -> Sequence[str]: ...

    def get_link_table_name(self, table1: str, table2: str) -> str | None: ...

    def get_column_base(self, table_name: str) -> str: ...

    def get_link_column(
        self,
        table1: str,
        table2: str,
        secondary_id_column: str,
    ) -> str: ...


@runtime_checkable
class MetadataReadSourceAPI(Protocol):
    """Small database-like read surface required by metadata hydrators."""

    driver_wrapper: MetadataDriverWrapperAPI

    def get_tables(self, force_refresh: bool = False) -> Sequence[str]: ...

    def get_tables_and_columns(self) -> MetadataTableColumns: ...

    def get_column_headings(self, table: str) -> set[str]: ...

    def get_row_from_id(self, table: str, row_id: int) -> SupportsRowMapping | None: ...

    def get_all_rows(
        self,
        table: str,
        iterator_return: bool = False,
    ) -> MetadataRowSequence: ...

    def get_record_count(self, table: str) -> int: ...

    def search(
        self,
        table: str,
        column: str,
        search_term: MetadataSearchTerm,
    ) -> MetadataRowSequence: ...

    def get_interlink_rows(
        self,
        primary_row: SupportsRowMapping,
        secondary_table: str,
    ) -> MetadataLinkRowSequence: ...

    def get_interlinked_rows(
        self,
        target_row: SupportsRowMapping,
        secondary_table: str,
        type_filter: str | None = None,
    ) -> MetadataRowSequence: ...

    def refresh(self) -> bool: ...


__all__ = [
    "MetadataDriverWrapperAPI",
    "MetadataLinkRow",
    "MetadataLinkRowSequence",
    "MetadataRowSequence",
    "MetadataReadSourceAPI",
    "MetadataSearchTerm",
    "MetadataTableColumns",
]

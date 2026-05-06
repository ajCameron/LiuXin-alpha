"""Read-source contract used by metadata hydrators."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Protocol


class MetadataReadSourceAPI(Protocol):
    """Small database-like read surface required by metadata hydrators."""

    driver_wrapper: Any

    def get_tables(self, force_refresh: bool = False) -> Sequence[str]: ...

    def get_tables_and_columns(self) -> Mapping[str, Sequence[str]]: ...

    def get_column_headings(self, table: str) -> set[str]: ...

    def get_row_from_id(self, table: str, row_id: int) -> Any | None: ...

    def search(self, table: str, column: str, search_term: Any) -> Sequence[Any]: ...

    def get_interlink_rows(self, primary_row: Any, secondary_table: str) -> Sequence[Any]: ...


__all__ = ["MetadataReadSourceAPI"]

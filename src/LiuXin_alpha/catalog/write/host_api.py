"""Leaf protocol implemented by Catalog for schema-driven writers.

Keeping this dependency inside the write layer prevents writer modules from
importing the high-level ``catalog.api`` package that constructs them.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

from LiuXin_alpha.catalog.write.column_update import CatalogColumnUpdate
from LiuXin_alpha.catalog.write.link_update import LinkUpdate
from LiuXin_alpha.catalog.write.owned_row_update import CatalogOwnedRowUpdate
from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI
from LiuXin_alpha.databases.db_types import SrcTableID
from LiuXin_alpha.databases.macro_types import LinkRow


class CatalogWriterHostAPI(Protocol):
    """Minimal Catalog surface required by schema-driven writers."""

    db: DatabaseAPI

    def write_link_update(
        self,
        update: LinkUpdate,
    ) -> Mapping[SrcTableID, tuple[LinkRow, ...]]: ...

    def write_column_update(
        self,
        update: CatalogColumnUpdate[object],
    ) -> Mapping[SrcTableID, object]: ...

    def write_owned_row_update(
        self,
        update: CatalogOwnedRowUpdate[object],
    ) -> Mapping[SrcTableID, tuple[LinkRow, ...]]: ...


__all__ = ["CatalogWriterHostAPI"]

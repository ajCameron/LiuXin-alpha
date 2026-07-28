"""
Facade API for the catalog layer.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from LiuXin_alpha.catalog.api.metadata_tools_api import CatalogMetadataToolsAPI

if TYPE_CHECKING:
    from collections.abc import Mapping

    from LiuXin_alpha.catalog.api.matching_api import CatalogMatchingAPI
    from LiuXin_alpha.catalog.api.mutations_api import CatalogMutationsAPI
    from LiuXin_alpha.catalog.api.repositories import CatalogRepositoriesAPI
    from LiuXin_alpha.catalog.api.retrieval import CatalogRetrievalAPI
    from LiuXin_alpha.catalog.write import (
        CatalogColumnUpdate,
        CatalogOwnedRowUpdate,
        LinkUpdate,
        SchemaCatalogWriter,
    )

    from LiuXin_alpha.databases.api.database_api import DatabaseAPI
    from LiuXin_alpha.databases.db_types import SrcTableID
    from LiuXin_alpha.databases.macro_types import LinkRow


@runtime_checkable
class CatalogAddinsAPI(CatalogMetadataToolsAPI, Protocol):
    """
    Top-level catalog facade.

    API shape mirrors `LiuXin_alpha.catalog` module shape: metadata tools,
    repositories, matching, retrieval, and mutations are separate areas behind
    one convenience object.
    """

    repositories: "CatalogRepositoriesAPI"
    matching: "CatalogMatchingAPI"
    retrieval: "CatalogRetrievalAPI"
    mutations: "CatalogMutationsAPI"


@runtime_checkable
class CatalogAPI(CatalogAddinsAPI, Protocol):
    """
    Structural API for the metadata-aware facade over a database handle.
    """

    db: "DatabaseAPI"

    def create_writer(
        self,
        src_table: str,
        dst_column: str,
        *,
        force_refresh: bool = False,
        destination_owned: bool | None = None,
    ) -> "SchemaCatalogWriter":
        """Create a schema-backed writer for one catalog field."""

        ...

    def write(
        self,
        src_table: str,
        dst_column: str,
        *args: Any,
        force_refresh: bool = False,
        destination_owned: bool | None = None,
        **kwargs: Any,
    ) -> "Mapping[SrcTableID, object]":
        """Create a writer and apply one bulk catalog update."""

        ...

    # Todo: Might be a good idea to cache this writer....
    def write_one(
        self,
        src_table: str,
        dst_column: str,
        src_id: "SrcTableID",
        dst_value: object,
        *,
        force_refresh: bool = False,
        destination_owned: bool | None = None,
        **kwargs: Any,
    ) -> "Mapping[SrcTableID, object]":
        """
        Create a writer and apply one source/value instruction.

        :param src_table:
        :param dst_column:
        :param src_id:
        :param dst_value:
        :param force_refresh:
        :param destination_owned:
        :param kwargs:
        :return:
        """

        ...

    def write_link_update(
        self,
        update: "LinkUpdate",
    ) -> "Mapping[SrcTableID, tuple[LinkRow, ...]]":
        """
        Apply a normalized link update through this catalog's database.

        :param update:
        :return:
        """

        ...

    def write_column_update(
        self,
        update: "CatalogColumnUpdate[object]",
    ) -> "Mapping[SrcTableID, object]":
        """
        Apply a normalized same-table column update.

        :param update:
        :return:
        """

        ...

    def write_owned_row_update(
        self,
        update: "CatalogOwnedRowUpdate[object]",
    ) -> "Mapping[SrcTableID, tuple[LinkRow, ...]]":
        """
        Apply a normalized owned one-to-one destination-row update.

        :param update:
        :return:
        """

        ...

"""
Facade API for the catalog layer.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable, TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping

    from LiuXin_alpha.catalog.api.matching_api import CatalogMatchingAPI
    from LiuXin_alpha.catalog.api.mutations_api import CatalogMutationsAPI
    from LiuXin_alpha.catalog.api.repositories import CatalogRepositoriesAPI
    from LiuXin_alpha.catalog.api.retrieval import CatalogRetrievalAPI
    from LiuXin_alpha.catalog.write import LinkUpdate

    from LiuXin_alpha.databases.api.database_api import DatabaseAPI
    from LiuXin_alpha.databases.db_types import SrcTableID
    from LiuXin_alpha.databases.macro_types import LinkRow

@runtime_checkable
class CatalogAddinsAPI(Protocol):
    """
    Top-level catalog facade.

    API shape mirrors `LiuXin_alpha.catalog` module shape: repositories, matching,
    retrieval, and mutations are separate areas behind one convenience object.
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

    def write_link_update(
        self,
        update: "LinkUpdate",
    ) -> "Mapping[SrcTableID, tuple[LinkRow, ...]]":
        """Apply a normalized link update through this catalog's database."""

        ...

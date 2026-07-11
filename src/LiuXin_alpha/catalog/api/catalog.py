"""
Facade API for the catalog layer.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable, TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.catalog.api.matching_api import CatalogMatchingAPI
    from LiuXin_alpha.catalog.api.mutations_api import CatalogMutationsAPI
    from LiuXin_alpha.catalog.api.repositories import CatalogRepositoriesAPI
    from LiuXin_alpha.catalog.api.retrieval import CatalogRetrievalAPI

    from LiuXin_alpha.databases.api.database_api import DatabaseAPI

    from LiuXin_alpha.catalog.api.metadata_tools_api.add_api import AddAPI
    from LiuXin_alpha.catalog.api.metadata_tools_api.ensure_api import EnsureAPI
    from LiuXin_alpha.catalog.api.metadata_tools_api.apply_api import ApplyAPI




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
class CatalogAPI(CatalogAddinsAPI, DatabaseAPI):
    """
    API for the catalog - a database with metadata manipulation plugins.
    """
    add: "AddAPI"
    ensure: "EnsureAPI"
    apply: "ApplyAPI"

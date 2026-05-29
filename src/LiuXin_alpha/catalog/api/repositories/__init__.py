"""
Repository API contracts for catalog entities.
"""

from __future__ import annotations

from LiuXin_alpha.catalog.api.repositories.agents import AgentRepositoryAPI
from LiuXin_alpha.catalog.api.repositories.base import BaseRepositoryAPI
from LiuXin_alpha.catalog.api.repositories.expressions import ExpressionRepositoryAPI
from LiuXin_alpha.catalog.api.repositories.identifiers import IdentifierRepositoryAPI
from LiuXin_alpha.catalog.api.repositories.items import ItemRepositoryAPI
from LiuXin_alpha.catalog.api.repositories.manifestations import ManifestationRepositoryAPI
from LiuXin_alpha.catalog.api.repositories.notes import NoteRepositoryAPI
from LiuXin_alpha.catalog.api.repositories.titles import TitleRepositoryAPI
from LiuXin_alpha.catalog.api.repositories.works import WorkRepositoryAPI


class CatalogRepositoriesAPI(
    WorkRepositoryAPI,
    ExpressionRepositoryAPI,
    ManifestationRepositoryAPI,
    ItemRepositoryAPI,
    AgentRepositoryAPI,
    IdentifierRepositoryAPI,
    TitleRepositoryAPI,
    NoteRepositoryAPI,
):
    """
    Marker protocol group for repository API imports.

    The implementation facade does not need to inherit from this directly; it just
    needs to expose matching attributes.
    """


__all__ = [
    "AgentRepositoryAPI",
    "BaseRepositoryAPI",
    "CatalogRepositoriesAPI",
    "ExpressionRepositoryAPI",
    "IdentifierRepositoryAPI",
    "ItemRepositoryAPI",
    "ManifestationRepositoryAPI",
    "NoteRepositoryAPI",
    "TitleRepositoryAPI",
    "WorkRepositoryAPI",
]

"""Repository contracts for Catalog entities and their relationships.

Use repositories for direct entity persistence, deterministic relationship
traversal, and convenience identity operations::

    work_id = catalog.repositories.works.create({"title": "Frankenstein"})
    expression_id = catalog.expressions.match_or_create(
        work_id,
        MetadataCandidate({"language_id": english_id}),
    )

The shorter attributes on ``Catalog`` and the grouped attributes under
``catalog.repositories`` refer to the same repository instances.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from LiuXin_alpha.catalog.api.repositories.agents import AgentRepositoryAPI
from LiuXin_alpha.catalog.api.repositories.base import BaseRepositoryAPI
from LiuXin_alpha.catalog.api.repositories.expressions import ExpressionRepositoryAPI
from LiuXin_alpha.catalog.api.repositories.exact_entity import ExactEntityRepositoryAPI
from LiuXin_alpha.catalog.api.repositories.identifiers import IdentifierRepositoryAPI
from LiuXin_alpha.catalog.api.repositories.items import ItemRepositoryAPI
from LiuXin_alpha.catalog.api.repositories.item_identifiers import (
    ItemIdentifierRepositoryAPI,
)
from LiuXin_alpha.catalog.api.repositories.manifestations import ManifestationRepositoryAPI
from LiuXin_alpha.catalog.api.repositories.notes import NoteRepositoryAPI
from LiuXin_alpha.catalog.api.repositories.titles import TitleRepositoryAPI
from LiuXin_alpha.catalog.api.repositories.works import WorkRepositoryAPI


@runtime_checkable
class CatalogRepositoriesAPI(Protocol):
    """
    Grouped repository surface exposed by the catalog facade.

    The attributes deliberately compose repository protocols instead of
    inheriting them: the group is not itself a Work, Agent, or Note repository.

    WEMI repositories represent the bibliographic chain. Agent, Identifier,
    Title, and Note repositories own their corresponding relationships.
    Exact-entity repositories cover reusable value entities such as tags,
    genres, languages, and ratings.
    """

    works: WorkRepositoryAPI
    expressions: ExpressionRepositoryAPI
    manifestations: ManifestationRepositoryAPI
    items: ItemRepositoryAPI
    agents: AgentRepositoryAPI
    identifiers: IdentifierRepositoryAPI
    item_identifiers: ItemIdentifierRepositoryAPI
    titles: TitleRepositoryAPI
    notes: NoteRepositoryAPI
    tags: ExactEntityRepositoryAPI
    labels: ExactEntityRepositoryAPI
    genres: ExactEntityRepositoryAPI
    subjects: ExactEntityRepositoryAPI
    series: ExactEntityRepositoryAPI
    languages: ExactEntityRepositoryAPI
    ratings: ExactEntityRepositoryAPI
    comments: ExactEntityRepositoryAPI
    synopses: ExactEntityRepositoryAPI
    annotations: ExactEntityRepositoryAPI


__all__ = [
    "AgentRepositoryAPI",
    "BaseRepositoryAPI",
    "CatalogRepositoriesAPI",
    "ExpressionRepositoryAPI",
    "ExactEntityRepositoryAPI",
    "IdentifierRepositoryAPI",
    "ItemRepositoryAPI",
    "ItemIdentifierRepositoryAPI",
    "ManifestationRepositoryAPI",
    "NoteRepositoryAPI",
    "TitleRepositoryAPI",
    "WorkRepositoryAPI",
]

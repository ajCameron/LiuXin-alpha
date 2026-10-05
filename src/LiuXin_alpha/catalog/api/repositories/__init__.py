"""
Export entity repository protocols and declare the grouped Catalog repository API.

CATALOG_REPOSITORY_NAMES is the ordered tuple of nineteen public group attributes,
used by concrete name lookup. CatalogRepositoriesAPI composes those contracts;
BaseRepositoryAPI supplies common CRUD and specialized protocols add semantic
operations. The concrete Catalog shortcuts refer to its group's existing instances.
This module imports/declaratively groups APIs without creating a database or
repository instance, and it does not enforce runtime transaction behavior.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from LiuXin_alpha.catalog.api.repositories.agents import AgentRepositoryAPI
from LiuXin_alpha.catalog.api.repositories.annotations import AnnotationRepositoryAPI
from LiuXin_alpha.catalog.api.repositories.base import BaseRepositoryAPI
from LiuXin_alpha.catalog.api.repositories.comments import CommentRepositoryAPI
from LiuXin_alpha.catalog.api.repositories.expressions import ExpressionRepositoryAPI
from LiuXin_alpha.catalog.api.repositories.exact_entity import ExactEntityRepositoryAPI
from LiuXin_alpha.catalog.api.repositories.identifiers import IdentifierRepositoryAPI
from LiuXin_alpha.catalog.api.repositories.items import ItemRepositoryAPI
from LiuXin_alpha.catalog.api.repositories.item_identifiers import (
    ItemIdentifierRepositoryAPI,
)
from LiuXin_alpha.catalog.api.repositories.manifestations import ManifestationRepositoryAPI
from LiuXin_alpha.catalog.api.repositories.notes import NoteRepositoryAPI
from LiuXin_alpha.catalog.api.repositories.synopses import SynopsisRepositoryAPI
from LiuXin_alpha.catalog.api.repositories.titles import TitleRepositoryAPI
from LiuXin_alpha.catalog.api.repositories.works import WorkRepositoryAPI

CATALOG_REPOSITORY_NAMES = (
    "works",
    "expressions",
    "manifestations",
    "items",
    "agents",
    "identifiers",
    "item_identifiers",
    "titles",
    "notes",
    "tags",
    "labels",
    "genres",
    "subjects",
    "series",
    "languages",
    "ratings",
    "comments",
    "synopses",
    "annotations",
)


@runtime_checkable
class CatalogRepositoriesAPI(Protocol):
    """
    Group the nineteen Catalog repository contracts behind named attributes.

    The concrete Catalog exposes the same repository instances through its shortcuts and
    catalog.repositories. This group composes entity APIs rather than inheriting CRUD itself; writes
    and policy decisions remain with those owners. Languages are read-only at the repository layer,
    and Comment/Annotation generic reuse is disabled despite their shared exact-entity interface.

    Runtime protocol checks verify member presence, not signatures, database schema, member
    semantics, or transaction guarantees. for_name provides normalized public name lookup without
    constructing a repository.

    Example:
        >>> repositories: CatalogRepositoriesAPI = catalog.repositories  # doctest: +SKIP
        >>> repositories.for_name("work") is catalog.works  # doctest: +SKIP
        True


    :ivar works: Work metadata, title lookup, and global Work matching.
    :ivar expressions: Expression metadata and Work-scoped relationships/matching.
    :ivar manifestations: Publication/edition metadata and Expression-scoped relationships/matching.
    :ivar items: Copy metadata, direct Manifestation ownership, and Item bundle convenience.
    :ivar agents: Agent identity, specialized creation, and WEMI credits.
    :ivar identifiers: Curated bibliographic identifiers and their semantic ownership.
    :ivar item_identifiers: Raw identifier observations attached to Items.
    :ivar titles: WEMI title projection and update conveniences.
    :ivar notes: Reusable Note rows and WEMI attachment operations.
    :ivar tags: Reusable Tag identities and storage.
    :ivar labels: Operational Label identities and storage.
    :ivar genres: Hierarchical Genre identities with optional parent scope.
    :ivar subjects: Hierarchical Subject identities with optional parent scope.
    :ivar series: Series identities and optional parent scope.
    :ivar languages: Read-only seeded Language lookups through the repository API.
    :ivar ratings: Rating value/scale/source identity and storage.
    :ivar comments: Contextual Comment creation, replacement, and inspection without generic reuse.
    :ivar synopses: Reusable Synopsis identities and fresh contextual attachments.
    :ivar annotations: Item-scoped Annotation listing and identity inspection without generic reuse.
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
    comments: CommentRepositoryAPI
    synopses: SynopsisRepositoryAPI
    annotations: AnnotationRepositoryAPI

    def for_name(self, repository_name: str) -> BaseRepositoryAPI:
        """
        Return the existing repository registered under a normalized public name.

        The concrete group requires a string, strips surrounding whitespace, case-folds, and
        replaces hyphens with underscores. It then maps the fixed singular aliases to registered
        names and returns the corresponding attribute. Item-Identifier therefore selects
        item_identifiers, and synopsis selects synopses; series is already registered unchanged.
        Embedded spaces are not converted, and legacy names such as books are not aliases.

        Registry membership controls lookup rather than arbitrary attribute access. No repository
        construction, database query, or mutation occurs. A malformed concrete group can still fail
        attribute access after its name is accepted.

        Example:
            >>> repository = catalog.repositories.for_name(" Item-Identifier ")  # doctest: +SKIP
            >>> repository is catalog.item_identifiers  # doctest: +SKIP
            True


        :param repository_name: Singular alias or registered plural name, allowing surrounding whitespace and hyphens.
        :return: The existing repository instance stored under the resolved public name.
        :raises TypeError: If repository_name is not a string.
        :raises KeyError: If the normalized/aliased name is outside CATALOG_REPOSITORY_NAMES.
        :raises AttributeError: If a malformed concrete group lacks an accepted registered attribute.
        """


__all__ = [
    "AgentRepositoryAPI",
    "AnnotationRepositoryAPI",
    "BaseRepositoryAPI",
    "CATALOG_REPOSITORY_NAMES",
    "CatalogRepositoriesAPI",
    "CommentRepositoryAPI",
    "ExpressionRepositoryAPI",
    "ExactEntityRepositoryAPI",
    "IdentifierRepositoryAPI",
    "ItemRepositoryAPI",
    "ItemIdentifierRepositoryAPI",
    "ManifestationRepositoryAPI",
    "NoteRepositoryAPI",
    "SynopsisRepositoryAPI",
    "TitleRepositoryAPI",
    "WorkRepositoryAPI",
]

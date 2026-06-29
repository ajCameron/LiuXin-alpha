"""
Top-level catalog facade implementation scaffold.
"""

from __future__ import annotations

from dataclasses import dataclass

from LiuXin_alpha.catalog.api.common import DatabaseHandle
from LiuXin_alpha.catalog.matching import CatalogMatching
from LiuXin_alpha.catalog.repositories import (
    AgentRepository,
    ExpressionRepository,
    IdentifierRepository,
    ItemRepository,
    ManifestationRepository,
    NoteRepository,
    TitleRepository,
    WorkRepository,
)
from .mutations import CatalogMutations
from .retrieval import CatalogRetrieval


@dataclass(slots=True)
class CatalogRepositories:
    """Grouped repository implementations exposed by `Catalog.repositories`."""

    works: WorkRepository
    expressions: ExpressionRepository
    manifestations: ManifestationRepository
    items: ItemRepository
    agents: AgentRepository
    identifiers: IdentifierRepository
    titles: TitleRepository
    notes: NoteRepository


class Catalog:
    """Metadata-aware facade over a raw database handle.

    This object is where callers should enter the catalog layer. It should remain
    a composition root, not a God object: substantive behavior belongs in the
    repository, matcher, retrieval, and mutation modules.
    """

    def __init__(self, db: DatabaseHandle) -> None:
        self.db = db
        self.repositories = CatalogRepositories(
            works=WorkRepository(db),
            expressions=ExpressionRepository(db),
            manifestations=ManifestationRepository(db),
            items=ItemRepository(db),
            agents=AgentRepository(db),
            identifiers=IdentifierRepository(db),
            titles=TitleRepository(db),
            notes=NoteRepository(db),
        )
        self.matching = CatalogMatching(db=db, repositories=self.repositories)
        self.retrieval = CatalogRetrieval(db=db, repositories=self.repositories)
        self.mutations = CatalogMutations(db=db, repositories=self.repositories)

    @property
    def works(self) -> WorkRepository:
        """Convenience alias for `catalog.repositories.works`."""
        return self.repositories.works

    @property
    def expressions(self) -> ExpressionRepository:
        """Convenience alias for `catalog.repositories.expressions`."""
        return self.repositories.expressions

    @property
    def manifestations(self) -> ManifestationRepository:
        """Convenience alias for `catalog.repositories.manifestations`."""
        return self.repositories.manifestations

    @property
    def items(self) -> ItemRepository:
        """Convenience alias for `catalog.repositories.items`."""
        return self.repositories.items

    @property
    def agents(self) -> AgentRepository:
        """Convenience alias for `catalog.repositories.agents`."""
        return self.repositories.agents

    @property
    def identifiers(self) -> IdentifierRepository:
        """Convenience alias for `catalog.repositories.identifiers`."""
        return self.repositories.identifiers

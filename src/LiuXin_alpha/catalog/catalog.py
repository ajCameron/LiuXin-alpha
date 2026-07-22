"""
Top-level catalog facade implementation scaffold.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

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
from LiuXin_alpha.catalog.mutations import CatalogMutations
from LiuXin_alpha.catalog.retrieval import CatalogRetrieval
from LiuXin_alpha.catalog.write import (
    CatalogColumnUpdate,
    CatalogOwnedRowUpdate,
    LinkUpdate,
)

if TYPE_CHECKING:
    from LiuXin_alpha.databases.db_types import SrcTableID
    from LiuXin_alpha.databases.macro_types import LinkRow


@dataclass(slots=True)
class CatalogRepositories:
    """
    Grouped repository implementations exposed by `Catalog.repositories`.
    """

    works: WorkRepository
    expressions: ExpressionRepository
    manifestations: ManifestationRepository
    items: ItemRepository
    agents: AgentRepository
    identifiers: IdentifierRepository
    titles: TitleRepository
    notes: NoteRepository


class Catalog:
    """
    Metadata-aware facade over a raw database handle.

    This object is where callers should enter the catalog layer. It should remain
    a composition root, not a God object: substantive behavior belongs in the
    repository, matcher, retrieval, and mutation modules.
    """

    def __init__(self, db: DatabaseHandle) -> None:
        """
        Constructor.

        :param db:
        """
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

    def write_link_update(
        self,
        update: LinkUpdate,
    ) -> Mapping[SrcTableID, tuple[LinkRow, ...]]:
        """Apply a normalized link update through the catalog database.

        The update retains its replacement, incremental-composition, type-scope,
        atomicity, and empty-update semantics. The returned mapping contains the
        complete link rows written for each affected source id.
        """

        if not isinstance(update, LinkUpdate):
            raise TypeError("update must be a LinkUpdate")
        return update.write(self.db.macros)

    def write_column_update(
        self,
        update: CatalogColumnUpdate[object],
    ) -> Mapping[SrcTableID, object]:
        """
        Apply a normalized same-table column update.

        :param update: Immutable normalized column update.
        :return: Stable written values keyed by source-table ID.
        """

        if not isinstance(update, CatalogColumnUpdate):
            raise TypeError("update must be a CatalogColumnUpdate")
        return update.write(self.db)

    def write_owned_row_update(
        self,
        update: CatalogOwnedRowUpdate[object],
    ) -> Mapping[SrcTableID, tuple[LinkRow, ...]]:
        """
        Apply a normalized owned one-to-one destination-row update.

        :param update: Immutable normalized owned-row update.
        :return: Complete link rows keyed by affected source-table ID.
        """

        if not isinstance(update, CatalogOwnedRowUpdate):
            raise TypeError("update must be a CatalogOwnedRowUpdate")
        return update.write(self.db.macros)

    @property
    def works(self) -> WorkRepository:
        """
        Convenience alias for `catalog.repositories.works`.
        """
        return self.repositories.works

    @property
    def expressions(self) -> ExpressionRepository:
        """
        Convenience alias for `catalog.repositories.expressions`.
        """
        return self.repositories.expressions

    @property
    def manifestations(self) -> ManifestationRepository:
        """
        Convenience alias for `catalog.repositories.manifestations`.
        """
        return self.repositories.manifestations

    @property
    def items(self) -> ItemRepository:
        """
        Convenience alias for `catalog.repositories.items`.
        """
        return self.repositories.items

    @property
    def agents(self) -> AgentRepository:
        """
        Convenience alias for `catalog.repositories.agents`.
        """
        return self.repositories.agents

    @property
    def identifiers(self) -> IdentifierRepository:
        """
        Convenience alias for `catalog.repositories.identifiers`.
        """
        return self.repositories.identifiers

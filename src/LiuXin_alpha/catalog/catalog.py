"""
Top-level catalog facade implementation scaffold.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from LiuXin_alpha.catalog.api import CatalogAPI
from LiuXin_alpha.catalog.api.common import DatabaseHandle
from LiuXin_alpha.catalog.matching import (
    DEFAULT_MATCHING_POLICY,
    CatalogMatching,
    MatchingPolicy,
)
from LiuXin_alpha.catalog.repositories import (
    AgentRepository,
    AnnotationRepository,
    CommentRepository,
    ExpressionRepository,
    GenreRepository,
    IdentifierRepository,
    ItemRepository,
    ItemIdentifierRepository,
    LabelRepository,
    LanguageRepository,
    ManifestationRepository,
    NoteRepository,
    RatingRepository,
    SeriesRepository,
    SubjectRepository,
    SynopsisRepository,
    TagRepository,
    TitleRepository,
    WorkRepository,
)
from LiuXin_alpha.catalog.mutations import CatalogMutations
from LiuXin_alpha.catalog.retrieval import CatalogRetrieval
from LiuXin_alpha.catalog.write import (
    CatalogColumnUpdate,
    CatalogOwnedRowUpdate,
    LinkUpdate,
    SchemaCatalogWriter,
    create_catalog_writer,
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
    item_identifiers: ItemIdentifierRepository
    titles: TitleRepository
    notes: NoteRepository
    tags: TagRepository
    labels: LabelRepository
    genres: GenreRepository
    subjects: SubjectRepository
    series: SeriesRepository
    languages: LanguageRepository
    ratings: RatingRepository
    comments: CommentRepository
    synopses: SynopsisRepository
    annotations: AnnotationRepository


class Catalog:
    """
    Metadata-aware facade over a raw database handle.

    This object is where callers should enter the catalog layer. It should remain
    a composition root, not a God object: substantive behavior belongs in the
    repository, matcher, retrieval, and mutation modules.
    """

    def __init__(
        self,
        db: DatabaseHandle,
        *,
        matching_policy: MatchingPolicy = DEFAULT_MATCHING_POLICY,
    ) -> None:
        """
        Constructor.

        :param db: Database handle used by catalog services.
        :param matching_policy: Identity policy shared by repository and grouped
            matching entry points.
        """
        if not isinstance(matching_policy, MatchingPolicy):
            raise TypeError("matching_policy must be a MatchingPolicy")
        self.db = db
        self.repositories = CatalogRepositories(
            works=WorkRepository(db),
            expressions=ExpressionRepository(db),
            manifestations=ManifestationRepository(db),
            items=ItemRepository(db),
            agents=AgentRepository(db),
            identifiers=IdentifierRepository(db),
            item_identifiers=ItemIdentifierRepository(db),
            titles=TitleRepository(db),
            notes=NoteRepository(db),
            tags=TagRepository(db),
            labels=LabelRepository(db),
            genres=GenreRepository(db),
            subjects=SubjectRepository(db),
            series=SeriesRepository(db),
            languages=LanguageRepository(db),
            ratings=RatingRepository(db),
            comments=CommentRepository(db),
            synopses=SynopsisRepository(db),
            annotations=AnnotationRepository(db),
        )
        for repository in (
            self.repositories.works,
            self.repositories.expressions,
            self.repositories.manifestations,
            self.repositories.items,
            self.repositories.agents,
            self.repositories.identifiers,
            self.repositories.item_identifiers,
            self.repositories.titles,
            self.repositories.notes,
            self.repositories.tags,
            self.repositories.labels,
            self.repositories.genres,
            self.repositories.subjects,
            self.repositories.series,
            self.repositories.languages,
            self.repositories.ratings,
            self.repositories.comments,
            self.repositories.synopses,
            self.repositories.annotations,
        ):
            repository.bind_repositories(self.repositories)
            repository.bind_matching_policy(matching_policy)
        self.matching = CatalogMatching(
            db=db,
            repositories=self.repositories,
            policy=matching_policy,
        )
        self.retrieval = CatalogRetrieval(db=db, repositories=self.repositories)
        self.mutations = CatalogMutations(db=db, repositories=self.repositories)

    def create_writer(
        self,
        src_table: str,
        dst_column: str,
        *,
        force_refresh: bool = False,
        destination_owned: bool | None = None,
    ) -> SchemaCatalogWriter:
        """
        Create a schema-backed writer for one catalog field.

        The schema factory selects a same-table column writer, an owned
        one-to-one writer, or a shared-value link writer. The returned writer
        retains its build, inspection, bulk, and single-value methods.

        :param src_table: Table whose row IDs key writer updates.
        :param dst_column: Same-table or linked destination value column.
        :param force_refresh: Refresh schema discovery before construction.
        :param destination_owned: Optional one-to-one ownership override.
        :return: Concrete catalog writer for the resolved storage shape.
        """

        return create_catalog_writer(
            cast(CatalogAPI, self),
            src_table,
            dst_column,
            force_refresh=force_refresh,
            destination_owned=destination_owned,
        )

    def write(
        self,
        src_table: str,
        dst_column: str,
        *args: Any,
        force_refresh: bool = False,
        destination_owned: bool | None = None,
        **kwargs: Any,
    ) -> Mapping[SrcTableID, object]:
        """
        Create a writer and apply one bulk catalog update.

        Positional and keyword update arguments are passed unchanged to the
        selected writer, preserving scalar, replacement, incremental,
        typed-map, rich-link, and link-type-scope forms.

        :param src_table: Table whose row IDs key writer updates.
        :param dst_column: Same-table or linked destination value column.
        :param args: Positional arguments for the concrete writer.
        :param force_refresh: Refresh schema discovery before construction.
        :param destination_owned: Optional one-to-one ownership override.
        :param kwargs: Keyword arguments for the concrete writer.
        :return: Concrete writer result mapping.
        """

        writer = self.create_writer(
            src_table,
            dst_column,
            force_refresh=force_refresh,
            destination_owned=destination_owned,
        )
        return writer.write(*args, **kwargs)

    def write_one(
        self,
        src_table: str,
        dst_column: str,
        src_id: SrcTableID,
        dst_value: object,
        *,
        force_refresh: bool = False,
        destination_owned: bool | None = None,
        **kwargs: Any,
    ) -> Mapping[SrcTableID, object]:
        """
        Create a writer and apply one source/value catalog instruction.

        :param src_table: Table containing the source ID.
        :param dst_column: Same-table or linked destination value column.
        :param src_id: Source-table ID whose value or links should change.
        :param dst_value: Raw, resolved, rich, or clear destination value.
        :param force_refresh: Refresh schema discovery before construction.
        :param destination_owned: Optional one-to-one ownership override.
        :param kwargs: Options for the concrete writer, including link type.
        :return: Concrete writer result mapping without unwrapping it.
        """

        writer = self.create_writer(
            src_table,
            dst_column,
            force_refresh=force_refresh,
            destination_owned=destination_owned,
        )
        return writer.write_one(src_id, dst_value, **kwargs)

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

    @property
    def item_identifiers(self) -> ItemIdentifierRepository:
        """Return the observed Item identifier repository."""

        return self.repositories.item_identifiers

    @property
    def titles(self) -> TitleRepository:
        """Return the logical title repository."""

        return self.repositories.titles

    @property
    def notes(self) -> NoteRepository:
        """Return the note repository."""

        return self.repositories.notes

    @property
    def tags(self) -> TagRepository:
        """Return the exact-default Tag repository."""

        return self.repositories.tags

    @property
    def labels(self) -> LabelRepository:
        """Return the exact-default Label repository."""

        return self.repositories.labels

    @property
    def genres(self) -> GenreRepository:
        """Return the exact-default Genre repository."""

        return self.repositories.genres

    @property
    def subjects(self) -> SubjectRepository:
        """Return the exact-default Subject repository."""

        return self.repositories.subjects

    @property
    def series(self) -> SeriesRepository:
        """Return the exact-default Series repository."""

        return self.repositories.series

    @property
    def languages(self) -> LanguageRepository:
        """Return the exact-default immutable Language repository."""

        return self.repositories.languages

    @property
    def ratings(self) -> RatingRepository:
        """Return the exact-default Rating repository."""

        return self.repositories.ratings

    @property
    def comments(self) -> CommentRepository:
        """Return the read-only-match Comment repository."""

        return self.repositories.comments

    @property
    def synopses(self) -> SynopsisRepository:
        """Return the exact-default Synopsis repository."""

        return self.repositories.synopses

    @property
    def annotations(self) -> AnnotationRepository:
        """Return the item-scoped Annotation repository."""

        return self.repositories.annotations

"""
Compose concrete Catalog owners and expose their shared repository shortcuts.

Catalog retains a borrowed database, builds legacy metadata tools and nineteen
entity repositories, and wires matching, retrieval, and mutation services around
them. The facade delegates schema-driven writes to writer owners and normalized
update objects. It adds no database lifecycle management; individual mutation
owners establish their documented transaction scope.
CatalogRepositories supplies the grouped instances and validated name lookup.

Example:
    >>> catalog = Catalog(db)  # doctest: +SKIP
    >>> catalog.repositories.for_name("work") is catalog.works  # doctest: +SKIP
    True
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, cast

from LiuXin_alpha.catalog.api.common import DatabaseHandle
from LiuXin_alpha.catalog.matching import (
    DEFAULT_MATCHING_POLICY,
    CatalogMatching,
    MatchingPolicy,
)
from LiuXin_alpha.catalog.metadata_tools import Add, Apply, Ensure, Intralinker
from LiuXin_alpha.catalog.mutations import CatalogMutations
from LiuXin_alpha.catalog.repositories import (
    AgentRepository,
    AnnotationRepository,
    CommentRepository,
    ExpressionRepository,
    GenreRepository,
    IdentifierRepository,
    ItemIdentifierRepository,
    ItemRepository,
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
from LiuXin_alpha.catalog.repositories.group import CatalogRepositories
from LiuXin_alpha.catalog.retrieval import CatalogRetrieval
from LiuXin_alpha.catalog.write import (
    CatalogColumnUpdate,
    CatalogOwnedRowUpdate,
    LinkUpdate,
    SchemaCatalogWriter,
    create_catalog_writer,
)
from LiuXin_alpha.catalog.write.host_api import CatalogWriterHostAPI

if TYPE_CHECKING:
    from LiuXin_alpha.databases.db_types import SrcTableID
    from LiuXin_alpha.databases.macro_types import LinkRow




class Catalog:
    """
    Compose metadata-aware repositories and services over a borrowed database.

    Repositories provide entity operations and matching conveniences; matching explains identity
    decisions, retrieval assembles WEMI read models, and mutations coordinate semantic writes. The
    add/ensure/apply/intralink tools retain legacy Row-based entry points. Substantive behavior
    belongs to these owners, with the facade providing composition, shortcuts, and writer dispatch.

    The caller owns database lifetime. Catalog has no close method or context manager, and
    constructing it does not establish that every database capability is available. Each operation
    relies on its delegated owner's dependencies.

    Example:
        >>> catalog = Catalog(db)  # doctest: +SKIP
        >>> catalog.works is catalog.repositories.works  # doctest: +SKIP
        True
        >>> bundle = catalog.retrieval.bundles.for_item(item_id)  # doctest: +SKIP


    :ivar db: Borrowed database shared by the composed services.
    :ivar repositories: Mutable group of concrete entity repository instances.
    :ivar matching: Matching services using the supplied identity policy.
    :ivar retrieval: WEMI traversal, bundle, graph, and projection services.
    :ivar mutations: Coordinated writes and mutation-policy services.
    :ivar add: Legacy metadata creation helpers sharing this Catalog's ensure/apply tools.
    :ivar ensure: Legacy get-or-create helpers bound to the same add tool.
    :ivar apply: Legacy metadata application helpers sharing add and ensure.
    :ivar intralink: Legacy same-table relationship helpers using the borrowed database.
    """

    def __init__(
        self,
        db: DatabaseHandle,
        *,
        matching_policy: MatchingPolicy = DEFAULT_MATCHING_POLICY,
    ) -> None:
        """
        Build repository and service instances around one database and matching policy.

        MatchingPolicy is type-checked before any fields are assigned. Metadata tools are
        constructed and cross-wired, then all nineteen repositories receive the shared repository
        group and policy. Matching, retrieval, and mutation services receive the same database and
        repository group. The database is retained without an upfront capability check; construction
        failures propagate without facade cleanup or ownership transfer.

        Example:
            >>> catalog = Catalog(db)  # doctest: +SKIP
            >>> catalog.add.ensure is catalog.ensure  # doctest: +SKIP
            True
            >>> catalog.db is db  # doctest: +SKIP
            True


        :param db: Borrowed database whose row, macro, and schema capabilities are needed by subsequent operations.
        :param matching_policy: MatchingPolicy shared by all repositories and grouped matchers; defaults to the package policy.
        :return: None after composing the services and their shared dependencies.
        :raises TypeError: If matching_policy is not a MatchingPolicy.
        :raises Exception: Errors from constructing or binding delegated owners propagate unchanged.
        """
        if not isinstance(matching_policy, MatchingPolicy):
            raise TypeError("matching_policy must be a MatchingPolicy")
        self.db = db
        metadata_db = cast(Any, db)
        self.add = Add(database=metadata_db)
        self.ensure = Ensure(database=metadata_db)
        self.apply = Apply(database=metadata_db)
        self.intralink = Intralinker(database=metadata_db)

        # Keep internal collaboration on the same Catalog-owned instances.
        self.add.ensure = self.ensure
        self.add.apply = self.apply
        self.ensure.add = self.add
        self.apply.add = self.add
        self.apply.ensure = self.ensure

        self.repositories = CatalogRepositories.from_database(db, matching_policy)
        self.add.repositories = self.repositories
        self.ensure.repositories = self.repositories
        self.apply.repositories = self.repositories
        self.intralink.repositories = self.repositories
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
        Resolve one schema column to a configured Catalog writer.

        A column on the source table selects a same-table writer. Otherwise the factory requires one
        destination table and a directed link from the source. Declared or overridden ownership
        selects an owned-row writer only for a one-to-one link; other destinations use a
        shared-value link writer. Names are exact schema names, not repository field aliases.
        Construction discovers schema and configures a new writer but does not apply a value update.

        Example:
            >>> writer = catalog.create_writer("works", "work_canonical_title")  # doctest: +SKIP
            >>> result = writer.write_one(work_id, "Frankenstein")  # doctest: +SKIP


        :param src_table: Exact schema name of the main table whose row IDs key the update.
        :param dst_column: Exact schema column name on the source table or a uniquely identified linked destination table.
        :param force_refresh: Forwarded to schema discovery to request a refresh before writer selection.
        :param destination_owned: None to use declared ownership, or a boolean override for a separate destination; ownership requires one-to-one cardinality.
        :return: A new same-table, owned-row, or shared-value link writer for the resolved route.
        :raises TypeError: If names, the ownership override, or schema-discovery dependencies have invalid types.
        :raises KeyError: If the source table or destination column cannot be found.
        :raises ValueError: If the source is not a writable main table, the destination is ambiguous/unlinked, or requested ownership is not one-to-one.
        """

        return create_catalog_writer(
            cast(CatalogWriterHostAPI, self),
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
        Select a schema writer and forward its bulk update arguments.

        This is the public advanced, schema-driven write path. Selection uses create_writer on each
        call. Positional and remaining keyword arguments pass
        unchanged to writer.write, so accepted replacement, incremental, rich-link, and type-scope
        forms depend on that writer. The facade adds no transaction, exception translation, or
        result conversion; execution and failure guarantees belong to the selected writer and
        database operation.

        Example:
            >>> result = catalog.write(  # doctest: +SKIP
            ...     "works", "work_canonical_title", {work_id: "Frankenstein"},
            ... )


        :param src_table: Exact schema name of the main table whose row IDs key the update.
        :param dst_column: Exact schema column name on the source table or a uniquely identified linked destination table.
        :param args: Positional bulk-update arguments accepted by the selected writer.
        :param force_refresh: Forwarded to schema discovery to request a refresh before writer selection.
        :param destination_owned: None to use declared ownership, or a boolean override for a separate destination; ownership requires one-to-one cardinality.
        :param kwargs: Remaining writer options, forwarded unchanged after factory options are consumed.
        :return: The selected writer's mapping of source IDs to values or link rows, returned unchanged.
        :raises Exception: Writer-selection, validation, and database failures propagate from the delegated operations.
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
        Select a schema writer and apply one source/value instruction.

        This is the public advanced, schema-driven single-value path. The facade forwards src_id,
        dst_value, and kwargs to writer.write_one on a newly selected
        writer. Scalar, collection, rich-link, and clear values have the meanings accepted by that
        writer. The returned mapping is not unwrapped; writer-specific validation, atomicity, and
        failure behavior are preserved.

        Example:
            >>> result = catalog.write_one(  # doctest: +SKIP
            ...     "works", "work_canonical_title", work_id, "Frankenstein",
            ... )


        :param src_table: Exact schema name of the main table whose row IDs key the update.
        :param dst_column: Exact schema column name on the source table or a uniquely identified linked destination table.
        :param src_id: Source-table row ID to update, validated by the selected writer.
        :param dst_value: Value or clear instruction in the concrete writer's supported form.
        :param force_refresh: Forwarded to schema discovery to request a refresh before writer selection.
        :param destination_owned: None to use declared ownership, or a boolean override for a separate destination; ownership requires one-to-one cardinality.
        :param kwargs: Additional writer options, such as link_type, forwarded unchanged.
        :return: The concrete writer's source-ID mapping, without extracting a single value.
        :raises Exception: Writer-selection, validation, and database failures propagate from the delegated operations.
        """

        writer = self.create_writer(
            src_table,
            dst_column,
            force_refresh=force_refresh,
            destination_owned=destination_owned,
        )
        return writer.write_one(src_id, dst_value, **kwargs)

    def _apply_link_update(
        self,
        update: LinkUpdate,
    ) -> Mapping[SrcTableID, tuple[LinkRow, ...]]:
        """
        Apply a normalized link instruction through the private writer-host seam.

        The concrete facade checks the update type, then calls update.write(db.macros).
        Replacement/incremental composition and type scope belong to LinkUpdate; its final
        replacement delegates atomic execution to the portable macro. Empty updates retain their
        no-write behavior, though the facade still accesses db.macros. No surrounding transaction or
        error translation is added.

        Example:
            A configured link writer calls this seam after building and validating
            its ``LinkUpdate``; application code calls the writer instead.

        :param update: LinkUpdate containing replacement or incremental instructions for a directed link specification.
        :return: Complete resulting link rows keyed by affected source ID, or the update's empty mapping.
        :raises TypeError: If update is not a LinkUpdate in the concrete facade.
        :raises Exception: Macro access, update composition, and database failures propagate unchanged.
        """

        if not isinstance(update, LinkUpdate):
            raise TypeError("update must be a LinkUpdate")
        return update.write(self.db.macros)

    def _apply_column_update(
        self,
        update: CatalogColumnUpdate[object],
    ) -> Mapping[SrcTableID, object]:
        """
        Apply a normalized column instruction through the private writer-host seam.

        Catalog-owned tables use their repositories, including partial-value
        validation, within one available macro transaction. Other schema tables
        retain their database adapter. The receipt preserves the requested values.

        Example:
            A configured column writer calls this seam after building and validating
            its ``CatalogColumnUpdate``; application code calls the writer instead.

        :param update: CatalogColumnUpdate containing table/column specifications and source-ID values.
        :return: The update's stable value mapping, returned after successful application or immediately when empty.
        :raises TypeError: If update is not a CatalogColumnUpdate in the concrete facade.
        :raises Exception: Validation or persistence failures propagate.
        """

        if not isinstance(update, CatalogColumnUpdate):
            raise TypeError("update must be a CatalogColumnUpdate")
        if not update.values:
            return update.values
        repository = self.repositories.for_table(update.table_spec.name)
        if repository is None:
            return update.write(self.db)
        repository._update_many({
            row_id: {update.column_spec.name: value}
            for row_id, value in update.values.items()
        })
        return update.values

    def _apply_owned_row_update(
        self,
        update: CatalogOwnedRowUpdate[object],
    ) -> Mapping[SrcTableID, tuple[LinkRow, ...]]:
        """
        Apply normalized owned values through the private writer-host seam.

        The concrete facade checks the update type and calls update.write(db.macros). Non-null
        values replace existing destination values or create and link a row; None removes the link
        while leaving the destination row for explicit cleanup. The update delegates atomic
        execution to the portable macro. Empty values perform no write, although the facade still
        resolves the macros attribute.

        Example:
            A configured owned-row writer calls this seam after building and validating
            its ``CatalogOwnedRowUpdate``; application code calls the writer instead.

        :param update: CatalogOwnedRowUpdate with a one-to-one link, destination column, and replacement values.
        :return: Complete resulting link rows keyed by affected source ID, or an empty mapping for no values.
        :raises TypeError: If update is not a CatalogOwnedRowUpdate in the concrete facade.
        :raises Exception: Macro access and database failures propagate without additional facade handling.
        """

        if not isinstance(update, CatalogOwnedRowUpdate):
            raise TypeError("update must be a CatalogOwnedRowUpdate")
        repository = self.repositories.for_table(update.destination_table.name)
        if repository is None:
            return update.write(self.db.macros)
        if not update.values:
            return {}
        from LiuXin_alpha.databases.macro_types import LinkValue
        from LiuXin_alpha.catalog.api.common import CatalogMutationError

        macros = self.db.macros
        result = {}
        with macros.transaction():
            for source_id, value in update.values.items():
                current = macros.get_link_rows(update.link_spec, source_id)
                if len(current) > 1:
                    raise CatalogMutationError(
                        f"one-to-one source {source_id!r} has multiple destinations"
                    )
                if value is None:
                    result[source_id] = macros.replace_links(update.link_spec, source_id, ())
                elif current:
                    repository.update(
                        current[0].secondary_id, {update.destination_column.name: value}
                    )
                    result[source_id] = tuple(current)
                else:
                    destination_id = repository.create({update.destination_column.name: value})
                    result[source_id] = macros.replace_links(
                        update.link_spec, source_id, (LinkValue(destination_id),)
                    )
        return result

    @property
    def works(self) -> WorkRepository:
        """
        Expose the repository for works representing intellectual creations.

        This shortcut returns the current repositories.works member. Reading the property performs
        no lookup, copy, or repository construction; entity operations and their validation remain
        the responsibility of that repository.

        Example:
            >>> catalog.works is catalog.repositories.works  # doctest: +SKIP
            True


        :return: The same repository instance held in repositories.works.
        """
        return self.repositories.works

    @property
    def expressions(self) -> ExpressionRepository:
        """
        Expose the repository for expressions representing realizations of Works.

        This shortcut returns the current repositories.expressions member. Reading the property
        performs no lookup, copy, or repository construction; entity operations and their validation
        remain the responsibility of that repository.

        Example:
            >>> catalog.expressions is catalog.repositories.expressions  # doctest: +SKIP
            True


        :return: The same repository instance held in repositories.expressions.
        """
        return self.repositories.expressions

    @property
    def manifestations(self) -> ManifestationRepository:
        """
        Expose the repository for manifestations representing publication embodiments.

        This shortcut returns the current repositories.manifestations member. Reading the property
        performs no lookup, copy, or repository construction; entity operations and their validation
        remain the responsibility of that repository.

        Example:
            >>> catalog.manifestations is catalog.repositories.manifestations  # doctest: +SKIP
            True


        :return: The same repository instance held in repositories.manifestations.
        """
        return self.repositories.manifestations

    @property
    def items(self) -> ItemRepository:
        """
        Expose the repository for items representing individual copies.

        This shortcut returns the current repositories.items member. Reading the property performs
        no lookup, copy, or repository construction; entity operations and their validation remain
        the responsibility of that repository.

        Example:
            >>> catalog.items is catalog.repositories.items  # doctest: +SKIP
            True


        :return: The same repository instance held in repositories.items.
        """
        return self.repositories.items

    @property
    def agents(self) -> AgentRepository:
        """
        Expose the repository for agents and their contributions to WEMI entities.

        This shortcut returns the current repositories.agents member. Reading the property performs
        no lookup, copy, or repository construction; entity operations and their validation remain
        the responsibility of that repository.

        Example:
            >>> catalog.agents is catalog.repositories.agents  # doctest: +SKIP
            True


        :return: The same repository instance held in repositories.agents.
        """
        return self.repositories.agents

    @property
    def identifiers(self) -> IdentifierRepository:
        """
        Expose the repository for scheme-aware entity identifiers and their WEMI links.

        This shortcut returns the current repositories.identifiers member. Reading the property
        performs no lookup, copy, or repository construction; entity operations and their validation
        remain the responsibility of that repository.

        Example:
            >>> catalog.identifiers is catalog.repositories.identifiers  # doctest: +SKIP
            True


        :return: The same repository instance held in repositories.identifiers.
        """
        return self.repositories.identifiers

    @property
    def item_identifiers(self) -> ItemIdentifierRepository:
        """
        Expose the repository for identifiers observed on individual Items.

        This shortcut returns the current repositories.item_identifiers member. Reading the property
        performs no lookup, copy, or repository construction; entity operations and their validation
        remain the responsibility of that repository.

        Example:
            >>> catalog.item_identifiers is catalog.repositories.item_identifiers  # doctest: +SKIP
            True


        :return: The same repository instance held in repositories.item_identifiers.
        """

        return self.repositories.item_identifiers

    @property
    def titles(self) -> TitleRepository:
        """
        Expose the repository for logical titles and their WEMI relationships.

        This shortcut returns the current repositories.titles member. Reading the property performs
        no lookup, copy, or repository construction; entity operations and their validation remain
        the responsibility of that repository.

        Example:
            >>> catalog.titles is catalog.repositories.titles  # doctest: +SKIP
            True


        :return: The same repository instance held in repositories.titles.
        """

        return self.repositories.titles

    @property
    def notes(self) -> NoteRepository:
        """
        Expose the repository for notes attached to WEMI entities.

        This shortcut returns the current repositories.notes member. Reading the property performs
        no lookup, copy, or repository construction; entity operations and their validation remain
        the responsibility of that repository.

        Example:
            >>> catalog.notes is catalog.repositories.notes  # doctest: +SKIP
            True


        :return: The same repository instance held in repositories.notes.
        """

        return self.repositories.notes

    @property
    def tags(self) -> TagRepository:
        """
        Expose the repository for reusable Tag values.

        This shortcut returns the current repositories.tags member. Reading the property performs no
        lookup, copy, or repository construction; entity operations and their validation remain the
        responsibility of that repository.

        Example:
            >>> catalog.tags is catalog.repositories.tags  # doctest: +SKIP
            True


        :return: The same repository instance held in repositories.tags.
        """

        return self.repositories.tags

    @property
    def labels(self) -> LabelRepository:
        """
        Expose the repository for reusable Label values.

        This shortcut returns the current repositories.labels member. Reading the property performs
        no lookup, copy, or repository construction; entity operations and their validation remain
        the responsibility of that repository.

        Example:
            >>> catalog.labels is catalog.repositories.labels  # doctest: +SKIP
            True


        :return: The same repository instance held in repositories.labels.
        """

        return self.repositories.labels

    @property
    def genres(self) -> GenreRepository:
        """
        Expose the repository for genre values and their entity relationships.

        This shortcut returns the current repositories.genres member. Reading the property performs
        no lookup, copy, or repository construction; entity operations and their validation remain
        the responsibility of that repository.

        Example:
            >>> catalog.genres is catalog.repositories.genres  # doctest: +SKIP
            True


        :return: The same repository instance held in repositories.genres.
        """

        return self.repositories.genres

    @property
    def subjects(self) -> SubjectRepository:
        """
        Expose the repository for subject values and their entity relationships.

        This shortcut returns the current repositories.subjects member. Reading the property
        performs no lookup, copy, or repository construction; entity operations and their validation
        remain the responsibility of that repository.

        Example:
            >>> catalog.subjects is catalog.repositories.subjects  # doctest: +SKIP
            True


        :return: The same repository instance held in repositories.subjects.
        """

        return self.repositories.subjects

    @property
    def series(self) -> SeriesRepository:
        """
        Expose the repository for series values and their entity relationships.

        This shortcut returns the current repositories.series member. Reading the property performs
        no lookup, copy, or repository construction; entity operations and their validation remain
        the responsibility of that repository.

        Example:
            >>> catalog.series is catalog.repositories.series  # doctest: +SKIP
            True


        :return: The same repository instance held in repositories.series.
        """

        return self.repositories.series

    @property
    def languages(self) -> LanguageRepository:
        """
        Expose the repository for language values and their entity relationships.

        This shortcut returns the current repositories.languages member. Reading the property
        performs no lookup, copy, or repository construction; entity operations and their validation
        remain the responsibility of that repository.

        Example:
            >>> catalog.languages is catalog.repositories.languages  # doctest: +SKIP
            True


        :return: The same repository instance held in repositories.languages.
        """

        return self.repositories.languages

    @property
    def ratings(self) -> RatingRepository:
        """
        Expose the repository for rating values and their entity relationships.

        This shortcut returns the current repositories.ratings member. Reading the property performs
        no lookup, copy, or repository construction; entity operations and their validation remain
        the responsibility of that repository.

        Example:
            >>> catalog.ratings is catalog.repositories.ratings  # doctest: +SKIP
            True


        :return: The same repository instance held in repositories.ratings.
        """

        return self.repositories.ratings

    @property
    def comments(self) -> CommentRepository:
        """
        Expose the repository for comment values and their entity relationships.

        This shortcut returns the current repositories.comments member. Reading the property
        performs no lookup, copy, or repository construction; entity operations and their validation
        remain the responsibility of that repository.

        Example:
            >>> catalog.comments is catalog.repositories.comments  # doctest: +SKIP
            True


        :return: The same repository instance held in repositories.comments.
        """

        return self.repositories.comments

    @property
    def synopses(self) -> SynopsisRepository:
        """
        Expose the repository for synopsis text and its entity relationships.

        This shortcut returns the current repositories.synopses member. Reading the property
        performs no lookup, copy, or repository construction; entity operations and their validation
        remain the responsibility of that repository.

        Example:
            >>> catalog.synopses is catalog.repositories.synopses  # doctest: +SKIP
            True


        :return: The same repository instance held in repositories.synopses.
        """

        return self.repositories.synopses

    @property
    def annotations(self) -> AnnotationRepository:
        """
        Expose the repository for annotations scoped to individual Items.

        This shortcut returns the current repositories.annotations member. Reading the property
        performs no lookup, copy, or repository construction; entity operations and their validation
        remain the responsibility of that repository.

        Example:
            >>> catalog.annotations is catalog.repositories.annotations  # doctest: +SKIP
            True


        :return: The same repository instance held in repositories.annotations.
        """

        return self.repositories.annotations

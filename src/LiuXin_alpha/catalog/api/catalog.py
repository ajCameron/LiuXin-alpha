"""
Structural contracts for the Catalog facade and its composed service groups.

CatalogAPI describes the database, entity repositories, and generic writer methods.
CatalogAddinsAPI groups repositories, matching, retrieval, and coordinated mutations
with inherited Row-oriented compatibility helpers. Protocol methods declare the
contract; the concrete implementation lives in LiuXin_alpha.catalog.catalog.
Runtime protocol checks establish attribute presence, not database readiness.

Example:
    >>> from LiuXin_alpha.catalog import Catalog
    >>> from LiuXin_alpha.catalog.api.common import MetadataCandidate
    >>> catalog: CatalogAPI = Catalog(db)  # doctest: +SKIP
    >>> work_id = catalog.works.match_or_create(  # doctest: +SKIP
    ...     MetadataCandidate({"title": "Frankenstein"}),
    ... )
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from LiuXin_alpha.catalog.api.metadata_tools_api import CatalogMetadataToolsAPI

if TYPE_CHECKING:
    from collections.abc import Mapping

    from LiuXin_alpha.catalog.api.matching_api import CatalogMatchingAPI
    from LiuXin_alpha.catalog.api.mutations_api import CatalogMutationsAPI
    from LiuXin_alpha.catalog.api.repositories import (
        AgentRepositoryAPI,
        AnnotationRepositoryAPI,
        CatalogRepositoriesAPI,
        CommentRepositoryAPI,
        ExactEntityRepositoryAPI,
        ExpressionRepositoryAPI,
        IdentifierRepositoryAPI,
        ItemIdentifierRepositoryAPI,
        ItemRepositoryAPI,
        ManifestationRepositoryAPI,
        NoteRepositoryAPI,
        SynopsisRepositoryAPI,
        TitleRepositoryAPI,
        WorkRepositoryAPI,
    )
    from LiuXin_alpha.catalog.api.retrieval import CatalogRetrievalAPI
    from LiuXin_alpha.catalog.write import SchemaCatalogWriter
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI
    from LiuXin_alpha.databases.db_types import SrcTableID


@runtime_checkable
class CatalogAddinsAPI(CatalogMetadataToolsAPI, Protocol):
    """
    Group the semantic Catalog services and inherited metadata compatibility tools.

    Repositories own entity operations, matching explains identity decisions, retrieval assembles
    read models, and mutations coordinate writes. Inherited add/ensure/apply/intralink attributes
    support workflows using legacy database Row objects. This runtime-checkable protocol tests
    attribute presence, not signatures, return types, database readiness, or behavioral conformance.

    Example:
        >>> isinstance(catalog, CatalogAddinsAPI)  # doctest: +SKIP
        True


    :ivar repositories: Grouped entity repository contracts.
    :ivar matching: Read-only identity-decision services.
    :ivar retrieval: WEMI traversal, bundle, graph, and projection services.
    :ivar mutations: Coordinated mutation and merge-policy services.
    """

    repositories: "CatalogRepositoriesAPI"
    matching: "CatalogMatchingAPI"
    retrieval: "CatalogRetrievalAPI"
    mutations: "CatalogMutationsAPI"


@runtime_checkable
class CatalogAPI(CatalogAddinsAPI, Protocol):
    """
    Describe the supported metadata-aware facade and its public write boundary.

    Annotate callers with this protocol and construct the concrete Catalog with an existing
    database. Grouped repositories and their convenience attributes provide entity-specific
    operations; ``create_writer``, ``write``, and ``write_one`` route advanced writes through
    schema-selected writers. Repository and mutation methods own semantic validation. Prepared
    update applicators are deliberately absent: they are private writer-host seams, not supported
    application APIs. The concrete facade borrows the database and has no close or context-manager
    lifecycle. Runtime isinstance checks inspect structural presence only and cannot prove
    operational compatibility.

    Example:
        >>> from LiuXin_alpha.catalog import Catalog
        >>> catalog: CatalogAPI = Catalog(db)  # doctest: +SKIP
        >>> work = catalog.works.require(work_id)  # doctest: +SKIP


    :ivar db: Database providing row, macro, and schema APIs required by the selected operation.
    :ivar works: Works representing intellectual creations; also available in repositories.
    :ivar expressions: Expressions representing realizations of Works; also available in repositories.
    :ivar manifestations: Manifestations representing publication embodiments; also available in repositories.
    :ivar items: Items representing individual copies; also available in repositories.
    :ivar agents: Agents and their contributions to WEMI entities; also available in repositories.
    :ivar identifiers: Scheme-aware entity identifiers and their WEMI links; also available in repositories.
    :ivar item_identifiers: Identifiers observed on individual Items; also available in repositories.
    :ivar titles: Logical titles and their WEMI relationships; also available in repositories.
    :ivar notes: Notes attached to WEMI entities; also available in repositories.
    :ivar tags: Reusable Tag values; also available in repositories.
    :ivar labels: Reusable Label values; also available in repositories.
    :ivar genres: Genre values and their entity relationships; also available in repositories.
    :ivar subjects: Subject values and their entity relationships; also available in repositories.
    :ivar series: Series values and their entity relationships; also available in repositories.
    :ivar languages: Language values and their entity relationships; also available in repositories.
    :ivar ratings: Rating values and their entity relationships; also available in repositories.
    :ivar comments: Comment values and their entity relationships; also available in repositories.
    :ivar synopses: Synopsis text and its entity relationships; also available in repositories.
    :ivar annotations: Annotations scoped to individual Items; also available in repositories.
    """

    db: "DatabaseAPI"
    works: "WorkRepositoryAPI"
    expressions: "ExpressionRepositoryAPI"
    manifestations: "ManifestationRepositoryAPI"
    items: "ItemRepositoryAPI"
    agents: "AgentRepositoryAPI"
    identifiers: "IdentifierRepositoryAPI"
    item_identifiers: "ItemIdentifierRepositoryAPI"
    titles: "TitleRepositoryAPI"
    notes: "NoteRepositoryAPI"
    tags: "ExactEntityRepositoryAPI"
    labels: "ExactEntityRepositoryAPI"
    genres: "ExactEntityRepositoryAPI"
    subjects: "ExactEntityRepositoryAPI"
    series: "ExactEntityRepositoryAPI"
    languages: "ExactEntityRepositoryAPI"
    ratings: "ExactEntityRepositoryAPI"
    comments: "CommentRepositoryAPI"
    synopses: "SynopsisRepositoryAPI"
    annotations: "AnnotationRepositoryAPI"

    def create_writer(
        self,
        src_table: str,
        dst_column: str,
        *,
        force_refresh: bool = False,
        destination_owned: bool | None = None,
    ) -> "SchemaCatalogWriter":
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

        ...

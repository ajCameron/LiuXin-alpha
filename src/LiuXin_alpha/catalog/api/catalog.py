"""Public facade contract for LiuXin's metadata-aware Catalog.

Application code normally instantiates :class:`LiuXin_alpha.catalog.Catalog`,
not this protocol.  :class:`CatalogAPI` describes the structural contract used
by callers, tests, and alternate implementations.

The facade has five deliberately separate areas:

``catalog.repositories`` (also exposed as ``catalog.works``, ``catalog.items``,
etc.)
    Entity CRUD, relationship traversal, and convenient ``match_or_create``
    operations.
``catalog.matching``
    Read-only identity decisions with evidence.  Use this when ambiguity or
    conflict must be presented to a person instead of raising immediately.
``catalog.retrieval``
    Coherent WEMI bundles and display-neutral projections.
``catalog.mutations``
    Coordinated multi-table writes and merge policy.
``catalog.add`` / ``ensure`` / ``apply`` / ``intralink``
    Compatibility metadata helpers which work with database ``Row`` objects.

Example::

    from LiuXin_alpha.catalog import Catalog
    from LiuXin_alpha.catalog.api import MetadataCandidate

    catalog = Catalog(db)
    work_id = catalog.works.match_or_create(
        MetadataCandidate({"title": "Frankenstein"})
    )
    work = catalog.works.require(work_id)
    assert work["work_id"] == work_id

See ``dev-docs/catalog-api-usage.md`` for an end-to-end WEMI example and the
matching decision rules.
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
        CatalogRepositoriesAPI,
        ExactEntityRepositoryAPI,
        ExpressionRepositoryAPI,
        IdentifierRepositoryAPI,
        ItemIdentifierRepositoryAPI,
        ItemRepositoryAPI,
        ManifestationRepositoryAPI,
        NoteRepositoryAPI,
        TitleRepositoryAPI,
        WorkRepositoryAPI,
    )
    from LiuXin_alpha.catalog.api.retrieval import CatalogRetrievalAPI
    from LiuXin_alpha.catalog.write import (
        CatalogColumnUpdate,
        CatalogOwnedRowUpdate,
        LinkUpdate,
        SchemaCatalogWriter,
    )

    from LiuXin_alpha.databases.api.database_api import DatabaseAPI
    from LiuXin_alpha.databases.db_types import SrcTableID
    from LiuXin_alpha.databases.macro_types import LinkRow


@runtime_checkable
class CatalogAddinsAPI(CatalogMetadataToolsAPI, Protocol):
    """Composition groups exposed by the top-level Catalog facade.

    API shape mirrors `LiuXin_alpha.catalog` module shape: metadata tools,
    repositories, matching, retrieval, and mutations are separate areas behind
    one convenience object.

    Prefer the repository/matching/retrieval/mutation groups for new code.
    The inherited metadata-tool attributes are maintained for callers that work
    directly with legacy database ``Row`` objects.
    """

    repositories: "CatalogRepositoriesAPI"
    matching: "CatalogMatchingAPI"
    retrieval: "CatalogRetrievalAPI"
    mutations: "CatalogMutationsAPI"


@runtime_checkable
class CatalogAPI(CatalogAddinsAPI, Protocol):
    """Structural API for the metadata-aware facade over a database handle.

    ``CatalogAPI`` is a :class:`typing.Protocol`; it is useful as an annotation
    and for ``isinstance(value, CatalogAPI)`` checks.  Construct the concrete
    facade with ``Catalog(db)``.

    The generic ``write*`` methods are the lower-level schema-driven mutation
    surface.  For ordinary entity creation and editing, start with
    ``catalog.works``, ``catalog.items``, or another repository.

    Example::

        catalog: CatalogAPI = Catalog(db)
        work = catalog.works.require(work_id)
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
    comments: "ExactEntityRepositoryAPI"
    synopses: "ExactEntityRepositoryAPI"
    annotations: "ExactEntityRepositoryAPI"

    def create_writer(
        self,
        src_table: str,
        dst_column: str,
        *,
        force_refresh: bool = False,
        destination_owned: bool | None = None,
    ) -> "SchemaCatalogWriter":
        """Create the writer appropriate for one logical catalog field.

        Core schema metadata determines whether ``dst_column`` is stored on the
        source row, in an owned one-to-one row, or through a shared link table.
        The returned writer exposes its concrete validation and bulk methods;
        call :meth:`write` when that distinction is not needed.

        :param src_table: Source table whose integer IDs key the update.
        :param dst_column: Public or storage destination value column.
        :param force_refresh: Re-read schema metadata before selecting a writer.
        :param destination_owned: Override inferred one-to-one ownership only
            when the schema cannot express it.
        :return: A configured same-table, owned-row, or link writer.

        Example::

            writer = catalog.create_writer("works", "tag")
            result = writer.write({work_id: ["gothic", "science fiction"]})
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
        """Resolve a schema-backed writer and apply a bulk update.

        ``*args`` and ``**kwargs`` are passed to the selected writer unchanged.
        This convenience is best when the caller already knows the field's
        accepted write shape.  Use :meth:`create_writer` to inspect or retain
        the concrete writer.

        :param src_table: Source table whose IDs key the update.
        :param dst_column: Logical destination column or linked value column.
        :param args: Positional arguments accepted by the selected writer.
        :param force_refresh: Re-read schema metadata before writer selection.
        :param destination_owned: Optional ownership override for ambiguous
            one-to-one fields.
        :param kwargs: Keyword arguments accepted by the selected writer.
        :return: Written values or link rows keyed by source ID.

        Example::

            catalog.write("works", "work_canonical_title", {
                work_id: "Frankenstein; or, The Modern Prometheus",
            })
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
        """Create a writer and update one source entity.

        :param src_table: Table containing ``src_id``.
        :param dst_column: Logical destination column or linked value column.
        :param src_id: Existing source-row ID.
        :param dst_value: Scalar, collection, rich link value, or ``None`` to
            clear, as accepted by the selected writer.
        :param force_refresh: Re-read schema metadata before writer selection.
        :param destination_owned: Optional ownership override for ambiguous
            one-to-one fields.
        :param kwargs: Concrete-writer options such as ``link_type``.
        :return: A one-entry mapping keyed by ``src_id``; the mapping is not
            unwrapped so its shape matches :meth:`write`.

        Example::

            catalog.write_one(
                "works",
                "work_canonical_title",
                work_id,
                "Frankenstein; or, The Modern Prometheus",
            )
        """

        ...

    def write_link_update(
        self,
        update: "LinkUpdate",
    ) -> "Mapping[SrcTableID, tuple[LinkRow, ...]]":
        """Apply an already normalized many-to-many link update.

        Use this boundary when another component has built a ``LinkUpdate`` and
        the Catalog should own transaction execution.  Repository methods are
        clearer for common relationships such as Agent credits.

        :param update: Immutable replacement or incremental link instruction.
        :return: Complete resulting link rows keyed by affected source ID.
        """

        ...

    def write_column_update(
        self,
        update: "CatalogColumnUpdate[object]",
    ) -> "Mapping[SrcTableID, object]":
        """Apply an already normalized same-table column update.

        :param update: Immutable update containing source IDs and validated
            column values.
        :return: Stable written values keyed by affected source ID.
        """

        ...

    def write_owned_row_update(
        self,
        update: "CatalogOwnedRowUpdate[object]",
    ) -> "Mapping[SrcTableID, tuple[LinkRow, ...]]":
        """Apply an already normalized owned one-to-one row update.

        :param update: Immutable instruction for destination rows whose
            lifecycle belongs exclusively to their source rows.
        :return: Complete resulting link rows keyed by affected source ID.
        """

        ...

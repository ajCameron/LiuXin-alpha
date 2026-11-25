"""
Bind native metadata operations to Catalog persistence and cache freshness.

Use ``CatalogCache(catalog, cache=cache)`` for the loaded-cache path, or construct
``MetadataAPI(catalog, cache=cache)`` explicitly. Omitting cache uses Catalog
reads directly. The caller retains both lifetimes. Returned metadata is detached;
changes are persisted only by an explicit entity or compound mutation method.

Compound writes conservatively refresh the complete snapshot. Pending outer
transactions dirty the cache until commit or rollback; committed refresh failures
retain their result in MetadataReconciliationError and never retry persistence.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager, nullcontext
from contextvars import ContextVar
from copy import deepcopy
from dataclasses import replace
from typing import TYPE_CHECKING, Literal, cast, overload

from LiuXin_alpha.caches.api import CacheConsistency, CacheQuery, CacheSort
from LiuXin_alpha.caches.metadata.custom_fields import MetadataCustomFields
from LiuXin_alpha.caches.metadata.reads import (
    MetadataReadSource,
    MetadataValue,
    detached,
)
from LiuXin_alpha.caches.metadata.records import (
    AnnotationMetadata,
    IdentifierMetadata,
    ItemIdentifierMetadata,
    TextMetadata,
)
from LiuXin_alpha.caches.metadata.search import MetadataSearch
from LiuXin_alpha.caches.metadata.shared import AgentMetadata, VocabularyMetadata
from LiuXin_alpha.caches.metadata.trees import HierarchicalVocabularyMetadata
from LiuXin_alpha.catalog.api.common import (
    CatalogNotFoundError,
    CreatedWemiStack,
    WemiLevel,
)
from LiuXin_alpha.catalog.api.metadata_access import (
    MetadataAPIInterface,
    MetadataEntityInterface,
    MetadataMutationsInterface,
    MetadataReconciliationError,
    MetadataReference,
    MetadataSearchInterface,
    MetadataWriteReceipt,
)
from LiuXin_alpha.catalog.api.metadata_revisions import (
    MetadataConflictError,
    MetadataRevision,
    MetadataSnapshot,
)
from LiuXin_alpha.catalog.metadata_revisions import revision
from LiuXin_alpha.catalog.repositories.base import WEMI_TABLES, BaseRepository
from LiuXin_alpha.catalog.repositories.hierarchy import HierarchicalRepository
from LiuXin_alpha.catalog.retrieval.hydration.expression_metadata_hydrator import (
    ExpressionMetadataHydrator,
)
from LiuXin_alpha.catalog.retrieval.hydration.item_metadata_hydrator import (
    ItemMetadataHydrator,
)
from LiuXin_alpha.catalog.retrieval.hydration.manifestation_metadata_hydrator import (
    ManifestationMetadataHydrator,
)
from LiuXin_alpha.catalog.retrieval.hydration.work_metadata_hydrator import (
    WorkMetadataHydrator,
)
from LiuXin_alpha.databases.transaction_participants import (
    CommittedTransactionError,
    owned_transaction_active,
)

if TYPE_CHECKING:
    from LiuXin_alpha.caches.read_cache import ReadCache
    from LiuXin_alpha.catalog.api.metadata_fields import MetadataCustomFieldsInterface
    from LiuXin_alpha.catalog.api.metadata_payloads import (
        ExpressionCreate,
        ExpressionUpdate,
        ItemCreate,
        ItemUpdate,
        ManifestationCreate,
        ManifestationUpdate,
        WorkCreate,
        WorkUpdate,
    )
    from LiuXin_alpha.catalog.api.metadata_shared import (
        AgentMetadataInterface,
        AnnotationMetadataInterface,
        IdentifierMetadataInterface,
        ItemIdentifierMetadataInterface,
        MetadataRecordsReader,
        TextMetadataInterface,
        VocabularyKind,
        VocabularyMetadataInterface,
    )
    from LiuXin_alpha.catalog.api.metadata_trees import (
        HierarchicalVocabularyMetadataInterface,
    )
    from LiuXin_alpha.catalog.catalog import Catalog
    from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.expression_containers.expression_metadata_api import (
        ExpressionMetadataAPI,
    )
    from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.item_containers.item_metadata_api import (
        ItemMetadataAPI,
    )
    from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.manifestation_containers.manifestation_metadata_api import (
        ManifestationMetadataAPI,
    )
    from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.relation_target_api import (
        MetadataRecord,
    )
    from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.work_containers.work_metadata_api import (
        WorkMetadataAPI,
    )


class MetadataAPI:
    """
    Implement MetadataAPIInterface with one Catalog and optional loaded cache.

    Fallback is fixed at construction and disabled by default. Binding identity
    and readiness are checked on every operation, including retained child
    services. The read-cache lock spans hydration and the entire write/refresh
    boundary, preventing another facade operation from publishing half a change.
    Direct database writes still require owner-driven invalidation.

    Example:
        >>> metadata = MetadataAPI(catalog, cache=cache)  # doctest: +SKIP
        >>> metadata.works.get_many((7, 9))  # doctest: +SKIP
    """

    def __init__(
        self,
        catalog: Catalog,
        *,
        cache: ReadCache | None = None,
        allow_database_fallback: bool = False,
    ) -> None:
        """
        Bind one Catalog and optional loaded cache without taking resource ownership.

        Example:
            ``MetadataAPI(catalog)`` uses authoritative database reads without a cache.


        :param catalog: Catalog used for validation, hydration and persistence.
        :param cache: Loaded cache for the same database object, or None for uncached
            reads.
        :param allow_database_fallback: Allow unavailable cache operations to use
            database reads; confirmed misses still remain misses.
        :return: None; retains the binding and creates coordinating child services.
        """
        self._catalog = catalog
        self._database = catalog.db
        self._cache = cache
        self._fallback = allow_database_fallback
        self._batch_changes: ContextVar[list[bool] | None] = ContextVar(
            "metadata_batch_changes", default=None
        )
        self._revision_readers: dict[WemiLevel, Callable[[int], MetadataRevision]] = {}
        with self._lock():
            self._assert_attached()
        self._works: MetadataEntityInterface[
            WorkMetadataAPI, WorkCreate, WorkUpdate
        ] = _Entity(
            self,
            catalog.works,
            lambda source, entity_id: WorkMetadataHydrator(
                source, strict=True
            ).from_work_id(entity_id),
        )
        self._expressions: MetadataEntityInterface[
            ExpressionMetadataAPI, ExpressionCreate, ExpressionUpdate
        ] = _Entity(
            self,
            catalog.expressions,
            lambda source, entity_id: ExpressionMetadataHydrator(
                source, strict=True
            ).from_expression_id(entity_id),
        )
        self._manifestations: MetadataEntityInterface[
            ManifestationMetadataAPI, ManifestationCreate, ManifestationUpdate
        ] = _Entity(
            self,
            catalog.manifestations,
            lambda source, entity_id: ManifestationMetadataHydrator(
                source, strict=True
            ).from_manifestation_id(entity_id),
        )
        self._items: MetadataEntityInterface[
            ItemMetadataAPI, ItemCreate, ItemUpdate
        ] = _Entity(
            self,
            catalog.items,
            lambda source, entity_id: ItemMetadataHydrator(
                source, strict=True
            ).from_item_id(entity_id),
        )
        self._search = MetadataSearch(self)
        self._mutations = _Mutations(self)
        self._agents = AgentMetadata(self, catalog.agents)
        self._custom_fields = MetadataCustomFields(self)
        self._identifiers = IdentifierMetadata(self, catalog.identifiers)
        self._item_identifiers = ItemIdentifierMetadata(self, catalog.item_identifiers)
        self._notes = TextMetadata(self, catalog.notes)
        self._comments = TextMetadata(self, catalog.comments)
        self._synopses = TextMetadata(self, catalog.synopses)
        self._annotations = AnnotationMetadata(self, catalog.annotations)
        vocabulary_repositories: tuple[tuple[VocabularyKind, BaseRepository], ...] = (
            ("tags", catalog.tags),
            ("labels", catalog.labels),
            ("genres", catalog.genres),
            ("subjects", catalog.subjects),
            ("series", catalog.series),
            ("languages", catalog.languages),
            ("ratings", catalog.ratings),
        )
        self._vocabularies: dict[VocabularyKind, VocabularyMetadataInterface] = {
            kind: (
                HierarchicalVocabularyMetadata(self, repository, kind)
                if isinstance(repository, HierarchicalRepository)
                else VocabularyMetadata(self, repository, kind)
            )
            for kind, repository in vocabulary_repositories
        }

    @property
    def works(self) -> MetadataEntityInterface[WorkMetadataAPI, WorkCreate, WorkUpdate]:
        """
        Read and mutate Works through the coordinating metadata service.

        Example:
            ``metadata.works.create({"title": "Dune"})`` returns a write receipt.


        :return: Coordinated Work entity service.
        """
        return self._works

    @property
    def expressions(
        self,
    ) -> MetadataEntityInterface[
        ExpressionMetadataAPI, ExpressionCreate, ExpressionUpdate
    ]:
        """
        Read and mutate Expressions through the coordinating metadata service.

        Example:
            ``metadata.expressions.require(expression_id)`` hydrates one realization.


        :return: Coordinated Expression entity service.
        """
        return self._expressions

    @property
    def manifestations(
        self,
    ) -> MetadataEntityInterface[
        ManifestationMetadataAPI, ManifestationCreate, ManifestationUpdate
    ]:
        """
        Read and mutate Manifestations through the coordinating metadata service.

        Example:
            ``metadata.manifestations.read_for_edit(edition_id)`` supplies an edit token.


        :return: Coordinated Manifestation entity service.
        """
        return self._manifestations

    @property
    def items(self) -> MetadataEntityInterface[ItemMetadataAPI, ItemCreate, ItemUpdate]:
        """
        Read and mutate Items through the coordinating metadata service.

        Example:
            ``metadata.items.update(copy_id, {"location": "Shelf A"})`` edits one copy.


        :return: Coordinated Item entity service.
        """
        return self._items

    @property
    def search(self) -> MetadataSearchInterface:
        """
        Search with Catalog's semantics and this service's bound source policy.

        Example:
            ``metadata.search.find("work.title:Dune")`` returns Work references.


        :return: Search service using Catalog field meaning and the bound source policy.
        """
        return self._search

    @property
    def mutations(self) -> MetadataMutationsInterface:
        """
        Coordinate atomic compound Catalog mutations and cache publication.

        Example:
            ``metadata.mutations.attach_metadata(...)`` adds attachments atomically.


        :return: Service for compound Catalog writes and WEMI relationships.
        """
        return self._mutations

    def _lock(self) -> AbstractContextManager[object]:
        """
        Borrow the cache lock when a cache is configured.

        Example:
            Uncached operations still use database transactions but acquire no facade lock.


        :return: The owning cache RLock, or a no-op context for uncached operation.
        """
        return nullcontext() if self._cache is None else self._cache._lock

    @property
    def agents(self) -> AgentMetadataInterface:
        """
        Manage shared Agents through Catalog and cache reconciliation.

        Example:
            ``metadata.agents.create_person({"name": "Ursula Le Guin"})`` creates an Agent.


        :return: Coordinated shared Agent administration service.
        """
        return self._agents

    @overload
    def vocabulary(
        self, kind: Literal["genres", "subjects", "series"]
    ) -> HierarchicalVocabularyMetadataInterface: ...

    @overload
    def vocabulary(self, kind: VocabularyKind) -> VocabularyMetadataInterface: ...

    def vocabulary(self, kind: VocabularyKind) -> VocabularyMetadataInterface:
        """
        Select one supported native vocabulary without exposing raw repositories.

        Example:
            ``metadata.vocabulary("tags").create({"name": "History"})`` creates a tag.


        :param kind: Supported native vocabulary name; Languages retain their read-only
            policy.
        :return: Coordinated vocabulary service; unsupported names raise ValueError.
        """
        with self._lock():
            self._assert_attached()
            if kind not in self._vocabularies:
                raise ValueError(f"Unknown metadata vocabulary: {kind!r}")
            return self._vocabularies[kind]

    @property
    def tags(self) -> VocabularyMetadataInterface:
        """Access Tag values through the coordinated metadata service."""
        return self.vocabulary("tags")

    @property
    def labels(self) -> VocabularyMetadataInterface:
        """Access operational Labels through the coordinated metadata service."""
        return self.vocabulary("labels")

    @property
    def genres(self) -> HierarchicalVocabularyMetadataInterface:
        """Access hierarchical Genres through the coordinated metadata service."""
        return self.vocabulary("genres")

    @property
    def subjects(self) -> HierarchicalVocabularyMetadataInterface:
        """Access hierarchical Subjects through the coordinated metadata service."""
        return self.vocabulary("subjects")

    @property
    def series(self) -> HierarchicalVocabularyMetadataInterface:
        """Access Series identities through the coordinated metadata service."""
        return self.vocabulary("series")

    @property
    def languages(self) -> MetadataRecordsReader:
        """Access read-only language constants through the coordinated metadata service."""
        return self.vocabulary("languages")

    @property
    def ratings(self) -> VocabularyMetadataInterface:
        """Access Ratings with scale and source through the coordinated metadata service."""
        return self.vocabulary("ratings")

    @property
    def identifiers(self) -> IdentifierMetadataInterface:
        """Access curated entity identifiers through the coordinated metadata service."""
        return self._identifiers

    @property
    def item_identifiers(self) -> ItemIdentifierMetadataInterface:
        """Access raw Item identifier observations through the coordinated metadata service."""
        return self._item_identifiers

    @property
    def notes(self) -> TextMetadataInterface:
        """Access Note records and WEMI attachments through the coordinated metadata service."""
        return self._notes

    @property
    def comments(self) -> TextMetadataInterface:
        """Access contextual Comments and WEMI attachments through the coordinated metadata service."""
        return self._comments

    @property
    def synopses(self) -> TextMetadataInterface:
        """Access Synopsis records and WEMI attachments through the coordinated metadata service."""
        return self._synopses

    @property
    def annotations(self) -> AnnotationMetadataInterface:
        """Access Item-scoped annotations through the coordinated metadata service."""
        return self._annotations

    def ensure_tag(self, text: str) -> MetadataWriteReceipt[int]:
        """Reuse an exact tag or create it; existing attributes stay unchanged.

        Reuse reports changed=False. Ambiguity raises; approximate matching is
        disabled. Nested calls require a serialized outer transaction.
        """
        if not isinstance(text, str) or not text.strip():
            raise ValueError("text must be a nonblank string")
        return self._write(lambda: self._catalog.tags.ensure({"name": text}))

    def ensure_label(self, text: str) -> MetadataWriteReceipt[int]:
        """Reuse an exact label or create it; existing attributes stay unchanged.

        Reuse reports changed=False. Ambiguity raises; approximate matching is
        disabled. Nested calls require a serialized outer transaction.
        """
        if not isinstance(text, str) or not text.strip():
            raise ValueError("text must be a nonblank string")
        return self._write(lambda: self._catalog.labels.ensure({"name": text}))

    def ensure_genre(
        self, text: str, *, parent_id: int | None = None
    ) -> MetadataWriteReceipt[int]:
        """Reuse an exact genre or create it; existing attributes stay unchanged. Parent None explicitly selects a root.

        Reuse reports changed=False. Ambiguity raises; approximate matching is
        disabled. Nested calls require a serialized outer transaction.
        """
        if not isinstance(text, str) or not text.strip():
            raise ValueError("text must be a nonblank string")

        def operation() -> tuple[int, bool]:
            if parent_id is not None:
                self._catalog.genres.require(parent_id)
            return self._catalog.genres.ensure({"name": text, "parent_id": parent_id})

        return self._write(operation)

    def ensure_subject(
        self, text: str, *, parent_id: int | None = None
    ) -> MetadataWriteReceipt[int]:
        """Reuse an exact subject or create it; existing attributes stay unchanged. Parent None explicitly selects a root.

        Reuse reports changed=False. Ambiguity raises; approximate matching is
        disabled. Nested calls require a serialized outer transaction.
        """
        if not isinstance(text, str) or not text.strip():
            raise ValueError("text must be a nonblank string")

        def operation() -> tuple[int, bool]:
            if parent_id is not None:
                self._catalog.subjects.require(parent_id)
            return self._catalog.subjects.ensure({"name": text, "parent_id": parent_id})

        return self._write(operation)

    def ensure_series(
        self, text: str, *, parent_id: int | None = None
    ) -> MetadataWriteReceipt[int]:
        """Reuse an exact series or create it; existing attributes stay unchanged. Parent None explicitly selects a root.

        Reuse reports changed=False. Ambiguity raises; approximate matching is
        disabled. Nested calls require a serialized outer transaction.
        """
        if not isinstance(text, str) or not text.strip():
            raise ValueError("text must be a nonblank string")

        def operation() -> tuple[int, bool]:
            if parent_id is not None:
                self._catalog.series.require(parent_id)
            return self._catalog.series.ensure({"name": text, "parent_id": parent_id})

        return self._write(operation)

    def ensure_rating(
        self, value: float, *, out_of: float, source: str
    ) -> MetadataWriteReceipt[int]:
        """Reuse an exact rating or create it; existing attributes stay unchanged. Scale and source are required identity context.

        Reuse reports changed=False. Ambiguity raises; approximate matching is
        disabled. Nested calls require a serialized outer transaction.
        """
        if not isinstance(source, str) or not source.strip():
            raise ValueError("source must be a nonblank string")
        return self._write(
            lambda: self._catalog.ratings.ensure(
                {"value": value, "out_of": out_of, "source": source}
            )
        )

    @property
    def custom_fields(self) -> MetadataCustomFieldsInterface:
        """
        Manage native fields with field revisions and schema reconciliation.

        Example:
            ``metadata.custom_fields.list_fields()`` reads authoritative definitions.


        :return: Native custom-field administration with cache reconciliation.
        """
        return self._custom_fields

    def _assert_attached(self) -> None:
        """
        Reject a stale Catalog/cache binding or an unavailable cache.

        Example:
            A retained child service cannot write after its cache changes database.


        :return: None when identity and readiness checks pass; attachment errors
            propagate.
        """
        if self._catalog.db is not self._database:
            raise RuntimeError("Metadata Catalog was attached to a different database")
        cache = self._cache
        if cache is not None:
            cache._assert_ready()
            if (
                cache.database is not self._database
                or cache.storage.db is not self._database
            ):
                raise RuntimeError(
                    "Metadata cache is detached or attached to another database"
                )

    def _schema_scope(self) -> AbstractContextManager[object]:
        """
        Reuse the driver's verified schema fingerprint during composed reads.

        Example:
            PostgreSQL hydration shares a validated schema fingerprint across row reads.


        :return: Driver schema-read scope when available, otherwise a no-op context.
        """
        driver = getattr(self._database, "driver", None)
        scope = getattr(driver, "schema_read_scope", None)
        return (
            cast(AbstractContextManager[object], scope())
            if callable(scope)
            else nullcontext()
        )

    @contextmanager
    def _reading(self) -> Iterator[None]:
        """
        Bound composed database reads to a snapshot with observable completion.

        Portable read macros otherwise may borrow a persistent driver connection,
        retaining PostgreSQL table locks after the call. Recover any dirty cache
        before entering this read-only scope; nested calls borrow the caller's unit.

        An existing managed transaction retains its own isolation and visibility.
        This scope does not upgrade or complete the caller's transaction.

        Example:
            Standalone reads finish their read transaction before a later field migration.


        :return: Context yielding None under the cache lock and owned or borrowed SQL
            unit.
        """
        with self._lock():
            self._assert_attached()
            if self._cache is not None:
                self._cache._refresh_dirty()
            macros = self._database.macros
            active = macros.current_transaction_connection()
            with nullcontext() if active is not None else macros.read_transaction():
                with self._schema_scope():
                    yield

    def _reads(self, *, require_rows: bool = False) -> MetadataReadSource:
        """
        Build a strict-capable read source, capturing live storage once when supported.

        Example:
            A get_many call shares the returned source across every distinct root.


        :param require_rows: Raise for missing rows during hydration instead of
            returning a root miss.
        :return: Source bound to the database, selected cache view and fixed fallback
            policy.
        """
        self._assert_attached()
        cache = self._cache
        if cache is not None:
            cache._refresh_dirty()
            if cache.capabilities.consistency == CacheConsistency.LIVE:
                # A metadata projection is one composed read. Reuse the same
                # bounded capture used by live queries instead of rebuilding the
                # complete catalogue for each traversed row/link proxy.
                from LiuXin_alpha.caches.api import StorageCacheAPI
                from LiuXin_alpha.caches.read_cache import ReadCache

                capture = getattr(cache.storage, "query_view", None)
                if callable(capture):
                    cache = ReadCache.from_storage(cast(StorageCacheAPI, capture()))
        return MetadataReadSource(
            self._database,
            cache,
            allow_database_fallback=self._fallback,
            require_rows=require_rows,
        )

    def batch[ResultT](
        self,
        operation: Callable[[MetadataAPIInterface], ResultT],
        *,
        expected: Sequence[MetadataRevision] = (),
    ) -> MetadataWriteReceipt[ResultT]:
        """
        Group semantic edits, preserving results and one final reconciliation.

        Child receipts remain pending. Caller-owned outer transactions retain commit
        ownership; cache reads after edits defer until completion. A successful outer
        batch performs one full reconciliation, not necessarily one generation increment.

        Example:
            >>> receipt = metadata.batch(lambda api: api.works.create({"title": "Dune"}).result)  # doctest: +SKIP


        :param operation: Synchronous callback using this same metadata interface;
            escaping errors roll back its edits.
        :param expected: Authoritative content tokens checked before invoking the
            callback.
        :return: Aggregate receipt retaining the callback result and final
            persistence/cache state.
        """

        def run() -> tuple[ResultT, bool]:
            """
            Collect child change flags for this callback and restore the enclosing batch.

            Example:
                A nested batch contributes its successful change flag to its parent.


            :return: Callback result paired with whether any successful child requested
                a change.
            """
            changes: list[bool] = []
            token = self._batch_changes.set(changes)
            try:
                result = operation(self)
                return result, any(changes)
            finally:
                self._batch_changes.reset(token)

        return self._write(run, expected=tuple(expected), schema_changed=True)

    def _check_revisions(self, expected: Sequence[MetadataRevision]) -> None:
        """
        Check requested tokens within one reusable schema-read scope.

        Example:
            An empty precondition sequence avoids both hydration and writer reservation.


        :param expected: Tokens to compare against authoritative metadata.
        :return: None after every precondition matches; conflicts reject the operation.
        """
        if not expected:
            return
        with self._schema_scope():
            self._check_reserved_revisions(expected)

    def _check_reserved_revisions(self, expected: Sequence[MetadataRevision]) -> None:
        """
        Compare under one stable schema scope and writer reservation.

        Example:
            A root deleted since read_for_edit raises a conflict with actual=None.


        :param expected: Tokens to check while holding the database writer reservation.
        :return: None on agreement; corruption and MetadataConflictError propagate.
        """
        self._database.macros.lock_metadata_snapshot()
        for token in expected:
            if not isinstance(token, MetadataRevision):
                raise TypeError("expected revisions must be MetadataRevision values")
            reader = self._revision_readers.get(token.reference.level)
            if reader is None:
                raise ValueError(f"Unknown WEMI level: {token.reference.level!r}")
            BaseRepository._validate_entity_id(token.reference.entity_id)
            source = MetadataReadSource(
                self._database, None, allow_database_fallback=False
            )
            root = source.get_row_from_id(
                WEMI_TABLES[token.reference.level], token.reference.entity_id
            )
            actual = None if root is None else reader(token.reference.entity_id)
            if actual != token:
                raise MetadataConflictError(token, actual)

    def _write[ResultT](
        self,
        operation: Callable[[], tuple[ResultT, bool]],
        *,
        expected: Sequence[MetadataRevision] = (),
        schema_changed: bool = False,
    ) -> MetadataWriteReceipt[ResultT]:
        """
        Apply one semantic operation and publish an accurate persistence/cache receipt.

        Borrowed raw transactions are refused because their completion is not observable.
        Nested managed operations defer refresh. Publication failures retain the completed
        result and dirty the cache; persistence is never retried here.

        Example:
            If SQL commits and cache reload fails, the error retains a committed receipt.


        :param operation: Callback returning its result and an explicit changed flag.
        :param expected: Content tokens checked under reservation before the callback
            runs.
        :param schema_changed: Require a full schema/data reload after a committed
            change.
        :return: Receipt describing committed or caller-pending persistence and cache
            visibility.
        """
        with self._lock():
            self._assert_attached()
            macros = self._database.macros
            # Requiring ownership outside an existing managed transaction avoids
            # labelling a borrowed raw-driver savepoint as a durable commit.
            pending = macros.current_transaction_connection() is not None
            if pending and not owned_transaction_active(macros):
                raise RuntimeError(
                    "Metadata mutations require an owned managed transaction; "
                    "a borrowed raw transaction has no observable completion"
                )
            cache = self._cache
            if cache is not None and not pending:
                cache._refresh_dirty()
            publication_error = None
            completed: tuple[ResultT, bool] | None = None
            try:
                with macros.transaction(
                    require_outermost=not pending, serialized=not pending
                ):
                    self._check_revisions(expected)
                    completed = operation()
            except CommittedTransactionError as exc:
                # Only our own completed operation has a result to preserve.
                # A failure raised by an unrelated operation inside the callback
                # must not be mistaken for this transaction's successful commit.
                if completed is None or pending:
                    raise
                publication_error = exc
            if completed is None:
                raise RuntimeError("Metadata operation did not return a result")
            result, changed = completed
            batch_changes = self._batch_changes.get()
            if batch_changes is not None:
                batch_changes.append(changed)
            receipt = MetadataWriteReceipt(
                result=result,
                changed=changed,
                persistence="pending" if pending else "committed",
                cache="not_configured"
                if cache is None
                else "deferred"
                if pending
                else "current",
                generation=None if cache is None else cache.generation,
            )
            if publication_error is not None:
                if cache is not None:
                    cache._full_reload_required = True
                    cache._mark_dependencies_dirty((), (), ())
                    receipt = replace(
                        receipt, cache="dirty", generation=cache.generation
                    )
                raise MetadataReconciliationError(
                    "Metadata committed but a transaction publication failed; do not repeat the write",
                    receipt=receipt,
                ) from publication_error
            if cache is None:
                return receipt
            try:
                if pending and changed:
                    # Thread-local macro depth alone cannot stop another thread
                    # from publishing an old snapshot before the outer commit.
                    cache._full_reload_required = True
                    cache._mark_dependencies_dirty((), (), ())
                    cache._defer_refresh_until_transaction_ends()
                elif not pending and changed:
                    if schema_changed:
                        cache.reload()
                    elif cache.capabilities.consistency == CacheConsistency.LIVE:
                        cache._advance_generation()
                    else:
                        cache.reload_data()
            except Exception as exc:
                cache._full_reload_required = True
                cache._mark_dependencies_dirty((), (), ())
                raise MetadataReconciliationError(
                    "Metadata mutation applied but cache reconciliation failed; "
                    "inspect the receipt before retrying",
                    receipt=replace(
                        receipt, cache="dirty", generation=cache.generation
                    ),
                ) from exc
            return replace(receipt, generation=cache.generation)


class _Entity[MetadataT: MetadataValue]:
    """
    Bind a WEMI repository without exposing its uncoordinated methods.

    Example:
        The owner exposes one _Entity through each typed WEMI property.
    """

    def __init__(
        self,
        owner: MetadataAPI,
        repository: BaseRepository,
        hydrate: Callable[[MetadataReadSource, int], MetadataT],
    ) -> None:
        """
        Bind a family repository and register its authoritative revision reader.

        Example:
            The Works binding hydrates WorkMetadata and checks Work content tokens.


        :param owner: Metadata service that owns attachment checks, transaction
            coordination and cache freshness.
        :param repository: Catalog repository for exactly one native WEMI table.
        :param hydrate: Strict hydrator for that family, invoked with one read source
            and identity.
        :return: None; retains the bindings and discovers the matching WEMI level.
        """
        self._owner = owner
        self._repository = repository
        self._hydrate = hydrate
        self._level: WemiLevel = cast(
            WemiLevel,
            next(
                level
                for level, table in WEMI_TABLES.items()
                if table == repository.table_name
            ),
        )
        owner._revision_readers[self._level] = lambda entity_id: (
            self._snapshot(entity_id).revision
        )

    def _snapshot(self, entity_id: int) -> MetadataSnapshot[MetadataT]:
        """
        Read an edit snapshot while sharing validated schema discovery.

        Example:
            An existing outer schema scope is reused rather than probed per row.


        :param entity_id: Existing identity in this WEMI family; booleans and negative
            IDs are invalid.
        :return: Detached authoritative metadata, public scalar fields and matching
            revision.
        """
        with self._owner._schema_scope():
            return self._snapshot_values(entity_id)

    def _snapshot_values(self, entity_id: int) -> MetadataSnapshot[MetadataT]:
        """
        Hydrate authoritative content and fingerprint the complete root row.

        Example:
            A creator_sort edit changes the token even when older containers omit that field.


        :param entity_id: Existing identity in this WEMI family; booleans and negative
            IDs are invalid.
        :return: Snapshot with copied scalar aliases and full-root/hydrated-content
            revision.
        """
        BaseRepository._validate_entity_id(entity_id)
        source = MetadataReadSource(
            self._owner._database,
            None,
            allow_database_fallback=False,
            require_rows=True,
        )
        row = source.get_row_from_id(self._repository.table_name, entity_id)
        if row is None or not isinstance(row.row_dict, Mapping):
            raise CatalogNotFoundError(
                f"Missing {self._repository.table_name} row: {entity_id}"
            )
        root = deepcopy(dict(row.row_dict))
        value = detached(self._hydrate(source, entity_id))
        fields = {
            alias: deepcopy(root[column])
            for alias, column in self._repository.input_aliases.items()
            if column != self._repository.id_column and column in root
        }
        return MetadataSnapshot(
            value,
            revision(
                MetadataReference(self._level, entity_id), value.to_mapping(), root
            ),
            fields,
        )

    def read_for_edit(self, entity_id: int) -> MetadataSnapshot[MetadataT]:
        """
        Read authoritative metadata and its token in one database snapshot.

        Outside a managed transaction, this owns a read-only snapshot. Inside one,
        it borrows the caller's unit and inherits that unit's isolation and visibility.

        Example:
            >>> observed = metadata.works.read_for_edit(work_id)  # doctest: +SKIP


        :param entity_id: Existing identity in this WEMI family; booleans and negative
            IDs are invalid.
        :return: Authoritative detached metadata, scalar fields and content token.
        """
        with self._owner._lock():
            self._owner._assert_attached()
            macros = self._owner._database.macros
            active = macros.current_transaction_connection()
            with nullcontext() if active is not None else macros.read_transaction():
                return self._snapshot(entity_id)

    def get(self, entity_id: int) -> MetadataT | None:
        """
        Return detached native metadata, or None for a complete root miss.

        Example:
            ``metadata.works.get(999999)`` can return None without database fallback.


        :param entity_id: Existing identity in this WEMI family; booleans and negative
            IDs are invalid.
        :return: Detached metadata, or None for a confirmed absent root.
        """
        BaseRepository._validate_entity_id(entity_id)
        with self._owner._reading():
            source = self._owner._reads()
            if source.get_row_from_id(self._repository.table_name, entity_id) is None:
                return None
            source.require_rows = True
            return detached(self._hydrate(source, entity_id))

    def require(self, entity_id: int) -> MetadataT:
        """
        Raise CatalogNotFoundError only for an absent root or referenced entity.

        Example:
            ``metadata.works.require(work_id)`` requires a complete hydrated result.


        :param entity_id: Existing identity in this WEMI family; booleans and negative
            IDs are invalid.
        :return: Detached metadata; absent roots or referenced rows raise
            CatalogNotFoundError.
        """
        result = self.get(entity_id)
        if result is None:
            raise CatalogNotFoundError(
                f"{self._repository.table_name} row not found: {entity_id}"
            )
        return result

    def get_many(
        self, entity_ids: Sequence[int], *, missing: Literal["none", "raise"] = "none"
    ) -> tuple[MetadataT | None, ...]:
        """
        Reuse one read source and preserve requested order, duplicates and misses.

        One live capture is shared across the call. Each distinct root is hydrated
        once before results are detached; an empty request avoids a catalogue capture.

        Example:
            ``metadata.works.get_many((7, 9, 7))`` returns three independently editable slots.


        :param entity_ids: Ordered IDs to hydrate; duplicates retain independent result
            positions.
        :param missing: Use none to retain None slots, or raise to reject any missing
            root.
        :return: Ordered tuple of independently detached metadata and optional missing
            slots.
        """
        ids = tuple(entity_ids)
        if missing not in ("none", "raise"):
            raise ValueError("missing must be 'none' or 'raise'")
        for entity_id in ids:
            BaseRepository._validate_entity_id(entity_id)
        with self._owner._reading():
            self._owner._assert_attached()
            if not ids:
                return ()
            source = self._owner._reads()
            present = {
                entity_id: source.get_row_from_id(
                    self._repository.table_name, entity_id
                )
                is not None
                for entity_id in dict.fromkeys(ids)
            }
            if missing == "raise" and not all(present.values()):
                absent = tuple(
                    entity_id for entity_id, exists in present.items() if not exists
                )
                raise CatalogNotFoundError(
                    f"Missing {self._repository.table_name} rows: {absent}"
                )
            source.require_rows = True
            values = {
                entity_id: self._hydrate(source, entity_id)
                for entity_id, exists in present.items()
                if exists
            }
            return tuple(
                detached(values[entity_id]) if present[entity_id] else None
                for entity_id in ids
            )

    def list(self, *, limit: int = 100, offset: int = 0) -> tuple[MetadataT, ...]:
        """
        Hydrate a complete ID-ordered page under the cache generation lock.

        Example:
            ``metadata.works.list(limit=20, offset=20)`` reads the second page.


        :param limit: Maximum records to hydrate; zero returns an empty page.
        :param offset: Number of ID-ordered roots to skip; must be nonnegative.
        :return: Complete tuple of detached metadata in ascending identity order.
        """
        BaseRepository._validate_page(limit, offset)
        with self._owner._reading():
            source = self._owner._reads()
            if limit == 0:
                return ()
            rows = source.rows(
                CacheQuery(
                    table=self._repository.table_name,
                    sort=(CacheSort(self._repository.id_column),),
                    limit=limit,
                    offset=offset,
                ),
                lambda: self._repository.list(limit=limit, offset=offset),
            )
            source.require_rows = True
            return tuple(
                detached(
                    self._hydrate(source, self._row_id(row, self._repository.id_column))
                )
                for row in rows
            )

    @staticmethod
    def _row_id(row: Mapping[str, object], column: str) -> int:
        """
        Require a real integer identity in a physical row mapping.

        Example:
            A boolean in an ID column is rejected instead of being treated as row 1.


        :param row: Physical row mapping selected by the read source.
        :param column: Identity column whose value must be an integer.
        :return: Integer row identity; malformed values raise TypeError.
        """
        value = row[column]
        if not isinstance(value, int) or isinstance(value, bool):
            raise TypeError(f"Invalid metadata identity in {column}: {value!r}")
        return value

    def create(self, data: Mapping[str, object]) -> MetadataWriteReceipt[int]:
        """
        Create through Catalog, then publish or defer the cache generation.

        Example:
            ``metadata.works.create({"title": "Dune"})`` creates without implicit matching.


        :param data: Public scalar aliases and values validated by the Catalog
            repository.
        :return: Receipt whose result is the new root identity.
        """
        return self._owner._write(lambda: (self._repository.create(data), True))

    def update(
        self,
        entity_id: int,
        data: Mapping[str, object],
        *,
        expected_revision: MetadataRevision | None = None,
    ) -> MetadataWriteReceipt[int]:
        """
        Update only supplied fields, preserving Catalog validation and aliases.

        Example:
            >>> metadata.works.update(work_id, {"title": "Revised"}, expected_revision=observed.revision)  # doctest: +SKIP


        :param entity_id: Existing identity in this WEMI family; booleans and negative
            IDs are invalid.
        :param data: Public scalar aliases and values validated by the Catalog
            repository. Omitted keys are preserved; explicit None requests NULL.
        :param expected_revision: Optional token from read_for_edit; mismatched or stale
            content rejects the write.
        :return: Receipt retaining the edited identity; an empty payload is a known no-
            op.
        """

        def operation() -> tuple[int, bool]:
            """
            Update the repository row and report whether a payload was supplied.

            Example:
                An empty mapping still delegates repository identity validation.


            :return: Edited identity and bool(data); this is not a full content-equality
                check.
            """
            self._repository.update(entity_id, data)
            return entity_id, bool(data)

        return self._owner._write(
            operation, expected=self._expected(entity_id, expected_revision)
        )

    def delete(
        self, entity_id: int, *, expected_revision: MetadataRevision | None = None
    ) -> MetadataWriteReceipt[int]:
        """
        Delete an entity and refresh relationships affected by Catalog policy.

        Example:
            >>> metadata.works.delete(work_id, expected_revision=observed.revision)  # doctest: +SKIP


        :param entity_id: Existing identity in this WEMI family; booleans and negative
            IDs are invalid.
        :param expected_revision: Optional token from read_for_edit; mismatched or stale
            content rejects the write.
        :return: Receipt retaining the deleted identity after Catalog deletion policy
            runs.
        """

        def operation() -> tuple[int, bool]:
            """
            Delete the requested root through its Catalog repository.

            Example:
                Dependent-row constraints remain the repository/database's responsibility.


            :return: Deleted identity and True after successful repository deletion.
            """
            self._repository.delete(entity_id)
            return entity_id, True

        return self._owner._write(
            operation, expected=self._expected(entity_id, expected_revision)
        )

    def _expected(
        self, entity_id: int, token: MetadataRevision | None
    ) -> tuple[MetadataRevision, ...]:
        """
        Require a supplied token to name this exact WEMI entity.

        Example:
            An Item revision cannot guard an update to a Work with the same integer ID.


        :param entity_id: Identity targeted by the mutation.
        :param token: Optional content precondition supplied by the caller.
        :return: Empty tuple without a token, otherwise its singleton tuple; mismatches
            raise ValueError.
        """
        if token is None:
            return ()
        if token.reference != MetadataReference(self._level, entity_id):
            raise ValueError("Revision belongs to a different metadata entity")
        return (token,)


class _Mutations:
    """
    Preserve the Catalog writer's compound operation and validation boundary.

    Example:
        ``metadata.mutations`` supplies relationship and attachment operations.
    """

    def __init__(self, owner: MetadataAPI) -> None:
        """
        Borrow the metadata coordinator for compound Catalog writes.

        Example:
            The child uses its owner's transaction and cache-reconciliation policy.


        :param owner: Metadata service that owns attachment checks, transaction
            coordination and cache freshness.
        :return: None; retains the coordinator without creating another Catalog.
        """
        self._owner = owner

    def create_wemi_stack(
        self,
        *,
        work: MetadataRecord,
        expression: MetadataRecord,
        manifestation: MetadataRecord,
        items: Sequence[MetadataRecord] = (),
        origin: str | None = None,
        work_id: int | None = None,
    ) -> MetadataWriteReceipt[CreatedWemiStack]:
        """
        Create one atomic WEMI path and reconcile it once.

        Example:
            A caller can create one Work, realization, edition and two copies in one transaction.


        :param work: Work scalar/attachment payload used when creating the root.
        :param expression: Expression payload for the new realization.
        :param manifestation: Manifestation payload for the new edition.
        :param items: Payloads for copies owned by the new Manifestation.
        :param origin: Optional provenance forwarded to Catalog relationship creation.
        :param work_id: Existing Work to reuse, or None to create from work.
        :return: Receipt containing all identities in the created WEMI stack.
        """
        return self._owner._write(
            lambda: (
                self._owner._catalog.mutations.writer.create_wemi_stack(
                    work=work,
                    expression=expression,
                    manifestation=manifestation,
                    items=items,
                    origin=origin,
                    work_id=work_id,
                ),
                True,
            )
        )

    def _metadata(
        self,
        *,
        level: WemiLevel,
        entity_id: int,
        data: MetadataRecord,
        replace_groups: bool,
    ) -> MetadataWriteReceipt[MetadataReference]:
        """
        Coordinate attachment or replacement while requiring an existing root.

        Example:
            An empty replacement still rejects an absent root.


        :param level: Native WEMI level identifying the target repository.
        :param entity_id: Existing root identity at that level.
        :param data: Native scalar and attachment groups interpreted by Catalog.
        :param replace_groups: Replace supplied groups when true; otherwise attach using
            Catalog policy.
        :return: Receipt containing the target reference and payload-based change flag.
        """

        def operation() -> tuple[MetadataReference, bool]:
            """
            Validate the target and delegate the selected Catalog metadata operation.

            Example:
                Only supplied groups are passed to attach_metadata or replace_metadata.


            :return: Target reference and bool(data) after the Catalog operation
                succeeds.
            """
            if level not in WEMI_TABLES:
                raise ValueError(f"unknown WEMI level: {level!r}")
            # Catalog replacement of an empty payload does not itself require
            # the root. The metadata API never reports success for a missing ID.
            repository = self._owner._catalog.repositories.for_table(WEMI_TABLES[level])
            if repository is None:
                raise ValueError(f"No Catalog repository for {level!r}")
            repository.require(entity_id)
            writer = self._owner._catalog.mutations.writer
            method = (
                writer.replace_metadata if replace_groups else writer.attach_metadata
            )
            method(level=level, entity_id=entity_id, data=data)
            return MetadataReference(level, entity_id), bool(data)

        return self._owner._write(operation)

    def attach_metadata(
        self, *, level: WemiLevel, entity_id: int, data: MetadataRecord
    ) -> MetadataWriteReceipt[MetadataReference]:
        """
        Apply direct fields and attachments through the Catalog writer.

        Example:
            >>> metadata.mutations.attach_metadata(level="work", entity_id=work_id, data={"notes": ["Draft"]})  # doctest: +SKIP


        :param level: Native WEMI level identifying the target repository.
        :param entity_id: Existing root identity at that level.
        :param data: Native scalar and attachment groups interpreted by Catalog.
        :return: Receipt containing the target WEMI reference.
        """
        return self._metadata(
            level=level, entity_id=entity_id, data=data, replace_groups=False
        )

    def replace_metadata(
        self, *, level: WemiLevel, entity_id: int, data: MetadataRecord
    ) -> MetadataWriteReceipt[MetadataReference]:
        """
        Replace supplied groups while retaining every omitted group.

        Example:
            An explicit empty notes group clears notes; an omitted group preserves them.


        :param level: Native WEMI level identifying the target repository.
        :param entity_id: Existing root identity at that level.
        :param data: Native scalar and attachment groups interpreted by Catalog.
        :return: Receipt containing the target WEMI reference.
        """
        return self._metadata(
            level=level, entity_id=entity_id, data=data, replace_groups=True
        )

    def link_wemi(
        self,
        *,
        parent_level: WemiLevel,
        parent_id: int,
        child_level: WemiLevel,
        child_id: int,
        primary: bool | None = None,
        priority: int | None = None,
        origin: str | None = None,
    ) -> MetadataWriteReceipt[Mapping[str, object]]:
        """
        Link adjacent WEMI entities, including primary-link reconciliation.

        Example:
            Link an existing Expression to its Work without creating another entity.


        :param parent_level: Parent level of an adjacent WEMI pair.
        :param parent_id: Existing parent identity.
        :param child_level: Child level immediately below the parent.
        :param child_id: Existing child identity.
        :param primary: Optional preferred-parent flag interpreted by Catalog.
        :param priority: Optional relationship order.
        :param origin: Optional relationship provenance.
        :return: Receipt containing Catalog's relationship mapping.
        """
        return self._owner._write(
            lambda: (
                self._owner._catalog.mutations.writer.link_wemi(
                    parent_level=parent_level,
                    parent_id=parent_id,
                    child_level=child_level,
                    child_id=child_id,
                    primary=primary,
                    priority=priority,
                    origin=origin,
                ),
                True,
            )
        )

    def unlink_wemi(
        self,
        *,
        parent_level: WemiLevel,
        parent_id: int,
        child_level: WemiLevel,
        child_id: int,
    ) -> MetadataWriteReceipt[bool]:
        """
        Remove the relationship and retain both endpoint entities.

        Example:
            Unlinking a Work and Expression retains both root records.


        :param parent_level: Parent level of an adjacent WEMI pair.
        :param parent_id: Existing parent identity.
        :param child_level: Child level immediately below the parent.
        :param child_id: Existing child identity.
        :return: Receipt containing whether an existing association was removed.
        """

        def operation() -> tuple[bool, bool]:
            """
            Remove the association and reuse Catalog's result as the change flag.

            Example:
                A missing association produces a known no-op receipt.


            :return: Pair containing the same removal boolean as result and changed
                flag.
            """
            result = self._owner._catalog.mutations.writer.unlink_wemi(
                parent_level=parent_level,
                parent_id=parent_id,
                child_level=child_level,
                child_id=child_id,
            )
            return result, result

        return self._owner._write(operation)


__all__ = ["MetadataAPI"]

"""Specify native metadata access over a bound Catalog and optional cache.

The bound implementation lives in ``caches.metadata.MetadataAPI``. Catalog remains the
semantic persistence authority; the implementing service must coordinate reads,
writes and cache freshness. Names follow Catalog's WEMI repositories and mutation
services, while reads return existing native metadata containers, not SQL rows.

The contract belongs with Catalog's other public protocols. The bound
adapter lives in caches alongside its Catalog write bridge; this declaration
does not give Catalog mutations responsibility for cache reconciliation.

Returned containers must be detached from mutable cache storage. Editing one does
not persist changes. Implementations must not expose raw repositories as these
services: that would bypass the required cache reconciliation on writes.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Literal, Protocol, overload

from LiuXin_alpha.catalog.api.metadata_relationships import (
    MetadataCreditsInterface,
    MetadataWorkMembershipsInterface,
)
from LiuXin_alpha.catalog.api.metadata_results import (
    MetadataReconciliationError,
    MetadataWriteReceipt,
)
from LiuXin_alpha.catalog.api.metadata_revisions import (
    MetadataEditRevision,
    MetadataRevision,
    MetadataSnapshot,
)
from LiuXin_alpha.catalog.api.metadata_search import MetadataReference

if TYPE_CHECKING:
    from LiuXin_alpha.catalog.api.common import CreatedWemiStack, WemiLevel
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
    from LiuXin_alpha.catalog.api.metadata_search import (
        HydratedMetadataSearchPage,
        MetadataSearchField,
        MetadataSearchFilter,
        MetadataSearchPage,
        MetadataSearchRequest,
        MetadataSearchSort,
        MetadataSortField,
    )
    from LiuXin_alpha.catalog.api.metadata_shared import (
        AgentMetadataInterface,
        AnnotationMetadataInterface,
        IdentifierMetadataInterface,
        ItemIdentifierMetadataInterface,
        MetadataRecordsReader,
        SuggestibleMetadataReader,
        TextMetadataInterface,
        TextVocabularyMetadataInterface,
        VocabularyKind,
        VocabularyMetadataInterface,
        WorkMembershipMetadataInterface,
    )
    from LiuXin_alpha.catalog.api.metadata_trees import (
        HierarchicalVocabularyMetadataInterface,
        SeriesMetadataInterface,
    )
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


class MetadataEntityInterface[MetadataT, CreateT, UpdateT](
    MetadataCreditsInterface, Protocol
):
    """Read and mutate one WEMI family using native metadata values.

    The service binds the database, optional cache and fallback policy once.
    Read methods return detached metadata, with related entities represented by
    the existing container contracts. Missing entities and unavailable reads are
    distinct; only a confirmed miss permits get to return None.

    Write inputs use the Catalog repository's public field aliases. Unsupported
    fields raise rather than silently disappearing. Catalog owns normalization,
    matching policy and deletion constraints. Each write must coordinate cache
    freshness before returning a receipt; it cannot return raw Catalog results
    while silently leaving an attached snapshot stale.
    """

    def get(self, entity_id: int) -> MetadataT | None:
        """Return metadata for an existing entity, or None for a confirmed miss."""
        ...

    def require(self, entity_id: int) -> MetadataT:
        """Return metadata or raise CatalogNotFoundError for a confirmed miss."""
        ...

    def get_many(
        self, entity_ids: Sequence[int], *, missing: Literal["none", "raise"] = "none"
    ) -> tuple[MetadataT | None, ...]:
        """Hydrate one capture in input order, retaining duplicates and missing slots.

        Each returned value is independently detached. With missing='raise', any
        confirmed absent root raises CatalogNotFoundError instead of returning a
        partial tuple. Unavailable or corrupt reads always raise.
        """
        ...

    def search(
        self,
        query: str = "",
        *,
        filters: Sequence[MetadataSearchFilter] = (),
        sort: Sequence[MetadataSearchSort] = (),
        limit: int | None = 100,
        offset: int = 0,
    ) -> HydratedMetadataSearchPage[MetadataT]:
        """Search this level and hydrate only its page from one captured read."""
        ...

    def read_for_edit(self, entity_id: int) -> MetadataSnapshot[MetadataT]:
        """Read an authoritative database snapshot with a matching content token.

        This explicitly bypasses cached values and requires an existing root.
        The token covers this hydrated projection, including its relationships;
        custom fields retain separate native revision checks.
        """
        ...

    def list(self, *, limit: int = 100, offset: int = 0) -> tuple[MetadataT, ...]:
        """Return an ID-ordered page; negative limits or offsets are invalid."""
        ...

    def create(self, data: CreateT) -> MetadataWriteReceipt[int]:
        """Create a new entity through Catalog without implicitly matching one."""
        ...

    def update(
        self,
        entity_id: int,
        data: UpdateT,
        *,
        expected_revision: MetadataRevision | None = None,
    ) -> MetadataWriteReceipt[int]:
        """Update supplied public fields, preserving omitted fields."""
        ...

    def delete(
        self, entity_id: int, *, expected_revision: MetadataRevision | None = None
    ) -> MetadataWriteReceipt[int]:
        """Delete through Catalog policy and reconcile affected relationships too."""
        ...


class MetadataWorkInterface[MetadataT, CreateT, UpdateT](
    MetadataEntityInterface[MetadataT, CreateT, UpdateT],
    MetadataWorkMembershipsInterface,
    Protocol,
):
    """Combine Work metadata editing with its declared Tag and Series memberships."""


class MetadataSearchInterface(Protocol):
    """Apply Catalog's native WEMI search meaning through the bound read service."""

    def query(self, request: MetadataSearchRequest) -> MetadataSearchPage:
        """Return a complete page with totals, stable ordering and provenance."""
        ...

    def fields(self, *, level: WemiLevel = "work") -> tuple[MetadataSearchField, ...]:
        """Discover semantic filters and aliases for this result level."""
        ...

    def sort_fields(
        self, *, level: WemiLevel = "work"
    ) -> tuple[MetadataSortField, ...]:
        """Discover scalar ordering keys without implying relationship aggregation."""
        ...

    def validate(self, request: MetadataSearchRequest) -> None:
        """Validate syntax and options without executing or hydrating matches.

        Invalid input raises MetadataSearchValidationError with a parameter path.
        Execution availability and database integrity are checked during query.
        """
        ...

    def find(
        self,
        query: str,
        *,
        level: WemiLevel = "work",
        limit: int | None = 100,
        offset: int = 0,
    ) -> tuple[MetadataReference, ...]:
        """Return ID-ordered semantic references at the requested WEMI level.

        Terms, traversal and validation follow CatalogSearchAPI. Execution uses
        the configured cache when present, with fallback fixed at binding time.
        Corrupt reads and unsupported operations must not become empty results.
        References carry identity only; later hydration is a separate read and
        does not imply a shared snapshot across those calls.
        """
        ...


class MetadataMutationsInterface(Protocol):
    """Coordinate Catalog's semantic compound writes with cache freshness.

    Payload interpretation follows Catalog's MetadataWriterAPI, including omission
    versus clearing and relationship ownership. Every method must retain that
    validation boundary and return a reconciliation-aware receipt. Implementing
    an atomic operation requires a supported transaction capability; a facade
    must not silently fall back to partial writes.
    """

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
        """Create a connected WEMI path using Catalog's existing operation."""
        ...

    def attach_metadata(
        self, *, level: WemiLevel, entity_id: int, data: MetadataRecord
    ) -> MetadataWriteReceipt[MetadataReference]:
        """Apply direct fields and supported attachment groups through Catalog."""
        ...

    def replace_metadata(
        self, *, level: WemiLevel, entity_id: int, data: MetadataRecord
    ) -> MetadataWriteReceipt[MetadataReference]:
        """Replace supplied groups, retaining omitted groups and their links."""
        ...

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
        """Link adjacent entities and retain Catalog's authoritative link receipt."""
        ...

    def unlink_wemi(
        self,
        *,
        parent_level: WemiLevel,
        parent_id: int,
        child_level: WemiLevel,
        child_id: int,
    ) -> MetadataWriteReceipt[bool]:
        """Remove an adjacent relationship while retaining both entities."""
        ...


class MetadataAPIInterface(Protocol):
    """Expose a native metadata service independently of Core's command transport.

    An implementation borrows Catalog and an optional cache for the same database.
    The composing caller retains their lifecycle and closes them. Binding is a
    construction concern, not a per-operation argument.
    These properties must return coordinating services, never raw repositories.
    Catalog alone and Cache alone do not implement this interface.
    caches.metadata.CatalogCache is the explicit composition with a required
    loaded cache; MetadataAPI also permits uncached service use. The generic
    cache core does not expose metadata operations or construct this service.

    This contract covers WEMI reads, search, CRUD, compound and batch writes,
    edit preconditions, core record administration, exact vocabulary ensures and
    native custom fields. Titles are embedded WEMI fields; languages are constants.
    Matching and merges remain Catalog services. Calibre views stay in their
    compatibility layer. No raw database, SQL rows or physical writer routes are
    part of this interface.
    """

    @property
    def works(self) -> MetadataWorkInterface[WorkMetadataAPI, WorkCreate, WorkUpdate]:
        """Access abstract Works and their native metadata."""
        ...

    @property
    def expressions(
        self,
    ) -> MetadataEntityInterface[
        ExpressionMetadataAPI, ExpressionCreate, ExpressionUpdate
    ]:
        """Access realizations, translations and revisions of Works."""
        ...

    @property
    def manifestations(
        self,
    ) -> MetadataEntityInterface[
        ManifestationMetadataAPI, ManifestationCreate, ManifestationUpdate
    ]:
        """Access publication and edition metadata."""
        ...

    @property
    def items(self) -> MetadataEntityInterface[ItemMetadataAPI, ItemCreate, ItemUpdate]:
        """Access individual copies and their native metadata."""
        ...

    @property
    def search(self) -> MetadataSearchInterface:
        """Search semantic WEMI fields using the service's bound read policy."""
        ...

    @property
    def mutations(self) -> MetadataMutationsInterface:
        """Apply coordinated compound mutations through Catalog."""
        ...

    @property
    def agents(self) -> AgentMetadataInterface:
        """Read and administer shared Agent identities with cache reconciliation."""
        ...

    @overload
    def vocabulary(self, kind: Literal["tags"]) -> WorkMembershipMetadataInterface: ...

    @overload
    def vocabulary(self, kind: Literal["series"]) -> SeriesMetadataInterface: ...

    @overload
    def vocabulary(
        self, kind: Literal["genres", "subjects"]
    ) -> HierarchicalVocabularyMetadataInterface: ...

    @overload
    def vocabulary(
        self, kind: Literal["labels"]
    ) -> TextVocabularyMetadataInterface: ...

    @overload
    def vocabulary(self, kind: Literal["languages"]) -> SuggestibleMetadataReader: ...

    @overload
    def vocabulary(self, kind: Literal["ratings"]) -> VocabularyMetadataInterface: ...

    @overload
    def vocabulary(self, kind: VocabularyKind) -> MetadataRecordsReader: ...

    def vocabulary(self, kind: VocabularyKind) -> MetadataRecordsReader:
        """Select a native vocabulary; Languages retain Catalog's immutable policy."""
        ...

    @property
    def tags(self) -> WorkMembershipMetadataInterface:
        """Access Tag values through the coordinated metadata service."""
        ...

    @property
    def labels(self) -> TextVocabularyMetadataInterface:
        """Access operational Labels through the coordinated metadata service."""
        ...

    @property
    def genres(self) -> HierarchicalVocabularyMetadataInterface:
        """Access hierarchical Genres through the coordinated metadata service."""
        ...

    @property
    def subjects(self) -> HierarchicalVocabularyMetadataInterface:
        """Access hierarchical Subjects through the coordinated metadata service."""
        ...

    @property
    def series(self) -> SeriesMetadataInterface:
        """Access Series identities through the coordinated metadata service."""
        ...

    @property
    def languages(self) -> SuggestibleMetadataReader:
        """Access read-only language constants through the coordinated metadata service."""
        ...

    @property
    def ratings(self) -> VocabularyMetadataInterface:
        """Access Ratings with scale and source through the coordinated metadata service."""
        ...

    @property
    def identifiers(self) -> IdentifierMetadataInterface:
        """Access curated entity identifiers through the coordinated metadata service."""
        ...

    @property
    def item_identifiers(self) -> ItemIdentifierMetadataInterface:
        """Access raw Item identifier observations through the coordinated metadata service."""
        ...

    @property
    def notes(self) -> TextMetadataInterface:
        """Access Note records and WEMI attachments through the coordinated metadata service."""
        ...

    @property
    def comments(self) -> TextMetadataInterface:
        """Access contextual Comments and WEMI attachments through the coordinated metadata service."""
        ...

    @property
    def synopses(self) -> TextMetadataInterface:
        """Access Synopsis records and WEMI attachments through the coordinated metadata service."""
        ...

    @property
    def annotations(self) -> AnnotationMetadataInterface:
        """Access Item-scoped annotations through the coordinated metadata service."""
        ...

    def ensure_tag(self, text: str) -> MetadataWriteReceipt[int]:
        """Reuse an exact tag or create it; existing attributes stay unchanged.

        Reuse reports changed=False. Ambiguity raises; approximate matching is
        disabled. Nested calls require a serialized outer transaction.
        """
        ...

    def ensure_label(self, text: str) -> MetadataWriteReceipt[int]:
        """Reuse an exact label or create it; existing attributes stay unchanged.

        Reuse reports changed=False. Ambiguity raises; approximate matching is
        disabled. Nested calls require a serialized outer transaction.
        """
        ...

    def ensure_genre(
        self, text: str, *, parent_id: int | None = None
    ) -> MetadataWriteReceipt[int]:
        """Reuse an exact genre or create it; existing attributes stay unchanged. Parent None explicitly selects a root.

        Reuse reports changed=False. Ambiguity raises; approximate matching is
        disabled. Nested calls require a serialized outer transaction.
        """
        ...

    def ensure_subject(
        self, text: str, *, parent_id: int | None = None
    ) -> MetadataWriteReceipt[int]:
        """Reuse an exact subject or create it; existing attributes stay unchanged. Parent None explicitly selects a root.

        Reuse reports changed=False. Ambiguity raises; approximate matching is
        disabled. Nested calls require a serialized outer transaction.
        """
        ...

    def ensure_series(
        self, text: str, *, parent_id: int | None = None
    ) -> MetadataWriteReceipt[int]:
        """Reuse an exact series or create it; existing attributes stay unchanged. Parent None explicitly selects a root.

        Reuse reports changed=False. Ambiguity raises; approximate matching is
        disabled. Nested calls require a serialized outer transaction.
        """
        ...

    def ensure_rating(
        self, value: float, *, out_of: float, source: str
    ) -> MetadataWriteReceipt[int]:
        """Reuse an exact rating or create it; existing attributes stay unchanged. Scale and source are required identity context.

        Reuse reports changed=False. Ambiguity raises; approximate matching is
        disabled. Nested calls require a serialized outer transaction.
        """
        ...

    @property
    def custom_fields(self) -> MetadataCustomFieldsInterface:
        """Manage native definitions, stored values, revisions and migration plans."""
        ...

    def batch[ResultT](
        self,
        operation: Callable[[MetadataAPIInterface], ResultT],
        *,
        expected: Sequence[MetadataEditRevision] = (),
    ) -> MetadataWriteReceipt[ResultT]:
        """Run related edits atomically and reconcile the cache once on success.

        The callback uses this same interface and may use created IDs in later
        writes. Child receipts are pending; the returned aggregate receipt owns
        the final persistence/cache outcome. An escaping exception rolls back all
        edits in the batch. Caller-owned outer transactions remain pending.
        Cache reads after edits defer until transaction completion; callbacks
        should use mutation results rather than rehydrate uncommitted snapshots.
        All expected content tokens are checked before the callback under a
        database writer reservation. A mismatch raises MetadataConflictError.
        """
        ...


__all__ = [
    "MetadataAPIInterface",
    "MetadataEntityInterface",
    "MetadataWorkInterface",
    "MetadataMutationsInterface",
    "MetadataReconciliationError",
    "MetadataReference",
    "MetadataSearchInterface",
    "MetadataWriteReceipt",
]

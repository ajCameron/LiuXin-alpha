"""Specify native metadata access over a bound Catalog and optional cache.

This is an interface-first contract, not a working facade. Catalog remains the
semantic persistence authority; the implementing service must coordinate reads,
writes and cache freshness. Names follow Catalog's WEMI repositories and mutation
services, while reads return existing native metadata containers, not SQL rows.

The contract belongs with Catalog's other public protocols. The future bound
adapter belongs in caches alongside its Catalog write bridge; this declaration
does not give Catalog mutations responsibility for cache reconciliation.

Returned containers must be detached from mutable cache storage. Editing one does
not persist changes. Implementations must not expose raw repositories as these
services: that would bypass the required cache reconciliation on writes.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, Literal, Protocol, TypeVar

if TYPE_CHECKING:
    from LiuXin_alpha.catalog.api.common import CreatedWemiStack, WemiLevel
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


@dataclass(frozen=True, slots=True)
class MetadataReference:
    """Identify a persisted WEMI entity without a physical table name.

    Adapters validate IDs and existence; this value supplies no runtime validation
    or database binding. References belong to the service's bound database.
    """

    level: WemiLevel
    entity_id: int


# Declared covariant on purpose. From Python 3.13, type checkers give a dataclass a
# synthesised ``__replace__`` that takes ``result``. Pyright's inferred (PEP 695)
# variance then makes the receipt invariant, so a ``MetadataWriteReceipt[int]``
# could no longer travel as ``MetadataWriteReceipt[object]``. Mypy, in turn,
# rejects a covariant field because of that same ``__replace__`` parameter; the
# receipt is frozen and ``replace`` returns a new value, so covariance holds here.
_ResultT_co = TypeVar("_ResultT_co", covariant=True)


@dataclass(frozen=True, slots=True)
class MetadataWriteReceipt(Generic[_ResultT_co]):  # noqa: UP046 - explicit variance
    """Keep persistence and cache visibility explicit after a semantic write.

    ``pending`` means an outer transaction still controls commit or rollback; it
    must never be reported as durable success. ``deferred`` requires the owner to
    refresh after that transaction closes, including after rollback. ``current``
    means the bound cache can safely serve the completed change. A configured
    cache cannot be current for a pending write. ``dirty`` records failed cache
    reconciliation and is carried by an error, never a successful write return.
    ``not_configured`` and a None generation describe operation without a cache.

    ``changed`` describes effects within the transaction, not a durable commit.
    A receipt captures return-time state; a pending receipt does not later become
    committed in place. Its generation is not evidence of transaction durability.
    This passive value does not validate state combinations or freeze result
    payloads. A reconciliation failure must preserve this receipt in the error;
    it must not turn a committed write into an apparent persistence failure.
    """

    result: _ResultT_co  # type: ignore[misc]  # mypy: covariant __replace__ parameter
    changed: bool
    persistence: Literal["committed", "pending"]
    cache: Literal["not_configured", "current", "deferred", "dirty"]
    generation: int | None


class MetadataReconciliationError(RuntimeError):
    """Preserve a completed Catalog operation whose cache reconciliation failed.

    Inspect receipt.persistence before deciding how an enclosing transaction must
    finish. A committed receipt must not trigger an automatic repeat of the write.
    The implementing owner must leave the cache dirty and recoverable.
    """

    def __init__(self, message: str, *, receipt: MetadataWriteReceipt[object]) -> None:
        """Retain the authoritative receipt without changing persistence state."""
        super().__init__(message)
        self.receipt = receipt


class MetadataEntityInterface[MetadataT](Protocol):
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

    def list(self, *, limit: int = 100, offset: int = 0) -> tuple[MetadataT, ...]:
        """Return an ID-ordered page; negative limits or offsets are invalid."""
        ...

    def create(self, data: MetadataRecord) -> MetadataWriteReceipt[int]:
        """Create a new entity through Catalog without implicitly matching one."""
        ...

    def update(self, entity_id: int, data: MetadataRecord) -> MetadataWriteReceipt[int]:
        """Update supplied public fields, preserving omitted fields."""
        ...

    def delete(self, entity_id: int) -> MetadataWriteReceipt[int]:
        """Delete through Catalog policy and reconcile affected relationships too."""
        ...


class MetadataSearchInterface(Protocol):
    """Apply Catalog's native WEMI search meaning through the bound read service."""

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

    This first contract covers WEMI reads, search, CRUD and compound writes.
    Matching, merges, custom-field administration and dedicated Agent/vocabulary
    services remain explicit follow-up design work. Calibre views stay in their
    compatibility layer. No raw database, SQL rows or physical writer routes are
    part of this interface.
    """

    @property
    def works(self) -> MetadataEntityInterface[WorkMetadataAPI]:
        """Access abstract Works and their native metadata."""
        ...

    @property
    def expressions(self) -> MetadataEntityInterface[ExpressionMetadataAPI]:
        """Access realizations, translations and revisions of Works."""
        ...

    @property
    def manifestations(self) -> MetadataEntityInterface[ManifestationMetadataAPI]:
        """Access publication and edition metadata."""
        ...

    @property
    def items(self) -> MetadataEntityInterface[ItemMetadataAPI]:
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


__all__ = [
    "MetadataAPIInterface",
    "MetadataEntityInterface",
    "MetadataMutationsInterface",
    "MetadataReconciliationError",
    "MetadataReference",
    "MetadataSearchInterface",
    "MetadataWriteReceipt",
]

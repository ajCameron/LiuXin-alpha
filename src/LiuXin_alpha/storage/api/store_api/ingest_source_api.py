"""Optional Store-level preparation contract for advanced ingest sources."""

from __future__ import annotations

import dataclasses
from enum import StrEnum
from typing import BinaryIO, Protocol, runtime_checkable

from LiuXin_alpha.storage.api.models import (
    Digest,
    FileInfo,
    StoreInventoryEntry,
)


class IngestReadConsistency(StrEnum):
    """Strongest relationship a Store can guarantee between inspect and read.

    Example:
        >>> IngestReadConsistency.VERSION_PINNED.value
        'version_pinned'
    """

    UNGUARDED = "unguarded"
    VERSION_PINNED = "version_pinned"
    IMMUTABLE = "immutable"


class IngestObjectDelivery(StrEnum):
    """How a source produces one object stream for its caller.

    Example:
        >>> IngestObjectDelivery.DISK_SPOOLED.value
        'disk_spooled'
    """

    STREAMING = "streaming"
    DISK_SPOOLED = "disk_spooled"
    MEMORY_BUFFERED = "memory_buffered"


class IngestInventoryResume(StrEnum):
    """Strongest inventory checkpointing mechanism exposed by a Store.

    Example:
        >>> IngestInventoryResume.SNAPSHOT.value
        'snapshot'
    """

    NONE = "none"
    CURSOR = "cursor"
    SNAPSHOT = "snapshot"


class IngestObjectResume(StrEnum):
    """Whether an interrupted object can resume without mixing versions.

    Example:
        >>> IngestObjectResume.STABLE_RANGE.value
        'stable_range'
    """

    NONE = "none"
    STABLE_RANGE = "stable_range"


class IngestMetadataAvailability(StrEnum):
    """Where backend-native rich discovery metadata becomes available.

    Example:
        >>> IngestMetadataAvailability.INSPECTION.value
        'inspection'
    """

    NONE = "none"
    INVENTORY = "inventory"
    INSPECTION = "inspection"


@dataclasses.dataclass(slots=True, frozen=True)
class IngestSourceCapabilities:
    """Advanced ingest qualities advertised by one configured source Store.

    These values describe the strongest behavior the Store may provide. A
    :class:`PreparedIngestObject` records the guarantees actually available
    for one object, since version tokens and checksums may be object-specific.

    Example:
        >>> profile = IngestSourceCapabilities(
        ...     read_consistency=IngestReadConsistency.VERSION_PINNED,
        ...     object_delivery=IngestObjectDelivery.STREAMING,
        ...     object_resume=IngestObjectResume.STABLE_RANGE,
        ...     authoritative_digest_algorithms=("sha256",),
        ... )
        >>> profile.object_resume
        <IngestObjectResume.STABLE_RANGE: 'stable_range'>
    """

    read_consistency: IngestReadConsistency
    object_delivery: IngestObjectDelivery
    inventory_resume: IngestInventoryResume = IngestInventoryResume.NONE
    object_resume: IngestObjectResume = IngestObjectResume.NONE
    authoritative_digest_algorithms: tuple[str, ...] = ()
    metadata_availability: IngestMetadataAvailability = (
        IngestMetadataAvailability.NONE
    )

    def __post_init__(self) -> None:
        """Normalize enum and algorithm values and reject contradictions.

        Example:
            >>> IngestSourceCapabilities(
            ...     IngestReadConsistency.UNGUARDED,
            ...     IngestObjectDelivery.STREAMING,
            ...     object_resume=IngestObjectResume.STABLE_RANGE,
            ... )
            Traceback (most recent call last):
            ...
            ValueError: stable object resume requires stable reads.
        """

        read_consistency = IngestReadConsistency(self.read_consistency)
        object_delivery = IngestObjectDelivery(self.object_delivery)
        inventory_resume = IngestInventoryResume(self.inventory_resume)
        object_resume = IngestObjectResume(self.object_resume)
        metadata_availability = IngestMetadataAvailability(
            self.metadata_availability
        )
        algorithms = tuple(
            str(algorithm).strip().lower()
            for algorithm in self.authoritative_digest_algorithms
        )
        if any(not algorithm for algorithm in algorithms):
            raise ValueError(
                "authoritative digest algorithm names must not be empty."
            )
        if len(algorithms) != len(set(algorithms)):
            raise ValueError(
                "authoritative digest algorithm names must be unique."
            )
        if (
            object_resume is IngestObjectResume.STABLE_RANGE
            and read_consistency is IngestReadConsistency.UNGUARDED
        ):
            raise ValueError("stable object resume requires stable reads.")
        object.__setattr__(self, "read_consistency", read_consistency)
        object.__setattr__(self, "object_delivery", object_delivery)
        object.__setattr__(self, "inventory_resume", inventory_resume)
        object.__setattr__(self, "object_resume", object_resume)
        object.__setattr__(self, "metadata_availability", metadata_availability)
        object.__setattr__(
            self,
            "authoritative_digest_algorithms",
            algorithms,
        )

    def validate_prepared(self, prepared: PreparedIngestObject) -> None:
        """Require per-object claims to fit within this Store profile.

        Example:
            >>> profile = IngestSourceCapabilities(
            ...     IngestReadConsistency.UNGUARDED,
            ...     IngestObjectDelivery.STREAMING,
            ... )
            >>> from uuid import UUID
            >>> from LiuXin_alpha.storage.api.location_api import Location
            >>> prepared = PreparedIngestObject(
            ...     StoreInventoryEntry(Location(UUID(int=1), "book")),
            ...     IngestReadConsistency.UNGUARDED,
            ... )
            >>> profile.validate_prepared(prepared)

        :param prepared: Per-object claims returned by the source Store.
        :return: None after successful validation.
        """

        consistency_strength = {
            IngestReadConsistency.UNGUARDED: 0,
            IngestReadConsistency.VERSION_PINNED: 1,
            IngestReadConsistency.IMMUTABLE: 2,
        }
        if consistency_strength[prepared.read_consistency] > (
            consistency_strength[self.read_consistency]
        ):
            raise ValueError(
                "prepared read consistency exceeds the Store profile."
            )
        advertised = set(self.authoritative_digest_algorithms)
        if any(
            digest.algorithm not in advertised
            for digest in prepared.authoritative_digests
        ):
            raise ValueError(
                "prepared ingest returned an unadvertised authoritative digest."
            )


@dataclasses.dataclass(slots=True, frozen=True)
class PreparedIngestObject:
    """Per-object observations bound to the safest available read operation.

    The value contains no open handle or credential and may be retained by a
    workflow checkpoint. ``read_consistency`` is the guarantee actually
    available for this object, which may be weaker than the Store profile when
    the backend did not return a version token.

    Example:
        >>> from uuid import UUID
        >>> from LiuXin_alpha.storage.api.location_api import Location
        >>> prepared = PreparedIngestObject(
        ...     StoreInventoryEntry(Location(UUID(int=1), "book.epub")),
        ...     read_consistency=IngestReadConsistency.UNGUARDED,
        ... )
        >>> prepared.info.location.key
        'book.epub'
    """

    info: FileInfo | StoreInventoryEntry
    read_consistency: IngestReadConsistency
    authoritative_digests: tuple[Digest, ...] = ()
    provenance_uri: str | None = None

    def __post_init__(self) -> None:
        """Validate per-object consistency, digests, and provenance.

        Example:
            >>> from uuid import UUID
            >>> from LiuXin_alpha.storage.api.location_api import Location
            >>> PreparedIngestObject(
            ...     StoreInventoryEntry(Location(UUID(int=1), "book")),
            ...     IngestReadConsistency.VERSION_PINNED,
            ... )
            Traceback (most recent call last):
            ...
            ValueError: version-pinned ingest requires an object version.
        """

        consistency = IngestReadConsistency(self.read_consistency)
        if (
            consistency is IngestReadConsistency.VERSION_PINNED
            and self.info.version is None
        ):
            raise ValueError(
                "version-pinned ingest requires an object version."
            )
        algorithms = tuple(
            digest.algorithm for digest in self.authoritative_digests
        )
        if len(algorithms) != len(set(algorithms)):
            raise ValueError(
                "prepared authoritative digest algorithms must be unique."
            )
        if self.provenance_uri == "":
            raise ValueError("prepared provenance URI must not be empty.")
        object.__setattr__(self, "read_consistency", consistency)


@runtime_checkable
class IngestSourceStoreAPI(Protocol):
    """Optional advanced source contract consumed by generic Store ingest.

    The protocol contains source mechanics only. Asset identity, placement,
    replica policy, and destination selection remain manager responsibilities.

    Example:
        >>> isinstance(store, IngestSourceStoreAPI)  # doctest: +SKIP
        True
    """

    @property
    def ingest_capabilities(self) -> IngestSourceCapabilities:
        """Return the configured Store's strongest ingest qualities.

        Example:
            >>> store.ingest_capabilities.object_delivery  # doctest: +SKIP
            <IngestObjectDelivery.STREAMING: 'streaming'>
        """

        ...

    def prepare_ingest(
        self,
        info: FileInfo | StoreInventoryEntry,
        *,
        inspect: bool = True,
    ) -> PreparedIngestObject:
        """Prepare one candidate without opening or materialising its bytes.

        ``inspect=True`` asks the Store for its richest authoritative
        observations. Backends may satisfy that request from inventory data or
        perform a ``stat``-like operation. Unsupported inspection may retain
        the supplied inventory entry, but other failures remain visible.

        Example:
            >>> prepared = store.prepare_ingest(entry, inspect=True)  # doctest: +SKIP

        :param info: Candidate returned by this Store.
        :param inspect: Whether rich authoritative observations are requested.
        :return: Immutable per-object ingest preparation.
        """

        ...

    def open_prepared_ingest(
        self,
        prepared: PreparedIngestObject,
        *,
        offset: int = 0,
    ) -> BinaryIO:
        """Open the prepared object with its advertised stability guarantee.

        Example:
            >>> with store.open_prepared_ingest(prepared) as source:  # doctest: +SKIP
            ...     payload = source.read()

        :param prepared: Value previously returned by this Store.
        :param offset: Byte offset used for a supported stable-range resume.
        :return: Context-managed binary input stream.
        """

        ...


__all__ = [
    "IngestInventoryResume",
    "IngestMetadataAvailability",
    "IngestObjectDelivery",
    "IngestObjectResume",
    "IngestReadConsistency",
    "IngestSourceCapabilities",
    "IngestSourceStoreAPI",
    "PreparedIngestObject",
]

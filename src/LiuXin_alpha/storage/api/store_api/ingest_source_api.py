"""
Describe optional Store ingest preparation, stability, delivery, and resume behavior.

Source-wide profiles state the strongest advertised qualities; prepared values
state the observations and guarantees available for one object. Frozen records
normalize selected values and reject selected contradictions without proving
backend behavior, validating every nested value, or redacting metadata.
Manager policy and destination selection remain outside this protocol.

Example:
    >>> profile = IngestSourceCapabilities("unguarded", "streaming")
    >>> profile.object_resume is IngestObjectResume.NONE
    True
"""

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
    """
    Strongest relationship a Store can guarantee between inspect and read.

    UNGUARDED makes no inspect/read binding promise; VERSION_PINNED uses an object version;
    IMMUTABLE advertises content that does not change. Enum ordering is not the comparison mechanism
    used by validation.

    Example:
        >>> IngestReadConsistency.VERSION_PINNED.value
        'version_pinned'
    """

    UNGUARDED = "unguarded"
    VERSION_PINNED = "version_pinned"
    IMMUTABLE = "immutable"


class IngestObjectDelivery(StrEnum):
    """
    How a source produces one object stream for its caller.

    STREAMING delivers incrementally, DISK_SPOOLED uses local file staging, and MEMORY_BUFFERED
    retains the object in memory. The enum itself imposes no byte or resource limit.

    Example:
        >>> IngestObjectDelivery.DISK_SPOOLED.value
        'disk_spooled'
    """

    STREAMING = "streaming"
    DISK_SPOOLED = "disk_spooled"
    MEMORY_BUFFERED = "memory_buffered"


class IngestInventoryResume(StrEnum):
    """
    Strongest inventory checkpointing mechanism exposed by a Store.

    NONE provides no inventory resume mechanism; CURSOR exposes a continuation position; SNAPSHOT
    advertises snapshot-based continuation. This enum does not create or validate checkpoints.

    Example:
        >>> IngestInventoryResume.SNAPSHOT.value
        'snapshot'
    """

    NONE = "none"
    CURSOR = "cursor"
    SNAPSHOT = "snapshot"


class IngestObjectResume(StrEnum):
    """
    Whether an interrupted object can resume without mixing versions.

    STABLE_RANGE advertises range resumption tied to stable object bytes. The profile rejects that
    claim with UNGUARDED reads; it does not implement resumption.

    Example:
        >>> IngestObjectResume.STABLE_RANGE.value
        'stable_range'
    """

    NONE = "none"
    STABLE_RANGE = "stable_range"


class IngestMetadataAvailability(StrEnum):
    """
    Where backend-native rich discovery metadata becomes available.

    NONE advertises no rich discovery metadata, INVENTORY exposes it while listing, and INSPECTION
    exposes it through preparation/inspection. These are source declarations rather than metadata
    contents.

    Example:
        >>> IngestMetadataAvailability.INSPECTION.value
        'inspection'
    """

    NONE = "none"
    INVENTORY = "inventory"
    INSPECTION = "inspection"


@dataclasses.dataclass(slots=True, frozen=True)
class IngestSourceCapabilities:
    """
    Advanced ingest qualities advertised by one configured source Store.

    These values describe the strongest behavior the Store may provide. A
    :class:`PreparedIngestObject` records the guarantees actually available for one object, since
    version tokens and checksums may be object-specific.

    Example:
        >>> profile = IngestSourceCapabilities(
        ...     read_consistency=IngestReadConsistency.VERSION_PINNED,
        ...     object_delivery=IngestObjectDelivery.STREAMING,
        ...     object_resume=IngestObjectResume.STABLE_RANGE,
        ...     authoritative_digest_algorithms=("sha256",),
        ... )
        >>> profile.object_resume
        <IngestObjectResume.STABLE_RANGE: 'stable_range'>


    :ivar read_consistency: Strongest advertised inspect/read relationship, normalized to its enum.
    :ivar object_delivery: Streaming, disk-spooled, or memory-buffered source delivery model.
    :ivar inventory_resume: Strongest advertised inventory checkpoint mechanism; NONE by default.
    :ivar object_resume: Whether stable byte-range resumption is available; NONE by default.
    :ivar authoritative_digest_algorithms: Unique normalized names for trusted source digests, without an implementation-support check.
    :ivar metadata_availability: Where rich discovery metadata is advertised to become available.
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
        """
        Normalize enum and algorithm values and reject contradictions.

        Algorithm names are converted with str, trimmed, and lowercased before uniqueness checks.
        The constructor does not check whether hashlib or the backend supports each name. Frozen
        fields are assigned only after validation.

        Example:
            >>> IngestSourceCapabilities(
            ...     IngestReadConsistency.UNGUARDED,
            ...     IngestObjectDelivery.STREAMING,
            ...     object_resume=IngestObjectResume.STABLE_RANGE,
            ... )
            Traceback (most recent call last):
            ...
            ValueError: stable object resume requires stable reads.


        :return: None after normalizing enums and digest names; invalid enums, empty/duplicate names, or unstable resume claims raise ValueError.
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
        """
        Require per-object claims to fit within this Store profile.

        Consistency ranks are explicit: unguarded, version-pinned, then immutable. A weaker object
        claim is accepted. This method does not validate Store ownership, delivery mode, inventory
        checkpoints, or stream contents.

        Example:
            >>> profile = IngestSourceCapabilities(
            ...     IngestReadConsistency.UNGUARDED,
            ...     IngestObjectDelivery.STREAMING,
            ... )
            >>> from uuid import UUID
            >>> from LiuXin_alpha.storage.api.models import Location
            >>> prepared = PreparedIngestObject(
            ...     StoreInventoryEntry(Location(UUID(int=1), "book")),
            ...     IngestReadConsistency.UNGUARDED,
            ... )
            >>> profile.validate_prepared(prepared)


        :param prepared: Object preparation whose consistency rank and authoritative digest algorithms are compared with this profile.
        :return: None when those claims fit; excessive consistency or an unadvertised algorithm raises ValueError.
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
    """
    Record per-object observations and the claimed binding to a later read.

    read_consistency is the guarantee available for this object, which may be weaker than the Store
    profile when no version is supplied. The record has no stream field and can be retained by a
    checkpoint, but it does not sanitize supplied metadata or provenance. Frozen fields do not imply
    deep validation or isolation of caller-supplied nested values.

    Example:
        >>> from uuid import UUID
        >>> from LiuXin_alpha.storage.api.models import Location
        >>> prepared = PreparedIngestObject(
        ...     StoreInventoryEntry(Location(UUID(int=1), "book.epub")),
        ...     read_consistency=IngestReadConsistency.UNGUARDED,
        ... )
        >>> prepared.info.location.key
        'book.epub'


    :ivar info: File or inventory observations for the candidate; Store ownership is not checked by this record.
    :ivar read_consistency: Actual per-object inspect/read guarantee, normalized to its enum.
    :ivar authoritative_digests: Trusted source digest claims with unique algorithm names; no byte verification occurs here.
    :ivar provenance_uri: Optional provenance text; credential removal and URI validation belong to its producer.
    """

    info: FileInfo | StoreInventoryEntry
    read_consistency: IngestReadConsistency
    authoritative_digests: tuple[Digest, ...] = ()
    provenance_uri: str | None = None

    def __post_init__(self) -> None:
        """
        Validate per-object consistency, digests, and provenance.

        VERSION_PINNED requires info.version to be non-None, without checking its type or nonempty
        text here. IMMUTABLE does not require a version. Authoritative algorithm names must be
        unique as supplied, and only the exact empty provenance string is rejected; URI syntax and
        credential redaction are not checked.

        Example:
            >>> from uuid import UUID
            >>> from LiuXin_alpha.storage.api.models import Location
            >>> PreparedIngestObject(
            ...     StoreInventoryEntry(Location(UUID(int=1), "book")),
            ...     IngestReadConsistency.VERSION_PINNED,
            ... )
            Traceback (most recent call last):
            ...
            ValueError: version-pinned ingest requires an object version.


        :return: None after consistency normalization and version/digest/provenance checks; rejected combinations raise ValueError.
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
    """
    Optional advanced source contract consumed by generic Store ingest.

    The protocol contains source mechanics only. Asset identity, placement, replica policy, and
    destination selection remain manager responsibilities.

    Runtime membership checks property/method presence, not whether returned preparations or opened
    streams satisfy these guarantees.

    Example:
        >>> isinstance(store, IngestSourceStoreAPI)  # doctest: +SKIP
        True
    """

    @property
    def ingest_capabilities(self) -> IngestSourceCapabilities:
        """
        Return the configured Store's strongest ingest qualities.

        Example:
            >>> store.ingest_capabilities.object_delivery  # doctest: +SKIP
            <IngestObjectDelivery.STREAMING: 'streaming'>


        :return: Strongest source-wide ingest guarantees; individual prepared objects may provide weaker consistency.
        """

        ...

    def prepare_ingest(
        self,
        info: FileInfo | StoreInventoryEntry,
        *,
        inspect: bool = True,
    ) -> PreparedIngestObject:
        """
        Prepare one candidate without opening or materialising its bytes.

        ``inspect=True`` asks the Store for its richest authoritative observations. Backends may
        satisfy that request from inventory data or perform a ``stat``-like operation. Unsupported
        inspection may retain the supplied inventory entry, but other failures remain visible.

        Example:
            >>> prepared = store.prepare_ingest(entry, inspect=True)  # doctest: +SKIP


        :param info: FileInfo or inventory observation from this Store to validate and optionally enrich.
        :param inspect: Whether to request richer authoritative observations, possibly through a stat-like call.
        :return: Prepared object observations and actual stability claims, without opening its byte stream.
        """

        ...

    def open_prepared_ingest(
        self,
        prepared: PreparedIngestObject,
        *,
        offset: int = 0,
    ) -> BinaryIO:
        """
        Open the prepared object with its advertised stability guarantee.

        Example:
            >>> with store.open_prepared_ingest(prepared) as source:  # doctest: +SKIP
            ...     payload = source.read()


        :param prepared: Previously prepared value from this Store, including any version required for stable reads.
        :param offset: Starting byte offset; nonzero offsets require supported stable-range resumption.
        :return: Binary input stream owned and closed by the caller, honoring the prepared stability guarantee.
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

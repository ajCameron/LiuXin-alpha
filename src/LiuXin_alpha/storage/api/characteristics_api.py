"""
Describe storage publication mechanics, temporary-space costs, and declared limits.

These values and the optional Store protocol supplement operation capabilities.
They normalize selected fields but do not probe storage, allocate capacity, or
turn an unknown limit into a guarantee that an operation is supported.
"""

from __future__ import annotations

import dataclasses

from enum import StrEnum
from typing import Protocol, runtime_checkable


class StoragePublicationModel(StrEnum):
    """
    Classify the publication mechanics advertised by a storage backend.

    UNKNOWN leaves the mechanism unspecified; READ_ONLY has no mutation publication, PER_OBJECT
    publishes an individual object, STAGING_THEN_SEAL collects writes for a later seal, and
    WHOLE_STORE_REBUILD recreates the container. These labels describe behavior without executing or
    independently verifying it.

    Example:
        >>> StoragePublicationModel("staging_then_seal") is StoragePublicationModel.STAGING_THEN_SEAL
        True
    """

    UNKNOWN = "unknown"
    READ_ONLY = "read_only"
    PER_OBJECT = "per_object"
    STAGING_THEN_SEAL = "staging_then_seal"
    WHOLE_STORE_REBUILD = "whole_store_rebuild"


class StorageTemporarySpaceRequirement(StrEnum):
    """
    Classify ordinary private-space requirements beyond the final published bytes.

    UNKNOWN makes no space claim, NONE advertises no private staging, OBJECT_STAGE stages a member,
    and STORE_COPY requires a Store/container copy. The category does not specify a byte budget or
    reserve space.

    Example:
        >>> StorageTemporarySpaceRequirement.STORE_COPY.value
        'store_copy'
    """

    UNKNOWN = "unknown"
    NONE = "none"
    OBJECT_STAGE = "object_stage"
    STORE_COPY = "store_copy"


class StorageWriteUsage(StrEnum):
    """
    Describe the write workload recommended for a backend's mechanics.

    UNKNOWN leaves guidance unspecified; NOT_APPLICABLE covers no writes, GENERAL ordinary mutation,
    OCCASIONAL infrequent mutation, and ARCHIVAL_SNAPSHOT finite packs or snapshots. This
    recommendation does not enable operations or enforce scheduling.

    Example:
        >>> StorageWriteUsage.ARCHIVAL_SNAPSHOT.value
        'archival_snapshot'
    """

    UNKNOWN = "unknown"
    NOT_APPLICABLE = "not_applicable"
    GENERAL = "general"
    OCCASIONAL = "occasional"
    ARCHIVAL_SNAPSHOT = "archival_snapshot"


@dataclasses.dataclass(slots=True, frozen=True)
class StorageLimitation:
    """
    Retain a stable limitation code and its human-readable explanation after stripping outer
    whitespace.

    Example:
        >>> StorageLimitation(" archive_limit ", " Bounded expansion. ").code
        'archive_limit'


    :ivar code: Nonempty stripped code used for exact lookup and uniqueness checks.
    :ivar message: Nonempty stripped explanation without code-registry or markup validation.
    """

    code: str
    message: str

    def __post_init__(self) -> None:
        """
        Strip both strings, require nonempty results, and retain them on the frozen record.

        No case normalization or code-registry validation occurs; inputs are expected to provide
        string methods.

        Example:
            >>> StorageLimitation("", "missing code")
            Traceback (most recent call last):
            ...
            ValueError: storage limitation code must not be empty.


        :return: None after normalization; empty code or message raises ValueError.
        """

        code = self.code.strip()
        message = self.message.strip()
        if not code:
            raise ValueError("storage limitation code must not be empty.")
        if not message:
            raise ValueError("storage limitation message must not be empty.")
        object.__setattr__(self, "code", code)
        object.__setattr__(self, "message", message)


@dataclasses.dataclass(slots=True, frozen=True)
class StorageCharacteristics:
    """
    Describe publication, staging cost, declared bounds, and limitations beyond capability flags.

    None and UNKNOWN mean no claim was supplied; callers must not equate them with unlimited
    resources or a supported operation. The local size helper only rejects a known exceeded limit.
    Frozen attributes do not coerce limitation containers or deeply freeze supplied objects.

    Example:
        >>> profile = StorageCharacteristics(max_object_bytes=4)
        >>> profile.accepts_object_size(5)
        False


    :ivar publication_model: Advertised mutation-publication category, normalized to its enum.
    :ivar temporary_space: Advertised staging-space category, normalized to its enum.
    :ivar recommended_write_usage: Workload recommendation, normalized to its enum.
    :ivar max_object_bytes: Optional declared logical object-size ceiling in bytes.
    :ivar max_component_bytes: Optional declared address-component byte ceiling; backend documentation defines encoding.
    :ivar max_path_depth: Optional declared maximum address depth.
    :ivar preserves_unmodelled_entries: Optional claim about retaining entries outside the backend's model.
    :ivar rewrites_container_format: Optional claim that mutation rewrites container representation.
    :ivar limitations: Ordered limitation records with unique codes, retained as supplied.
    """

    publication_model: StoragePublicationModel = StoragePublicationModel.UNKNOWN
    temporary_space: StorageTemporarySpaceRequirement = (
        StorageTemporarySpaceRequirement.UNKNOWN
    )
    recommended_write_usage: StorageWriteUsage = StorageWriteUsage.UNKNOWN
    max_object_bytes: int | None = None
    max_component_bytes: int | None = None
    max_path_depth: int | None = None
    preserves_unmodelled_entries: bool | None = None
    rewrites_container_format: bool | None = None
    limitations: tuple[StorageLimitation, ...] = ()

    def __post_init__(self) -> None:
        """
        Coerce the three enum fields and reject bounds below one or duplicate limitation codes.

        Numeric fields are not coerced or checked for integer type/finiteness. Optional boolean
        claims and limitation object/container types are not otherwise validated. Enum conversion
        and attribute/comparison errors may propagate.

        Example:
            >>> StorageCharacteristics(publication_model="read_only").publication_model is StoragePublicationModel.READ_ONLY
            True


        :return: None after enum normalization and the stated consistency checks; invalid values raise through those operations.
        """

        object.__setattr__(
            self,
            "publication_model",
            StoragePublicationModel(self.publication_model),
        )
        object.__setattr__(
            self,
            "temporary_space",
            StorageTemporarySpaceRequirement(self.temporary_space),
        )
        object.__setattr__(
            self,
            "recommended_write_usage",
            StorageWriteUsage(self.recommended_write_usage),
        )
        for field_name in (
            "max_object_bytes",
            "max_component_bytes",
            "max_path_depth",
        ):
            value = getattr(self, field_name)
            if value is not None and value < 1:
                raise ValueError(f"{field_name} must be positive when provided.")
        codes = tuple(limitation.code for limitation in self.limitations)
        if len(codes) != len(set(codes)):
            raise ValueError("storage limitation codes must be unique.")

    def accepts_object_size(self, size: int) -> bool:
        """
        Reject negative declared sizes and compare nonnegative sizes with the optional object
        ceiling.

        No declared ceiling returns True, meaning only that no known size limit rejects the request.
        This method does not test write support, free capacity, or payload contents, and adds no
        integer/finiteness check.

        Example:
            >>> StorageCharacteristics().accepts_object_size(100)
            True
            >>> StorageCharacteristics(max_object_bytes=4).accepts_object_size(5)
            False


        :param size: Expected nonnegative logical byte count to compare with max_object_bytes.
        :return: True when the ceiling is absent or not exceeded; negative values raise ValueError.
        """

        if size < 0:
            raise ValueError("object size must not be negative.")
        return self.max_object_bytes is None or size <= self.max_object_bytes

    def limitation(self, code: str) -> StorageLimitation | None:
        """
        Return the first limitation whose retained code exactly equals the requested code.

        Lookup does not strip or normalize the query, probe a backend, or enforce the limitation.

        Example:
            >>> profile = StorageCharacteristics(limitations=(StorageLimitation(" x ", "X"),))
            >>> profile.limitation("x").message
            'X'
            >>> profile.limitation(" x ") is None
            True


        :param code: Exact stable limitation code to find.
        :return: Matching retained StorageLimitation, or None when absent.
        """

        return next(
            (item for item in self.limitations if item.code == code),
            None,
        )


@runtime_checkable
class StoreCharacteristicsAPI(Protocol):
    """
    Describe an optional configured-Store property exposing structured characteristics.

    Runtime protocol checks inspect structural membership rather than proving return types, current
    availability, or correctness of advertised limits.

    Example:
        >>> isinstance(store, StoreCharacteristicsAPI)  # doctest: +SKIP
        True
    """

    @property
    def characteristics(self) -> StorageCharacteristics:
        """
        Expose the implementation's structured publication, staging, and limitation claims.

        This protocol supplies no default probe or enforcement behavior.

        Example:
            >>> store.characteristics.publication_model  # doctest: +SKIP
            <StoragePublicationModel.PER_OBJECT: 'per_object'>


        :return: StorageCharacteristics for the configured Store, with unknown fields left explicit.
        """

        ...


__all__ = [
    "StorageCharacteristics",
    "StorageLimitation",
    "StoragePublicationModel",
    "StorageTemporarySpaceRequirement",
    "StorageWriteUsage",
    "StoreCharacteristicsAPI",
]

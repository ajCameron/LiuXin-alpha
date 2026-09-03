"""Structured storage behaviour beyond simple operation support."""

from __future__ import annotations

import dataclasses

from enum import StrEnum
from typing import Protocol, runtime_checkable


class StoragePublicationModel(StrEnum):
    """Granularity at which one committed mutation changes stored bytes.

    Example:
        >>> StoragePublicationModel.WHOLE_STORE_REBUILD.value
        'whole_store_rebuild'
    """

    UNKNOWN = "unknown"
    READ_ONLY = "read_only"
    PER_OBJECT = "per_object"
    STAGING_THEN_SEAL = "staging_then_seal"
    WHOLE_STORE_REBUILD = "whole_store_rebuild"


class StorageTemporarySpaceRequirement(StrEnum):
    """Private staging space normally required for one publication.

    Example:
        >>> StorageTemporarySpaceRequirement.STORE_COPY.value
        'store_copy'
    """

    UNKNOWN = "unknown"
    NONE = "none"
    OBJECT_STAGE = "object_stage"
    STORE_COPY = "store_copy"


class StorageWriteUsage(StrEnum):
    """Workload for which a backend's mutation mechanics are intended.

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
    """One stable machine code paired with an operator-facing explanation.

    Example:
        >>> StorageLimitation("whole_store_rebuild", "Each mutation rebuilds the Store.").code
        'whole_store_rebuild'
    """

    code: str
    message: str

    def __post_init__(self) -> None:
        """Require non-empty, normalized limitation metadata.

        Example:
            >>> StorageLimitation("", "missing code")
            Traceback (most recent call last):
            ...
            ValueError: storage limitation code must not be empty.
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
    """Constraints and cost characteristics not captured by capability flags.

    ``None`` and ``UNKNOWN`` deliberately mean that a backend made no claim;
    callers must not interpret missing information as an unlimited or cheap
    operation.

    Example:
        >>> profile = StorageCharacteristics(
        ...     publication_model=StoragePublicationModel.WHOLE_STORE_REBUILD,
        ...     max_object_bytes=(1 << 32) - 1,
        ... )
        >>> profile.accepts_object_size(1 << 32)
        False
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
        """Normalize enums and validate numeric bounds and limitation codes.

        Example:
            >>> StorageCharacteristics(max_object_bytes=0)
            Traceback (most recent call last):
            ...
            ValueError: max_object_bytes must be positive when provided.
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
        """Return whether a declared object size fits the advertised limit.

        Example:
            >>> StorageCharacteristics(max_object_bytes=4).accepts_object_size(5)
            False

        :param size: Non-negative logical object size in bytes.
        :return: Whether the size is not known to exceed the backend limit.
        """

        if size < 0:
            raise ValueError("object size must not be negative.")
        return self.max_object_bytes is None or size <= self.max_object_bytes

    def limitation(self, code: str) -> StorageLimitation | None:
        """Return one limitation by stable code, when advertised.

        Example:
            >>> profile = StorageCharacteristics(limitations=(StorageLimitation("x", "X"),))
            >>> profile.limitation("x").message
            'X'

        :param code: Stable limitation code.
        :return: Matching limitation or ``None``.
        """

        return next(
            (item for item in self.limitations if item.code == code),
            None,
        )


@runtime_checkable
class StoreCharacteristicsAPI(Protocol):
    """Optional configured-Store contract for structured constraints.

    Example:
        >>> isinstance(store, StoreCharacteristicsAPI)  # doctest: +SKIP
        True
    """

    @property
    def characteristics(self) -> StorageCharacteristics:
        """Return characteristics for this configured Store.

        Example:
            >>> store.characteristics.publication_model  # doctest: +SKIP
            <StoragePublicationModel.PER_OBJECT: 'per_object'>
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

"""
Atomic Digital Asset identity and descriptive metadata values.
"""

from __future__ import annotations

import dataclasses

from LiuXin_alpha.storage.api.models import Digest
from LiuXin_alpha.storage.api.storage_manager_api.models.identifiers import (
    BackupPolicyID,
    DigitalAssetID,
    ReplicationPolicyID,
)


@dataclasses.dataclass(slots=True, frozen=True)
class DigitalAssetMetadata:
    """
    Descriptive and technical metadata belonging to byte identity.

    Example:
        >>> metadata = DigitalAssetMetadata(original_name="book.epub")
        >>> metadata.original_name
        'book.epub'
    """

    name: str | None = None
    media_type: str | None = None
    original_name: str | None = None
    attributes: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        """
        Validate optional labels and extension attributes.

        Example:
            >>> DigitalAssetMetadata(media_type="")
            Traceback (most recent call last):
            ...
            ValueError: media_type must not be empty when supplied.


        :return:
        """

        for field_name, value in (
            ("name", self.name),
            ("media_type", self.media_type),
            ("original_name", self.original_name),
        ):
            if value is not None and not value.strip():
                raise ValueError(f"{field_name} must not be empty when supplied.")
        names = [name for name, _ in self.attributes]
        if any(not name.strip() for name in names):
            raise ValueError("asset attribute names must not be empty.")
        if len(names) != len(set(names)):
            raise ValueError("asset attribute names must be unique.")


@dataclasses.dataclass(slots=True, frozen=True)
class DigitalAssetDeclaration:
    """
    Input for declaring a known atomic byte sequence.

    Example:
        >>> declaration = DigitalAssetDeclaration(
        ...     4, (Digest("sha256", "abcd"),),
        ... )
        >>> declaration.size_bytes
        4
    """

    size_bytes: int
    digests: tuple[Digest, ...]
    metadata: DigitalAssetMetadata = dataclasses.field(
        default_factory=DigitalAssetMetadata
    )
    replication_policy_id: ReplicationPolicyID | None = None
    backup_policy_id: BackupPolicyID | None = None

    def __post_init__(self) -> None:
        """
        Require a valid size-and-digest identity.

        Example:
            >>> DigitalAssetDeclaration(1, ())
            Traceback (most recent call last):
            ...
            ValueError: a Digital Asset requires at least one digest.


        :return:
        """

        validate_asset_identity(self.size_bytes, self.digests)


@dataclasses.dataclass(slots=True, frozen=True)
class DigitalAssetRecord:
    """
    Manager-maintained facts about one atomic byte identity.

    Example:
        >>> record = DigitalAssetRecord(
        ...     DigitalAssetID(7), 4, (Digest("sha256", "abcd"),),
        ... )
        >>> record.digital_asset_id
        7
    """

    digital_asset_id: DigitalAssetID
    size_bytes: int
    digests: tuple[Digest, ...]
    metadata: DigitalAssetMetadata = dataclasses.field(
        default_factory=DigitalAssetMetadata
    )
    replication_policy_id: ReplicationPolicyID | None = None
    backup_policy_id: BackupPolicyID | None = None
    revision: str | None = None

    def __post_init__(self) -> None:
        """
        Validate identity and the optional optimistic-lock revision.

        Example:
            >>> DigitalAssetRecord(
            ...     DigitalAssetID(0), 1, (Digest("sha256", "a"),),
            ... )
            Traceback (most recent call last):
            ...
            ValueError: digital_asset_id must be positive.


        :return:
        """

        if self.digital_asset_id <= 0:
            raise ValueError("digital_asset_id must be positive.")
        validate_asset_identity(self.size_bytes, self.digests)
        if self.revision is not None and not self.revision:
            raise ValueError("revision must not be empty when supplied.")


def validate_asset_identity(
    size_bytes: int,
    digests: tuple[Digest, ...],
) -> None:
    """
    Validate the size-and-digest identity shared by inputs and records.

    Example:
        >>> validate_asset_identity(1, (Digest("sha256", "aa"),))


    :param size_bytes:
    :param digests:
    :return:
    """

    if size_bytes < 0:
        raise ValueError("size_bytes must not be negative.")
    if not digests:
        raise ValueError("a Digital Asset requires at least one digest.")
    validate_unique_digests(digests)


def validate_unique_digests(digests: tuple[Digest, ...]) -> None:
    """
    Require at most one digest value per algorithm.

    Example:
        >>> validate_unique_digests((Digest("sha256", "aa"),))


    :param digests:
    :return:
    """

    algorithms = [digest.algorithm for digest in digests]
    if len(algorithms) != len(set(algorithms)):
        raise ValueError("digest algorithms must be unique.")


__all__ = [
    "DigitalAssetDeclaration",
    "DigitalAssetMetadata",
    "DigitalAssetRecord",
]

"""
Represent expected atomic byte identities and descriptive Asset metadata.

Declarations and records validate selected value constraints without reading bytes
or consulting repositories. Shared validators check size comparisons and digest
algorithm uniqueness; manager operations own policy lookup and registration.
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
    Retain descriptive labels and extension attributes for an atomic byte identity.

    Optional labels must be nonblank when supplied, and attribute names must be nonblank and unique
    in their original spelling. Validation does not strip retained text, check attribute values, or
    copy nested containers; the frozen record is not a deep immutability boundary. These values
    describe an Asset without proving any bytes exist.

    Example:
        >>> metadata = DigitalAssetMetadata(original_name="book.epub")
        >>> metadata.original_name
        'book.epub'


    :ivar name: Optional display label, retained without whitespace normalization.
    :ivar media_type: Optional media-type text; no MIME syntax or registry validation occurs.
    :ivar original_name: Optional original filename or source label, not a validated filesystem path.
    :ivar attributes: Ordered extension name/value pairs; only names receive nonblank and uniqueness checks.
    """

    name: str | None = None
    media_type: str | None = None
    original_name: str | None = None
    attributes: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        """
        Reject blank optional labels and blank or repeated attribute names.

        Names are compared exactly, so differently spaced names can coexist. String methods, pair
        unpacking, and hashing may raise for malformed inputs. Attribute values remain unchecked and
        nothing is reassigned or copied.

        Example:
            >>> DigitalAssetMetadata(media_type="")
            Traceback (most recent call last):
            ...
            ValueError: media_type must not be empty when supplied.


        :return: None when label/name checks pass; validation and malformed-input errors propagate.
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
    Describe an expected byte identity before the manager assigns its Asset ID.

    Construction checks the size comparison, presence of digests, and unique digest algorithms. It
    does not read bytes, verify digest values, require a Replica, or validate the supplied metadata
    and policy references. The manager owns registration, deduplication, and policy existence
    checks.

    Example:
        >>> declaration = DigitalAssetDeclaration(
        ...     4, (Digest("sha256", "abcd"),),
        ... )
        >>> declaration.size_bytes
        4


    :ivar size_bytes: Expected byte count, rejected only when it compares below zero.
    :ivar digests: Nonempty sequence of expected digests with distinct algorithm attributes.
    :ivar metadata: Descriptive values, defaulting to a new empty metadata record.
    :ivar replication_policy_id: Optional desired policy ID; construction does not resolve it.
    :ivar backup_policy_id: Optional desired backup-policy ID; construction does not resolve it.
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
        Delegate the shared size/digest identity checks without validating metadata or policy
        references.

        Example:
            >>> DigitalAssetDeclaration(1, ())
            Traceback (most recent call last):
            ...
            ValueError: a Digital Asset requires at least one digest.


        :return: None after validate_asset_identity succeeds; its errors propagate.
        """

        validate_asset_identity(self.size_bytes, self.digests)


@dataclasses.dataclass(slots=True, frozen=True)
class DigitalAssetRecord:
    """
    Retain a manager-assigned Asset identity with descriptive metadata and policy references.

    The record describes expected bytes independently of physical Replica availability. Direct
    construction checks an ID that does not compare at or below zero, shared size/digest
    constraints, and a truthy supplied revision; it does not enforce integer types, query
    repositories, or copy retained containers.

    Example:
        >>> record = DigitalAssetRecord(
        ...     DigitalAssetID(7), 4, (Digest("sha256", "abcd"),),
        ... )
        >>> record.digital_asset_id
        7


    :ivar digital_asset_id: Manager identity rejected when it compares at or below zero; integer types and finiteness are not enforced.
    :ivar size_bytes: Expected byte count checked by validate_asset_identity.
    :ivar digests: Expected digests with at least one entry and unique algorithm attributes.
    :ivar metadata: Retained descriptive metadata, separate from size/digest identity.
    :ivar replication_policy_id: Optional stored replication-policy reference, not resolved here.
    :ivar backup_policy_id: Optional stored backup-policy reference, not resolved here.
    :ivar revision: Optional optimistic-lock token; false values reject, but whitespace is not stripped.
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
        Reject nonpositive IDs, invalid size/digest structure, and false revisions when supplied.

        These comparisons do not enforce integer IDs or sizes. Revision text is not normalized,
        metadata and policy IDs are not examined, and no physical or repository lookup occurs.

        Example:
            >>> DigitalAssetRecord(
            ...     DigitalAssetID(0), 1, (Digest("sha256", "a"),),
            ... )
            Traceback (most recent call last):
            ...
            ValueError: digital_asset_id must be positive.


        :return: None when these value checks succeed; comparison and identity-validation errors propagate.
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
    Reject a size below zero, an empty digest collection, or repeated digest algorithms.

    The size is compared directly without integer or finiteness checks. Digest entries are inspected
    through their algorithm attributes; this function neither calculates hashes nor compares values
    with a byte source.

    Example:
        >>> validate_asset_identity(1, (Digest("sha256", "aa"),))


    :param size_bytes: Expected byte count; only the less-than-zero comparison is validated.
    :param digests: Nonempty digest collection passed to validate_unique_digests without copying.
    :return: None when the size/digest structure passes; invalid values or malformed entries raise.
    """

    if size_bytes < 0:
        raise ValueError("size_bytes must not be negative.")
    if not digests:
        raise ValueError("a Digital Asset requires at least one digest.")
    validate_unique_digests(digests)


def validate_unique_digests(digests: tuple[Digest, ...]) -> None:
    """
    Require distinct algorithm attributes across the supplied digest entries.

    An empty collection is allowed here. Algorithm values are compared as supplied; ordinary Digest
    construction has already stripped and lowercased its fields. This helper does not validate entry
    types, digest values, supported algorithms, or hash length.

    Example:
        >>> validate_unique_digests((Digest("sha256", "aa"),))


    :param digests: Digest entries whose algorithm attributes must be unique and hashable.
    :return: None if no algorithm repeats; duplicate algorithms raise ValueError and malformed entries can raise their own errors.
    """

    algorithms = [digest.algorithm for digest in digests]
    if len(algorithms) != len(set(algorithms)):
        raise ValueError("digest algorithms must be unique.")


__all__ = [
    "DigitalAssetDeclaration",
    "DigitalAssetMetadata",
    "DigitalAssetRecord",
]

"""
Define private ingest requests, completed results, replay branches, and helper type aliases.

Request dataclasses retain equality evidence without performing normalization,
validation, or deep copying. Five persisted request/result classes deliberately
have stable journal identifiers supplied by the database codec. ID conversion
and backed-Store UUID helpers are internal implementation details; type aliases
describe factory, registration, target, and metadata-key shapes without runtime checks.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable
from typing import Literal, Protocol
from uuid import NAMESPACE_URL, uuid5

from LiuXin_alpha.storage.api import models as storage_models
from LiuXin_alpha.storage.api import placement_hints_api, store_api
from LiuXin_alpha.storage.api import storage_manager_api as manager_api

# Explicit exports within the private mixin implementation package.
__all__ = [
    "StoreFactory",
    "StoreRegistration",
    "_AdoptIngestRequest",
    "_Hasher",
    "_IdentifiedStreamIngestRequest",
    "_IngestOperation",
    "_IngestRequest",
    "_ItemTarget",
    "_ItemTargetID",
    "_ItemTargetKind",
    "_MetadataRecordKind",
    "_RecreationBranch",
    "_StoreObjectIngestRequest",
    "_StreamIngestRequest",
]

type StoreFactory = Callable[[manager_api.StoreConfiguration], store_api.StoreAPI]
type StoreRegistration = tuple[manager_api.StoreConfiguration, store_api.StoreAPI]
type _ItemTargetKind = Literal["digital_asset", "composite_digital_asset"]
type _ItemTargetID = manager_api.DigitalAssetID | manager_api.CompositeDigitalAssetID
type _ItemTarget = tuple[_ItemTargetKind, _ItemTargetID]
type _MetadataRecordKind = Literal[
    "digital_asset",
    "replica",
    "composite",
    "derivation",
    "replication_policy",
    "backup_policy",
]


@dataclasses.dataclass(slots=True, frozen=True)
class _StreamIngestRequest:
    """
    Retain normalized ordinary-stream input and caller intent for completed-operation equality.

    The ingest caller computes and sorts identity evidence before constructing this value. This
    dataclass neither normalizes nor validates fields. Equality includes expectations and all
    options, so equal bytes with a changed expected_size can be a different request. Frozen fields
    retain nested objects; the database codec supplies the stable journal type identifier.

    Example:
        >>> request = _StreamIngestRequest(  # doctest: +SKIP
        ...     size, observed, None, (), None, None, manager_api.DigitalAssetMetadata(),
        ...     None, None, manager_api.ReplicaMode.ACTIVE, True,
        ... )


    :ivar size_bytes: Measured remaining stream length supplied by the ingest caller.
    :ivar observed_digests: Computed input digests, normally including SHA-256 in stable algorithm order.
    :ivar expected_size: Original optional size expectation; None remains distinct from an explicit size.
    :ivar expected_digests: Original digest expectations normalized by the caller, not by construction.
    :ivar item_id: Optional Item identity retained for completion-time linking and retry equality.
    :ivar role: Requested role retained before the linking layer applies its None default.
    :ivar metadata: Retained Asset description; this value performs no copying or metadata update.
    :ivar replica_mode: Requested Replica purpose, compared as part of the request without validation here.
    :ivar verify: Requested verification flag, distinct from a completed verification result.
    :ivar placement_hints: Optional advisory placement record, retained in request equality.
    :ivar preferred_store_ref: Requested destination UUID or None for deferred manager-default selection.
    """

    size_bytes: int
    observed_digests: tuple[storage_models.Digest, ...]
    expected_size: int | None
    expected_digests: tuple[storage_models.Digest, ...]
    item_id: manager_api.ItemID | None
    role: str | None
    metadata: manager_api.DigitalAssetMetadata
    placement_hints: placement_hints_api.StoragePlacementHints | None
    preferred_store_ref: storage_models.StoreUUID | None
    replica_mode: manager_api.ReplicaMode
    verify: bool


@dataclasses.dataclass(slots=True, frozen=True)
class _AdoptIngestRequest:
    """
    Retain the existing Location and adoption options used to recognize a completed retry.

    Identity excludes freshly observed bytes or a Store version. A completed equal request can
    therefore return its recorded result without stat or hashing. Construction performs no
    validation or deep copy; its legacy module identity is retained for persisted journal values.

    Example:
        >>> request = _AdoptIngestRequest(  # doctest: +SKIP
        ...     location, None, None, None, manager_api.DigitalAssetMetadata(),
        ...     manager_api.ReplicaMode.UNMANAGED, False,
        ... )


    :ivar location: Existing concrete Store address to adopt.
    :ivar digital_asset_id: Optional explicitly requested Asset identity, retained before byte comparison.
    :ivar item_id: Optional Item identity retained for completion-time linking and retry equality.
    :ivar role: Requested role retained before the linking layer applies its None default.
    :ivar metadata: Retained Asset description; this value performs no copying or metadata update.
    :ivar replica_mode: Requested Replica purpose, compared as part of the request without validation here.
    :ivar verify: Requested verification flag, distinct from a completed verification result.
    """

    location: storage_models.Location
    digital_asset_id: manager_api.DigitalAssetID | None
    item_id: manager_api.ItemID | None
    role: str | None
    metadata: manager_api.DigitalAssetMetadata
    replica_mode: manager_api.ReplicaMode
    verify: bool


@dataclasses.dataclass(slots=True, frozen=True)
class _IdentifiedStreamIngestRequest:
    """
    Retain authoritative stream identity and options without storing or verifying source bytes.

    Callers validate size/digest structure and normalize ordering before construction. Equality uses
    this request class and its fields; it does not equate an ordinary-stream or native-transfer
    request with similar content. Completed matching requests can be reused without consuming the
    source. Nested values remain shared and legacy journal naming is preserved.

    Example:
        >>> request = _IdentifiedStreamIngestRequest(  # doctest: +SKIP
        ...     size, digests, None, None, manager_api.DigitalAssetMetadata(),
        ...     None, None, manager_api.ReplicaMode.ACTIVE, True,
        ... )


    :ivar size_bytes: Trusted remaining byte count supplied to the identified ingest path.
    :ivar authoritative_digests: Trusted digest tuple, normally validated to contain unique algorithms and SHA-256.
    :ivar item_id: Optional Item identity retained for completion-time linking and retry equality.
    :ivar role: Requested role retained before the linking layer applies its None default.
    :ivar metadata: Retained Asset description; this value performs no copying or metadata update.
    :ivar replica_mode: Requested Replica purpose, compared as part of the request without validation here.
    :ivar verify: Requested verification flag, distinct from a completed verification result.
    :ivar placement_hints: Optional advisory placement record, retained in request equality.
    :ivar preferred_store_ref: Requested destination UUID or None for deferred manager-default selection.
    """

    size_bytes: int
    authoritative_digests: tuple[storage_models.Digest, ...]
    item_id: manager_api.ItemID | None
    role: str | None
    metadata: manager_api.DigitalAssetMetadata
    placement_hints: placement_hints_api.StoragePlacementHints | None
    preferred_store_ref: storage_models.StoreUUID | None
    replica_mode: manager_api.ReplicaMode
    verify: bool


@dataclasses.dataclass(slots=True, frozen=True)
class _StoreObjectIngestRequest:
    """
    Retain native Store-transfer source evidence and ingest options for retry comparison.

    Source version is part of request equality; retaining it does not enforce a version-pinned
    transfer. The value neither probes Stores nor validates digests, and it retains caller-owned
    nested values. The database codec owns its stable serialization identifier.

    Example:
        >>> request = _StoreObjectIngestRequest(  # doctest: +SKIP
        ...     location, version, size, digests, None, None, manager_api.DigitalAssetMetadata(),
        ...     None, None, manager_api.ReplicaMode.ACTIVE, True,
        ... )


    :ivar source_location: Concrete source address supplied to the native transfer path.
    :ivar source_version: Optional observed source-version token retained without comparison to live bytes.
    :ivar size_bytes: Expected source byte count supplied by stat or preparation.
    :ivar authoritative_digests: Trusted identity used by native publication, normally a single SHA-256.
    :ivar item_id: Optional Item identity retained for completion-time linking and retry equality.
    :ivar role: Requested role retained before the linking layer applies its None default.
    :ivar metadata: Retained Asset description; this value performs no copying or metadata update.
    :ivar replica_mode: Requested Replica purpose, compared as part of the request without validation here.
    :ivar verify: Requested verification flag, distinct from a completed verification result.
    :ivar placement_hints: Optional advisory placement record, retained in request equality.
    :ivar preferred_store_ref: Requested destination UUID or None for deferred manager-default selection.
    """

    source_location: storage_models.Location
    source_version: str | None
    size_bytes: int
    authoritative_digests: tuple[storage_models.Digest, ...]
    item_id: manager_api.ItemID | None
    role: str | None
    metadata: manager_api.DigitalAssetMetadata
    placement_hints: placement_hints_api.StoragePlacementHints | None
    preferred_store_ref: storage_models.StoreUUID | None
    replica_mode: manager_api.ReplicaMode
    verify: bool


type _IngestRequest = (
    _StreamIngestRequest
    | _IdentifiedStreamIngestRequest
    | _StoreObjectIngestRequest
    | _AdoptIngestRequest
)


@dataclasses.dataclass(slots=True, frozen=True)
class _IngestOperation:
    """
    Pair a completed ingest request with the result returned for a matching retry.

    No relationship between request and result is validated, and both remain retained references.
    The operation UUID belongs to the surrounding repository key, not this value. The database
    codec retains stable journal identifiers independently of this class import path.

    Example:
        >>> _IngestOperation.__module__
        'LiuXin_alpha.storage.storage_manager.mixins._types'


    :ivar request: Completed request whose class and field equality identify a retry.
    :ivar result: Recorded ingest result returned on a matching completed request without reconstruction.
    """

    request: _IngestRequest
    result: manager_api.DigitalAssetIngestResult


@dataclasses.dataclass(slots=True, frozen=True)
class _RecreationBranch:
    """
    Carry one recursive replay proposal and its availability evidence between policy helpers.

    Construction does not validate consistency, unique IDs, step ordering, exactness, or
    availability. Callers own those decisions and may memoize the value under an Asset ID. Frozen
    fields do not copy supplied containers; viable is evidence for a branch, not proof of execution.

    Example:
        >>> branch = _RecreationBranch(False, unavailable_digital_asset_ids=frozenset({manager_api.DigitalAssetID(7)}))
        >>> branch.viable
        False


    :ivar viable: Caller-computed feasibility flag, independent of constructor validation.
    :ivar steps: Proposed prerequisite-before-consumer derivation records, retained in supplied order.
    :ivar available_digital_asset_ids: Supplied Asset IDs considered currently readable.
    :ivar unavailable_digital_asset_ids: Supplied Asset IDs for which no viable route was found.
    :ivar selected_derivation_id: Optional chosen recipe for the branch root, not checked against steps here.
    :ivar alternative_derivation_ids: Other viable recipe IDs accumulated by recursive planning.
    :ivar warnings: Retained diagnostics from branch selection and rejected alternatives.
    """

    viable: bool
    steps: tuple[manager_api.DigitalAssetDerivationRecord, ...] = ()
    available_digital_asset_ids: frozenset[manager_api.DigitalAssetID] = frozenset()
    unavailable_digital_asset_ids: frozenset[manager_api.DigitalAssetID] = frozenset()
    selected_derivation_id: manager_api.DigitalAssetDerivationID | None = None
    alternative_derivation_ids: tuple[manager_api.DigitalAssetDerivationID, ...] = ()
    warnings: tuple[str, ...] = ()


class _Hasher(Protocol):
    """
    Describe the two hashlib operations used by incremental manager digest calculation.

    The protocol supplies typing structure only and is not runtime-checkable. Concrete hash objects
    own algorithm support, digest state, and input validation.

    Example:
        >>> import hashlib
        >>> hasher: _Hasher = hashlib.sha256(b"data")
        >>> len(hasher.hexdigest())
        64
    """

    def update(self, data: bytes, /) -> None:
        """
        Feed one byte chunk into the concrete hash object's accumulated state.

        Example:
            >>> import hashlib
            >>> hasher = hashlib.sha256()
            >>> hasher.update(b"data")


        :param data: Next positional byte chunk to incorporate in the digest.
        :return: None after updating the concrete hash state.
        """

        ...

    def hexdigest(self) -> str:
        """
        Read the current digest as hexadecimal text without finalizing or resetting the hash state.

        Example:
            >>> import hashlib
            >>> hashlib.sha256(b"data").hexdigest()[:8]
            '3a6eb079'


        :return: Lowercase hexadecimal digest produced by the concrete hash implementation.
        """

        ...


def _replication_policy_id(
    value: manager_api.ReplicationPolicyID | manager_api.ReplicationPolicyRecord | None,
) -> manager_api.ReplicationPolicyID | None:
    """
    Accept None, extract a replication-policy record ID, or coerce another value with int.

    The record branch returns its stored ID without a positivity check. Other values must convert to
    an integer greater than zero; this can admit numeric strings, booleans, and truncated positive
    floats. No registry lookup or record-content validation occurs.

    Example:
        >>> _replication_policy_id(manager_api.ReplicationPolicyID(4))
        4
        >>> _replication_policy_id(None) is None
        True


    :param value: Optional policy ID or record; non-record inputs use int conversion.
    :return: None, the record ID as retained, or a positive nominal ID; nonpositive converted IDs raise TypeError and conversion errors propagate.
    """

    if value is None:
        return None
    if isinstance(value, manager_api.ReplicationPolicyRecord):
        return value.replication_policy_id
    identifier = int(value)
    if identifier <= 0:
        raise TypeError("replication must be a positive policy ID or policy record.")
    return manager_api.ReplicationPolicyID(identifier)


def _backup_policy_id(
    value: manager_api.BackupPolicyID | manager_api.BackupPolicyRecord | None,
) -> manager_api.BackupPolicyID | None:
    """
    Accept None, extract a backup-policy record ID, or coerce another value with int.

    Record IDs bypass the positive comparison applied to converted values. This helper does not
    query registered policies or validate the policy definition; int conversion can accept values
    outside the annotated input types.

    Example:
        >>> _backup_policy_id(manager_api.BackupPolicyID(5))
        5
        >>> _backup_policy_id(None) is None
        True


    :param value: Optional backup-policy ID or record; non-record values are converted with int.
    :return: None, the unvalidated stored record ID, or a positive nominal ID; nonpositive converted IDs raise TypeError.
    """

    if value is None:
        return None
    if isinstance(value, manager_api.BackupPolicyRecord):
        return value.backup_policy_id
    identifier = int(value)
    if identifier <= 0:
        raise TypeError("backup must be a positive policy ID or policy record.")
    return manager_api.BackupPolicyID(identifier)


def _backed_store_uuid(
    asset_record: manager_api.DigitalAssetRecord,
    kind: str,
    options: tuple[tuple[str, object], ...],
) -> storage_models.StoreUUID:
    """
    Derive a UUID5 Store-view key from size, one preferred digest, normalized kind, and option
    representation.

    Prefer SHA-256, otherwise the lexically first algorithm/value pair. Asset ID, metadata, and
    extra digests do not enter the key. Kind is stripped, lowercased, and has hyphens replaced with
    underscores. Options are sorted only by their top-level names, then serialized with repr; nested
    ordering, duplicate-name order, or unstable object representations can change the UUID. This is
    deterministic only when those representations are stable.

    Example:
        >>> asset = manager_api.DigitalAssetRecord(manager_api.DigitalAssetID(1), 4, (storage_models.Digest("sha256", "abcd"),))
        >>> _backed_store_uuid(asset, " ZIP ", ()) == _backed_store_uuid(asset, "zip", ())
        True


    :param asset_record: Expected byte identity supplying size and a nonempty digest sequence; no bytes are read.
    :param kind: Store-view kind normalized for identity without registry validation.
    :param options: Option name/value pairs sorted by name and represented with repr, without deep canonicalization.
    :return: UUID5 in NAMESPACE_URL using the versioned backed-Store identity string; empty digests or malformed options can raise.
    """

    ordered_digests = sorted(
        asset_record.digests,
        key=lambda value: (
            value.algorithm != "sha256",
            value.algorithm,
            value.value,
        ),
    )
    preferred_digest = ordered_digests[0]
    normalized_kind = kind.strip().lower().replace("-", "_")
    normalized_options = repr(tuple(sorted(options, key=lambda item: item[0])))
    identity = (
        "liuxin-backed-store:v1:"
        f"{asset_record.size_bytes}:{preferred_digest.algorithm}:"
        f"{preferred_digest.value}:{normalized_kind}:"
        f"{normalized_options}"
    )
    return uuid5(NAMESPACE_URL, identity)

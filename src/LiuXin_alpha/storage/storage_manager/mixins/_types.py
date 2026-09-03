"""
Private orchestration values shared by storage-manager mixins.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable
from typing import Literal, Protocol
from uuid import NAMESPACE_URL, uuid5

import LiuXin_alpha.storage.api as api

type StoreFactory = Callable[[api.StoreConfiguration], api.StoreAPI]
type StoreRegistration = tuple[api.StoreConfiguration, api.StoreAPI]
type _ItemTargetKind = Literal["digital_asset", "composite_digital_asset"]
type _ItemTargetID = api.DigitalAssetID | api.CompositeDigitalAssetID
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
    Normalized semantics bound to one stream-ingest operation UUID.
    """

    size_bytes: int
    observed_digests: tuple[api.Digest, ...]
    expected_size: int | None
    expected_digests: tuple[api.Digest, ...]
    item_id: api.ItemID | None
    role: str | None
    metadata: api.DigitalAssetMetadata
    placement_hints: api.StoragePlacementHints | None
    preferred_store_ref: api.StoreUUID | None
    replica_mode: api.ReplicaMode
    verify: bool


@dataclasses.dataclass(slots=True, frozen=True)
class _AdoptIngestRequest:
    """
    Normalized semantics bound to one adopt operation UUID.
    """

    location: api.Location
    digital_asset_id: api.DigitalAssetID | None
    item_id: api.ItemID | None
    role: str | None
    metadata: api.DigitalAssetMetadata
    replica_mode: api.ReplicaMode
    verify: bool


@dataclasses.dataclass(slots=True, frozen=True)
class _IdentifiedStreamIngestRequest:
    """
    Normalized semantics bound to one trusted-identity stream ingest.
    """

    size_bytes: int
    authoritative_digests: tuple[api.Digest, ...]
    item_id: api.ItemID | None
    role: str | None
    metadata: api.DigitalAssetMetadata
    placement_hints: api.StoragePlacementHints | None
    preferred_store_ref: api.StoreUUID | None
    replica_mode: api.ReplicaMode
    verify: bool


@dataclasses.dataclass(slots=True, frozen=True)
class _StoreObjectIngestRequest:
    """
    Normalized semantics for one Store-to-Store object ingest.
    """

    source_location: api.Location
    source_version: str | None
    size_bytes: int
    authoritative_digests: tuple[api.Digest, ...]
    item_id: api.ItemID | None
    role: str | None
    metadata: api.DigitalAssetMetadata
    placement_hints: api.StoragePlacementHints | None
    preferred_store_ref: api.StoreUUID | None
    replica_mode: api.ReplicaMode
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
    A completed idempotent ingest and its complete request fingerprint.
    """

    request: _IngestRequest
    result: api.DigitalAssetIngestResult


@dataclasses.dataclass(slots=True, frozen=True)
class _RecreationBranch:
    """
    Internal exact-replay route for one requested Digital Asset.
    """

    viable: bool
    steps: tuple[api.DigitalAssetDerivationRecord, ...] = ()
    available_digital_asset_ids: frozenset[api.DigitalAssetID] = frozenset()
    unavailable_digital_asset_ids: frozenset[api.DigitalAssetID] = frozenset()
    selected_derivation_id: api.DigitalAssetDerivationID | None = None
    alternative_derivation_ids: tuple[api.DigitalAssetDerivationID, ...] = ()
    warnings: tuple[str, ...] = ()


class _Hasher(Protocol):
    """
    Small structural view of a ``hashlib`` hash object.
    """

    def update(self, data: bytes, /) -> None:
        """
        Add bytes to the running digest.


        :param data:
        :return:
        """

        ...

    def hexdigest(self) -> str:
        """
        Return the lowercase hexadecimal digest.


        :return:
        """

        ...


def _replication_policy_id(
    value: api.ReplicationPolicyID | api.ReplicationPolicyRecord | None,
) -> api.ReplicationPolicyID | None:
    """
    Extract an optional replication-policy identity.


    :param value:
    :return:
    """

    if value is None:
        return None
    if isinstance(value, api.ReplicationPolicyRecord):
        return value.replication_policy_id
    identifier = int(value)
    if identifier <= 0:
        raise TypeError("replication must be a positive policy ID or policy record.")
    return api.ReplicationPolicyID(identifier)


def _backup_policy_id(
    value: api.BackupPolicyID | api.BackupPolicyRecord | None,
) -> api.BackupPolicyID | None:
    """
    Extract an optional backup-policy identity.


    :param value:
    :return:
    """

    if value is None:
        return None
    if isinstance(value, api.BackupPolicyRecord):
        return value.backup_policy_id
    identifier = int(value)
    if identifier <= 0:
        raise TypeError("backup must be a positive policy ID or policy record.")
    return api.BackupPolicyID(identifier)


def _backed_store_uuid(
    asset_record: api.DigitalAssetRecord,
    kind: str,
    options: tuple[tuple[str, object], ...],
) -> api.StoreUUID:
    """
    Derive globally stable Store-view identity from content identity.


    :param asset_record:
    :param kind:
    :param options:
    :return:
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


# Ingest journals written before the mixin extraction contain these qualified
# names. Preserve that wire identity so existing durable operations remain
# readable and new envelopes do not churn solely because code moved modules.
_LEGACY_MANAGER_MODULE = "LiuXin_alpha.storage.storage_manager.manager"
for _persisted_type in (
    _StreamIngestRequest,
    _AdoptIngestRequest,
    _IdentifiedStreamIngestRequest,
    _StoreObjectIngestRequest,
    _IngestOperation,
):
    _persisted_type.__module__ = _LEGACY_MANAGER_MODULE

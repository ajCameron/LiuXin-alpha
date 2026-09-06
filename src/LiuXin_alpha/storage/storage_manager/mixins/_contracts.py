"""Internal helper contracts shared by storage-manager components.

These protocols describe calls across mixin boundaries. Implementations live
in the support and Store-administration mixins; helpers used only within their
own component stay there. Abstract declarations also prevent an incomplete
manager composition from silently inheriting an empty helper implementation.
"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Callable, Iterable, Mapping
from contextlib import AbstractContextManager
from typing import Protocol
from uuid import UUID

import LiuXin_alpha.storage.api as api
from LiuXin_alpha.storage.storage_manager.mixins._types import (
    StoreFactory,
    _Hasher,
    _IngestRequest,
    _ItemTargetID,
    _ItemTargetKind,
    _MetadataRecordKind,
    _RecreationBranch,
)

__all__ = [
    "_StorageManagerMechanics",
    "_StorageManagerPolicyHooks",
    "_StorageManagerStoreHooks",
]


class _StorageManagerMechanics(Protocol):
    """Metadata transactions, identity checks, and byte-publication helpers."""

    @abstractmethod
    def _new_revision_locked(self) -> str:
        """Allocate a monotonically increasing manager revision token."""
        ...

    @abstractmethod
    def _metadata_transaction(self) -> AbstractContextManager[None]:
        """Return the transaction enclosing one metadata mutation."""
        ...

    @abstractmethod
    def _ingest_journal_statuses(self) -> tuple[Mapping[str, object], ...]:
        """Describe durable ingest progress, or return no entries when transient."""
        ...

    @abstractmethod
    def _allocate_metadata_id_locked(self, kind: _MetadataRecordKind) -> int:
        """Allocate an identity within the manager's metadata repository."""
        ...

    @staticmethod
    @abstractmethod
    def _check_revision(current: str | None, expected: str | None) -> None:
        """Enforce an optional optimistic-lock revision."""
        ...

    @abstractmethod
    def _require_asset_locked(
        self, digital_asset_id: api.DigitalAssetID
    ) -> api.DigitalAssetRecord:
        """Return a locked Asset lookup or raise the domain error."""
        ...

    @abstractmethod
    def _require_replica_locked(self, replica_id: api.ReplicaID) -> api.ReplicaRecord:
        """Return a locked Replica lookup or raise the domain error."""
        ...

    @abstractmethod
    def _require_composite_locked(
        self, composite_digital_asset_id: api.CompositeDigitalAssetID
    ) -> api.CompositeDigitalAssetRecord:
        """Return a locked Composite lookup or raise the domain error."""
        ...

    @abstractmethod
    def _find_asset_locked(
        self, digests: tuple[api.Digest, ...], size_bytes: int | None
    ) -> api.DigitalAssetRecord | None:
        """Find a non-conflicting digest match in stable Asset-ID order."""
        ...

    @staticmethod
    @abstractmethod
    def _require_expected_digests(
        expected: tuple[api.Digest, ...], observed: tuple[api.Digest, ...]
    ) -> None:
        """Require every expected algorithm and value in observations."""
        ...

    @abstractmethod
    def _complete_authoritative_ingest(
        self,
        *,
        request: _IngestRequest,
        operation_id: UUID,
        size_bytes: int,
        digests: tuple[api.Digest, ...],
        item_id: api.ItemID | None,
        role: str | None,
        metadata: api.DigitalAssetMetadata,
        placement_hints: api.StoragePlacementHints | None,
        preferred_store_ref: api.StoreUUID | None,
        replica_mode: api.ReplicaMode,
        verify: bool,
        publish: Callable[[api.StoreAPI, api.Location, api.Digest], None],
    ) -> api.DigitalAssetIngestResult:
        """Serialize matching identities while allowing distinct parallel ingest."""
        ...

    @abstractmethod
    def _require_same_identity(
        self,
        record: api.DigitalAssetRecord,
        size_bytes: int,
        observed_digests: tuple[api.Digest, ...],
    ) -> None:
        """Require size plus all comparable digests to identify one Asset."""
        ...

    @staticmethod
    @abstractmethod
    def _new_hashers(algorithms: Iterable[str]) -> dict[str, _Hasher]:
        """Create normalized hashlib objects for unique algorithms."""
        ...

    @abstractmethod
    def _calculate_location_digests(
        self, location: api.Location, algorithms: Iterable[str]
    ) -> tuple[api.Digest, ...]:
        """Stream a Location once and calculate all requested digests."""
        ...

    @staticmethod
    @abstractmethod
    def _preferred_digest(record: api.DigitalAssetRecord) -> api.Digest:
        """Prefer SHA-256 for Store verification, then stable first digest."""
        ...

    @abstractmethod
    def _inspect_replica(
        self,
        record: api.ReplicaRecord,
        asset_record: api.DigitalAssetRecord,
        *,
        calculate_digests: bool,
    ) -> api.ReplicaVerificationReport:
        """Inspect a Replica without mutating manager repository state."""
        ...

    @abstractmethod
    def _update_replica_observation(
        self, replica_id: api.ReplicaID, observation: api.ReplicaObservation
    ) -> api.ReplicaRecord:
        """Replace one Replica observation and advance repository generation."""
        ...

    @abstractmethod
    def _add_replica(self, declaration: api.ReplicaDeclaration) -> api.ReplicaRecord:
        """Add one non-conflicting Replica claim."""
        ...

    @abstractmethod
    def _require_writable_destination(
        self,
        store_ref: api.StoreUUID,
        mode: api.ReplicaMode,
        *,
        expected_size: int | None = None,
    ) -> api.StoreAPI:
        """Require a configured, online Store supporting the Replica mode."""
        ...

    @abstractmethod
    def _require_supported_object_size(
        self, store_ref: api.StoreUUID, expected_size: int | None
    ) -> None:
        """Reject a declared write that exceeds a Store's advertised limit."""
        ...

    @abstractmethod
    def _allocate_asset_location(
        self,
        store: api.StoreAPI,
        record: api.DigitalAssetRecord,
        *,
        placement_hints: api.StoragePlacementHints | None = None,
    ) -> api.Location:
        """Ask the Store to allocate a key, with an opaque portable fallback."""
        ...


class _StorageManagerPolicyHooks(Protocol):
    """Placement and recovery decisions consumed by sibling components."""

    @abstractmethod
    def _require_store_factory(self) -> StoreFactory:
        """Return the configured constructor or reject lifecycle mutation."""
        ...

    @abstractmethod
    def _validate_declared_policy_ids(
        self,
        replication_policy_id: api.ReplicationPolicyID | None,
        backup_policy_id: api.BackupPolicyID | None,
    ) -> None:
        """Require every supplied policy identifier to be registered."""
        ...

    @abstractmethod
    def _validate_store_policy_references(
        self, configuration: api.StoreConfiguration
    ) -> None:
        """Require a Store configuration's default policy references."""
        ...

    @abstractmethod
    def _placement_policy_ids(
        self, store_ref: api.StoreUUID
    ) -> tuple[api.ReplicationPolicyID | None, api.BackupPolicyID | None]:
        """Return policy identifiers captured for a new placement."""
        ...

    @abstractmethod
    def _capture_first_placement_policies(
        self,
        asset: api.DigitalAssetRecord,
        replication_policy_id: api.ReplicationPolicyID | None,
        backup_policy_id: api.BackupPolicyID | None,
    ) -> api.DigitalAssetRecord:
        """Capture Store defaults on a declared but not yet placed Asset."""
        ...

    @abstractmethod
    def _validate_all_recreation_policies(self) -> None:
        """Require every effective recreate-on-loss policy to remain safe."""
        ...

    @abstractmethod
    def _set_item_target(
        self,
        item_id: api.ItemID,
        role: str,
        kind: _ItemTargetKind,
        target_id: _ItemTargetID,
    ) -> None:
        """Set one well-formed Item role link in reference metadata."""
        ...

    @abstractmethod
    def _asset_has_derivation_reference_locked(
        self, digital_asset_id: api.DigitalAssetID
    ) -> bool:
        """Return whether an Asset participates in stored provenance."""
        ...

    @abstractmethod
    def _store_satisfies_policy(
        self, store_ref: api.StoreUUID, policy: api.ReplicationPolicy | api.BackupPolicy
    ) -> bool:
        """Return whether Store tags and supported mode satisfy a policy."""
        ...

    @abstractmethod
    def _separated_copy_capacity(
        self,
        records: Iterable[api.ReplicaRecord],
        policy: api.ReplicationPolicy | api.BackupPolicy,
    ) -> int:
        """Count policy-eligible copies after per-bucket copy limits."""
        ...

    @abstractmethod
    def _record_is_readable(self, record: api.ReplicaRecord) -> bool:
        """Return whether state and Store status currently permit reading."""
        ...

    @abstractmethod
    def _assess_policy(
        self,
        digital_asset_id: api.DigitalAssetID,
        policy: api.ReplicationPolicy | api.BackupPolicy,
    ) -> api.StoragePolicyAssessment:
        """Assess one policy against eligible, separated Replica claims."""
        ...

    @abstractmethod
    def _plan_destination_stores(
        self,
        policy: api.ReplicationPolicy | api.BackupPolicy,
        existing: tuple[api.ReplicaRecord, ...],
        needed: int,
        *,
        expected_size: int | None = None,
        excluded_store_refs: set[api.StoreUUID] | None = None,
    ) -> tuple[api.StoreUUID, ...]:
        """Select writable policy-compliant Stores without mutating state."""
        ...

    @abstractmethod
    def _plan_recreation_branch(
        self,
        digital_asset_id: api.DigitalAssetID,
        *,
        visiting: frozenset[api.DigitalAssetID],
        memo: dict[api.DigitalAssetID, _RecreationBranch],
    ) -> _RecreationBranch:
        """Select an exact route for one currently unavailable Asset."""
        ...

    @abstractmethod
    def _source_asset_ids(
        self,
        record: api.DigitalAssetDerivationRecord,
        *,
        include_recipe_artifacts: bool = True,
    ) -> set[api.DigitalAssetID]:
        """Expand derivation sources and pinned recipe Assets."""
        ...

    @abstractmethod
    def _reject_derivation_cycle(
        self,
        result_digital_asset_id: api.DigitalAssetID,
        source_asset_ids: set[api.DigitalAssetID],
    ) -> None:
        """Reject a result-to-source edge that closes a provenance cycle."""
        ...

    @abstractmethod
    def _derivation_is_recoverable(
        self,
        record: api.DigitalAssetDerivationRecord,
        visiting: set[api.DigitalAssetID],
    ) -> bool:
        """Return whether an exact recipe and all pinned inputs are reachable."""
        ...


class _StorageManagerStoreHooks(Protocol):
    """Store attachment used while constructing the shared manager state."""

    @abstractmethod
    def attach_store(
        self,
        configuration: api.StoreConfiguration,
        store: api.StoreAPI,
        *,
        startup: bool = True,
        replace_existing: bool = False,
    ) -> api.StoreConfiguration:
        """Attach an already constructed Store to this manager.

        :param configuration: Persisted or transient configuration for the Store.
        :param store: Constructed backend whose UUID matches the configuration.
        :param startup: Whether to start the backend before attaching it.
        :param replace_existing: Whether an existing registration may be replaced.
        :return: The attached Store configuration.
        """
        ...

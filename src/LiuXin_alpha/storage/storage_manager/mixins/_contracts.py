"""
Declare private callable contracts used across storage-manager mixin boundaries.

Abstract protocol methods make missing implementations visible when composing a
concrete manager. Support, policy, and Store-administration components provide the
behavior; these declarations preserve signatures and ownership without performing
I/O, validation, locking, or metadata mutation themselves.
"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Callable, Iterable, Mapping
from contextlib import AbstractContextManager
from typing import Protocol
from uuid import UUID

from LiuXin_alpha.storage.api import models as storage_models
from LiuXin_alpha.storage.api import placement_hints_api, store_api
from LiuXin_alpha.storage.api import storage_manager_api as manager_api
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
    """
    Require cross-component metadata, identity, digest, and publication operations.

    Abstract methods keep incomplete concrete manager compositions from inheriting empty
    implementations. This protocol defines the callable boundary without implementing transactions,
    locking, or I/O; transient and persistent managers supply their own storage behavior.

    Example:
        >>> result = manager._complete_authoritative_ingest(**ingest_arguments)  # doctest: +SKIP
    """

    @abstractmethod
    def _new_revision_locked(self) -> str:
        """
        Allocate a fresh opaque revision while the caller holds the manager lock.

        The implementation owns token format and persistence scope; callers compare equality rather
        than sorting tokens lexically.

        Example:
            >>> revision = manager._new_revision_locked()  # doctest: +SKIP


        :return: New revision token for a metadata replacement.
        """
        ...

    @abstractmethod
    def _metadata_transaction(self) -> AbstractContextManager[None]:
        """
        Supply the context for an explicit metadata mutation boundary.

        The implementation determines commit/rollback behavior; the transient default is a no-op.
        Acquiring this context does not by itself promise a manager lock or atomic physical Store
        publication.

        Example:
            >>> with manager._metadata_transaction():  # doctest: +SKIP
            ...     update_metadata()


        :return: Context manager yielding None and applying the implementation-specific metadata boundary.
        """
        ...

    @abstractmethod
    def _ingest_journal_statuses(self) -> tuple[Mapping[str, object], ...]:
        """
        Expose durable ingest progress for operational reporting when supported.

        Transient implementations return no entries. These mappings describe journal state and need
        not be the completed in-memory operation cache.

        Example:
            >>> statuses = manager._ingest_journal_statuses()  # doctest: +SKIP


        :return: Tuple of journal-status mappings, or an empty tuple when no durable journal is exposed.
        """
        ...

    @abstractmethod
    def _allocate_metadata_id_locked(self, kind: _MetadataRecordKind) -> int:
        """
        Allocate an identity within the requested metadata family under the caller's lock.

        Persistence implementations own the allocation mechanism and uniqueness scope; the transient
        implementation uses per-kind process counters.

        Example:
            >>> replica_id = manager._allocate_metadata_id_locked("replica")  # doctest: +SKIP


        :param kind: Supported metadata family whose next identity is required.
        :return: Allocated integer identity; unsupported kinds or repository failures propagate.
        """
        ...

    @staticmethod
    @abstractmethod
    def _check_revision(current: str | None, expected: str | None) -> None:
        """
        Require equality with a supplied expected token, with None disabling the precondition.

        This check adds no repository read or token-ordering semantics.

        Example:
            >>> manager._check_revision(current, expected)  # doctest: +SKIP


        :param current: Current retained revision token, possibly None.
        :param expected: Optional exact token required by the caller; None accepts any current value.
        :return: None on acceptance; StoragePreconditionFailed when a supplied expected token differs.
        """
        ...

    @abstractmethod
    def _require_asset_locked(
        self, digital_asset_id: manager_api.DigitalAssetID
    ) -> manager_api.DigitalAssetRecord:
        """
        Require an atomic Asset record while the caller holds the manager lock.

        The returned record is metadata evidence, without a physical-readability guarantee.

        Example:
            >>> asset = manager._require_asset_locked(asset_id)  # doctest: +SKIP


        :param digital_asset_id: Registered atomic Asset identity used directly as a mapping key.
        :return: Retained record; DigitalAssetNotFound for an absent key, with other mapping/input failures visible.
        """
        ...

    @abstractmethod
    def _require_replica_locked(
        self, replica_id: manager_api.ReplicaID
    ) -> manager_api.ReplicaRecord:
        """
        Require a Replica record under the caller's manager lock, without probing its Location.

        Example:
            >>> replica = manager._require_replica_locked(replica_id)  # doctest: +SKIP


        :param replica_id: Registered Replica identity used directly as a mapping key.
        :return: Retained record; ReplicaNotFound for an absent key, with other mapping/input failures visible.
        """
        ...

    @abstractmethod
    def _require_composite_locked(
        self, composite_digital_asset_id: manager_api.CompositeDigitalAssetID
    ) -> manager_api.CompositeDigitalAssetRecord:
        """
        Require a Composite record under the caller's manager lock, without resolving member bytes.

        Example:
            >>> composite = manager._require_composite_locked(composite_id)  # doctest: +SKIP


        :param composite_digital_asset_id: Registered Composite identity used directly as a mapping key.
        :return: Retained record; CompositeDigitalAssetNotFound for an absent key, with other mapping/input failures visible.
        """
        ...

    @abstractmethod
    def _find_asset_locked(
        self, digests: tuple[storage_models.Digest, ...], size_bytes: int | None
    ) -> manager_api.DigitalAssetRecord | None:
        """
        Find the earliest registered Asset whose optional size and shared digest evidence agree.

        At least one algorithm must overlap and no shared value may conflict. This is metadata
        identity matching, not byte verification; callers supply the lock.

        Example:
            >>> asset = manager._find_asset_locked(digests, size_bytes)  # doctest: +SKIP


        :param digests: Supplied digest evidence compared on shared algorithm attributes.
        :param size_bytes: Optional exact registered byte count; None accepts any size.
        :return: First matching retained Asset record in sorted-ID order, or None.
        """
        ...

    @staticmethod
    @abstractmethod
    def _require_expected_digests(
        expected: tuple[storage_models.Digest, ...],
        observed: tuple[storage_models.Digest, ...],
    ) -> None:
        """
        Require each expected algorithm/value among the supplied observations.

        Extra observations are allowed; this helper compares evidence without calculating hashes.

        Example:
            >>> manager._require_expected_digests(expected, observed)  # doctest: +SKIP


        :param expected: Digest expectations, each of which must be present with an equal value.
        :param observed: Supplied evidence converted to an algorithm-to-value mapping.
        :return: None when all expectations match; StorageIntegrityError for a missing or unequal observed value.
        """
        ...

    @abstractmethod
    def _complete_authoritative_ingest(
        self,
        *,
        request: _IngestRequest,
        operation_id: UUID,
        size_bytes: int,
        digests: tuple[storage_models.Digest, ...],
        item_id: manager_api.ItemID | None,
        role: str | None,
        metadata: manager_api.DigitalAssetMetadata,
        placement_hints: placement_hints_api.StoragePlacementHints | None,
        preferred_store_ref: storage_models.StoreUUID | None,
        replica_mode: manager_api.ReplicaMode,
        verify: bool,
        publish: Callable[
            [store_api.StoreAPI, storage_models.Location, storage_models.Digest], None
        ],
    ) -> manager_api.DigitalAssetIngestResult:
        """
        Coordinate authoritative identity, publication/reuse, and completed-ingest metadata.

        Implementations serialize matching identity requests and reject incompatible
        completed-operation reuse. Publication callbacks may perform external I/O; journal,
        transaction, and cleanup hooks govern failures rather than one atomic transaction across all
        steps.

        Example:
            >>> result = manager._complete_authoritative_ingest(  # doctest: +SKIP
            ...     request=request, operation_id=operation_id, size_bytes=size, digests=digests,
            ...     item_id=None, role=None, metadata=metadata, placement_hints=None,
            ...     preferred_store_ref=None, replica_mode=manager_api.ReplicaMode.ACTIVE,
            ...     verify=True, publish=publish,
            ... )


        :param request: Normalized request used for completed-operation equality; this helper does not derive it from bytes.
        :param operation_id: Logical operation key used by journal hooks and the completed-result mapping.
        :param size_bytes: Established byte count used for identity matching and destination size checks.
        :param digests: Established ordered digest tuple; callers own normalization and content authority.
        :param item_id: Optional Item identity to link during completion; None omits linking.
        :param role: Optional Item role; None selects primary_payload when linking is requested.
        :param metadata: Description for a newly declared Asset; existing Asset metadata is retained.
        :param placement_hints: Optional advisory metadata forwarded to allocation/publication and a new claim.
        :param preferred_store_ref: Optional destination UUID; None resolves the current manager default after retry lookup.
        :param replica_mode: Mode used when finding or creating the destination Replica.
        :param verify: Whether to inspect the selected Replica after publication or reuse.
        :param publish: Caller callback accepting destination Store, allocated Location, and preferred Asset digest.
        :return: New or reused ingest result; publication and metadata failures remain visible to recovery callers.
        """
        ...

    @abstractmethod
    def _require_same_identity(
        self,
        record: manager_api.DigitalAssetRecord,
        size_bytes: int,
        observed_digests: tuple[storage_models.Digest, ...],
    ) -> None:
        """
        Require equal size and agreement on all overlapping digest algorithms, with at least one
        overlap.

        Neither identical digest sets nor a fresh physical read is required by this comparison.

        Example:
            >>> manager._require_same_identity(record, size_bytes, observed_digests)  # doctest: +SKIP


        :param record: Registered Asset supplying expected size and digest evidence.
        :param size_bytes: Observed or asserted byte count to compare exactly.
        :param observed_digests: Supplied digest evidence checked only on shared algorithms.
        :return: None on agreement; StorageIntegrityError for size mismatch, no overlap, or any conflicting shared digest.
        """
        ...

    @staticmethod
    @abstractmethod
    def _new_hashers(algorithms: Iterable[str]) -> dict[str, _Hasher]:
        """
        Construct incremental hash objects keyed by normalized algorithm names.

        Unsupported algorithms and malformed names remain errors rather than being silently omitted.

        Example:
            >>> hashers = manager._new_hashers(("sha256",))  # doctest: +SKIP


        :param algorithms: Iterable of algorithm names consumed for raw sorting/deduplication and normalized hashlib construction.
        :return: New normalized-name mapping of empty concrete hashlib objects.
        """
        ...

    @abstractmethod
    def _calculate_location_digests(
        self, location: storage_models.Location, algorithms: Iterable[str]
    ) -> tuple[storage_models.Digest, ...]:
        """
        Open a Location, consume its bytes, and return computed digests in stable algorithm order.

        The concrete reader lifetime belongs to this operation. It does not update Replica
        observations or imply a version-pinned read.

        Example:
            >>> observed = manager._calculate_location_digests(location, ("sha256",))  # doctest: +SKIP


        :param location: Concrete Store address opened through manager.get.
        :param algorithms: Algorithm names used to construct normalized hashers before reading.
        :return: Tuple of computed Digest values, or an empty tuple after reading when no algorithms were requested; read/hash failures propagate.
        """
        ...

    @staticmethod
    @abstractmethod
    def _preferred_digest(
        record: manager_api.DigitalAssetRecord,
    ) -> storage_models.Digest:
        """
        Select an Asset's SHA-256 evidence when present, otherwise its first retained digest.

        Example:
            >>> digest = manager._preferred_digest(asset)  # doctest: +SKIP


        :param record: Asset record whose retained digests supply a Store verification preference.
        :return: Retained SHA-256 Digest if present, otherwise the first Digest in record order.
        """
        ...

    @abstractmethod
    def _inspect_replica(
        self,
        record: manager_api.ReplicaRecord,
        asset_record: manager_api.DigitalAssetRecord,
        *,
        calculate_digests: bool,
    ) -> manager_api.ReplicaVerificationReport:
        """
        Return physical-state evidence without mutating the manager's Replica observation.

        Expected size/digests come from the supplied Asset. Digest inspection may use authoritative
        Store stat evidence; ordinary Store failures become missing/unavailable reports where the
        implementation defines that boundary, while unexpected failures propagate.

        Example:
            >>> report = manager._inspect_replica(replica, asset, calculate_digests=True)  # doctest: +SKIP


        :param record: Replica whose Location and reported IDs identify the inspected claim.
        :param asset_record: Expected byte identity for comparison; agreement with record.digital_asset_id is not checked here.
        :param calculate_digests: Whether to compare authoritative stat SHA-256 or compute registered algorithms in addition to size.
        :return: Verification report with existence/state/evidence and diagnostics; manager observations remain unchanged.
        """
        ...

    @abstractmethod
    def _update_replica_observation(
        self,
        replica_id: manager_api.ReplicaID,
        observation: manager_api.ReplicaObservation,
    ) -> manager_api.ReplicaRecord:
        """
        Replace one existing observation, allocate its revision, and advance Replica generation.

        Implementations acquire the required mutation lock/transaction. Supplied evidence is
        retained without an additional physical probe.

        Example:
            >>> updated = manager._update_replica_observation(replica_id, observation)  # doctest: +SKIP


        :param replica_id: Existing claim identity to look up under the lock.
        :param observation: New evidence replacing the previous observation wholesale.
        :return: New stored Replica record; missing IDs and revision/repository failures propagate.
        """
        ...

    @abstractmethod
    def _add_replica(
        self, declaration: manager_api.ReplicaDeclaration
    ) -> manager_api.ReplicaRecord:
        """
        Register a new claim after resolving its references and rejecting an occupied live Location.

        A matching existing claim is still a conflict at this primitive boundary. Registration does
        not prove that bytes exist or that placement policy is satisfied.

        Example:
            >>> replica = manager._add_replica(declaration)  # doctest: +SKIP


        :param declaration: Asset/Location/mode/evidence for the new claim; its references must already be registered.
        :return: New stored Replica record; reference errors, live-location conflict, or allocation/repository failures raise.
        """
        ...

    @abstractmethod
    def _require_writable_destination(
        self,
        store_ref: storage_models.StoreUUID,
        mode: manager_api.ReplicaMode,
        *,
        expected_size: int | None = None,
    ) -> store_api.StoreAPI:
        """
        Require configured mode support, current available/writable state, creation capability, and
        supported object size.

        These checks neither reserve capacity nor guarantee a later publication will succeed.

        Example:
            >>> store = manager._require_writable_destination(store_ref, mode, expected_size=size)  # doctest: +SKIP


        :param store_ref: Configured Store UUID whose current facade must support the proposed write.
        :param mode: Replica purpose required in configured supported_replica_modes.
        :param expected_size: Optional proposed byte count checked against per-object characteristics; None skips that size check.
        :return: Retained eligible Store; read-only, unsupported, unavailable, size, or lookup failures raise.
        """
        ...

    @abstractmethod
    def _require_supported_object_size(
        self, store_ref: storage_models.StoreUUID, expected_size: int | None
    ) -> None:
        """
        Check a supplied nonnegative write size against advertised per-object characteristics.

        None skips the check. This boundary does not compare total free space or reserve storage.

        Example:
            >>> manager._require_supported_object_size(store_ref, size)  # doctest: +SKIP


        :param store_ref: Store UUID whose characteristics are consulted only for a supplied nonnegative size.
        :param expected_size: Optional proposed object size in bytes; None means no size check.
        :return: None when omitted or accepted; ValueError for a negative expectation and StoreUnsupportedOperation for an exceeded limit.
        """
        ...

    @abstractmethod
    def _allocate_asset_location(
        self,
        store: store_api.StoreAPI,
        record: manager_api.DigitalAssetRecord,
        *,
        placement_hints: placement_hints_api.StoragePlacementHints | None = None,
    ) -> storage_models.Location:
        """
        Ask a Store to choose an address from identity/name/placement hints, with an opaque fallback
        when unsupported.

        Address choice is separate from publication and does not reserve a unique destination.

        Example:
            >>> location = manager._allocate_asset_location(store, asset, placement_hints=hints)  # doctest: +SKIP


        :param store: Destination facade owning allocation or opaque-key construction.
        :param record: Asset identity and metadata supplying size, preferred digest, and optional name hint.
        :param placement_hints: Optional advisory layout metadata forwarded only if the Store advertises support.
        :return: Store-returned allocated Location or a UUID-key fallback; no publication occurs.
        """
        ...


class _StorageManagerPolicyHooks(Protocol):
    """
    Require placement, reference, and recovery decisions consumed across workflow components.

    These abstract calls distinguish configured policy feasibility from current readability and
    proposed placement. Implementations supply the checks; the protocol does not perform them or
    make a multi-call snapshot atomic.

    Example:
        >>> assessment = manager._assess_policy(asset_id, policy)  # doctest: +SKIP
    """

    @abstractmethod
    def _require_store_factory(self) -> StoreFactory:
        """
        Return a retained Store constructor for lifecycle operations without invoking it.

        An absent factory prevents construction through this route, while already constructed Stores
        may be attached separately.

        Example:
            >>> factory = manager._require_store_factory()  # doctest: +SKIP


        :return: Configured StoreFactory; StoreUnsupportedOperation when no constructor is available.
        """
        ...

    @abstractmethod
    def _validate_declared_policy_ids(
        self,
        replication_policy_id: manager_api.ReplicationPolicyID | None,
        backup_policy_id: manager_api.BackupPolicyID | None,
    ) -> None:
        """
        Require all supplied policy references to resolve before dependent metadata work.

        None references are omitted. The check does not enforce the policy against current bytes or
        validate every definition field.

        Example:
            >>> manager._validate_declared_policy_ids(replication_id, backup_id)  # doctest: +SKIP


        :param replication_policy_id: Optional replication-policy identity required in the registry.
        :param backup_policy_id: Optional backup-policy identity required in the registry.
        :return: None once references resolve; lookup failures propagate.
        """
        ...

    @abstractmethod
    def _validate_store_policy_references(
        self, configuration: manager_api.StoreConfiguration
    ) -> None:
        """
        Require the optional default policies named by a Store configuration.

        This does not capture those defaults on an Asset or probe the configured Store.

        Example:
            >>> manager._validate_store_policy_references(configuration)  # doctest: +SKIP


        :param configuration: Store configuration supplying default replication and backup references.
        :return: None when supplied default references resolve; lookup errors propagate.
        """
        ...

    @abstractmethod
    def _placement_policy_ids(
        self, store_ref: storage_models.StoreUUID
    ) -> tuple[
        manager_api.ReplicationPolicyID | None, manager_api.BackupPolicyID | None
    ]:
        """
        Read the default policy IDs from a Store configuration without assigning them to an Asset.

        Example:
            >>> replication_id, backup_id = manager._placement_policy_ids(store_ref)  # doctest: +SKIP


        :param store_ref: Store whose retained configuration supplies placement defaults.
        :return: Replication/backup ID pair, either of which may be None; configuration lookup errors propagate.
        """
        ...

    @abstractmethod
    def _capture_first_placement_policies(
        self,
        asset: manager_api.DigitalAssetRecord,
        replication_policy_id: manager_api.ReplicationPolicyID | None,
        backup_policy_id: manager_api.BackupPolicyID | None,
    ) -> manager_api.DigitalAssetRecord:
        """
        Fill absent Asset policy references from placement defaults when no live claim exists.

        Explicit references win. The supplied Asset revision participates in any delegated
        assignment, and an already placed Asset returns without recapturing defaults. This hook can
        mutate metadata; it does not publish bytes.

        Example:
            >>> asset = manager._capture_first_placement_policies(asset, replication_id, backup_id)  # doctest: +SKIP


        :param asset: Supplied Asset record whose references and revision guide first-placement capture.
        :param replication_policy_id: Optional Store default to fill only an absent Asset replication reference.
        :param backup_policy_id: Optional Store default to fill only an absent Asset backup reference.
        :return: Original or updated Asset record; assignment, revision, and policy validation failures propagate.
        """
        ...

    @abstractmethod
    def _validate_all_recreation_policies(self) -> None:
        """
        Require effective recreate-on-loss policies to have an exact route with policy-feasible
        prerequisites.

        This assesses configured retention/recreation intent, not current physical readability.
        Callers own any broader candidate-update transaction and restoration behavior.

        Example:
            >>> manager._validate_all_recreation_policies()  # doctest: +SKIP


        :return: None when all affected policies remain feasible; StoragePolicyUnsatisfied or lookup failures otherwise propagate.
        """
        ...

    @abstractmethod
    def _set_item_target(
        self,
        item_id: manager_api.ItemID,
        role: str,
        kind: _ItemTargetKind,
        target_id: _ItemTargetID,
    ) -> None:
        """
        Replace one exact Item/role target after checking the Item ID and nonblank role.

        Callers validate target existence and kind. The implementation retains exact role spelling
        and owns the mutation lock/transaction.

        Example:
            >>> manager._set_item_target(item_id, "cover", "digital_asset", asset_id)  # doctest: +SKIP


        :param item_id: Positive-convertible Item identity used as part of the link key.
        :param role: Nonblank role retained without whitespace normalization.
        :param kind: Atomic or Composite target discriminator supplied by the caller.
        :param target_id: Target identity already validated by the calling workflow.
        :return: None after replacing the target; invalid key values or repository errors raise.
        """
        ...

    @abstractmethod
    def _asset_has_derivation_reference_locked(
        self, digital_asset_id: manager_api.DigitalAssetID
    ) -> bool:
        """
        Check direct stored provenance and recipe references while the caller holds the manager
        lock.

        Result IDs, atomic sources, recipe inputs, and managed artefacts participate. The default
        implementation does not expand Composite members for this direct-reference check.

        Example:
            >>> referenced = manager._asset_has_derivation_reference_locked(asset_id)  # doctest: +SKIP


        :param digital_asset_id: Atomic identity sought among stored direct derivation/recipe references.
        :return: True when any direct reference uses the Asset, otherwise False.
        """
        ...

    @abstractmethod
    def _store_satisfies_policy(
        self,
        store_ref: storage_models.StoreUUID,
        policy: manager_api.ReplicationPolicy | manager_api.BackupPolicy,
    ) -> bool:
        """
        Compare configured supported mode and required/forbidden tags with a policy.

        Preferred tags, current health, writable capacity, and separation are independent checks.
        Missing configuration returns False in the composed implementation.

        Example:
            >>> eligible = manager._store_satisfies_policy(store_ref, policy)  # doctest: +SKIP


        :param store_ref: Store UUID whose configuration is examined.
        :param policy: Replication or backup constraints supplying mode and tag requirements.
        :return: Whether configuration satisfies these mode/tag constraints; other lookup failures remain visible.
        """
        ...

    @abstractmethod
    def _separated_copy_capacity(
        self,
        records: Iterable[manager_api.ReplicaRecord],
        policy: manager_api.ReplicationPolicy | manager_api.BackupPolicy,
    ) -> int:
        """
        Count a jointly compliant subset under every separation dimension's bucket limit.

        Callers provide already eligible claims. The composed calculation applies all dimensions to
        one selected subset, caps useful work at the target, and performs no readability check.

        Example:
            >>> capacity = manager._separated_copy_capacity(healthy_records, policy)  # doctest: +SKIP


        :param records: Claims whose eligibility has been selected by the caller; iterable is consumed for counting.
        :param policy: Dimensions and maximum-per-bucket count applied to Store topology.
        :return: Jointly constrained count up to the policy target, zero for no records; configuration errors can propagate.
        """
        ...

    @abstractmethod
    def _select_separated_records(
        self,
        records: Iterable[manager_api.ReplicaRecord],
        policy: manager_api.ReplicationPolicy | manager_api.BackupPolicy,
        *,
        limit: int,
    ) -> tuple[manager_api.ReplicaRecord, ...]:
        """
        Choose a deterministic largest subset satisfying every separation dimension jointly.

        Input order is the tie breaker between equally large subsets. The limit bounds useful
        selection, normally at the policy target. Implementations perform no health or policy-tag
        checks beyond the topology bucket constraints.

        :param records: Candidate claims already filtered by the caller.
        :param policy: Dimensions and maximum-per-bucket count applied simultaneously.
        :param limit: Maximum number of records required by the caller.
        :return: Ordered jointly compliant subset containing at most ``limit`` records.
        """
        ...

    @abstractmethod
    def _record_is_readable(self, record: manager_api.ReplicaRecord) -> bool:
        """
        Check a claim's eligible state, Store availability, and current size agreement.

        The default catches StorageError as unreadability without opening bytes, hashing, or
        updating observations. Recorded VERIFIED state alone is not sufficient.

        Example:
            >>> readable = manager._record_is_readable(replica)  # doctest: +SKIP


        :param record: Replica claim whose state and Location identify the proposed readable copy.
        :return: True when state/status/size checks permit reading, otherwise False; unexpected failures propagate.
        """
        ...

    @abstractmethod
    def _assess_policy(
        self,
        digital_asset_id: manager_api.DigitalAssetID,
        policy: manager_api.ReplicationPolicy | manager_api.BackupPolicy,
    ) -> manager_api.StoragePolicyAssessment:
        """
        Assess current readable, recorded-verified, configured-eligible claims against copy
        thresholds.

        The result retains evidence and diagnostics without mutating observations or executing
        repairs. Separate lookups do not establish an atomic snapshot.

        Example:
            >>> assessment = manager._assess_policy(asset_id, policy)  # doctest: +SKIP


        :param digital_asset_id: Registered Asset whose claims are assessed.
        :param policy: Policy mode, tags, separation limits, and minimum/target thresholds.
        :return: StoragePolicyAssessment based on inspected and configured evidence; lookup/inspection errors follow implementation boundaries.
        """
        ...

    @abstractmethod
    def _plan_destination_stores(
        self,
        policy: manager_api.ReplicationPolicy | manager_api.BackupPolicy,
        existing: tuple[manager_api.ReplicaRecord, ...],
        needed: int,
        *,
        expected_size: int | None = None,
        excluded_store_refs: set[storage_models.StoreUUID] | None = None,
    ) -> tuple[storage_models.StoreUUID, ...]:
        """
        Rank and choose writable destinations using policy constraints, existing buckets, and
        explicit exclusions.

        Preferred tags and Store/default ordering rank candidates before capacity/separation checks.
        A proposal may contain fewer destinations than needed; selection neither reserves capacity
        nor creates Replicas.

        Example:
            >>> destinations = manager._plan_destination_stores(policy, existing, 2, expected_size=size)  # doctest: +SKIP


        :param policy: Replication/backup constraints and ranking preferences.
        :param existing: Claims whose Stores and topology buckets are already occupied.
        :param needed: Requested additional destination count; nonpositive values yield no choices.
        :param expected_size: Optional proposed object byte count for destination eligibility checks.
        :param excluded_store_refs: Optional extra Store UUIDs that must not receive a new copy.
        :return: Ordered tuple of selected Store UUIDs, possibly empty or shorter than requested.
        """
        ...

    @abstractmethod
    def _plan_recreation_branch(
        self,
        digital_asset_id: manager_api.DigitalAssetID,
        *,
        visiting: frozenset[manager_api.DigitalAssetID],
        memo: dict[manager_api.DigitalAssetID, _RecreationBranch],
    ) -> _RecreationBranch:
        """
        Assess a root's current readability or select a recursively viable exact replay proposal.

        Callers provide traversal context and a memo scoped to the planning operation. The composed
        helper rejects cycles before checking memo/readability and caches completed outcomes by
        Asset ID; it does not execute recipes or solve a global cost optimization.

        Example:
            >>> branch = manager._plan_recreation_branch(asset_id, visiting=frozenset(), memo={})  # doctest: +SKIP


        :param digital_asset_id: Atomic identity whose readable evidence or replay route is requested.
        :param visiting: Ancestor Asset IDs used to reject a cyclic recursive path.
        :param memo: Caller-owned cache updated with branch outcomes keyed by Asset ID.
        :return: Branch containing feasibility, ordered steps, availability IDs, alternatives, and diagnostics.
        """
        ...

    @abstractmethod
    def _source_asset_ids(
        self,
        record: manager_api.DigitalAssetDerivationRecord,
        *,
        include_recipe_artifacts: bool = True,
    ) -> set[manager_api.DigitalAssetID]:
        """
        Expand atomic provenance, current Composite membership, and pinned recipe inputs into a set.

        All Composite members participate, including optional ones. Managed executables/dependencies
        are included by default but can be excluded for provenance traversal.

        Example:
            >>> sources = manager._source_asset_ids(record, include_recipe_artifacts=False)  # doctest: +SKIP


        :param record: Derivation whose source and recipe references should be expanded.
        :param include_recipe_artifacts: Whether managed executor/dependency Asset IDs join the source set.
        :return: Deduplicated set of referenced atomic IDs; required Composite lookups can raise.
        """
        ...

    @abstractmethod
    def _reject_derivation_cycle(
        self,
        result_digital_asset_id: manager_api.DigitalAssetID,
        source_asset_ids: set[manager_api.DigitalAssetID],
    ) -> None:
        """
        Reject proposed result-to-source edges that would make the result reachable from its
        sources.

        Existing derivations are expanded for the check, including recipe/managed artefact inputs.
        The helper validates a proposed graph change without registering it.

        Example:
            >>> manager._reject_derivation_cycle(result_id, source_ids)  # doctest: +SKIP


        :param result_digital_asset_id: Proposed derivation result whose source paths must not lead back to it.
        :param source_asset_ids: Expanded atomic prerequisite IDs proposed for that result.
        :return: None when acyclic; StoragePreconditionFailed for a detected cycle and lookup errors otherwise propagate.
        """
        ...

    @abstractmethod
    def _derivation_is_recoverable(
        self,
        record: manager_api.DigitalAssetDerivationRecord,
        visiting: set[manager_api.DigitalAssetID],
    ) -> bool:
        """
        Require an exact recipe whose managed/external artefacts and atomic inputs are currently
        recoverable.

        This combines present readability with recursive exact routes, rather than positive policy
        minima alone. No recipe is executed, and an external provider's evidence is interpreted by
        the concrete helper.

        Example:
            >>> recoverable = manager._derivation_is_recoverable(record, set())  # doctest: +SKIP


        :param record: Derivation whose exact recipe and prerequisites should be assessed.
        :param visiting: Current recursive Asset path used by prerequisite recovery checks.
        :return: True when exactness and all current prerequisite routes succeed, otherwise False; unexpected lookup/provider failures can propagate.
        """
        ...


class _StorageManagerStoreHooks(Protocol):
    """
    Require attachment of constructed Store facades during shared-state initialization.

    The concrete Store-administration component supplies lifecycle and registry behavior. This
    abstract structural contract prevents an incomplete composition from silently inheriting an
    empty attachment method.

    Example:
        >>> configuration = manager.attach_store(configuration, store)  # doctest: +SKIP
    """

    @abstractmethod
    def attach_store(
        self,
        configuration: manager_api.StoreConfiguration,
        store: store_api.StoreAPI,
        *,
        startup: bool = True,
        replace_existing: bool = False,
    ) -> manager_api.StoreConfiguration:
        """
        Validate configuration/facade identity and policy references, optionally start the Store,
        and register it.

        The concrete implementation may close a replaced facade after installing the new one.
        Startup, assignment, and cleanup are separate failure boundaries; attachment does not
        promise automatic candidate cleanup or atomic replacement.

        Example:
            >>> attached = manager.attach_store(configuration, store, startup=False)  # doctest: +SKIP


        :param configuration: Portable Store configuration providing UUID and default policy references.
        :param store: Already constructed facade whose Store UUID matches the configuration.
        :param startup: Whether to invoke startup before registering the facade.
        :param replace_existing: Whether an existing registration may be replaced.
        :return: Supplied configuration after successful attachment/cleanup; identity, policy, duplicate, or lifecycle failures propagate.
        """
        ...

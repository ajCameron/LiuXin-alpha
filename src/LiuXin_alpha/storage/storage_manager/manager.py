"""Repository-neutral storage orchestration and its transient implementation.

The shared orchestration core owns policy and workflow behaviour while Store
objects own bytes. The application manager binds that core to durable database
repository views; :class:`TransientStorageManager` supplies disposable mapping
state for focused tests and one-shot work.
"""

from __future__ import annotations

import dataclasses
import hashlib
import os
import tempfile

from collections import Counter, deque
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import nullcontext
from datetime import UTC, datetime
from threading import RLock
from typing import BinaryIO, Literal, Protocol, cast, override
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import LiuXin_alpha.storage.api as api


StoreFactory = Callable[[api.StoreConfiguration], api.StoreAPI]
StoreRegistration = tuple[api.StoreConfiguration, api.StoreAPI]
_ItemTargetKind = Literal["digital_asset", "composite_digital_asset"]
_ItemTargetID = api.DigitalAssetID | api.CompositeDigitalAssetID
_ItemTarget = tuple[_ItemTargetKind, _ItemTargetID]
_MetadataRecordKind = Literal[
    "digital_asset",
    "replica",
    "composite",
    "derivation",
    "replication_policy",
    "backup_policy",
]


@dataclasses.dataclass(slots=True, frozen=True)
class _StreamIngestRequest:
    """Normalized semantics bound to one stream-ingest operation UUID."""

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
    """Normalized semantics bound to one adopt operation UUID."""

    location: api.Location
    digital_asset_id: api.DigitalAssetID | None
    item_id: api.ItemID | None
    role: str | None
    metadata: api.DigitalAssetMetadata
    replica_mode: api.ReplicaMode
    verify: bool


@dataclasses.dataclass(slots=True, frozen=True)
class _IdentifiedStreamIngestRequest:
    """Normalized semantics bound to one trusted-identity stream ingest."""

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
    """Normalized semantics for one Store-to-Store object ingest."""

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


_IngestRequest = (
    _StreamIngestRequest
    | _IdentifiedStreamIngestRequest
    | _StoreObjectIngestRequest
    | _AdoptIngestRequest
)


@dataclasses.dataclass(slots=True, frozen=True)
class _IngestOperation:
    """A completed idempotent ingest and its complete request fingerprint."""

    request: _IngestRequest
    result: api.DigitalAssetIngestResult


@dataclasses.dataclass(slots=True, frozen=True)
class _RecreationBranch:
    """Internal exact-replay route for one requested Digital Asset."""

    viable: bool
    steps: tuple[api.DigitalAssetDerivationRecord, ...] = ()
    available_digital_asset_ids: frozenset[api.DigitalAssetID] = (
        frozenset()
    )
    unavailable_digital_asset_ids: frozenset[api.DigitalAssetID] = (
        frozenset()
    )
    selected_derivation_id: api.DigitalAssetDerivationID | None = None
    alternative_derivation_ids: tuple[api.DigitalAssetDerivationID, ...] = ()
    warnings: tuple[str, ...] = ()


class _Hasher(Protocol):
    """Small structural view of a ``hashlib`` hash object."""

    def update(self, data: bytes, /) -> None:
        """Add bytes to the running digest."""

        ...

    def hexdigest(self) -> str:
        """Return the lowercase hexadecimal digest."""

        ...


def _replication_policy_id(
    value: api.ReplicationPolicyID | api.ReplicationPolicyRecord | None,
) -> api.ReplicationPolicyID | None:
    """Extract an optional replication-policy identity."""

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
    """Extract an optional backup-policy identity."""

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
    """Derive globally stable Store-view identity from content identity."""

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


class _StorageManagerOrchestrator(api.StorageManagerAPI):
    """
    Repository-neutral storage orchestration over injected Stores.

    Concrete subclasses select the manager-state repository. The shared core
    implements optimistic revisions, idempotent ingest operation IDs, policy
    assessment, provenance, and reconciliation without defining persistence
    or cache policy.

    Store registrations are explicit ``(StoreConfiguration, StoreAPI)`` pairs.
    ``store_factory`` is needed only for ``create_store()``, ``update_store()``,
    or ``reload_stores()``.

    Example:
        >>> manager = TransientStorageManager()  # no Store until one is attached
        >>> list(manager.iter_digital_asset_records())
        []
    """

    def __init__(
        self,
        *,
        store_registrations: Iterable[StoreRegistration] = (),
        store_factory: StoreFactory | None = None,
        default_store_ref: api.StoreUUID | None = None,
        default_replication_policy: api.ReplicationPolicy | None = None,
        default_backup_policy: api.BackupPolicy | None = None,
        artifact_resolver: (
            api.ReproductionRecipeArtifactResolverAPI | None
        ) = None,
    ) -> None:
        """Initialize empty manager state and attach supplied Store instances."""

        self._lock = RLock()
        self._store_factory = store_factory
        self._artifact_resolver = artifact_resolver
        self._store_configurations: dict[
            api.StoreUUID, api.StoreConfiguration
        ] = {}
        self._stores: dict[api.StoreUUID, api.StoreAPI] = {}
        self._default_store_ref = default_store_ref

        self._assets: dict[api.DigitalAssetID, api.DigitalAssetRecord] = {}
        self._replicas: dict[api.ReplicaID, api.ReplicaRecord] = {}
        self._composites: dict[
            api.CompositeDigitalAssetID, api.CompositeDigitalAssetRecord
        ] = {}
        self._derivations: dict[
            api.DigitalAssetDerivationID, api.DigitalAssetDerivationRecord
        ] = {}
        self._replication_policies: dict[
            api.ReplicationPolicyID, api.ReplicationPolicyRecord
        ] = {}
        self._backup_policies: dict[
            api.BackupPolicyID, api.BackupPolicyRecord
        ] = {}
        self._item_targets: dict[tuple[api.ItemID, str], _ItemTarget] = {}
        self._ingest_operations: dict[UUID, _IngestOperation] = {}
        self._ingest_identity_locks: dict[
            tuple[int, tuple[api.Digest, ...]], RLock
        ] = {}

        self._next_asset_id = 1
        self._next_replica_id = 1
        self._next_composite_id = 1
        self._next_derivation_id = 1
        self._next_replication_policy_id = 1
        self._next_backup_policy_id = 1
        self._revision_counter = 0
        self._replica_generation = 0

        self._default_replication_policy = (
            api.ReplicationPolicy()
            if default_replication_policy is None
            else default_replication_policy
        )
        self._default_backup_policy = (
            api.BackupPolicy()
            if default_backup_policy is None
            else default_backup_policy
        )

        for configuration, store in store_registrations:
            self.attach_store(configuration, store)
        if self._default_store_ref is not None:
            self.set_default_store(self._default_store_ref)

    # ------------------------------------------------------------------
    # Store administration and byte routing
    # ------------------------------------------------------------------

    def attach_store(
        self,
        configuration: api.StoreConfiguration,
        store: api.StoreAPI,
        *,
        startup: bool = True,
        replace_existing: bool = False,
    ) -> api.StoreConfiguration:
        """Attach an already constructed Store to this manager."""

        if store.store_ref != configuration.store_uuid:
            raise api.StoreInvalidLocation(
                "Store instance UUID does not match its manager configuration."
            )
        self._validate_store_policy_references(configuration)
        with self._lock:
            exists = configuration.store_uuid in self._store_configurations
            if exists and not replace_existing:
                raise api.StoreAlreadyExists(str(configuration.store_uuid))
            old_store = self._stores.get(configuration.store_uuid)
        if startup:
            store.startup()
        with self._lock:
            self._store_configurations[configuration.store_uuid] = configuration
            self._stores[configuration.store_uuid] = store
            if self._default_store_ref is None:
                self._default_store_ref = configuration.store_uuid
        if old_store is not None and old_store is not store:
            old_store.close()
        return configuration

    @override
    def create_store(
        self,
        configuration: api.StoreConfiguration,
        *,
        startup: bool = True,
    ) -> api.StoreConfiguration:
        """Construct and register a Store through the configured factory."""

        factory = self._require_store_factory()
        with self._lock:
            if configuration.store_uuid in self._store_configurations:
                raise api.StoreAlreadyExists(str(configuration.store_uuid))
        return self.attach_store(
            configuration,
            factory(configuration),
            startup=startup,
        )

    @override
    def add_store(
        self,
        name: str,
        kind: str,
        root: str | os.PathLike[str],
        *,
        store_uuid: api.StoreUUID | None = None,
        url: str | None = None,
        protocol: str | None = None,
        failure_domain: str | None = None,
        region: str | None = None,
        host: UUID | None = None,
        device: UUID | None = None,
        tags: Iterable[str] = (),
        replication: (
            api.ReplicationPolicyID | api.ReplicationPolicyRecord | None
        ) = None,
        backup: api.BackupPolicyID | api.BackupPolicyRecord | None = None,
        modes: Iterable[api.ReplicaMode | str] = (
            api.ReplicaMode.ACTIVE,
            api.ReplicaMode.BACKUP,
            api.ReplicaMode.ARCHIVE,
        ),
        operational_role: str | None = None,
        read_only: bool = False,
        folders: bool = True,
        options: (
            Mapping[str, object] | Iterable[tuple[str, object]]
        ) = (),
        start: bool = True,
    ) -> api.StoreConfiguration:
        """Build and register one Store through this manager's factory."""

        configuration = api.StoreConfiguration.for_backend(
            name,
            kind,
            root,
            store_uuid=store_uuid,
            url=url,
            protocol=protocol,
            failure_domain=failure_domain,
            region=region,
            host=host,
            device=device,
            tags=tags,
            replication_policy=_replication_policy_id(replication),
            backup_policy=_backup_policy_id(backup),
            modes=modes,
            operational_role=operational_role,
            read_only=read_only,
            folders=folders,
            options=options,
        )
        return self.create_store(configuration, startup=start)

    @override
    def add_filesystem_store(
        self,
        name: str,
        root: str | os.PathLike[str],
        *,
        store_uuid: api.StoreUUID | None = None,
        failure_domain: str | None = None,
        region: str | None = None,
        host: UUID | None = None,
        device: UUID | None = None,
        tags: Iterable[str] = (),
        replication: (
            api.ReplicationPolicyID | api.ReplicationPolicyRecord | None
        ) = None,
        backup: api.BackupPolicyID | api.BackupPolicyRecord | None = None,
        modes: Iterable[api.ReplicaMode | str] = (
            api.ReplicaMode.ACTIVE,
            api.ReplicaMode.BACKUP,
            api.ReplicaMode.ARCHIVE,
        ),
        operational_role: str | None = None,
        read_only: bool = False,
        options: (
            Mapping[str, object] | Iterable[tuple[str, object]]
        ) = (),
        start: bool = True,
    ) -> api.StoreConfiguration:
        """Build and register a filesystem Store through this manager's factory."""

        configuration = api.StoreConfiguration.filesystem(
            name,
            root,
            store_uuid=store_uuid,
            failure_domain=failure_domain,
            region=region,
            host=host,
            device=device,
            tags=tags,
            replication_policy=_replication_policy_id(replication),
            backup_policy=_backup_policy_id(backup),
            modes=modes,
            operational_role=operational_role,
            read_only=read_only,
            options=options,
        )
        return self.create_store(configuration, startup=start)

    @override
    def add_backed_store(
        self,
        name: str,
        kind: str,
        digital_asset_id: api.DigitalAssetID,
        *,
        source_replica_id: api.ReplicaID | None = None,
        materialization_store_ref: api.StoreUUID | None = None,
        store_uuid: api.StoreUUID | None = None,
        protocol: str | None = None,
        tags: Iterable[str] = (),
        modes: Iterable[api.ReplicaMode | str] = (api.ReplicaMode.ARCHIVE,),
        operational_role: str | None = "archive",
        folders: bool = True,
        options: (
            Mapping[str, object] | Iterable[tuple[str, object]]
        ) = (),
        start: bool = True,
    ) -> api.StoreConfiguration:
        """Create a read-only Store view over a catalogued container Asset."""

        asset_record = self.get_digital_asset_record(digital_asset_id)
        if source_replica_id is not None:
            source = self.get_replica_record(source_replica_id)
            if source.digital_asset_id != digital_asset_id:
                raise api.StoragePreconditionFailed(
                    "source Replica belongs to another Digital Asset."
                )
        option_pairs = (
            tuple(cast(Mapping[str, object], options).items())
            if isinstance(options, Mapping)
            else tuple(options)
        )
        effective_store_uuid = store_uuid or _backed_store_uuid(
            asset_record,
            kind,
            option_pairs,
        )
        configuration = api.StoreConfiguration.for_backed_backend(
            name,
            kind,
            digital_asset_id,
            preferred_replica_id=source_replica_id,
            materialization_store_ref=materialization_store_ref,
            store_uuid=effective_store_uuid,
            protocol=protocol,
            tags=tags,
            modes=modes,
            operational_role=operational_role,
            folders=folders,
            options=option_pairs,
        )
        return self.create_store(configuration, startup=start)

    @override
    def update_store(
        self,
        store_ref: api.StoreUUID,
        configuration: api.StoreConfiguration,
    ) -> api.StoreConfiguration:
        """Replace Store configuration and its live Store atomically in memory."""

        if configuration.store_uuid != store_ref:
            raise api.StoreInvalidLocation(
                "updated Store configuration must retain its Store UUID."
            )
        self.get_store_configuration(store_ref)
        factory = self._require_store_factory()
        replacement = factory(configuration)
        return self.attach_store(
            configuration,
            replacement,
            startup=True,
            replace_existing=True,
        )

    @override
    def remove_store(
        self,
        store_ref: api.StoreUUID,
        *,
        forget_configuration: bool = False,
    ) -> bool:
        """Stop a Store and optionally discard its in-memory configuration."""

        with self._lock:
            if forget_configuration and any(
                record.location.store_ref == store_ref
                and record.state is not api.ReplicaState.DELETED
                for record in self._replicas.values()
            ):
                raise api.StoragePreconditionFailed(
                    "cannot forget Store configuration with live Replica claims."
                )
            store = self._stores.pop(store_ref, None)
            known = store is not None or store_ref in self._store_configurations
            if forget_configuration:
                self._store_configurations.pop(store_ref, None)
            if self._default_store_ref == store_ref:
                remaining = sorted(
                    self._stores,
                    key=lambda value: value.int,
                )
                self._default_store_ref = remaining[0] if remaining else None
        if store is not None:
            store.close()
        return known

    @override
    def get_store_configuration(
        self,
        store_ref: api.StoreUUID,
    ) -> api.StoreConfiguration:
        """Return one registered Store configuration."""

        with self._lock:
            try:
                return self._store_configurations[store_ref]
            except KeyError as error:
                raise api.StoreConfigurationNotFound(str(store_ref)) from error

    @override
    def iter_store_configurations(self) -> Iterator[api.StoreConfiguration]:
        """Iterate over a stable snapshot of Store configurations."""

        with self._lock:
            values = tuple(
                self._store_configurations[key]
                for key in sorted(self._store_configurations, key=lambda value: value.int)
            )
        return iter(values)

    @override
    def get_store(self, store_ref: api.StoreUUID) -> api.StoreAPI:
        """Return one live Store facade."""

        with self._lock:
            store = self._stores.get(store_ref)
            configured = store_ref in self._store_configurations
        if store is not None:
            return store
        if not configured:
            raise api.StoreConfigurationNotFound(str(store_ref))
        raise api.StoreUnavailable(
            f"configured Store {store_ref} has no live facade"
        )

    @override
    def iter_stores(self) -> Iterator[api.StoreAPI]:
        """Iterate over a stable snapshot of live Stores."""

        with self._lock:
            stores = tuple(
                self._stores[key]
                for key in sorted(self._stores, key=lambda value: value.int)
            )
        return iter(stores)

    @override
    def iter_store_statuses(
        self,
        *,
        refresh: bool = False,
    ) -> Iterator[api.StoreStatusObservation]:
        """Yield attributable status for every configured Store."""

        return super().iter_store_statuses(refresh=refresh)

    @override
    def get_operational_status(
        self,
        *,
        refresh_stores: bool = False,
    ) -> api.StorageOperationalStatus:
        """Return an attributable, actionable snapshot of storage health."""

        store_statuses = tuple(
            self.iter_store_statuses(refresh=refresh_stores)
        )
        issues: list[api.StorageOperationalIssue] = []
        actions: list[api.StorageRecoveryAction] = []

        for observation in store_statuses:
            for warning in observation.status.warnings:
                issues.append(
                    api.StorageOperationalIssue(
                        "store_warning",
                        api.StorageOperationalSeverity.WARNING,
                        warning,
                        store_ref=observation.store_ref,
                    )
                )
            if observation.status.available:
                continue
            message = (
                observation.status.message
                or f"Store {observation.store_ref} is unavailable."
            )
            issues.append(
                api.StorageOperationalIssue(
                    "store_unavailable",
                    api.StorageOperationalSeverity.WARNING,
                    message,
                    store_ref=observation.store_ref,
                )
            )
            actions.append(
                api.StorageRecoveryAction(
                    "reload_stores",
                    "Reload the Store after its endpoint becomes available.",
                    store_ref=observation.store_ref,
                )
            )

        for journal in self._ingest_journal_statuses():
            state = str(journal.get("state") or "unknown")
            if state == "committed":
                continue
            operation_id = journal.get("operation_id")
            if not isinstance(operation_id, UUID):
                operation_id = None
            last_error = journal.get("last_error")
            if state == "failed":
                message = f"Ingest {operation_id} failed"
                if last_error:
                    message += f": {last_error}"
                issues.append(
                    api.StorageOperationalIssue(
                        "ingest_failed",
                        api.StorageOperationalSeverity.ERROR,
                        message,
                        operation_id=operation_id,
                    )
                )
                actions.append(
                    api.StorageRecoveryAction(
                        "retry_ingest",
                        "Retry with the same operation UUID after correcting the failure.",
                        operation_id=operation_id,
                    )
                )
                continue
            issues.append(
                api.StorageOperationalIssue(
                    "ingest_pending",
                    api.StorageOperationalSeverity.WARNING,
                    f"Ingest {operation_id} remains in journal state {state!r}.",
                    operation_id=operation_id,
                )
            )
            actions.append(
                api.StorageRecoveryAction(
                    "recover_pending_ingests",
                    "Run pending-ingest recovery after required Stores are online.",
                    operation_id=operation_id,
                )
            )

        unhealthy_states = {
            api.ReplicaState.MISSING,
            api.ReplicaState.UNAVAILABLE,
            api.ReplicaState.CORRUPT,
        }
        for replica in self.iter_replica_records():
            if replica.state not in unhealthy_states:
                continue
            corrupt = replica.state is api.ReplicaState.CORRUPT
            code = "replica_corrupt" if corrupt else "replica_unavailable"
            issues.append(
                api.StorageOperationalIssue(
                    code,
                    (
                        api.StorageOperationalSeverity.ERROR
                        if corrupt or replica.state is api.ReplicaState.MISSING
                        else api.StorageOperationalSeverity.WARNING
                    ),
                    "Replica {} for Digital Asset {} is {}.".format(
                        replica.replica_id,
                        replica.digital_asset_id,
                        replica.state.value,
                    ),
                    digital_asset_id=replica.digital_asset_id,
                    replica_id=replica.replica_id,
                    store_ref=replica.location.store_ref,
                )
            )
            actions.append(
                api.StorageRecoveryAction(
                    "replicate_digital_asset",
                    "Create and verify another Replica from a healthy source.",
                    digital_asset_id=replica.digital_asset_id,
                    replica_id=replica.replica_id,
                    store_ref=replica.location.store_ref,
                )
            )

        for asset in self.iter_digital_asset_records():
            try:
                assessment = self.assess_digital_asset(
                    asset.digital_asset_id
                )
            except Exception as error:
                issues.append(
                    api.StorageOperationalIssue(
                        "policy_assessment_failed",
                        api.StorageOperationalSeverity.ERROR,
                        "Could not assess Digital Asset {}: {}".format(
                            asset.digital_asset_id,
                            str(error) or type(error).__name__,
                        ),
                        digital_asset_id=asset.digital_asset_id,
                    )
                )
                continue
            for code, satisfied, action, reason in (
                (
                    "replication_policy_violation",
                    assessment.replication_satisfied,
                    "plan_replication",
                    "Plan or execute additional live Replica placement.",
                ),
                (
                    "backup_policy_violation",
                    assessment.backup_satisfied,
                    "plan_backup",
                    "Plan or execute an additional backup/archive Replica.",
                ),
            ):
                if satisfied:
                    continue
                issues.append(
                    api.StorageOperationalIssue(
                        code,
                        (
                            api.StorageOperationalSeverity.ERROR
                            if assessment.unavailable
                            else api.StorageOperationalSeverity.WARNING
                        ),
                        "Digital Asset {} does not meet its {}.".format(
                            asset.digital_asset_id,
                            code.replace("_", " "),
                        ),
                        digital_asset_id=asset.digital_asset_id,
                    )
                )
                actions.append(
                    api.StorageRecoveryAction(
                        action,
                        reason,
                        digital_asset_id=asset.digital_asset_id,
                    )
                )

        for message in tuple(getattr(self, "ingest_recovery_issues", ())):
            issues.append(
                api.StorageOperationalIssue(
                    "ingest_recovery_deferred",
                    api.StorageOperationalSeverity.WARNING,
                    str(message),
                )
            )

        return api.StorageOperationalStatus(
            checked_at=datetime.now(UTC),
            store_statuses=store_statuses,
            issues=tuple(issues),
            recovery_actions=tuple(dict.fromkeys(actions)),
        )

    @override
    def reload_stores(
        self,
        *,
        include_offline: bool = False,
        replace_existing: bool = True,
    ) -> api.StorageBootstrapReport:
        """Rebuild live Store facades from the current configurations."""

        configurations = tuple(self.iter_store_configurations())
        issues: list[api.StorageBootstrapIssue] = []
        loaded = skipped = failed = 0
        for configuration in configurations:
            with self._lock:
                already_loaded = configuration.store_uuid in self._stores
            if already_loaded and not replace_existing:
                skipped += 1
                continue
            try:
                factory = self._require_store_factory()
                store = factory(configuration)
                status = store.startup()
                if not status.available and not include_offline:
                    store.close()
                    skipped += 1
                    issues.append(
                        api.StorageBootstrapIssue(
                            configuration.store_uuid,
                            configuration.store_name,
                            "Store is offline.",
                        )
                    )
                    continue
                self.attach_store(
                    configuration,
                    store,
                    startup=False,
                    replace_existing=True,
                )
                loaded += 1
            except Exception as error:
                failed += 1
                issues.append(
                    api.StorageBootstrapIssue(
                        configuration.store_uuid,
                        configuration.store_name,
                        str(error) or type(error).__name__,
                    )
                )
        return api.StorageBootstrapReport(
            discovered_configurations=len(configurations),
            loaded_stores=loaded,
            skipped_configurations=skipped,
            failed_configurations=failed,
            issues=tuple(issues),
        )

    @override
    def set_default_store(self, store_ref: api.StoreUUID) -> None:
        """Select the default destination Store."""

        self.get_store(store_ref)
        with self._lock:
            self._default_store_ref = store_ref

    @override
    def get_default_store_ref(self) -> api.StoreUUID:
        """Return the current default destination Store UUID."""

        with self._lock:
            store_ref = self._default_store_ref
        if store_ref is None:
            raise api.StoreConfigurationNotFound(
                "no default Store is configured"
            )
        self.get_store(store_ref)
        return store_ref

    @override
    def close(self) -> None:
        """Close every live Store, attempting all closes before re-raising."""

        stores = tuple(self.iter_stores())
        first_error: BaseException | None = None
        for store in stores:
            try:
                store.close()
            except BaseException as error:
                if first_error is None:
                    first_error = error
        if first_error is not None:
            raise first_error

    @override
    def stat(self, location: api.Location) -> api.FileInfo:
        """Route ``stat`` by the Location's Store UUID."""

        return self.get_store(location.store_ref).stat(location)

    @override
    def get(
        self,
        location: api.Location,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> BinaryIO:
        """Route a binary read by the Location's Store UUID."""

        store = self.get_store(location.store_ref)
        if if_version is None:
            return store.open_read(location, offset=offset, length=length)
        return store.open_read(
            location, offset=offset, length=length, if_version=if_version
        )

    @override
    def put(
        self,
        location: api.Location,
        source: BinaryIO,
        *,
        mode: api.WriteMode = api.WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: api.Digest | None = None,
    ) -> api.FileInfo:
        """Route one transactional Store publication."""

        self._require_supported_object_size(location.store_ref, expected_size)
        return self.get_store(location.store_ref).put(
            location,
            source,
            mode=mode,
            expected_size=expected_size,
            expected_digest=expected_digest,
        )

    @override
    def delete(
        self,
        location: api.Location,
        *,
        missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        """Route deletion while preserving Store errors and preconditions."""

        self.get_store(location.store_ref).delete(
            location,
            missing_ok=missing_ok,
            if_version=if_version,
        )

    @override
    def iter_locations(
        self,
        *,
        store_ref: api.StoreUUID | None = None,
        prefix: api.Location | None = None,
    ) -> Iterator[api.Location]:
        """Enumerate one Store or every live Store in stable UUID order."""

        if prefix is not None:
            if store_ref is not None and prefix.store_ref != store_ref:
                raise api.StoreInvalidLocation(
                    "prefix Location does not belong to the requested Store."
                )
            store_ref = prefix.store_ref
        stores = (
            (self.get_store(store_ref),)
            if store_ref is not None
            else tuple(self.iter_stores())
        )
        for store in stores:
            yield from store.iter_locations(prefix=prefix)

    @override
    def capabilities(self, store_ref: api.StoreUUID) -> api.StoreCapabilities:
        """Return one routed Store's inherent capabilities."""

        return self.get_store(store_ref).capabilities

    @override
    def characteristics(
        self,
        store_ref: api.StoreUUID,
    ) -> api.StorageCharacteristics:
        """Return structured constraints for one routed Store."""

        store = self.get_store(store_ref)
        if isinstance(store, api.StoreCharacteristicsAPI):
            return store.characteristics
        return api.StorageCharacteristics()

    @override
    def status(self, store_ref: api.StoreUUID) -> api.StoreStatus:
        """Return one routed Store's cached dynamic status."""

        return self.get_store(store_ref).status()

    # ------------------------------------------------------------------
    # Digital Asset records and item links
    # ------------------------------------------------------------------

    @override
    def declare_digital_asset(
        self,
        declaration: api.DigitalAssetDeclaration,
    ) -> api.DigitalAssetRecord:
        """Declare a content identity, idempotently reusing an exact match."""

        self._validate_declared_policy_ids(
            declaration.replication_policy_id,
            declaration.backup_policy_id,
        )
        if declaration.replication_policy_id is not None:
            policy = self.get_replication_policy_record(
                declaration.replication_policy_id
            ).policy
            if policy.loss_action is api.DigitalAssetLossAction.RECREATE:
                raise api.StoragePolicyUnsatisfied(
                    "declare the Asset and its exact derivation before assigning "
                    "a recreate-on-loss policy."
                )
        with self._lock, self._metadata_transaction():
            existing = self._find_asset_locked(
                declaration.digests,
                declaration.size_bytes,
            )
            if existing is not None:
                return existing
            digital_asset_id = api.DigitalAssetID(
                self._allocate_metadata_id_locked("digital_asset")
            )
            record = api.DigitalAssetRecord(
                digital_asset_id,
                declaration.size_bytes,
                declaration.digests,
                declaration.metadata,
                declaration.replication_policy_id,
                declaration.backup_policy_id,
                self._new_revision_locked(),
            )
            self._assets[digital_asset_id] = record
            return record

    @override
    def get_digital_asset_record(
        self,
        digital_asset_id: api.DigitalAssetID,
    ) -> api.DigitalAssetRecord:
        """Return one Digital Asset record or raise a typed domain error."""

        with self._lock:
            try:
                return self._assets[digital_asset_id]
            except KeyError as error:
                raise api.DigitalAssetNotFound(
                    f"Digital Asset {digital_asset_id} is not registered."
                ) from error

    @override
    def update_digital_asset_metadata(
        self,
        digital_asset_id: api.DigitalAssetID,
        metadata: api.DigitalAssetMetadata,
        *,
        if_revision: str | None = None,
    ) -> api.DigitalAssetRecord:
        """Replace metadata under an optimistic revision precondition."""

        with self._lock, self._metadata_transaction():
            current = self._require_asset_locked(digital_asset_id)
            self._check_revision(current.revision, if_revision)
            updated = dataclasses.replace(
                current,
                metadata=metadata,
                revision=self._new_revision_locked(),
            )
            self._assets[digital_asset_id] = updated
            return updated

    @override
    def iter_digital_asset_records(self) -> Iterator[api.DigitalAssetRecord]:
        """Iterate over a stable ID-ordered Asset snapshot."""

        with self._lock:
            records = tuple(self._assets[key] for key in sorted(self._assets))
        return iter(records)

    @override
    def find_digital_asset_record_by_digest(
        self,
        digest: api.Digest,
        *,
        size_bytes: int | None = None,
    ) -> api.DigitalAssetRecord | None:
        """Return the first stable-ID record matching digest and size."""

        with self._lock:
            return self._find_asset_locked(
                (digest,),
                size_bytes,
            )

    @override
    def forget_digital_asset(
        self,
        digital_asset_id: api.DigitalAssetID,
        *,
        require_no_replicas: bool = True,
        if_revision: str | None = None,
    ) -> bool:
        """Forget an unreferenced Asset record without touching Store bytes."""

        with self._lock, self._metadata_transaction():
            current = self._assets.get(digital_asset_id)
            if current is None:
                return False
            self._check_revision(current.revision, if_revision)
            replicas = tuple(
                record
                for record in self._replicas.values()
                if record.digital_asset_id == digital_asset_id
            )
            if require_no_replicas and replicas:
                raise api.StoragePreconditionFailed(
                    "Digital Asset still has Replica claims."
                )
            if any(
                member.digital_asset_id == digital_asset_id
                for composite in self._composites.values()
                for member in composite.members
            ):
                raise api.StoragePreconditionFailed(
                    "Digital Asset is still a Composite member."
                )
            if self._asset_has_derivation_reference_locked(digital_asset_id):
                raise api.StoragePreconditionFailed(
                    "Digital Asset is still referenced by derivation provenance."
                )
            if any(
                kind == "digital_asset" and target_id == digital_asset_id
                for kind, target_id in self._item_targets.values()
            ):
                raise api.StoragePreconditionFailed(
                    "Digital Asset is still linked to an Item."
                )
            del self._assets[digital_asset_id]
            return True

    @override
    def link_item_to_digital_asset(
        self,
        item_id: api.ItemID,
        digital_asset_id: api.DigitalAssetID,
        *,
        role: str = "primary_payload",
    ) -> None:
        """Link one Item role to an atomic Digital Asset in reference state."""

        self.get_digital_asset_record(digital_asset_id)
        self._set_item_target(item_id, role, "digital_asset", digital_asset_id)

    @override
    def link_item_to_composite_digital_asset(
        self,
        item_id: api.ItemID,
        composite_digital_asset_id: api.CompositeDigitalAssetID,
        *,
        role: str = "primary_payload",
    ) -> None:
        """Link one Item role to a Composite Digital Asset."""

        self.get_composite_digital_asset_record(composite_digital_asset_id)
        self._set_item_target(
            item_id,
            role,
            "composite_digital_asset",
            composite_digital_asset_id,
        )

    @override
    def unlink_item_digital_asset(
        self,
        item_id: api.ItemID,
        *,
        role: str = "primary_payload",
    ) -> bool:
        """Remove one manager-owned Item-to-Asset role link."""

        with self._lock, self._metadata_transaction():
            return self._item_targets.pop((item_id, role), None) is not None

    # ------------------------------------------------------------------
    # Ingest, retrieval, and Replica lifecycle
    # ------------------------------------------------------------------

    @override
    def ingest_stream(
        self,
        stream: BinaryIO,
        *,
        operation_id: UUID | None = None,
        expected_size: int | None = None,
        expected_digests: tuple[api.Digest, ...] = (),
        item_id: api.ItemID | None = None,
        role: str | None = None,
        metadata: api.DigitalAssetMetadata | None = None,
        placement_hints: api.StoragePlacementHints | None = None,
        preferred_store_ref: api.StoreUUID | None = None,
        replica_mode: api.ReplicaMode = api.ReplicaMode.ACTIVE,
        verify: bool = True,
    ) -> api.DigitalAssetIngestResult:
        """Spool, identify, publish, and register one stream recoverably."""

        operation_id = uuid4() if operation_id is None else operation_id
        algorithms = {digest.algorithm for digest in expected_digests}
        algorithms.add("sha256")
        hashers = self._new_hashers(algorithms)
        total = 0
        with tempfile.SpooledTemporaryFile(max_size=8 * 1024 * 1024) as spool:
            while True:
                chunk = stream.read(1024 * 1024)
                if not chunk:
                    break
                if not isinstance(chunk, bytes):
                    raise TypeError("ingest streams must return bytes.")
                spool.write(chunk)
                total += len(chunk)
                for hasher in hashers.values():
                    hasher.update(chunk)
            if expected_size is not None and total != expected_size:
                raise api.StorageIntegrityError(
                    f"expected {expected_size} bytes, received {total}."
                )
            observed_digests = tuple(
                api.Digest(algorithm, hashers[algorithm].hexdigest())
                for algorithm in sorted(hashers)
            )
            self._require_expected_digests(expected_digests, observed_digests)

            normalized_metadata = (
                api.DigitalAssetMetadata() if metadata is None else metadata
            )
            request = _StreamIngestRequest(
                total,
                observed_digests,
                expected_size,
                tuple(
                    sorted(
                        expected_digests,
                        key=lambda digest: (digest.algorithm, digest.value),
                    )
                ),
                item_id,
                role,
                normalized_metadata,
                placement_hints,
                preferred_store_ref,
                replica_mode,
                verify,
            )

            def _publish(
                store: api.StoreAPI,
                location: api.Location,
                digest: api.Digest,
            ) -> None:
                spool.seek(0)
                store.put(
                    location,
                    cast(BinaryIO, cast(object, spool)),
                    expected_size=total,
                    expected_digest=digest,
                    placement_hints=placement_hints,
                )

            return self._complete_authoritative_ingest(
                request=request,
                operation_id=operation_id,
                size_bytes=total,
                digests=observed_digests,
                item_id=item_id,
                role=role,
                metadata=normalized_metadata,
                placement_hints=placement_hints,
                preferred_store_ref=preferred_store_ref,
                replica_mode=replica_mode,
                verify=verify,
                publish=_publish,
            )

    @override
    def ingest_identified_stream(
        self,
        stream: BinaryIO,
        *,
        size_bytes: int,
        authoritative_digests: tuple[api.Digest, ...],
        operation_id: UUID | None = None,
        item_id: api.ItemID | None = None,
        role: str | None = None,
        metadata: api.DigitalAssetMetadata | None = None,
        placement_hints: api.StoragePlacementHints | None = None,
        preferred_store_ref: api.StoreUUID | None = None,
        replica_mode: api.ReplicaMode = api.ReplicaMode.ACTIVE,
        verify: bool = True,
    ) -> api.DigitalAssetIngestResult:
        """Publish trusted identified bytes without manager-side spooling."""

        if size_bytes < 0:
            raise ValueError("size_bytes must not be negative.")
        digests = tuple(
            sorted(
                authoritative_digests,
                key=lambda digest: (digest.algorithm, digest.value),
            )
        )
        if not digests:
            raise ValueError("authoritative_digests must not be empty.")
        if len({digest.algorithm for digest in digests}) != len(digests):
            raise ValueError(
                "authoritative_digests must contain unique algorithms."
            )
        sha256 = next(
            (digest for digest in digests if digest.algorithm == "sha256"),
            None,
        )
        if sha256 is None:
            raise ValueError(
                "identified stream ingest requires an authoritative SHA-256 digest."
            )
        operation_id = uuid4() if operation_id is None else operation_id
        normalized_metadata = (
            api.DigitalAssetMetadata() if metadata is None else metadata
        )
        request = _IdentifiedStreamIngestRequest(
            size_bytes,
            digests,
            item_id,
            role,
            normalized_metadata,
            placement_hints,
            preferred_store_ref,
            replica_mode,
            verify,
        )

        def _publish(
            store: api.StoreAPI,
            location: api.Location,
            digest: api.Digest,
        ) -> None:
            store.put(
                location,
                stream,
                expected_size=size_bytes,
                expected_digest=digest,
                placement_hints=placement_hints,
            )

        return self._complete_authoritative_ingest(
            request=request,
            operation_id=operation_id,
            size_bytes=size_bytes,
            digests=digests,
            item_id=item_id,
            role=role,
            metadata=normalized_metadata,
            placement_hints=placement_hints,
            preferred_store_ref=preferred_store_ref,
            replica_mode=replica_mode,
            verify=verify,
            publish=_publish,
        )

    @override
    def ingest_store_object(
        self,
        source: api.StoreAPI,
        info: api.FileInfo | api.StoreInventoryEntry,
        *,
        operation_id: UUID | None = None,
        item_id: api.ItemID | None = None,
        role: str | None = None,
        metadata: api.DigitalAssetMetadata | None = None,
        placement_hints: api.StoragePlacementHints | None = None,
        preferred_store_ref: api.StoreUUID | None = None,
        replica_mode: api.ReplicaMode = api.ReplicaMode.ACTIVE,
        verify: bool = True,
    ) -> api.DigitalAssetIngestResult:
        """Prefer verified native transfer, then fall back to source streaming."""

        if isinstance(source, api.IngestSourceStoreAPI):
            return api.DigitalAssetIngestAPI.ingest_store_object(
                self,
                source,
                info,
                operation_id=operation_id,
                item_id=item_id,
                role=role,
                metadata=metadata,
                placement_hints=placement_hints,
                preferred_store_ref=preferred_store_ref,
                replica_mode=replica_mode,
                verify=verify,
            )

        def _fallback(
            fallback_operation_id: UUID | None,
        ) -> api.DigitalAssetIngestResult:
            return api.DigitalAssetIngestAPI.ingest_store_object(
                self,
                source,
                info,
                operation_id=fallback_operation_id,
                item_id=item_id,
                role=role,
                metadata=metadata,
                placement_hints=placement_hints,
                preferred_store_ref=preferred_store_ref,
                replica_mode=replica_mode,
                verify=verify,
            )

        digest = (
            info.digest
            if source.capabilities.stat_digest_authoritative
            else None
        )
        return self._ingest_store_object_natively_or_fallback(
            source,
            info,
            digest,
            operation_id=operation_id,
            item_id=item_id,
            role=role,
            metadata=metadata,
            placement_hints=placement_hints,
            preferred_store_ref=preferred_store_ref,
            replica_mode=replica_mode,
            verify=verify,
            fallback=_fallback,
        )

    @override
    def ingest_prepared_store_object(
        self,
        source: api.StoreAPI,
        prepared: api.PreparedIngestObject,
        *,
        operation_id: UUID | None = None,
        item_id: api.ItemID | None = None,
        role: str | None = None,
        metadata: api.DigitalAssetMetadata | None = None,
        placement_hints: api.StoragePlacementHints | None = None,
        preferred_store_ref: api.StoreUUID | None = None,
        replica_mode: api.ReplicaMode = api.ReplicaMode.ACTIVE,
        verify: bool = True,
    ) -> api.DigitalAssetIngestResult:
        """Prefer native transfer while reusing an existing preparation."""

        if not isinstance(source, api.IngestSourceStoreAPI):
            raise TypeError(
                "prepared Store ingest requires IngestSourceStoreAPI."
            )
        source.require_location(prepared.info.location)
        try:
            source.ingest_capabilities.validate_prepared(prepared)
        except ValueError as error:
            raise api.StoreIntegrityError(str(error)) from error

        def _fallback(
            fallback_operation_id: UUID | None,
        ) -> api.DigitalAssetIngestResult:
            return api.DigitalAssetIngestAPI.ingest_prepared_store_object(
                self,
                source,
                prepared,
                operation_id=fallback_operation_id,
                item_id=item_id,
                role=role,
                metadata=metadata,
                placement_hints=placement_hints,
                preferred_store_ref=preferred_store_ref,
                replica_mode=replica_mode,
                verify=verify,
            )

        digest = next(
            (
                candidate
                for candidate in prepared.authoritative_digests
                if candidate.algorithm == "sha256"
            ),
            None,
        )
        return self._ingest_store_object_natively_or_fallback(
            source,
            prepared.info,
            digest,
            operation_id=operation_id,
            item_id=item_id,
            role=role,
            metadata=metadata,
            placement_hints=placement_hints,
            preferred_store_ref=preferred_store_ref,
            replica_mode=replica_mode,
            verify=verify,
            fallback=_fallback,
        )

    def _ingest_store_object_natively_or_fallback(
        self,
        source: api.StoreAPI,
        info: api.FileInfo | api.StoreInventoryEntry,
        digest: api.Digest | None,
        *,
        operation_id: UUID | None,
        item_id: api.ItemID | None,
        role: str | None,
        metadata: api.DigitalAssetMetadata | None,
        placement_hints: api.StoragePlacementHints | None,
        preferred_store_ref: api.StoreUUID | None,
        replica_mode: api.ReplicaMode,
        verify: bool,
        fallback: Callable[
            [UUID | None],
            api.DigitalAssetIngestResult,
        ],
    ) -> api.DigitalAssetIngestResult:
        """Attempt verified native import and invoke one exact fallback."""

        destination_ref = (
            self.get_default_store_ref()
            if preferred_store_ref is None
            else preferred_store_ref
        )
        destination = self.get_store(destination_ref)
        if (
            not isinstance(destination, api.NativeImportStoreAPI)
            or not destination.can_import_from(source)
            or info.size is None
            or digest is None
            or digest.algorithm != "sha256"
        ):
            return fallback(operation_id)

        selected_operation_id = (
            uuid4() if operation_id is None else operation_id
        )
        digests = (digest,)
        normalized_metadata = (
            api.DigitalAssetMetadata() if metadata is None else metadata
        )
        request = _StoreObjectIngestRequest(
            info.location,
            info.version,
            info.size,
            digests,
            item_id,
            role,
            normalized_metadata,
            placement_hints,
            preferred_store_ref,
            replica_mode,
            verify,
        )

        def _publish(
            store: api.StoreAPI,
            location: api.Location,
            expected_digest: api.Digest,
        ) -> None:
            assert isinstance(store, api.NativeImportStoreAPI)
            assert info.size is not None
            store.import_from(
                source,
                info.location,
                location,
                expected_size=info.size,
                expected_digest=expected_digest,
                placement_hints=placement_hints,
            )

        try:
            return self._complete_authoritative_ingest(
                request=request,
                operation_id=selected_operation_id,
                size_bytes=info.size,
                digests=digests,
                item_id=item_id,
                role=role,
                metadata=normalized_metadata,
                placement_hints=placement_hints,
                preferred_store_ref=preferred_store_ref,
                replica_mode=replica_mode,
                verify=verify,
                publish=_publish,
            )
        except api.StoreUnsupportedOperation:
            return fallback(selected_operation_id)

    @override
    def adopt_location(
        self,
        location: api.Location,
        *,
        operation_id: UUID | None = None,
        digital_asset_id: api.DigitalAssetID | None = None,
        item_id: api.ItemID | None = None,
        role: str | None = None,
        metadata: api.DigitalAssetMetadata | None = None,
        replica_mode: api.ReplicaMode = api.ReplicaMode.UNMANAGED,
        verify: bool = False,
    ) -> api.DigitalAssetIngestResult:
        """Register bytes already present at one concrete Location."""

        operation_id = uuid4() if operation_id is None else operation_id
        normalized_metadata = (
            api.DigitalAssetMetadata() if metadata is None else metadata
        )
        request = _AdoptIngestRequest(
            location,
            digital_asset_id,
            item_id,
            role,
            normalized_metadata,
            replica_mode,
            verify,
        )
        with self._lock:
            prior = self._ingest_operations.get(operation_id)
        if prior is not None:
            if prior.request != request:
                raise api.StoragePreconditionFailed(
                    "ingest operation ID was already used for a different request."
                )
            return prior.result
        info = self.stat(location)

        if digital_asset_id is None:
            observed = self._calculate_location_digests(location, ("sha256",))
            with self._lock:
                existing = self._find_asset_locked(observed, info.size)
            asset_created = existing is None
            replication_policy_id, backup_policy_id = (
                self._placement_policy_ids(location.store_ref)
            )
            asset_record = (
                self.declare_digital_asset(
                    api.DigitalAssetDeclaration(
                        info.size,
                        observed,
                        normalized_metadata,
                        replication_policy_id=replication_policy_id,
                        backup_policy_id=backup_policy_id,
                    )
                )
                if existing is None
                else existing
            )
            if existing is not None:
                asset_record = self._capture_first_placement_policies(
                    asset_record,
                    replication_policy_id,
                    backup_policy_id,
                )
        else:
            asset_record = self.get_digital_asset_record(digital_asset_id)
            observed = self._calculate_location_digests(
                location,
                tuple(digest.algorithm for digest in asset_record.digests),
            )
            self._require_same_identity(asset_record, info.size, observed)
            asset_created = False

        with self._lock:
            conflicting = next(
                (
                    record
                    for record in self._replicas.values()
                    if record.location == location
                    and record.state is not api.ReplicaState.DELETED
                ),
                None,
            )
        if conflicting is not None:
            if conflicting.digital_asset_id != asset_record.digital_asset_id:
                raise api.StoragePreconditionFailed(
                    "Location is already claimed by another Digital Asset."
                )
            replica_record = conflicting
            replica_created = False
        else:
            replica_record = self._add_replica(
                api.ReplicaDeclaration(
                    asset_record.digital_asset_id,
                    location,
                    replica_mode,
                    api.ReplicaObservation(
                        api.ReplicaState.PRESENT,
                        observed_size_bytes=info.size,
                        observed_digests=observed,
                        checked_at=datetime.now(UTC),
                    ),
                )
            )
            replica_created = True
        if verify:
            report = self.verify_replica(replica_record.replica_id)
            replica_record = self.get_replica_record(replica_record.replica_id)
            verified = report.healthy
        else:
            verified = replica_record.state is api.ReplicaState.VERIFIED
        result = api.DigitalAssetIngestResult(
            operation_id,
            asset_record,
            replica_record,
            asset_created,
            replica_created,
            deduplicated=not asset_created,
            verified=verified,
        )
        with self._metadata_transaction():
            if item_id is not None:
                self.link_item_to_digital_asset(
                    item_id,
                    asset_record.digital_asset_id,
                    role="primary_payload" if role is None else role,
                )
            with self._lock:
                self._ingest_operations[operation_id] = _IngestOperation(
                    request,
                    result,
                )
        return result

    @override
    def get_replica_record(
        self,
        replica_id: api.ReplicaID,
    ) -> api.ReplicaRecord:
        """Return one Replica record."""

        with self._lock:
            try:
                return self._replicas[replica_id]
            except KeyError as error:
                raise api.ReplicaNotFound(
                    f"Replica {replica_id} is not registered."
                ) from error

    @override
    def iter_replica_records(
        self,
        *,
        digital_asset_id: api.DigitalAssetID | None = None,
        store_ref: api.StoreUUID | None = None,
        mode: api.ReplicaMode | None = None,
    ) -> Iterator[api.ReplicaRecord]:
        """Iterate over a filtered stable snapshot of Replica records."""

        with self._lock:
            records = tuple(
                record
                for _, record in sorted(self._replicas.items())
                if (
                    digital_asset_id is None
                    or record.digital_asset_id == digital_asset_id
                )
                and (store_ref is None or record.location.store_ref == store_ref)
                and (mode is None or record.mode is mode)
            )
        return iter(records)

    @override
    def select_replica(
        self,
        digital_asset_id: api.DigitalAssetID,
        *,
        preferred_store_ref: api.StoreUUID | None = None,
        mode: api.ReplicaMode = api.ReplicaMode.ACTIVE,
        require_verified: bool = False,
    ) -> api.ReplicaRecord:
        """Choose a currently readable Replica using stable preference rules."""

        asset_record = self.get_digital_asset_record(digital_asset_id)
        candidates = list(
            self.iter_replica_records(
                digital_asset_id=digital_asset_id,
                mode=mode,
            )
        )
        state_rank = {
            api.ReplicaState.VERIFIED: 0,
            api.ReplicaState.PRESENT: 1,
            api.ReplicaState.UNVERIFIED: 2,
        }
        candidates.sort(
            key=lambda record: (
                record.location.store_ref != preferred_store_ref
                if preferred_store_ref is not None
                else False,
                state_rank.get(record.state, 99),
                int(record.replica_id),
            )
        )
        for record in candidates:
            if require_verified and record.state is not api.ReplicaState.VERIFIED:
                continue
            if record.state not in state_rank:
                continue
            try:
                info = self.stat(record.location)
            except api.StorageError:
                continue
            if info.size != asset_record.size_bytes:
                continue
            return record
        raise api.NoReadableReplica(
            f"Digital Asset {digital_asset_id} has no readable {mode.value} Replica."
        )

    @override
    def resolve_digital_asset(
        self,
        digital_asset_id: api.DigitalAssetID,
        *,
        preferred_store_ref: api.StoreUUID | None = None,
        mode: api.ReplicaMode = api.ReplicaMode.ACTIVE,
        require_verified: bool = False,
    ) -> api.DigitalAssetResolution:
        """Pair a Digital Asset record with the selected readable Replica."""

        return api.DigitalAssetResolution(
            self.get_digital_asset_record(digital_asset_id),
            self.select_replica(
                digital_asset_id,
                preferred_store_ref=preferred_store_ref,
                mode=mode,
                require_verified=require_verified,
            ),
        )

    @override
    def locate_replica(self, replica_id: api.ReplicaID) -> api.Location:
        """Return the exact Location claimed by one Replica record."""

        return self.get_replica_record(replica_id).location

    @override
    def materialize_digital_asset(
        self,
        digital_asset_id: api.DigitalAssetID,
        *,
        preferred_store_ref: api.StoreUUID | None = None,
        source_replica_id: api.ReplicaID | None = None,
        source_modes: Iterable[api.ReplicaMode | str] = (
            api.ReplicaMode.ACTIVE,
        ),
        cache_store_ref: api.StoreUUID | None = None,
        verify: bool = True,
    ) -> api.DigitalAssetResolution:
        """Return an existing readable copy or create one in the cache Store.

        Exact Replica selection permits materializing container members and
        unmanaged source bytes without pretending they are ACTIVE Replicas.
        """

        if cache_store_ref is not None:
            try:
                cached = self.resolve_digital_asset(
                    digital_asset_id,
                    preferred_store_ref=cache_store_ref,
                    mode=api.ReplicaMode.CACHE,
                    require_verified=verify,
                )
            except api.NoReadableReplica:
                pass
            else:
                if cached.location.store_ref == cache_store_ref:
                    return cached

        source_record = self._select_materialization_source(
            digital_asset_id,
            preferred_store_ref=preferred_store_ref,
            source_replica_id=source_replica_id,
            source_modes=source_modes,
            require_verified=verify and cache_store_ref is None,
        )
        if cache_store_ref is None:
            return api.DigitalAssetResolution(
                self.get_digital_asset_record(digital_asset_id),
                source_record,
            )
        replica_record = self.replicate_digital_asset(
            digital_asset_id,
            destination_store_ref=cache_store_ref,
            source_replica_id=source_record.replica_id,
            mode=api.ReplicaMode.CACHE,
            verify=verify,
        )
        return api.DigitalAssetResolution(
            self.get_digital_asset_record(digital_asset_id),
            replica_record,
        )

    def _select_materialization_source(
        self,
        digital_asset_id: api.DigitalAssetID,
        *,
        preferred_store_ref: api.StoreUUID | None,
        source_replica_id: api.ReplicaID | None,
        source_modes: Iterable[api.ReplicaMode | str],
        require_verified: bool,
    ) -> api.ReplicaRecord:
        """Select one readable source using exact identity or ordered modes."""

        if source_replica_id is not None:
            record = self.get_replica_record(source_replica_id)
            if record.digital_asset_id != digital_asset_id:
                raise api.StoragePreconditionFailed(
                    "source Replica belongs to another Digital Asset."
                )
            if require_verified and record.state is not api.ReplicaState.VERIFIED:
                raise api.NoReadableReplica(
                    f"Replica {source_replica_id} is not verified."
                )
            if record.state not in {
                api.ReplicaState.VERIFIED,
                api.ReplicaState.PRESENT,
                api.ReplicaState.UNVERIFIED,
            }:
                raise api.NoReadableReplica(
                    f"Replica {source_replica_id} is not currently readable."
                )
            try:
                info = self.stat(record.location)
            except api.StorageError as error:
                raise api.NoReadableReplica(
                    f"Replica {source_replica_id} is not currently readable."
                ) from error
            asset = self.get_digital_asset_record(digital_asset_id)
            if info.size != asset.size_bytes:
                raise api.NoReadableReplica(
                    f"Replica {source_replica_id} has the wrong size."
                )
            return record

        modes = tuple(
            mode if isinstance(mode, api.ReplicaMode) else api.ReplicaMode(mode)
            for mode in source_modes
        )
        if not modes:
            raise ValueError("source_modes must contain at least one Replica mode.")
        for mode in dict.fromkeys(modes):
            try:
                return self.select_replica(
                    digital_asset_id,
                    preferred_store_ref=preferred_store_ref,
                    mode=mode,
                    require_verified=require_verified,
                )
            except api.NoReadableReplica:
                continue
        rendered = ", ".join(mode.value for mode in dict.fromkeys(modes))
        raise api.NoReadableReplica(
            f"Digital Asset {digital_asset_id} has no readable Replica in "
            f"source modes: {rendered}."
        )

    @override
    def resolve_item_digital_asset(
        self,
        item_id: api.ItemID,
        *,
        role: str = "primary_payload",
        preferred_store_ref: api.StoreUUID | None = None,
        require_verified: bool = False,
    ) -> api.ItemDigitalAssetResolution:
        """Resolve one implementation-managed Item role link."""

        with self._lock:
            target = self._item_targets.get((item_id, role))
        if target is None:
            raise api.StorageManagementError(
                f"Item {item_id} has no Digital Asset link for role {role!r}."
            )
        kind, target_id = target
        if kind == "digital_asset":
            return api.ItemDigitalAssetResolution(
                item_id,
                role,
                digital_asset_resolution=self.resolve_digital_asset(
                    api.DigitalAssetID(target_id),
                    preferred_store_ref=preferred_store_ref,
                    require_verified=require_verified,
                ),
            )
        composite_id = api.CompositeDigitalAssetID(target_id)
        record = self.get_composite_digital_asset_record(composite_id)
        members = self.resolve_composite_digital_asset(
            composite_id,
            preferred_store_ref=preferred_store_ref,
            require_verified=require_verified,
        )
        return api.ItemDigitalAssetResolution(
            item_id,
            role,
            composite_digital_asset_record=record,
            composite_member_resolutions=members,
        )

    @override
    def replicate_digital_asset(
        self,
        digital_asset_id: api.DigitalAssetID,
        *,
        destination_store_ref: api.StoreUUID | None = None,
        source_replica_id: api.ReplicaID | None = None,
        placement_hints: api.StoragePlacementHints | None = None,
        mode: api.ReplicaMode = api.ReplicaMode.ACTIVE,
        verify: bool = True,
    ) -> api.ReplicaRecord:
        """Copy one Asset through staged publication and register its Replica."""

        asset_record = self.get_digital_asset_record(digital_asset_id)
        source_record = (
            self.select_replica(digital_asset_id)
            if source_replica_id is None
            else self.get_replica_record(source_replica_id)
        )
        if source_record.digital_asset_id != digital_asset_id:
            raise api.StoragePreconditionFailed(
                "source Replica belongs to another Digital Asset."
            )
        effective_placement_hints = (
            source_record.placement_hints
            if placement_hints is None
            else placement_hints
        )
        destination_store_ref = (
            self.get_default_store_ref()
            if destination_store_ref is None
            else destination_store_ref
        )
        store = self._require_writable_destination(
            destination_store_ref,
            mode,
            expected_size=asset_record.size_bytes,
        )
        location = self._allocate_asset_location(
            store,
            asset_record,
            placement_hints=effective_placement_hints,
        )
        with self.get(source_record.location) as source:
            store.put(
                location,
                source,
                expected_size=asset_record.size_bytes,
                expected_digest=self._preferred_digest(asset_record),
                placement_hints=effective_placement_hints,
            )
        replica_record = self._add_replica(
            api.ReplicaDeclaration(
                digital_asset_id,
                location,
                mode,
                api.ReplicaObservation(api.ReplicaState.PRESENT),
                placement_hints=effective_placement_hints,
            )
        )
        if verify:
            self.verify_replica(replica_record.replica_id)
            replica_record = self.get_replica_record(replica_record.replica_id)
        return replica_record

    @override
    def verify_replica(
        self,
        replica_id: api.ReplicaID,
        *,
        calculate_digests: bool = True,
    ) -> api.ReplicaVerificationReport:
        """Inspect and persist one Replica's latest physical observation."""

        record = self.get_replica_record(replica_id)
        asset_record = self.get_digital_asset_record(record.digital_asset_id)
        report = self._inspect_replica(
            record,
            asset_record,
            calculate_digests=calculate_digests,
        )
        observation = api.ReplicaObservation(
            report.state,
            observed_size_bytes=report.observed_size_bytes,
            observed_digests=report.observed_digests,
            checked_at=report.checked_at,
            failure_reason="; ".join(report.errors) if report.errors else None,
        )
        self._update_replica_observation(replica_id, observation)
        return report

    @override
    def verify_digital_asset(
        self,
        digital_asset_id: api.DigitalAssetID,
        *,
        replica_ids: Iterable[api.ReplicaID] | None = None,
        stop_after_first_healthy: bool | None = None,
        all_replicas: bool | None = None,
    ) -> api.DigitalAssetVerificationReport:
        """Verify an exact subset, every Replica, or one healthy copy."""

        self.get_digital_asset_record(digital_asset_id)
        if all_replicas is not None and stop_after_first_healthy is not None:
            raise ValueError(
                "all_replicas and stop_after_first_healthy are mutually exclusive."
            )
        if all_replicas is not None:
            stop_after_first_healthy = not all_replicas
        selected_ids = None if replica_ids is None else tuple(replica_ids)
        if selected_ids is not None:
            if not selected_ids:
                raise ValueError("replica_ids must not be empty when supplied.")
            if len(selected_ids) != len(set(selected_ids)):
                raise ValueError("replica_ids must not contain duplicates.")
            records = tuple(
                self.get_replica_record(replica_id)
                for replica_id in selected_ids
            )
            for record in records:
                if record.digital_asset_id != digital_asset_id:
                    raise api.StoragePreconditionFailed(
                        "selected Replica belongs to another Digital Asset."
                    )
                if record.state is api.ReplicaState.DELETED:
                    raise api.StoragePreconditionFailed(
                        "selected Replica has been deleted."
                    )
        else:
            records = tuple(
                record
                for record in self.iter_replica_records(
                    digital_asset_id=digital_asset_id
                )
                if record.state is not api.ReplicaState.DELETED
            )
        should_stop = (
            selected_ids is None
            if stop_after_first_healthy is None
            else stop_after_first_healthy
        )
        reports: list[api.ReplicaVerificationReport] = []
        for record in records:
            report = self.verify_replica(record.replica_id)
            reports.append(report)
            if report.healthy and should_stop:
                break
        return api.DigitalAssetVerificationReport(
            digital_asset_id,
            tuple(reports),
        )

    @override
    def remove_replica(
        self,
        replica_id: api.ReplicaID,
        *,
        delete_bytes: bool = True,
        retain_tombstone: bool = True,
    ) -> api.ReplicaRemovalReport:
        """Coordinate Store deletion with record removal or tombstoning."""

        record = self.get_replica_record(replica_id)
        bytes_deleted = False
        warnings: list[str] = []
        if delete_bytes:
            try:
                info = self.stat(record.location)
            except api.StoreNotFound:
                pass
            else:
                capabilities = self.capabilities(record.location.store_ref)
                version = info.version if capabilities.conditional_delete else None
                self.delete(
                    record.location,
                    missing_ok=True,
                    if_version=version,
                )
                bytes_deleted = True
        elif retain_tombstone:
            warnings.append("tombstone retained while physical bytes were preserved")

        with self._lock, self._metadata_transaction():
            current = self._require_replica_locked(replica_id)
            if retain_tombstone:
                self._replicas[replica_id] = dataclasses.replace(
                    current,
                    observation=api.ReplicaObservation(
                        api.ReplicaState.DELETED,
                        checked_at=datetime.now(UTC),
                    ),
                    revision=self._new_revision_locked(),
                )
                replica_forgotten = False
            else:
                del self._replicas[replica_id]
                replica_forgotten = True
            self._replica_generation += 1
        return api.ReplicaRemovalReport(
            replica_id,
            bytes_deleted,
            replica_forgotten,
            retain_tombstone,
            tuple(warnings),
        )

    @override
    def forget_replica(
        self,
        replica_id: api.ReplicaID,
        *,
        require_bytes_absent: bool = True,
        if_revision: str | None = None,
    ) -> bool:
        """Forget one Replica claim after optional absence confirmation."""

        with self._lock:
            record = self._replicas.get(replica_id)
        if record is None:
            return False
        self._check_revision(record.revision, if_revision)
        if require_bytes_absent:
            try:
                self.stat(record.location)
            except api.StoreNotFound:
                pass
            else:
                raise api.StoragePreconditionFailed(
                    "Replica bytes still exist at the claimed Location."
                )
        with self._lock, self._metadata_transaction():
            current = self._replicas.get(replica_id)
            if current is None:
                return False
            self._check_revision(current.revision, if_revision)
            del self._replicas[replica_id]
            self._replica_generation += 1
            return True

    # ------------------------------------------------------------------
    # Policy persistence, resolution, assessment, and planning
    # ------------------------------------------------------------------

    @override
    def create_replication_policy(
        self,
        policy: api.ReplicationPolicy,
    ) -> api.ReplicationPolicyRecord:
        """Register one replication policy with stable manager identity."""

        with self._lock, self._metadata_transaction():
            policy_id = api.ReplicationPolicyID(
                self._allocate_metadata_id_locked("replication_policy")
            )
            record = api.ReplicationPolicyRecord(
                policy_id,
                policy,
                self._new_revision_locked(),
            )
            self._replication_policies[policy_id] = record
            return record

    @override
    def get_replication_policy_record(
        self,
        replication_policy_id: api.ReplicationPolicyID,
    ) -> api.ReplicationPolicyRecord:
        """Return one registered replication policy."""

        with self._lock:
            try:
                return self._replication_policies[replication_policy_id]
            except KeyError as error:
                raise api.StorageManagementError(
                    f"Replication policy {replication_policy_id} is not registered."
                ) from error

    @override
    def update_replication_policy(
        self,
        replication_policy_id: api.ReplicationPolicyID,
        policy: api.ReplicationPolicy,
        *,
        if_revision: str | None = None,
    ) -> api.ReplicationPolicyRecord:
        """Replace a policy without invalidating recreation guarantees."""

        with self._lock, self._metadata_transaction():
            current = self._replication_policies.get(replication_policy_id)
            if current is None:
                raise api.StorageManagementError(
                    f"Replication policy {replication_policy_id} is not registered."
                )
            self._check_revision(current.revision, if_revision)
            candidate = api.ReplicationPolicyRecord(
                replication_policy_id,
                policy,
                current.revision,
            )
            self._replication_policies[replication_policy_id] = candidate
            try:
                self._validate_all_recreation_policies()
            except BaseException:
                self._replication_policies[replication_policy_id] = current
                raise
            record = dataclasses.replace(
                candidate,
                revision=self._new_revision_locked(),
            )
            self._replication_policies[replication_policy_id] = record
            return record

    @override
    def delete_replication_policy(
        self,
        replication_policy_id: api.ReplicationPolicyID,
    ) -> bool:
        """Delete an unreferenced replication policy."""

        with self._lock, self._metadata_transaction():
            if replication_policy_id not in self._replication_policies:
                return False
            if any(
                record.replication_policy_id == replication_policy_id
                for record in self._assets.values()
            ) or any(
                configuration.store_default_replication_policy_id
                == replication_policy_id
                for configuration in self._store_configurations.values()
            ):
                raise api.StoragePreconditionFailed(
                    "replication policy is still assigned."
                )
            del self._replication_policies[replication_policy_id]
            return True

    @override
    def iter_replication_policy_records(
        self,
    ) -> Iterator[api.ReplicationPolicyRecord]:
        """Iterate over a stable snapshot of replication policies."""

        with self._lock:
            records = tuple(
                self._replication_policies[key]
                for key in sorted(self._replication_policies)
            )
        return iter(records)

    @override
    def create_backup_policy(
        self,
        policy: api.BackupPolicy,
    ) -> api.BackupPolicyRecord:
        """Register one backup policy with stable manager identity."""

        with self._lock, self._metadata_transaction():
            policy_id = api.BackupPolicyID(
                self._allocate_metadata_id_locked("backup_policy")
            )
            record = api.BackupPolicyRecord(
                policy_id,
                policy,
                self._new_revision_locked(),
            )
            self._backup_policies[policy_id] = record
            return record

    @override
    def get_backup_policy_record(
        self,
        backup_policy_id: api.BackupPolicyID,
    ) -> api.BackupPolicyRecord:
        """Return one registered backup policy."""

        with self._lock:
            try:
                return self._backup_policies[backup_policy_id]
            except KeyError as error:
                raise api.StorageManagementError(
                    f"Backup policy {backup_policy_id} is not registered."
                ) from error

    @override
    def update_backup_policy(
        self,
        backup_policy_id: api.BackupPolicyID,
        policy: api.BackupPolicy,
        *,
        if_revision: str | None = None,
    ) -> api.BackupPolicyRecord:
        """Replace a policy without invalidating recreation guarantees."""

        with self._lock, self._metadata_transaction():
            current = self._backup_policies.get(backup_policy_id)
            if current is None:
                raise api.StorageManagementError(
                    f"Backup policy {backup_policy_id} is not registered."
                )
            self._check_revision(current.revision, if_revision)
            candidate = api.BackupPolicyRecord(
                backup_policy_id,
                policy,
                current.revision,
            )
            self._backup_policies[backup_policy_id] = candidate
            try:
                self._validate_all_recreation_policies()
            except BaseException:
                self._backup_policies[backup_policy_id] = current
                raise
            record = dataclasses.replace(
                candidate,
                revision=self._new_revision_locked(),
            )
            self._backup_policies[backup_policy_id] = record
            return record

    @override
    def delete_backup_policy(
        self,
        backup_policy_id: api.BackupPolicyID,
    ) -> bool:
        """Delete an unreferenced backup policy."""

        with self._lock, self._metadata_transaction():
            if backup_policy_id not in self._backup_policies:
                return False
            if any(
                record.backup_policy_id == backup_policy_id
                for record in self._assets.values()
            ) or any(
                configuration.store_default_backup_policy_id == backup_policy_id
                for configuration in self._store_configurations.values()
            ):
                raise api.StoragePreconditionFailed(
                    "backup policy is still assigned."
                )
            del self._backup_policies[backup_policy_id]
            return True

    @override
    def iter_backup_policy_records(self) -> Iterator[api.BackupPolicyRecord]:
        """Iterate over a stable snapshot of backup policies."""

        with self._lock:
            records = tuple(
                self._backup_policies[key]
                for key in sorted(self._backup_policies)
            )
        return iter(records)

    @override
    def set_digital_asset_policies(
        self,
        digital_asset_id: api.DigitalAssetID,
        *,
        replication_policy_id: api.ReplicationPolicyID | None = None,
        backup_policy_id: api.BackupPolicyID | None = None,
        if_revision: str | None = None,
    ) -> api.DigitalAssetRecord:
        """Assign explicit policies after validating references and recreation."""

        self._validate_declared_policy_ids(
            replication_policy_id,
            backup_policy_id,
        )
        with self._lock, self._metadata_transaction():
            current = self._require_asset_locked(digital_asset_id)
            self._check_revision(current.revision, if_revision)
            candidate = dataclasses.replace(
                current,
                replication_policy_id=replication_policy_id,
                backup_policy_id=backup_policy_id,
                revision=current.revision,
            )
            self._assets[digital_asset_id] = candidate
            try:
                self._validate_all_recreation_policies()
            except BaseException:
                self._assets[digital_asset_id] = current
                raise
            updated = dataclasses.replace(
                candidate,
                revision=self._new_revision_locked(),
            )
            self._assets[digital_asset_id] = updated
            return updated

    @override
    def resolve_effective_policies(
        self,
        digital_asset_id: api.DigitalAssetID,
    ) -> api.ResolvedStoragePolicies:
        """Resolve captured Asset policy, then manager-default policy."""

        asset_record = self.get_digital_asset_record(digital_asset_id)
        replication: api.ReplicationPolicy | None = None
        backup: api.BackupPolicy | None = None
        replication_source = "manager_default"
        backup_source = "manager_default"
        if asset_record.replication_policy_id is not None:
            replication = self.get_replication_policy_record(
                asset_record.replication_policy_id
            ).policy
            replication_source = "digital_asset"
        if asset_record.backup_policy_id is not None:
            backup = self.get_backup_policy_record(
                asset_record.backup_policy_id
            ).policy
            backup_source = "digital_asset"

        return api.ResolvedStoragePolicies(
            self._default_replication_policy if replication is None else replication,
            self._default_backup_policy if backup is None else backup,
            replication_source,
            backup_source,
        )

    @override
    def assess_replication(
        self,
        digital_asset_id: api.DigitalAssetID,
    ) -> api.StoragePolicyAssessment:
        """Assess live Replicas against the effective replication policy."""

        policy = self.resolve_effective_policies(digital_asset_id).replication
        return self._assess_policy(digital_asset_id, policy)

    @override
    def assess_backup(
        self,
        digital_asset_id: api.DigitalAssetID,
    ) -> api.StoragePolicyAssessment:
        """Assess backup Replicas against the effective backup policy."""

        policy = self.resolve_effective_policies(digital_asset_id).backup
        return self._assess_policy(digital_asset_id, policy)

    @override
    def assess_digital_asset(
        self,
        digital_asset_id: api.DigitalAssetID,
    ) -> api.DigitalAssetStorageAssessment:
        """Combine readability, policy satisfaction, and exact recreation."""

        self.get_digital_asset_record(digital_asset_id)
        readable = tuple(
            record.replica_id
            for record in self.iter_replica_records(
                digital_asset_id=digital_asset_id
            )
            if self._record_is_readable(record)
        )
        derivations = tuple(
            record.digital_asset_derivation_id
            for record in self.iter_digital_asset_derivation_records(
                result_digital_asset_id=digital_asset_id,
                exact_only=True,
            )
            if self._derivation_is_recoverable(record, {digital_asset_id})
        )
        return api.DigitalAssetStorageAssessment(
            digital_asset_id,
            self.assess_replication(digital_asset_id),
            self.assess_backup(digital_asset_id),
            readable_replica_ids=readable,
            exact_recreation_derivation_ids=derivations,
        )

    @override
    def plan_replication(
        self,
        digital_asset_id: api.DigitalAssetID,
    ) -> api.DigitalAssetReplicationPlan:
        """Plan verification, placement, removal, or exact recreation."""

        asset = self.get_digital_asset_record(digital_asset_id)
        policies = self.resolve_effective_policies(digital_asset_id)
        policy = policies.replication
        records = tuple(
            self.iter_replica_records(
                digital_asset_id=digital_asset_id,
                mode=policy.mode,
            )
        )
        healthy = tuple(
            record
            for record in records
            if record.state is api.ReplicaState.VERIFIED
            and self._store_satisfies_policy(record.location.store_ref, policy)
        )
        target = policy.effective_target_copies
        needed = max(0, target - self._separated_copy_capacity(healthy, policy))
        destinations = self._plan_destination_stores(
            policy,
            healthy,
            needed,
            expected_size=asset.size_bytes,
        )
        remove = (
            tuple(record.replica_id for record in records)
            if target == 0
            else tuple(record.replica_id for record in healthy[target:])
        )
        verify_ids = tuple(
            record.replica_id
            for record in records
            if record.state
            in {
                api.ReplicaState.PRESENT,
                api.ReplicaState.UNVERIFIED,
                api.ReplicaState.STAGED,
            }
        )
        recreation_id: api.DigitalAssetDerivationID | None = None
        if not healthy and policy.loss_action is api.DigitalAssetLossAction.RECREATE:
            recreation_id = next(
                (
                    record.digital_asset_derivation_id
                    for record in self.iter_digital_asset_derivation_records(
                        result_digital_asset_id=digital_asset_id,
                        exact_only=True,
                    )
                    if self._derivation_is_recoverable(
                        record,
                        {digital_asset_id},
                    )
                ),
                None,
            )
        warnings: list[str] = []
        if len(destinations) < needed:
            warnings.append(
                f"only {len(destinations)} of {needed} required destinations are available"
            )
        if (
            policy.loss_action is api.DigitalAssetLossAction.RECREATE
            and not healthy
            and recreation_id is None
        ):
            warnings.append("no currently recoverable exact derivation is available")
        return api.DigitalAssetReplicationPlan(
            digital_asset_id,
            destination_store_refs=destinations,
            replica_ids_to_verify=verify_ids,
            replica_ids_to_remove=remove,
            exact_recreation_derivation_id=recreation_id,
            warnings=tuple(warnings),
        )

    @override
    def plan_backup(
        self,
        digital_asset_id: api.DigitalAssetID,
    ) -> api.DigitalAssetBackupPlan:
        """Plan backup placement, verification, and surplus removal."""

        asset = self.get_digital_asset_record(digital_asset_id)
        policy = self.resolve_effective_policies(digital_asset_id).backup
        records = tuple(
            self.iter_replica_records(
                digital_asset_id=digital_asset_id,
                mode=policy.mode,
            )
        )
        healthy = tuple(
            record
            for record in records
            if record.state is api.ReplicaState.VERIFIED
            and self._store_satisfies_policy(record.location.store_ref, policy)
        )
        target = policy.effective_target_copies
        needed = max(0, target - self._separated_copy_capacity(healthy, policy))
        destinations = self._plan_destination_stores(
            policy,
            healthy,
            needed,
            expected_size=asset.size_bytes,
        )
        sources = tuple(
            record.replica_id
            for record in self.iter_replica_records(
                digital_asset_id=digital_asset_id
            )
            if record.mode is not policy.mode and self._record_is_readable(record)
        )
        remove = (
            tuple(record.replica_id for record in records)
            if target == 0
            else tuple(record.replica_id for record in healthy[target:])
        )
        warnings = (
            (
                f"only {len(destinations)} of {needed} required destinations are available",
            )
            if len(destinations) < needed
            else ()
        )
        return api.DigitalAssetBackupPlan(
            digital_asset_id,
            destination_store_refs=destinations,
            source_replica_ids=sources,
            replica_ids_to_verify=tuple(
                record.replica_id
                for record in records
                if record.state is not api.ReplicaState.VERIFIED
                and record.state is not api.ReplicaState.DELETED
            ),
            replica_ids_to_remove=remove,
            warnings=warnings,
        )

    # ------------------------------------------------------------------
    # Composite Digital Assets
    # ------------------------------------------------------------------

    @override
    def declare_composite_digital_asset(
        self,
        declaration: api.CompositeDigitalAssetDeclaration,
    ) -> api.CompositeDigitalAssetRecord:
        """Register an ordered Composite after validating every member."""

        for member in declaration.members:
            self.get_digital_asset_record(member.digital_asset_id)
        with self._lock, self._metadata_transaction():
            composite_id = api.CompositeDigitalAssetID(
                self._allocate_metadata_id_locked("composite")
            )
            record = api.CompositeDigitalAssetRecord(
                composite_id,
                declaration.members,
                declaration.name,
                declaration.attributes,
                self._new_revision_locked(),
            )
            self._composites[composite_id] = record
            return record

    @override
    def get_composite_digital_asset_record(
        self,
        composite_digital_asset_id: api.CompositeDigitalAssetID,
    ) -> api.CompositeDigitalAssetRecord:
        """Return one Composite record."""

        with self._lock:
            try:
                return self._composites[composite_digital_asset_id]
            except KeyError as error:
                raise api.CompositeDigitalAssetNotFound(
                    f"Composite Digital Asset {composite_digital_asset_id} is not registered."
                ) from error

    @override
    def replace_composite_digital_asset(
        self,
        composite_digital_asset_id: api.CompositeDigitalAssetID,
        declaration: api.CompositeDigitalAssetDeclaration,
        *,
        if_revision: str | None = None,
    ) -> api.CompositeDigitalAssetRecord:
        """Replace Composite metadata and membership under revision control."""

        for member in declaration.members:
            self.get_digital_asset_record(member.digital_asset_id)
        with self._lock, self._metadata_transaction():
            current = self._require_composite_locked(composite_digital_asset_id)
            self._check_revision(current.revision, if_revision)
            record = api.CompositeDigitalAssetRecord(
                composite_digital_asset_id,
                declaration.members,
                declaration.name,
                declaration.attributes,
                self._new_revision_locked(),
            )
            self._composites[composite_digital_asset_id] = record
            return record

    @override
    def iter_composite_digital_asset_records(
        self,
    ) -> Iterator[api.CompositeDigitalAssetRecord]:
        """Iterate over a stable Composite snapshot."""

        with self._lock:
            records = tuple(
                self._composites[key] for key in sorted(self._composites)
            )
        return iter(records)

    @override
    def forget_composite_digital_asset(
        self,
        composite_digital_asset_id: api.CompositeDigitalAssetID,
        *,
        require_unlinked: bool = True,
        if_revision: str | None = None,
    ) -> bool:
        """Forget an unlinked Composite without touching member Assets."""

        with self._lock, self._metadata_transaction():
            current = self._composites.get(composite_digital_asset_id)
            if current is None:
                return False
            self._check_revision(current.revision, if_revision)
            if require_unlinked:
                if any(
                    kind == "composite_digital_asset"
                    and target_id == composite_digital_asset_id
                    for kind, target_id in self._item_targets.values()
                ):
                    raise api.StoragePreconditionFailed(
                        "Composite Digital Asset is still linked to an Item."
                    )
                if any(
                    source.composite_digital_asset_id
                    == composite_digital_asset_id
                    for record in self._derivations.values()
                    for source in record.declaration.sources
                ):
                    raise api.StoragePreconditionFailed(
                        "Composite Digital Asset is still derivation provenance."
                    )
            del self._composites[composite_digital_asset_id]
            return True

    @override
    def resolve_composite_digital_asset(
        self,
        composite_digital_asset_id: api.CompositeDigitalAssetID,
        *,
        preferred_store_ref: api.StoreUUID | None = None,
        require_verified: bool = False,
    ) -> tuple[api.CompositeDigitalAssetMemberResolution, ...]:
        """Resolve each readable Composite member without flattening context."""

        record = self.get_composite_digital_asset_record(
            composite_digital_asset_id
        )
        resolved: list[api.CompositeDigitalAssetMemberResolution] = []
        missing: list[api.DigitalAssetID] = []
        for membership in record.members:
            try:
                resolution = self.resolve_digital_asset(
                    membership.digital_asset_id,
                    preferred_store_ref=preferred_store_ref,
                    require_verified=require_verified,
                )
            except (api.DigitalAssetNotFound, api.NoReadableReplica):
                if membership.required:
                    missing.append(membership.digital_asset_id)
                continue
            resolved.append(
                api.CompositeDigitalAssetMemberResolution(
                    membership,
                    resolution,
                )
            )
        if missing:
            raise api.CompositeDigitalAssetIncomplete(
                "required member Assets are unavailable: "
                + ", ".join(str(value) for value in missing)
            )
        return tuple(resolved)

    @override
    def assess_composite_digital_asset(
        self,
        composite_digital_asset_id: api.CompositeDigitalAssetID,
    ) -> api.CompositeDigitalAssetAvailabilityAssessment:
        """Assess member existence and current Replica readability."""

        record = self.get_composite_digital_asset_record(
            composite_digital_asset_id
        )
        resolved = readable = 0
        missing: list[api.DigitalAssetID] = []
        errors: list[str] = []
        required_members = tuple(
            membership for membership in record.members if membership.required
        )
        for membership in required_members:
            try:
                self.get_digital_asset_record(membership.digital_asset_id)
                resolved += 1
            except api.DigitalAssetNotFound as error:
                missing.append(membership.digital_asset_id)
                errors.append(str(error))
                continue
            try:
                self.select_replica(membership.digital_asset_id)
                readable += 1
            except api.NoReadableReplica as error:
                missing.append(membership.digital_asset_id)
                errors.append(str(error))
        return api.CompositeDigitalAssetAvailabilityAssessment(
            composite_digital_asset_id,
            len(required_members),
            resolved,
            readable,
            tuple(dict.fromkeys(missing)),
            tuple(errors),
        )

    # ------------------------------------------------------------------
    # Digital Asset derivation provenance
    # ------------------------------------------------------------------

    @override
    def record_digital_asset_derivation(
        self,
        declaration: api.DigitalAssetDerivationDeclaration,
    ) -> api.DigitalAssetDerivationRecord:
        """Validate and record immutable provenance for a derived Asset."""

        result = self.get_digital_asset_record(
            declaration.result_digital_asset_id
        )
        source_asset_ids: set[api.DigitalAssetID] = set()
        for source in declaration.sources:
            if source.digital_asset_id is not None:
                self.get_digital_asset_record(source.digital_asset_id)
                source_asset_ids.add(source.digital_asset_id)
                continue
            if source.composite_digital_asset_id is None:
                raise api.StoragePreconditionFailed(
                    "derivation source has no Asset identity."
                )
            composite = self.get_composite_digital_asset_record(
                source.composite_digital_asset_id
            )
            source_asset_ids.update(
                member.digital_asset_id for member in composite.members
            )

        recipe = declaration.recipe
        if recipe is not None:
            recipe_asset_ids = {
                input_.digital_asset_id for input_ in recipe.inputs
            }
            if recipe.complete and not source_asset_ids <= recipe_asset_ids:
                missing = sorted(source_asset_ids - recipe_asset_ids)
                raise api.StoragePreconditionFailed(
                    "complete recipe does not pin every provenance source: "
                    + ", ".join(str(value) for value in missing)
                )
            for input_ in recipe.inputs:
                input_record = self.get_digital_asset_record(
                    input_.digital_asset_id
                )
                self._require_same_identity(
                    input_record,
                    input_.size_bytes,
                    input_.digests,
                )
            artifacts = (
                (() if recipe.executor is None else (recipe.executor,))
                + recipe.dependencies
            )
            for artifact in artifacts:
                if artifact.digital_asset_id is None:
                    continue
                artifact_record = self.get_digital_asset_record(
                    artifact.digital_asset_id
                )
                self._require_same_identity(
                    artifact_record,
                    artifact_record.size_bytes,
                    (artifact.digest,),
                )
                recipe_asset_ids.add(artifact.digital_asset_id)
            source_asset_ids.update(recipe_asset_ids)
            if recipe.can_recreate_exactly:
                if recipe.expected_output_size != result.size_bytes:
                    raise api.StorageIntegrityError(
                        "exact recipe output size differs from the result Asset."
                    )
                self._require_expected_digests(
                    recipe.expected_output_digests,
                    result.digests,
                )

        self._reject_derivation_cycle(
            declaration.result_digital_asset_id,
            source_asset_ids,
        )
        with self._lock, self._metadata_transaction():
            derivation_id = api.DigitalAssetDerivationID(
                self._allocate_metadata_id_locked("derivation")
            )
            record = api.DigitalAssetDerivationRecord(
                derivation_id,
                declaration,
                self._new_revision_locked(),
            )
            self._derivations[derivation_id] = record
            return record

    @override
    def get_digital_asset_derivation_record(
        self,
        digital_asset_derivation_id: api.DigitalAssetDerivationID,
    ) -> api.DigitalAssetDerivationRecord:
        """Return one registered derivation record."""

        with self._lock:
            try:
                return self._derivations[digital_asset_derivation_id]
            except KeyError as error:
                raise api.DigitalAssetDerivationNotFound(
                    "Digital Asset derivation "
                    f"{digital_asset_derivation_id} is not registered."
                ) from error

    @override
    def iter_digital_asset_derivation_records(
        self,
        *,
        result_digital_asset_id: api.DigitalAssetID | None = None,
        source_digital_asset_id: api.DigitalAssetID | None = None,
        source_composite_digital_asset_id: (
            api.CompositeDigitalAssetID | None
        ) = None,
        workflow_id: int | None = None,
        workflow_reference: str | None = None,
        exact_only: bool = False,
    ) -> Iterator[api.DigitalAssetDerivationRecord]:
        """Iterate over an ID-ordered, provenance-filtered snapshot."""

        if workflow_id is not None and workflow_id <= 0:
            raise ValueError("workflow_id must be positive when supplied.")
        if workflow_reference is not None and not workflow_reference.strip():
            raise ValueError(
                "workflow_reference must not be empty when supplied."
            )

        with self._lock:
            records = tuple(
                record
                for _, record in sorted(self._derivations.items())
                if (
                    result_digital_asset_id is None
                    or record.declaration.result_digital_asset_id
                    == result_digital_asset_id
                )
                and (
                    source_digital_asset_id is None
                    or any(
                        source.digital_asset_id == source_digital_asset_id
                        for source in record.declaration.sources
                    )
                )
                and (
                    source_composite_digital_asset_id is None
                    or any(
                        source.composite_digital_asset_id
                        == source_composite_digital_asset_id
                        for source in record.declaration.sources
                    )
                )
                and (
                    workflow_id is None
                    or record.declaration.workflow_id == workflow_id
                )
                and (
                    workflow_reference is None
                    or record.declaration.workflow_reference
                    == workflow_reference
                )
                and (not exact_only or record.can_recreate_exactly)
            )
        return iter(records)

    @override
    def get_derivation_graph(
        self,
        digital_asset_id: api.DigitalAssetID,
        *,
        direction: (
            api.DigitalAssetDerivationGraphDirection | str
        ) = api.DigitalAssetDerivationGraphDirection.BOTH,
        max_depth: int | None = None,
        workflow_id: int | None = None,
        workflow_reference: str | None = None,
        exact_only: bool = False,
    ) -> api.DigitalAssetDerivationGraph:
        """Return a stable breadth-first provenance graph around one Asset."""

        self.get_digital_asset_record(digital_asset_id)
        if max_depth is not None and max_depth < 0:
            raise ValueError("max_depth must not be negative.")
        try:
            graph_direction = api.DigitalAssetDerivationGraphDirection(
                direction
            )
        except ValueError as error:
            raise ValueError(
                "direction must be 'ancestors', 'descendants', or 'both'."
            ) from error

        records = tuple(
            self.iter_digital_asset_derivation_records(
                workflow_id=workflow_id,
                workflow_reference=workflow_reference,
                exact_only=exact_only,
            )
        )
        sources_by_derivation = {
            record.digital_asset_derivation_id: tuple(
                sorted(
                    self._source_asset_ids(
                        record,
                        include_recipe_artifacts=False,
                    )
                )
            )
            for record in records
        }
        by_result: dict[
            api.DigitalAssetID,
            list[api.DigitalAssetDerivationRecord],
        ] = {}
        by_source: dict[
            api.DigitalAssetID,
            list[api.DigitalAssetDerivationRecord],
        ] = {}
        for record in records:
            by_result.setdefault(
                record.declaration.result_digital_asset_id,
                [],
            ).append(record)
            for source_id in sources_by_derivation[
                record.digital_asset_derivation_id
            ]:
                by_source.setdefault(source_id, []).append(record)

        asset_ids: list[api.DigitalAssetID] = [digital_asset_id]
        seen_asset_ids = {digital_asset_id}
        composite_ids: list[api.CompositeDigitalAssetID] = []
        seen_composite_ids: set[api.CompositeDigitalAssetID] = set()
        graph_records: list[api.DigitalAssetDerivationRecord] = []
        seen_derivation_ids: set[api.DigitalAssetDerivationID] = set()
        truncated = False

        def walk(
            walk_direction: api.DigitalAssetDerivationGraphDirection,
        ) -> None:
            """Breadth-first walk in one direction while retaining branches."""

            nonlocal truncated
            queue: deque[tuple[api.DigitalAssetID, int]] = deque(
                ((digital_asset_id, 0),)
            )
            walked: set[api.DigitalAssetID] = set()
            while queue:
                current_id, depth = queue.popleft()
                if current_id in walked:
                    continue
                walked.add(current_id)
                adjacent = (
                    by_result.get(current_id, ())
                    if walk_direction
                    is api.DigitalAssetDerivationGraphDirection.ANCESTORS
                    else by_source.get(current_id, ())
                )
                if max_depth is not None and depth >= max_depth:
                    if adjacent:
                        truncated = True
                    continue
                for record in adjacent:
                    derivation_id = record.digital_asset_derivation_id
                    if derivation_id not in seen_derivation_ids:
                        graph_records.append(record)
                        seen_derivation_ids.add(derivation_id)
                        for source in record.declaration.sources:
                            composite_id = source.composite_digital_asset_id
                            if (
                                composite_id is not None
                                and composite_id not in seen_composite_ids
                            ):
                                composite_ids.append(composite_id)
                                seen_composite_ids.add(composite_id)
                    next_ids = (
                        sources_by_derivation[derivation_id]
                        if walk_direction
                        is api.DigitalAssetDerivationGraphDirection.ANCESTORS
                        else (
                            record.declaration.result_digital_asset_id,
                        )
                    )
                    for next_id in next_ids:
                        if next_id not in seen_asset_ids:
                            asset_ids.append(next_id)
                            seen_asset_ids.add(next_id)
                        if next_id not in walked:
                            queue.append((next_id, depth + 1))

        if graph_direction in {
            api.DigitalAssetDerivationGraphDirection.ANCESTORS,
            api.DigitalAssetDerivationGraphDirection.BOTH,
        }:
            walk(api.DigitalAssetDerivationGraphDirection.ANCESTORS)
        if graph_direction in {
            api.DigitalAssetDerivationGraphDirection.DESCENDANTS,
            api.DigitalAssetDerivationGraphDirection.BOTH,
        }:
            walk(api.DigitalAssetDerivationGraphDirection.DESCENDANTS)

        return api.DigitalAssetDerivationGraph(
            digital_asset_id,
            graph_direction,
            tuple(asset_ids),
            tuple(composite_ids),
            tuple(graph_records),
            truncated,
        )

    @override
    def plan_digital_asset_recreation(
        self,
        digital_asset_id: api.DigitalAssetID,
    ) -> api.DigitalAssetRecreationPlan:
        """Select a shortest viable, topologically ordered exact replay route."""

        self.get_digital_asset_record(digital_asset_id)
        branch = self._plan_recreation_branch(
            digital_asset_id,
            visiting=frozenset(),
            memo={},
        )
        alternatives = tuple(
            derivation_id
            for derivation_id in dict.fromkeys(
                branch.alternative_derivation_ids
            )
            if derivation_id != branch.selected_derivation_id
        )
        return api.DigitalAssetRecreationPlan(
            digital_asset_id,
            steps=branch.steps,
            available_digital_asset_ids=tuple(
                sorted(branch.available_digital_asset_ids)
            ),
            unavailable_digital_asset_ids=tuple(
                sorted(branch.unavailable_digital_asset_ids)
            ),
            selected_derivation_id=branch.selected_derivation_id,
            alternative_derivation_ids=alternatives,
            warnings=tuple(dict.fromkeys(branch.warnings)),
        )

    @override
    def forget_digital_asset_derivation(
        self,
        digital_asset_derivation_id: api.DigitalAssetDerivationID,
        *,
        if_revision: str | None = None,
    ) -> bool:
        """Forget one provenance assertion under revision control."""

        with self._lock, self._metadata_transaction():
            record = self._derivations.get(digital_asset_derivation_id)
            if record is None:
                return False
            self._check_revision(record.revision, if_revision)
            del self._derivations[digital_asset_derivation_id]
            return True

    # ------------------------------------------------------------------
    # Store reconciliation
    # ------------------------------------------------------------------

    @override
    def plan_reconciliation(
        self,
        store_ref: api.StoreUUID,
        *,
        verify_digests: bool = False,
    ) -> api.StoreReconciliationPlan:
        """Compare Replica claims with Store inventory without mutation."""

        store = self.get_store(store_ref)
        enumeration = store.capabilities.enumeration
        expected = tuple(
            record
            for record in self.iter_replica_records(store_ref=store_ref)
            if record.state is not api.ReplicaState.DELETED
        )
        inventory: set[api.Location] = set()
        warnings: list[str] = []
        errors: list[str] = []
        if enumeration is api.EnumerationCompleteness.UNAVAILABLE:
            warnings.append(
                "Store cannot enumerate inventory; claims were checked individually."
            )
        else:
            try:
                inventory.update(store.iter_locations())
            except api.StorageError as error:
                enumeration = api.EnumerationCompleteness.UNAVAILABLE
                errors.append(f"inventory enumeration failed: {error}")

        missing: list[api.ReplicaID] = []
        corrupt: list[api.ReplicaID] = []
        unavailable: list[api.ReplicaID] = []
        matched = 0
        for record in expected:
            if (
                enumeration is api.EnumerationCompleteness.COMPLETE
                and record.location not in inventory
            ):
                missing.append(record.replica_id)
                continue
            report = self._inspect_replica(
                record,
                self.get_digital_asset_record(record.digital_asset_id),
                calculate_digests=verify_digests,
            )
            if report.exists is False:
                missing.append(record.replica_id)
            elif report.exists is None:
                unavailable.append(record.replica_id)
                errors.extend(report.errors)
            elif report.state is api.ReplicaState.CORRUPT:
                corrupt.append(record.replica_id)
                inventory.add(record.location)
            else:
                matched += 1
                inventory.add(record.location)

        expected_locations = {record.location for record in expected}
        unexpected = tuple(
            sorted(
                inventory - expected_locations,
                key=lambda location: location.key,
            )
        )
        with self._lock:
            repository_revision = str(self._replica_generation)
        return api.StoreReconciliationPlan(
            uuid4(),
            store_ref,
            verify_digests,
            enumeration,
            expected_replicas=len(expected),
            observed_locations=len(inventory),
            matched_replicas=matched,
            missing_replica_ids=tuple(missing),
            unexpected_locations=unexpected,
            corrupt_replica_ids=tuple(corrupt),
            unavailable_replica_ids=tuple(unavailable),
            repository_revision=repository_revision,
            warnings=tuple(warnings),
            errors=tuple(errors),
        )

    @override
    def apply_reconciliation(
        self,
        plan: api.StoreReconciliationPlan,
    ) -> api.StoreReconciliationReport:
        """Apply one current plan to Replica observations only."""

        with self._lock, self._metadata_transaction():
            if plan.repository_revision != str(self._replica_generation):
                raise api.StoreReconciliationPlanStale(
                    "Replica repository changed after reconciliation planning."
                )
            classifications = (
                (plan.missing_replica_ids, api.ReplicaState.MISSING),
                (plan.corrupt_replica_ids, api.ReplicaState.CORRUPT),
                (plan.unavailable_replica_ids, api.ReplicaState.UNAVAILABLE),
            )
            updated: list[api.ReplicaID] = []
            for replica_ids, state in classifications:
                for replica_id in replica_ids:
                    record = self._require_replica_locked(replica_id)
                    if record.location.store_ref != plan.store_ref:
                        raise api.StoreReconciliationPlanStale(
                            "reconciliation plan contains a Replica from another Store."
                        )
                    self._replicas[replica_id] = dataclasses.replace(
                        record,
                        observation=api.ReplicaObservation(
                            state,
                            checked_at=datetime.now(UTC),
                            failure_reason=(
                                "reconciliation observed missing bytes"
                                if state is api.ReplicaState.MISSING
                                else "reconciliation could not confirm healthy bytes"
                            ),
                        ),
                        revision=self._new_revision_locked(),
                    )
                    updated.append(replica_id)
            if updated:
                self._replica_generation += 1
        return api.StoreReconciliationReport(
            plan,
            applied=True,
            updated_replica_ids=tuple(updated),
        )

    # ------------------------------------------------------------------
    # Internal repository and domain invariants
    # ------------------------------------------------------------------

    def _new_revision_locked(self) -> str:
        """Allocate a monotonically increasing manager revision token."""

        self._revision_counter += 1
        return f"m{self._revision_counter}"

    def _metadata_transaction(self):
        """Return the transaction enclosing one metadata mutation."""

        return nullcontext()

    def _ingest_journal_statuses(self) -> tuple[Mapping[str, object], ...]:
        """Return no durable journal entries for the transient manager."""

        return ()

    def _allocate_metadata_id_locked(
        self,
        kind: _MetadataRecordKind,
    ) -> int:
        """Allocate one process-local identity for the transient manager."""

        attribute = {
            "digital_asset": "_next_asset_id",
            "replica": "_next_replica_id",
            "composite": "_next_composite_id",
            "derivation": "_next_derivation_id",
            "replication_policy": "_next_replication_policy_id",
            "backup_policy": "_next_backup_policy_id",
        }[kind]
        identifier = int(getattr(self, attribute))
        setattr(self, attribute, identifier + 1)
        return identifier

    @staticmethod
    def _check_revision(current: str | None, expected: str | None) -> None:
        """Enforce an optional optimistic-lock revision."""

        if expected is not None and current != expected:
            raise api.StoragePreconditionFailed(
                f"revision precondition failed: expected {expected!r}, "
                f"found {current!r}."
            )

    def _require_asset_locked(
        self,
        digital_asset_id: api.DigitalAssetID,
    ) -> api.DigitalAssetRecord:
        """Return a locked Asset lookup or raise the domain error."""

        try:
            return self._assets[digital_asset_id]
        except KeyError as error:
            raise api.DigitalAssetNotFound(
                f"Digital Asset {digital_asset_id} is not registered."
            ) from error

    def _require_replica_locked(
        self,
        replica_id: api.ReplicaID,
    ) -> api.ReplicaRecord:
        """Return a locked Replica lookup or raise the domain error."""

        try:
            return self._replicas[replica_id]
        except KeyError as error:
            raise api.ReplicaNotFound(
                f"Replica {replica_id} is not registered."
            ) from error

    def _require_composite_locked(
        self,
        composite_digital_asset_id: api.CompositeDigitalAssetID,
    ) -> api.CompositeDigitalAssetRecord:
        """Return a locked Composite lookup or raise the domain error."""

        try:
            return self._composites[composite_digital_asset_id]
        except KeyError as error:
            raise api.CompositeDigitalAssetNotFound(
                "Composite Digital Asset "
                f"{composite_digital_asset_id} is not registered."
            ) from error

    def _find_asset_locked(
        self,
        digests: tuple[api.Digest, ...],
        size_bytes: int | None,
    ) -> api.DigitalAssetRecord | None:
        """Find a non-conflicting digest match in stable Asset-ID order."""

        supplied = {digest.algorithm: digest.value for digest in digests}
        for digital_asset_id in sorted(self._assets):
            record = self._assets[digital_asset_id]
            if size_bytes is not None and record.size_bytes != size_bytes:
                continue
            registered = {
                digest.algorithm: digest.value for digest in record.digests
            }
            overlap = supplied.keys() & registered.keys()
            if overlap and all(
                supplied[algorithm] == registered[algorithm]
                for algorithm in overlap
            ):
                return record
        return None

    @staticmethod
    def _require_expected_digests(
        expected: tuple[api.Digest, ...],
        observed: tuple[api.Digest, ...],
    ) -> None:
        """Require every expected algorithm and value in observations."""

        observed_by_algorithm = {
            digest.algorithm: digest.value for digest in observed
        }
        for digest in expected:
            if observed_by_algorithm.get(digest.algorithm) != digest.value:
                raise api.StorageIntegrityError(
                    f"{digest.algorithm} digest does not match expected value."
                )

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

        identity = (size_bytes, digests)
        with self._lock:
            identity_lock = self._ingest_identity_locks.setdefault(
                identity,
                RLock(),
            )
        with identity_lock:
            return self._complete_authoritative_ingest_locked(
                request=request,
                operation_id=operation_id,
                size_bytes=size_bytes,
                digests=digests,
                item_id=item_id,
                role=role,
                metadata=metadata,
                placement_hints=placement_hints,
                preferred_store_ref=preferred_store_ref,
                replica_mode=replica_mode,
                verify=verify,
                publish=publish,
            )

    def _complete_authoritative_ingest_locked(
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
        """Declare, publish, and register one authoritative object identity."""

        with self._lock:
            prior = self._ingest_operations.get(operation_id)
        if prior is not None:
            if prior.request != request:
                raise api.StoragePreconditionFailed(
                    "ingest operation ID was already used for a different request."
                )
            return prior.result
        self._journal_ingest_started(operation_id, request)
        destination_ref = (
            self.get_default_store_ref()
            if preferred_store_ref is None
            else preferred_store_ref
        )
        replication_policy_id, backup_policy_id = self._placement_policy_ids(
            destination_ref
        )
        with self._lock:
            existing = self._find_asset_locked(digests, size_bytes)
        asset_created = existing is None
        asset_record = (
            self.declare_digital_asset(
                api.DigitalAssetDeclaration(
                    size_bytes,
                    digests,
                    metadata,
                    replication_policy_id,
                    backup_policy_id,
                )
            )
            if existing is None
            else existing
        )
        try:
            existing_replica = self._find_replica_for_store(
                asset_record.digital_asset_id,
                destination_ref,
                replica_mode,
            )
            if (
                existing_replica is not None
                and not self._record_is_readable(existing_replica)
            ):
                existing_replica = None
            replica_created = existing_replica is None
            if existing_replica is None:
                store = self._require_writable_destination(
                    destination_ref,
                    replica_mode,
                    expected_size=asset_record.size_bytes,
                )
                location = self._allocate_asset_location(
                    store,
                    asset_record,
                    placement_hints=placement_hints,
                )
                self._journal_ingest_publication_pending(
                    operation_id,
                    asset_record=asset_record,
                    asset_created=asset_created,
                    location=location,
                    replica_mode=replica_mode,
                    placement_hints=placement_hints,
                )
                publish(store, location, self._preferred_digest(asset_record))
                self._journal_ingest_published(operation_id)
                if existing is not None:
                    # Placement-policy defaults become part of an existing,
                    # previously unplaced Asset only after its first bytes have
                    # actually published.
                    asset_record = self._capture_first_placement_policies(
                        asset_record,
                        replication_policy_id,
                        backup_policy_id,
                    )
                replica_record = self._add_replica(
                    api.ReplicaDeclaration(
                        asset_record.digital_asset_id,
                        location,
                        replica_mode,
                        api.ReplicaObservation(api.ReplicaState.PRESENT),
                        placement_hints=placement_hints,
                    )
                )
            else:
                replica_record = existing_replica
        except Exception as error:
            self._journal_ingest_failed(operation_id, error)
            if asset_created:
                # Declaring identity is an implementation prerequisite for
                # allocation, not a successful ingest result.  Do not leave a
                # phantom Asset when destination selection, allocation, or
                # publication fails before a Replica can be registered.
                try:
                    self.forget_digital_asset(
                        asset_record.digital_asset_id,
                        if_revision=asset_record.revision,
                    )
                except api.StoragePreconditionFailed:
                    # Preserve the original Store failure if concurrent work
                    # acquired a legitimate reference to the declaration.
                    pass
            raise
        if verify:
            report = self.verify_replica(replica_record.replica_id)
            replica_record = self.get_replica_record(replica_record.replica_id)
            verified = report.healthy
        else:
            verified = replica_record.state is api.ReplicaState.VERIFIED
        result = api.DigitalAssetIngestResult(
            operation_id,
            asset_record,
            replica_record,
            asset_created,
            replica_created,
            deduplicated=not asset_created,
            verified=verified,
        )
        with self._metadata_transaction():
            if item_id is not None:
                self.link_item_to_digital_asset(
                    item_id,
                    asset_record.digital_asset_id,
                    role="primary_payload" if role is None else role,
                )
            with self._lock:
                self._ingest_operations[operation_id] = _IngestOperation(
                    request,
                    result,
                )
        return result

    def _journal_ingest_started(
        self,
        operation_id: UUID,
        request: _IngestRequest,
    ) -> None:
        """Hook for durable managers to record operation intent."""

    def _journal_ingest_publication_pending(
        self,
        operation_id: UUID,
        *,
        asset_record: api.DigitalAssetRecord,
        asset_created: bool,
        location: api.Location,
        replica_mode: api.ReplicaMode,
        placement_hints: api.StoragePlacementHints | None,
    ) -> None:
        """Hook immediately before external Store publication begins."""

    def _journal_ingest_published(self, operation_id: UUID) -> None:
        """Hook after Store publication and before metadata completion."""

    def _journal_ingest_failed(
        self,
        operation_id: UUID,
        error: BaseException,
    ) -> None:
        """Hook for a handled ingest failure."""

    def _require_same_identity(
        self,
        record: api.DigitalAssetRecord,
        size_bytes: int,
        observed_digests: tuple[api.Digest, ...],
    ) -> None:
        """Require size plus all comparable digests to identify one Asset."""

        if record.size_bytes != size_bytes:
            raise api.StorageIntegrityError(
                "observed size differs from the registered Digital Asset."
            )
        expected = {
            digest.algorithm: digest.value for digest in record.digests
        }
        observed = {
            digest.algorithm: digest.value for digest in observed_digests
        }
        overlap = expected.keys() & observed.keys()
        if not overlap or any(
            expected[algorithm] != observed[algorithm]
            for algorithm in overlap
        ):
            raise api.StorageIntegrityError(
                "observed digests do not identify the registered Digital Asset."
            )

    @staticmethod
    def _new_hashers(algorithms: Iterable[str]) -> dict[str, _Hasher]:
        """Create normalized hashlib objects for unique algorithms."""

        return {
            algorithm.strip().lower(): hashlib.new(algorithm.strip().lower())
            for algorithm in sorted(set(algorithms))
        }

    def _calculate_location_digests(
        self,
        location: api.Location,
        algorithms: Iterable[str],
    ) -> tuple[api.Digest, ...]:
        """Stream a Location once and calculate all requested digests."""

        hashers = self._new_hashers(algorithms)
        with self.get(location) as source:
            while True:
                chunk = source.read(1024 * 1024)
                if not chunk:
                    break
                if not isinstance(chunk, bytes):
                    raise TypeError("Store read streams must return bytes.")
                for hasher in hashers.values():
                    hasher.update(chunk)
        return tuple(
            api.Digest(algorithm, hashers[algorithm].hexdigest())
            for algorithm in sorted(hashers)
        )

    @staticmethod
    def _preferred_digest(record: api.DigitalAssetRecord) -> api.Digest:
        """Prefer SHA-256 for Store verification, then stable first digest."""

        return next(
            (
                digest
                for digest in record.digests
                if digest.algorithm == "sha256"
            ),
            record.digests[0],
        )

    def _inspect_replica(
        self,
        record: api.ReplicaRecord,
        asset_record: api.DigitalAssetRecord,
        *,
        calculate_digests: bool,
    ) -> api.ReplicaVerificationReport:
        """Inspect a Replica without mutating manager repository state."""

        checked_at = datetime.now(UTC)
        try:
            info = self.stat(record.location)
        except api.StoreNotFound as error:
            return api.ReplicaVerificationReport(
                record.replica_id,
                record.digital_asset_id,
                api.ReplicaState.MISSING,
                False,
                checked_at=checked_at,
                errors=(str(error) or "object is missing",),
            )
        except api.StorageError as error:
            return api.ReplicaVerificationReport(
                record.replica_id,
                record.digital_asset_id,
                api.ReplicaState.UNAVAILABLE,
                None,
                checked_at=checked_at,
                errors=(str(error) or type(error).__name__,),
            )

        size_matches = info.size == asset_record.size_bytes
        observed: tuple[api.Digest, ...] = ()
        digest_matches: bool | None = None
        errors: list[str] = []
        if not size_matches:
            errors.append(
                f"expected {asset_record.size_bytes} bytes, observed {info.size}"
            )
        store = self.get_store(record.location.store_ref)
        authoritative_stat_digest = (
            store.capabilities.stat_digest_authoritative
            and info.digest is not None
            and info.digest.algorithm == "sha256"
        )
        if calculate_digests and authoritative_stat_digest:
            assert info.digest is not None
            expected_by_algorithm = {
                digest.algorithm: digest.value
                for digest in asset_record.digests
            }
            observed = (info.digest,)
            digest_matches = (
                expected_by_algorithm.get(info.digest.algorithm)
                == info.digest.value
            )
            if not digest_matches:
                errors.append(
                    "authoritative Store digest does not identify the Digital Asset."
                )
        elif calculate_digests:
            try:
                observed = self._calculate_location_digests(
                    record.location,
                    (digest.algorithm for digest in asset_record.digests),
                )
                self._require_expected_digests(asset_record.digests, observed)
                digest_matches = True
            except api.StorageIntegrityError as error:
                digest_matches = False
                errors.append(str(error))
            except api.StorageError as error:
                return api.ReplicaVerificationReport(
                    record.replica_id,
                    record.digital_asset_id,
                    api.ReplicaState.UNAVAILABLE,
                    None,
                    size_matches=size_matches,
                    observed_size_bytes=info.size,
                    checked_at=checked_at,
                    errors=(str(error) or type(error).__name__,),
                )
        if not size_matches or digest_matches is False:
            state = api.ReplicaState.CORRUPT
        elif digest_matches is True:
            state = api.ReplicaState.VERIFIED
        else:
            state = api.ReplicaState.PRESENT
        return api.ReplicaVerificationReport(
            record.replica_id,
            record.digital_asset_id,
            state,
            True,
            size_matches=size_matches,
            digest_matches=digest_matches,
            observed_size_bytes=info.size,
            observed_digests=observed,
            checked_at=checked_at,
            errors=tuple(errors),
        )

    def _update_replica_observation(
        self,
        replica_id: api.ReplicaID,
        observation: api.ReplicaObservation,
    ) -> api.ReplicaRecord:
        """Replace one Replica observation and advance repository generation."""

        with self._lock, self._metadata_transaction():
            current = self._require_replica_locked(replica_id)
            updated = dataclasses.replace(
                current,
                observation=observation,
                revision=self._new_revision_locked(),
            )
            self._replicas[replica_id] = updated
            self._replica_generation += 1
            return updated

    def _add_replica(
        self,
        declaration: api.ReplicaDeclaration,
    ) -> api.ReplicaRecord:
        """Add one non-conflicting Replica claim."""

        self.get_digital_asset_record(declaration.digital_asset_id)
        self.get_store_configuration(declaration.location.store_ref)
        with self._lock, self._metadata_transaction():
            conflict = next(
                (
                    record
                    for record in self._replicas.values()
                    if record.location == declaration.location
                    and record.state is not api.ReplicaState.DELETED
                ),
                None,
            )
            if conflict is not None:
                raise api.StoragePreconditionFailed(
                    "Location already has a live Replica claim."
                )
            replica_id = api.ReplicaID(
                self._allocate_metadata_id_locked("replica")
            )
            record = api.ReplicaRecord(
                replica_id,
                declaration.digital_asset_id,
                declaration.location,
                declaration.mode,
                declaration.observation,
                revision=self._new_revision_locked(),
                placement_hints=declaration.placement_hints,
            )
            self._replicas[replica_id] = record
            self._replica_generation += 1
            return record

    def _find_replica_for_store(
        self,
        digital_asset_id: api.DigitalAssetID,
        store_ref: api.StoreUUID,
        mode: api.ReplicaMode,
    ) -> api.ReplicaRecord | None:
        """Find the first non-deleted matching Replica claim."""

        return next(
            (
                record
                for record in self.iter_replica_records(
                    digital_asset_id=digital_asset_id,
                    store_ref=store_ref,
                    mode=mode,
                )
                if record.state is not api.ReplicaState.DELETED
            ),
            None,
        )

    def _require_writable_destination(
        self,
        store_ref: api.StoreUUID,
        mode: api.ReplicaMode,
        *,
        expected_size: int | None = None,
    ) -> api.StoreAPI:
        """Require a configured, online Store supporting the Replica mode."""

        configuration = self.get_store_configuration(store_ref)
        if configuration.read_only:
            raise api.StoreReadOnly(configuration.store_name)
        if mode not in configuration.supported_replica_modes:
            raise api.StoreUnsupportedOperation(
                f"Store {configuration.store_name!r} does not support "
                f"{mode.value} Replicas."
            )
        store = self.get_store(store_ref)
        status = store.status()
        if not status.available:
            raise api.StoreUnavailable(configuration.store_name)
        if not status.writable:
            raise api.StoreReadOnly(configuration.store_name)
        if not store.capabilities.create:
            raise api.StoreUnsupportedOperation(
                f"Store {configuration.store_name!r} cannot create objects."
            )
        self._require_supported_object_size(store_ref, expected_size)
        return store

    def _require_supported_object_size(
        self,
        store_ref: api.StoreUUID,
        expected_size: int | None,
    ) -> None:
        """Reject a declared write that exceeds a Store's advertised limit."""

        if expected_size is None:
            return
        if expected_size < 0:
            raise ValueError("expected_size must not be negative.")
        characteristics = self.characteristics(store_ref)
        if characteristics.accepts_object_size(expected_size):
            return
        assert characteristics.max_object_bytes is not None
        raise api.StoreUnsupportedOperation(
            f"Store {store_ref} accepts objects up to "
            f"{characteristics.max_object_bytes} bytes; requested "
            f"{expected_size} bytes."
        )

    def _allocate_asset_location(
        self,
        store: api.StoreAPI,
        record: api.DigitalAssetRecord,
        *,
        placement_hints: api.StoragePlacementHints | None = None,
    ) -> api.Location:
        """Ask the Store to allocate a key, with an opaque portable fallback."""

        name_hint = record.metadata.original_name or record.metadata.name
        try:
            if placement_hints is None or not store.capabilities.placement_hints:
                return store.allocate_location(
                    expected_size=record.size_bytes,
                    expected_digest=self._preferred_digest(record),
                    name_hint=name_hint,
                )
            return store.allocate_location(
                expected_size=record.size_bytes,
                expected_digest=self._preferred_digest(record),
                name_hint=name_hint,
                placement_hints=placement_hints,
            )
        except api.StoreUnsupportedOperation:
            return store.location(uuid4().hex)

    # ------------------------------------------------------------------
    # Internal policy and provenance helpers
    # ------------------------------------------------------------------

    def _require_store_factory(self) -> StoreFactory:
        """Return the configured constructor or reject lifecycle mutation."""

        if self._store_factory is None:
            raise api.StoreUnsupportedOperation(
                "manager has no Store factory; attach a Store instance explicitly."
            )
        return self._store_factory

    def _validate_declared_policy_ids(
        self,
        replication_policy_id: api.ReplicationPolicyID | None,
        backup_policy_id: api.BackupPolicyID | None,
    ) -> None:
        """Require every supplied policy identifier to be registered."""

        if replication_policy_id is not None:
            self.get_replication_policy_record(replication_policy_id)
        if backup_policy_id is not None:
            self.get_backup_policy_record(backup_policy_id)

    def _validate_store_policy_references(
        self,
        configuration: api.StoreConfiguration,
    ) -> None:
        """Require a Store configuration's default policy references."""

        self._validate_declared_policy_ids(
            configuration.store_default_replication_policy_id,
            configuration.store_default_backup_policy_id,
        )

    def _placement_policy_ids(
        self,
        store_ref: api.StoreUUID,
    ) -> tuple[
        api.ReplicationPolicyID | None,
        api.BackupPolicyID | None,
    ]:
        """Return policy identifiers captured for a new placement."""

        configuration = self.get_store_configuration(store_ref)
        return (
            configuration.store_default_replication_policy_id,
            configuration.store_default_backup_policy_id,
        )

    def _capture_first_placement_policies(
        self,
        asset: api.DigitalAssetRecord,
        replication_policy_id: api.ReplicationPolicyID | None,
        backup_policy_id: api.BackupPolicyID | None,
    ) -> api.DigitalAssetRecord:
        """Capture Store defaults on a declared but not yet placed Asset."""

        with self._lock:
            has_replica = any(
                replica.digital_asset_id == asset.digital_asset_id
                and replica.state is not api.ReplicaState.DELETED
                for replica in self._replicas.values()
            )
            if has_replica:
                return asset
            effective_replication_id = (
                asset.replication_policy_id
                if asset.replication_policy_id is not None
                else replication_policy_id
            )
            effective_backup_id = (
                asset.backup_policy_id
                if asset.backup_policy_id is not None
                else backup_policy_id
            )
            if (
                effective_replication_id == asset.replication_policy_id
                and effective_backup_id == asset.backup_policy_id
            ):
                return asset
            return self.set_digital_asset_policies(
                asset.digital_asset_id,
                replication_policy_id=effective_replication_id,
                backup_policy_id=effective_backup_id,
                if_revision=asset.revision,
            )

    def _validate_all_recreation_policies(self) -> None:
        """Require every effective recreate-on-loss policy to remain safe."""

        for asset in tuple(self.iter_digital_asset_records()):
            policies = self.resolve_effective_policies(
                asset.digital_asset_id
            )
            if (
                policies.replication.loss_action
                is api.DigitalAssetLossAction.RECREATE
            ):
                self._validate_recreation_policy(
                    asset.digital_asset_id,
                    set(),
                )

    def _set_item_target(
        self,
        item_id: api.ItemID,
        role: str,
        kind: _ItemTargetKind,
        target_id: _ItemTargetID,
    ) -> None:
        """Set one well-formed Item role link in reference metadata."""

        if int(item_id) <= 0:
            raise ValueError("item_id must be positive.")
        if not role.strip():
            raise ValueError("role must not be empty.")
        with self._lock, self._metadata_transaction():
            self._item_targets[(item_id, role)] = (kind, target_id)

    def _asset_has_derivation_reference_locked(
        self,
        digital_asset_id: api.DigitalAssetID,
    ) -> bool:
        """Return whether an Asset participates in stored provenance."""

        for record in self._derivations.values():
            declaration = record.declaration
            if declaration.result_digital_asset_id == digital_asset_id:
                return True
            if any(
                source.digital_asset_id == digital_asset_id
                for source in declaration.sources
            ):
                return True
            recipe = declaration.recipe
            if recipe is None:
                continue
            if any(
                input_.digital_asset_id == digital_asset_id
                for input_ in recipe.inputs
            ):
                return True
            artifacts = (
                (() if recipe.executor is None else (recipe.executor,))
                + recipe.dependencies
            )
            if any(
                artifact.digital_asset_id == digital_asset_id
                for artifact in artifacts
            ):
                return True
        return False

    def _store_satisfies_policy(
        self,
        store_ref: api.StoreUUID,
        policy: api.ReplicationPolicy | api.BackupPolicy,
    ) -> bool:
        """Return whether Store tags and supported mode satisfy a policy."""

        try:
            configuration = self.get_store_configuration(store_ref)
        except api.StoreConfigurationNotFound:
            return False
        tags = set(configuration.store_tags)
        return (
            policy.mode in configuration.supported_replica_modes
            and policy.required_store_tags <= tags
            and not policy.forbidden_store_tags & tags
        )

    def _policy_bucket(
        self,
        store_ref: api.StoreUUID,
        dimension: api.ReplicaSeparationDimension,
    ) -> object:
        """Resolve one declared failure-domain bucket without inventing data."""

        configuration = self.get_store_configuration(store_ref)
        if dimension is api.ReplicaSeparationDimension.STORE:
            return configuration.store_uuid
        if dimension is api.ReplicaSeparationDimension.HOST:
            return configuration.store_host_uuid or ("unknown_host",)
        if dimension is api.ReplicaSeparationDimension.DEVICE:
            return configuration.store_device_uuid or ("unknown_device",)
        if dimension is api.ReplicaSeparationDimension.FAILURE_DOMAIN:
            return configuration.store_failure_domain or ("unknown_failure_domain",)
        return configuration.store_region or ("unknown_region",)

    def _separated_copy_capacity(
        self,
        records: Iterable[api.ReplicaRecord],
        policy: api.ReplicationPolicy | api.BackupPolicy,
    ) -> int:
        """Count policy-eligible copies after per-bucket copy limits."""

        records = tuple(records)
        if not records:
            return 0
        capacities = [len(records)]
        for dimension in policy.distinct_by:
            counts = Counter(
                self._policy_bucket(record.location.store_ref, dimension)
                for record in records
            )
            capacities.append(
                sum(
                    min(count, policy.max_copies_per_bucket)
                    for count in counts.values()
                )
            )
        return min(capacities)

    def _record_is_readable(self, record: api.ReplicaRecord) -> bool:
        """Return whether state and Store status currently permit reading."""

        if record.state not in {
            api.ReplicaState.PRESENT,
            api.ReplicaState.UNVERIFIED,
            api.ReplicaState.VERIFIED,
        }:
            return False
        try:
            if not self.status(record.location.store_ref).available:
                return False
            asset = self.get_digital_asset_record(record.digital_asset_id)
            return self.stat(record.location).size == asset.size_bytes
        except api.StorageError:
            return False

    def _assess_policy(
        self,
        digital_asset_id: api.DigitalAssetID,
        policy: api.ReplicationPolicy | api.BackupPolicy,
    ) -> api.StoragePolicyAssessment:
        """Assess one policy against eligible, separated Replica claims."""

        self.get_digital_asset_record(digital_asset_id)
        records = tuple(
            self.iter_replica_records(
                digital_asset_id=digital_asset_id,
                mode=policy.mode,
            )
        )
        present = tuple(
            record
            for record in records
            if self._record_is_readable(record)
        )
        healthy = tuple(
            record
            for record in present
            if record.state is api.ReplicaState.VERIFIED
            and self._store_satisfies_policy(record.location.store_ref, policy)
        )
        capacity = self._separated_copy_capacity(healthy, policy)
        errors: list[str] = []
        ineligible = tuple(
            record.replica_id
            for record in present
            if not self._store_satisfies_policy(record.location.store_ref, policy)
        )
        if ineligible:
            errors.append(
                "Replicas fail Store policy constraints: "
                + ", ".join(str(value) for value in ineligible)
            )
        return api.StoragePolicyAssessment(
            digital_asset_id,
            policy.name,
            policy.mode,
            present_replica_ids=tuple(record.replica_id for record in present),
            healthy_replica_ids=tuple(record.replica_id for record in healthy),
            meets_minimum=capacity >= policy.min_copies,
            meets_target=capacity >= policy.effective_target_copies,
            errors=tuple(errors),
        )

    def _plan_destination_stores(
        self,
        policy: api.ReplicationPolicy | api.BackupPolicy,
        existing: tuple[api.ReplicaRecord, ...],
        needed: int,
        *,
        expected_size: int | None = None,
    ) -> tuple[api.StoreUUID, ...]:
        """Select writable policy-compliant Stores without mutating state."""

        if needed <= 0:
            return ()
        occupied = {record.location.store_ref for record in existing}
        configurations = list(self.iter_store_configurations())
        configurations.sort(
            key=lambda configuration: (
                -len(
                    policy.preferred_store_tags
                    & set(configuration.store_tags)
                ),
                configuration.store_uuid != self._default_store_ref,
                configuration.store_name,
                str(configuration.store_uuid),
            )
        )
        selected_store_refs = [
            record.location.store_ref for record in existing
        ]
        selected_refs: list[api.StoreUUID] = []
        for configuration in configurations:
            store_ref = configuration.store_uuid
            if store_ref in occupied or not self._store_satisfies_policy(
                store_ref,
                policy,
            ):
                continue
            try:
                characteristics = self.characteristics(store_ref)
                if (
                    characteristics.recommended_write_usage
                    is api.StorageWriteUsage.ARCHIVAL_SNAPSHOT
                    and policy.mode is not api.ReplicaMode.ARCHIVE
                ):
                    continue
                self._require_writable_destination(
                    store_ref,
                    policy.mode,
                    expected_size=expected_size,
                )
            except api.StorageError:
                continue
            if any(
                sum(
                    self._policy_bucket(selected_store_ref, dimension)
                    == self._policy_bucket(store_ref, dimension)
                    for selected_store_ref in selected_store_refs
                )
                >= policy.max_copies_per_bucket
                for dimension in policy.distinct_by
            ):
                continue
            selected_refs.append(store_ref)
            occupied.add(store_ref)
            selected_store_refs.append(store_ref)
            if len(selected_refs) == needed:
                break
        return tuple(selected_refs)

    def _plan_recreation_branch(
        self,
        digital_asset_id: api.DigitalAssetID,
        *,
        visiting: frozenset[api.DigitalAssetID],
        memo: dict[api.DigitalAssetID, _RecreationBranch],
    ) -> _RecreationBranch:
        """Recursively select an exact route for one currently unavailable Asset."""

        if digital_asset_id in visiting:
            return _RecreationBranch(
                False,
                unavailable_digital_asset_ids=frozenset(
                    {digital_asset_id}
                ),
                warnings=(
                    f"recreation planning encountered a cycle at Asset "
                    f"{digital_asset_id}",
                ),
            )
        cached = memo.get(digital_asset_id)
        if cached is not None:
            return cached
        if self._asset_has_readable_replica(digital_asset_id):
            branch = _RecreationBranch(
                True,
                available_digital_asset_ids=frozenset(
                    {digital_asset_id}
                ),
            )
            memo[digital_asset_id] = branch
            return branch

        candidates = tuple(
            self.iter_digital_asset_derivation_records(
                result_digital_asset_id=digital_asset_id,
                exact_only=True,
            )
        )
        if not candidates:
            branch = _RecreationBranch(
                False,
                unavailable_digital_asset_ids=frozenset(
                    {digital_asset_id}
                ),
                warnings=(
                    f"Asset {digital_asset_id} has no complete exact "
                    "derivation recipe",
                ),
            )
            memo[digital_asset_id] = branch
            return branch

        next_visiting = visiting | {digital_asset_id}
        viable: list[
            tuple[api.DigitalAssetDerivationRecord, _RecreationBranch]
        ] = []
        unavailable_ids: set[api.DigitalAssetID] = {digital_asset_id}
        failed_warnings: list[str] = []
        for candidate in candidates:
            attempt = self._plan_recreation_derivation(
                candidate,
                visiting=next_visiting,
                memo=memo,
            )
            if attempt.viable:
                viable.append((candidate, attempt))
            else:
                unavailable_ids.update(
                    attempt.unavailable_digital_asset_ids
                )
                failed_warnings.extend(attempt.warnings)

        if not viable:
            branch = _RecreationBranch(
                False,
                unavailable_digital_asset_ids=frozenset(unavailable_ids),
                warnings=tuple(failed_warnings),
            )
            memo[digital_asset_id] = branch
            return branch

        viable.sort(
            key=lambda item: (
                len(item[1].steps),
                item[0].digital_asset_derivation_id,
            )
        )
        selected_record, selected = viable[0]
        alternatives = list(selected.alternative_derivation_ids)
        alternatives.extend(
            record.digital_asset_derivation_id
            for record, _ in viable[1:]
        )
        branch = dataclasses.replace(
            selected,
            selected_derivation_id=(
                selected_record.digital_asset_derivation_id
            ),
            alternative_derivation_ids=tuple(
                dict.fromkeys(alternatives)
            ),
            warnings=tuple(
                dict.fromkeys(selected.warnings + tuple(failed_warnings))
            ),
        )
        memo[digital_asset_id] = branch
        return branch

    def _plan_recreation_derivation(
        self,
        record: api.DigitalAssetDerivationRecord,
        *,
        visiting: frozenset[api.DigitalAssetID],
        memo: dict[api.DigitalAssetID, _RecreationBranch],
    ) -> _RecreationBranch:
        """Plan every managed prerequisite of one exact derivation recipe."""

        recipe = record.declaration.recipe
        if recipe is None or not record.can_recreate_exactly:
            return _RecreationBranch(
                False,
                warnings=(
                    f"derivation {record.digital_asset_derivation_id} is not "
                    "a complete exact recipe",
                ),
            )

        prerequisite_branches: list[_RecreationBranch] = []
        unavailable_ids: set[api.DigitalAssetID] = set()
        warnings: list[str] = []
        for source_id in sorted(
            self._source_asset_ids(
                record,
                include_recipe_artifacts=False,
            )
        ):
            source_branch = self._plan_recreation_branch(
                source_id,
                visiting=visiting,
                memo=memo,
            )
            if source_branch.viable:
                prerequisite_branches.append(source_branch)
            else:
                unavailable_ids.update(
                    source_branch.unavailable_digital_asset_ids
                )
                warnings.extend(source_branch.warnings)

        artifacts = (
            (() if recipe.executor is None else (recipe.executor,))
            + recipe.dependencies
        )
        for artifact in artifacts:
            managed_branch: _RecreationBranch | None = None
            if artifact.digital_asset_id is not None:
                managed_branch = self._plan_recreation_branch(
                    artifact.digital_asset_id,
                    visiting=visiting,
                    memo=memo,
                )
                if managed_branch.viable:
                    prerequisite_branches.append(managed_branch)
                    continue
            if self._external_recipe_artifact_is_available(artifact):
                if managed_branch is not None:
                    warnings.append(
                        f"derivation {record.digital_asset_derivation_id} "
                        f"will retrieve artefact {artifact.name!r} by URI "
                        "because its managed Asset is unavailable"
                    )
                continue
            if managed_branch is not None:
                unavailable_ids.update(
                    managed_branch.unavailable_digital_asset_ids
                )
                warnings.extend(managed_branch.warnings)
            warnings.append(
                f"derivation {record.digital_asset_derivation_id} requires "
                f"unavailable artefact {artifact.name!r}"
            )

        if unavailable_ids or any(
            "requires unavailable artefact" in warning
            for warning in warnings
        ):
            return _RecreationBranch(
                False,
                unavailable_digital_asset_ids=frozenset(unavailable_ids),
                warnings=tuple(dict.fromkeys(warnings)),
            )

        steps: list[api.DigitalAssetDerivationRecord] = []
        seen_steps: set[api.DigitalAssetDerivationID] = set()
        available_ids: set[api.DigitalAssetID] = set()
        alternatives: list[api.DigitalAssetDerivationID] = []
        for prerequisite in prerequisite_branches:
            available_ids.update(
                prerequisite.available_digital_asset_ids
            )
            warnings.extend(prerequisite.warnings)
            alternatives.extend(
                prerequisite.alternative_derivation_ids
            )
            for step in prerequisite.steps:
                if step.digital_asset_derivation_id in seen_steps:
                    continue
                steps.append(step)
                seen_steps.add(step.digital_asset_derivation_id)
        if record.digital_asset_derivation_id not in seen_steps:
            steps.append(record)

        return _RecreationBranch(
            True,
            steps=tuple(steps),
            available_digital_asset_ids=frozenset(available_ids),
            selected_derivation_id=record.digital_asset_derivation_id,
            alternative_derivation_ids=tuple(dict.fromkeys(alternatives)),
            warnings=tuple(dict.fromkeys(warnings)),
        )

    def _external_recipe_artifact_is_available(
        self,
        artifact: api.ReproductionRecipeArtifactReference,
    ) -> bool:
        """Check a pinned URI artefact through the configured resolver."""

        resolver = self._artifact_resolver
        if artifact.uri is None or resolver is None:
            return False
        try:
            return resolver.is_available(artifact)
        except Exception:
            return False

    def _source_asset_ids(
        self,
        record: api.DigitalAssetDerivationRecord,
        *,
        include_recipe_artifacts: bool = True,
    ) -> set[api.DigitalAssetID]:
        """Expand derivation sources and pinned recipe Assets."""

        source_ids: set[api.DigitalAssetID] = set()
        for source in record.declaration.sources:
            if source.digital_asset_id is not None:
                source_ids.add(source.digital_asset_id)
            elif source.composite_digital_asset_id is not None:
                composite = self.get_composite_digital_asset_record(
                    source.composite_digital_asset_id
                )
                source_ids.update(
                    member.digital_asset_id for member in composite.members
                )
        recipe = record.declaration.recipe
        if recipe is not None:
            source_ids.update(
                input_.digital_asset_id for input_ in recipe.inputs
            )
            if include_recipe_artifacts:
                artifacts = (
                    (() if recipe.executor is None else (recipe.executor,))
                    + recipe.dependencies
                )
                source_ids.update(
                    artifact.digital_asset_id
                    for artifact in artifacts
                    if artifact.digital_asset_id is not None
                )
        return source_ids

    def _reject_derivation_cycle(
        self,
        result_digital_asset_id: api.DigitalAssetID,
        source_asset_ids: set[api.DigitalAssetID],
    ) -> None:
        """Reject a new result-to-source edge that closes a provenance cycle."""

        adjacency: dict[api.DigitalAssetID, set[api.DigitalAssetID]] = {}
        for record in self.iter_digital_asset_derivation_records():
            adjacency.setdefault(
                record.declaration.result_digital_asset_id,
                set(),
            ).update(self._source_asset_ids(record))
        adjacency.setdefault(result_digital_asset_id, set()).update(
            source_asset_ids
        )

        def reaches_result(
            current: api.DigitalAssetID,
            visited: set[api.DigitalAssetID],
        ) -> bool:
            """Walk result-to-source edges looking for the proposed result."""

            if current == result_digital_asset_id:
                return True
            if current in visited:
                return False
            visited.add(current)
            return any(
                reaches_result(child, visited)
                for child in adjacency.get(current, ())
            )

        if any(reaches_result(source_id, set()) for source_id in source_asset_ids):
            raise api.StoragePreconditionFailed(
                "Digital Asset derivation would create a provenance cycle."
            )

    def _asset_has_readable_replica(
        self,
        digital_asset_id: api.DigitalAssetID,
    ) -> bool:
        """Return whether any mode contains a currently readable Replica."""

        return any(
            self._record_is_readable(record)
            for record in self.iter_replica_records(
                digital_asset_id=digital_asset_id
            )
        )

    def _asset_is_recoverable_now(
        self,
        digital_asset_id: api.DigitalAssetID,
        visiting: set[api.DigitalAssetID],
    ) -> bool:
        """Return whether bytes are readable or exactly recreatable now."""

        if self._asset_has_readable_replica(digital_asset_id):
            return True
        if digital_asset_id in visiting:
            return False
        return any(
            self._derivation_is_recoverable(
                derivation,
                visiting | {digital_asset_id},
            )
            for derivation in self.iter_digital_asset_derivation_records(
                result_digital_asset_id=digital_asset_id,
                exact_only=True,
            )
        )

    def _derivation_is_recoverable(
        self,
        record: api.DigitalAssetDerivationRecord,
        visiting: set[api.DigitalAssetID],
    ) -> bool:
        """Return whether an exact recipe and all pinned inputs are reachable."""

        if not record.can_recreate_exactly:
            return False
        if not self._recipe_artifacts_are_recoverable(
            record,
            visiting,
            for_policy=False,
        ):
            return False
        return all(
            self._asset_is_recoverable_now(source_id, visiting)
            for source_id in self._source_asset_ids(
                record,
                include_recipe_artifacts=False,
            )
        )

    def _asset_policy_recoverable(
        self,
        digital_asset_id: api.DigitalAssetID,
        visiting: set[api.DigitalAssetID],
    ) -> bool:
        """Return whether policy retains or exactly recreates one input Asset."""

        if digital_asset_id in visiting:
            return False
        policies = self.resolve_effective_policies(digital_asset_id)
        if (
            policies.replication.min_copies > 0
            or policies.backup.min_copies > 0
        ):
            return True
        if (
            policies.replication.loss_action
            is not api.DigitalAssetLossAction.RECREATE
        ):
            return False
        return any(
            self._recipe_artifacts_are_recoverable(
                record,
                visiting | {digital_asset_id},
                for_policy=True,
            )
            and all(
                self._asset_policy_recoverable(
                    source_id,
                    visiting | {digital_asset_id},
                )
                for source_id in self._source_asset_ids(
                    record,
                    include_recipe_artifacts=False,
                )
            )
            for record in self.iter_digital_asset_derivation_records(
                result_digital_asset_id=digital_asset_id,
                exact_only=True,
            )
        )

    def _validate_recreation_policy(
        self,
        digital_asset_id: api.DigitalAssetID,
        visiting: set[api.DigitalAssetID],
    ) -> None:
        """Require an exact recipe whose pinned inputs remain policy-recoverable."""

        candidates = tuple(
            self.iter_digital_asset_derivation_records(
                result_digital_asset_id=digital_asset_id,
                exact_only=True,
            )
        )
        if not candidates or not any(
            self._recipe_artifacts_are_recoverable(
                record,
                visiting | {digital_asset_id},
                for_policy=True,
            )
            and all(
                self._asset_policy_recoverable(
                    source_id,
                    visiting | {digital_asset_id},
                )
                for source_id in self._source_asset_ids(
                    record,
                    include_recipe_artifacts=False,
                )
            )
            for record in candidates
        ):
            raise api.StoragePolicyUnsatisfied(
                "recreate-on-loss requires an exact complete derivation whose "
                "pinned inputs and artefacts remain recoverable."
            )

    def _recipe_artifacts_are_recoverable(
        self,
        record: api.DigitalAssetDerivationRecord,
        visiting: set[api.DigitalAssetID],
        *,
        for_policy: bool,
    ) -> bool:
        """Require a managed or resolver-verified route for every artefact."""

        recipe = record.declaration.recipe
        if recipe is None:
            return False
        artifacts = (
            (() if recipe.executor is None else (recipe.executor,))
            + recipe.dependencies
        )
        for artifact in artifacts:
            managed_available = False
            if artifact.digital_asset_id is not None:
                if for_policy:
                    managed_available = self._asset_policy_recoverable(
                        artifact.digital_asset_id,
                        visiting,
                    )
                else:
                    managed_available = self._asset_is_recoverable_now(
                        artifact.digital_asset_id,
                        visiting,
                    )
            if managed_available:
                continue
            if artifact.uri is None:
                return False
            resolver = self._artifact_resolver
            if resolver is None:
                return False
            try:
                if not resolver.is_available(artifact):
                    return False
            except Exception:
                return False
        return True


class TransientStorageManager(_StorageManagerOrchestrator):
    """Disposable manager state for focused tests and one-shot work.

    Store publication is real, but manager-owned records disappear with the
    process. Applications should use the database-backed ``StorageManager``;
    this implementation is not a cache and does not participate in LiuXin's
    cache lifecycle.
    """


# Compatibility for callers written before the persistence boundary was made
# explicit. New code should prefer the honest ``TransientStorageManager`` name.
InMemoryStorageManager = TransientStorageManager


__all__ = [
    "InMemoryStorageManager",
    "TransientStorageManager",
]

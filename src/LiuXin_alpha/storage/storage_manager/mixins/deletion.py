"""Own managed byte deletion, committed reservations, and partial-outcome evidence."""

from __future__ import annotations

import dataclasses
from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import override

from LiuXin_alpha.storage.api import errors, models, persistence_api, store_api
from LiuXin_alpha.storage.api import storage_manager_api as api
from LiuXin_alpha.storage.storage_manager.mixins._state import _StorageManagerState


class StorageDeletionMixin(_StorageManagerState):
    """Apply one loss-policy decision to both manager deletion entry points.

    Database-backed reservations commit before external I/O. Ordinary metadata
    writers share their serialization boundary; nested caller transactions are
    refused for deletion. Transient managers coordinate only their own threads.
    Raw Store/driver operations remain privileged mechanisms outside this policy.
    """

    @contextmanager
    def _deletion_transaction(self) -> Generator[None, None, None]:
        """Commit a durable phase independently, or use transient metadata hooks."""
        factory: persistence_api.StorageUnitOfWorkFactoryAPI | None = getattr(
            self, "_metadata_unit_of_work_factory", None
        )
        if factory is None:
            with self._metadata_transaction():
                yield
        else:
            with factory.begin(serialized=True, require_outermost=True) as unit:
                yield
                unit.commit()

    def _require_deletion_survivors(
        self, targets: tuple[api.ReplicaRecord, ...], location: models.Location
    ) -> None:
        """Require fresh, distinct verified survivors and applicable policy minima."""
        blocked_locations = {
            record.location
            for record in self._replicas.values()
            if record.state is api.ReplicaState.DELETING
        }
        for target in targets:
            asset = self.get_digital_asset_record(target.digital_asset_id)
            survivors: dict[models.Location, api.ReplicaRecord] = {}
            for record in self.iter_replica_records(
                digital_asset_id=asset.digital_asset_id
            ):
                if (
                    record.location == location
                    or record.location in blocked_locations
                    or record.state is not api.ReplicaState.VERIFIED
                    or record.location in survivors
                ):
                    continue
                if self._inspect_replica(record, asset, calculate_digests=True).healthy:
                    survivors[record.location] = record
            if not survivors:
                raise api.StoragePolicyUnsatisfied(
                    "Deletion requires another distinct, freshly verified copy."
                )
            policies = self.resolve_effective_policies(asset.digital_asset_id)
            for policy in (policies.replication, policies.backup):
                # An unrelated pre-existing deficit is not made worse by this removal.
                if target.mode is not policy.mode:
                    continue
                eligible = tuple(
                    record
                    for record in survivors.values()
                    if record.mode is policy.mode
                    and self._store_satisfies_policy(record.location.store_ref, policy)
                )
                if self._separated_copy_capacity(eligible, policy) < policy.min_copies:
                    raise api.StoragePolicyUnsatisfied(
                        f"Deletion would violate {policy.mode.value} copy/separation minima."
                    )

    def _deletion_store(
        self, location: models.Location, if_version: str | None
    ) -> store_api.StoreAPI:
        """Reject configured read-only and unsupported operations before reserving."""
        store = self.get_store(location.store_ref)
        if (
            self.get_store_configuration(location.store_ref).read_only
            or store.configuration.read_only
        ):
            raise errors.StoreReadOnly("Store configuration forbids deletion.")
        if not store.capabilities.delete:
            raise errors.StoreUnsupportedOperation("Store does not support deletion.")
        if if_version is not None and not store.capabilities.conditional_delete:
            raise errors.StoreUnsupportedOperation(
                "Store does not support conditional deletion."
            )
        return store

    def _prepare_deletion(
        self,
        location: models.Location,
        *,
        missing_ok: bool,
        if_version: str | None,
        expected_replica_id: api.ReplicaID | None,
    ) -> tuple[
        store_api.StoreAPI,
        tuple[api.ReplicaRecord, ...],
        models.FileInfo | None,
        str | None,
    ]:
        """Check registered claims, policy and Store evidence inside the reserved SQL writer."""
        store = self._deletion_store(location, if_version)
        targets = tuple(
            record
            for record in self._replicas.values()
            if record.location == location
            and record.state is not api.ReplicaState.DELETED
        )
        if not targets:
            raise api.StoragePolicyUnsatisfied(
                "Managed deletion requires a live Replica claim; raw Store access is privileged."
            )
        if expected_replica_id is not None and not any(
            record.replica_id == expected_replica_id for record in targets
        ):
            raise errors.StoragePreconditionFailed("Replica changed before deletion.")
        for record in targets:
            if record.state is api.ReplicaState.DELETING:
                raise errors.StoragePreconditionFailed(
                    "Replica has a pending deletion reservation."
                )
            if record.mode in {
                api.ReplicaMode.UNMANAGED,
                api.ReplicaMode.ARCHIVE,
                api.ReplicaMode.BACKUP,
            }:
                raise api.StoragePolicyUnsatisfied(
                    f"Managed deletion protects {record.mode.value} copies."
                )
        try:
            info = store.stat(location)
        except errors.StoreNotFound:
            if not missing_ok:
                raise
            info = None
        if info is not None:
            if if_version is not None and info.version != if_version:
                raise errors.StoragePreconditionFailed(
                    "Object version changed before deletion."
                )
            self._require_deletion_survivors(targets, location)
        version = if_version
        if (
            version is None
            and info is not None
            and store.capabilities.conditional_delete
        ):
            version = info.version
        return store, targets, info, version

    def _delete_location(
        self,
        location: models.Location,
        *,
        missing_ok: bool,
        if_version: str | None,
        forget_replica_id: api.ReplicaID | None = None,
        expected_replica_id: api.ReplicaID | None = None,
    ) -> bool:
        """Reserve claims, remove bytes, then finalize metadata with revision checks."""
        reserved: list[api.ReplicaRecord] = []
        phase = "reservation"
        started = False
        bytes_deleted: bool | None = False
        try:
            with self._lock, self._deletion_transaction():
                store, targets, info, version = self._prepare_deletion(
                    location,
                    missing_ok=missing_ok,
                    if_version=if_version,
                    expected_replica_id=expected_replica_id,
                )
                started = True
                for record in targets:
                    reservation = dataclasses.replace(
                        record,
                        observation=api.ReplicaObservation(
                            api.ReplicaState.DELETING,
                            checked_at=datetime.now(UTC),
                            failure_reason="Deletion reserved; outcome awaits final metadata.",
                        ),
                        revision=self._new_revision_locked(),
                    )
                    reserved.append(reservation)
                    self._replicas[record.replica_id] = reservation
                self._replica_generation += 1

            phase = "bytes"
            if info is not None:
                bytes_deleted = None
                store.delete(location, missing_ok=missing_ok, if_version=version)
                bytes_deleted = True

            phase = "metadata"
            with self._lock, self._deletion_transaction():
                for record in reserved:
                    current = self._require_replica_locked(record.replica_id)
                    self._check_revision(current.revision, record.revision)
                    if current.state is not api.ReplicaState.DELETING:
                        raise errors.StoragePreconditionFailed(
                            "Deletion reservation changed."
                        )
                for record in reserved:
                    if record.replica_id == forget_replica_id:
                        del self._replicas[record.replica_id]
                    else:
                        self._replicas[record.replica_id] = dataclasses.replace(
                            record,
                            observation=api.ReplicaObservation(
                                api.ReplicaState.DELETED, checked_at=datetime.now(UTC)
                            ),
                            revision=self._new_revision_locked(),
                        )
                self._replica_generation += 1
        except Exception as error:
            if not started:
                raise
            recovery = api.ReplicaDeletionRecovery(
                location,
                tuple((record.replica_id, record.revision) for record in reserved),
                phase,
                bytes_deleted,
            )
            raise api.StorageDeletionError(
                f"Deletion incomplete during {phase}; inspect reservation and bytes before repair: {error}",
                recovery,
            ) from error
        return bool(bytes_deleted)

    @override
    def delete(
        self,
        location: models.Location,
        *,
        missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        """Delete registered bytes under loss policy and retain honest tombstones.

        Unknown claims, protected modes, pending reservations, last verified
        copies and violated minima fail before mutation. Failures after reserving
        raise StorageDeletionError with phase and revision evidence. Raw Store
        deletion is a privileged escape from manager policy.

        Example:
            >>> manager.delete(redundant.location)  # doctest: +SKIP

        :param location: Registered object whose live claims share this deletion.
        :param missing_ok: Accept absent registered bytes while finalizing metadata.
        :param if_version: Optional Store-supported object version precondition.
        :return: None after successful byte and metadata completion.
        """
        self._delete_location(location, missing_ok=missing_ok, if_version=if_version)

    @override
    def remove_replica(
        self,
        replica_id: api.ReplicaID,
        *,
        delete_bytes: bool = True,
        retain_tombstone: bool = True,
    ) -> api.ReplicaRemovalReport:
        """Remove a claim, enforcing shared deletion policy when erasing bytes.

        Metadata-only removal preserves physical bytes and refuses active
        reservations. A byte deletion commits intent first and can leave a
        recoverable partial outcome; SQL rollback cannot restore deleted bytes.

        Example:
            >>> manager.remove_replica(replica_id, delete_bytes=False)  # doctest: +SKIP

        :param replica_id: Registered claim to remove.
        :param delete_bytes: Enforce deletion policy and erase bytes when True.
        :param retain_tombstone: Keep a DELETED claim rather than forgetting it.
        :return: Completed removal flags, or a typed failure with recovery evidence.
        """
        if delete_bytes:
            record = self.get_replica_record(replica_id)
            deleted = self._delete_location(
                record.location,
                missing_ok=True,
                if_version=None,
                forget_replica_id=None if retain_tombstone else replica_id,
                expected_replica_id=replica_id,
            )
            return api.ReplicaRemovalReport(
                replica_id, deleted, not retain_tombstone, retain_tombstone
            )
        with self._lock, self._metadata_transaction():
            record = self._require_replica_locked(replica_id)
            if record.state is api.ReplicaState.DELETING:
                raise errors.StoragePreconditionFailed(
                    "Replica has a pending deletion reservation."
                )
            if retain_tombstone:
                self._replicas[replica_id] = dataclasses.replace(
                    record,
                    observation=api.ReplicaObservation(
                        api.ReplicaState.DELETED, checked_at=datetime.now(UTC)
                    ),
                    revision=self._new_revision_locked(),
                )
            else:
                del self._replicas[replica_id]
            self._replica_generation += 1
        warnings = (
            ("tombstone retained while physical bytes were preserved",)
            if retain_tombstone
            else ()
        )
        return api.ReplicaRemovalReport(
            replica_id, False, not retain_tombstone, retain_tombstone, warnings
        )


__all__ = ["StorageDeletionMixin"]

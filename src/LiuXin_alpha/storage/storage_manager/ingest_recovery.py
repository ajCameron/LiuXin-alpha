"""Recover and retry durable ingest operations from journal evidence.

The service owns recovery control flow while its host owns manager state, Store
I/O, metadata operations, and public issue reporting.
"""

from __future__ import annotations

from collections.abc import MutableMapping
from datetime import UTC, datetime
from typing import Any, Protocol
from uuid import UUID

from LiuXin_alpha.storage.api import errors as storage_errors
from LiuXin_alpha.storage.api import models as storage_models
from LiuXin_alpha.storage.api import storage_manager_api as manager_api
from LiuXin_alpha.storage.api import store_api
from LiuXin_alpha.storage.storage_manager.database_repository import (
    DatabaseStorageMetadataRepository,
)
from LiuXin_alpha.storage.storage_manager.mixins._types import (
    _AdoptIngestRequest,
    _IdentifiedStreamIngestRequest,
    _IngestOperation,
    _StoreObjectIngestRequest,
    _StreamIngestRequest,
)


class IngestRecoveryHost(Protocol):
    """Declare manager operations used by durable recovery.

    Example:
        >>> service = IngestRecoveryService(manager, repository)  # doctest: +SKIP
    """

    _lock: Any
    _replicas: MutableMapping[manager_api.ReplicaID, manager_api.ReplicaRecord]
    _ingest_operations: MutableMapping[UUID, _IngestOperation]
    ingest_recovery_issues: tuple[str, ...]

    def get_digital_asset_record(
        self, asset_id: manager_api.DigitalAssetID
    ) -> manager_api.DigitalAssetRecord:
        """Load registered Asset facts for recovery validation.

        :param asset_id: Registered Asset identity.
        :return: Current Asset record.
        """
        ...

    def stat(self, location: storage_models.Location) -> Any:
        """Inspect the currently published Store object.

        :param location: Routed object Location.
        :return: Current Store object information.
        """
        ...

    def _calculate_location_digests(
        self, location: storage_models.Location, algorithms: tuple[str, ...]
    ) -> tuple[storage_models.Digest, ...]:
        """Hash one Location using the requested algorithms."""
        ...

    def _require_same_identity(
        self,
        asset: manager_api.DigitalAssetRecord,
        size: int,
        digests: tuple[storage_models.Digest, ...],
    ) -> None:
        """Reject observed evidence that differs from registered Asset identity."""
        ...

    def _add_replica(
        self, declaration: manager_api.ReplicaDeclaration
    ) -> manager_api.ReplicaRecord:
        """Persist one recovered Replica declaration."""
        ...

    def link_item_to_digital_asset(
        self,
        item_id: manager_api.ItemID,
        asset_id: manager_api.DigitalAssetID,
        *,
        role: str,
    ) -> None:
        """Restore an optional Item association after publication recovery.

        :param item_id: Item identity retained in the ingest request.
        :param asset_id: Recovered Asset identity.
        :param role: Exact association role.
        :return: None after persistence.
        """
        ...

    def recover_pending_ingests(
        self, operation_id: UUID | None = None
    ) -> tuple[str, ...]:
        """Invoke host-level recovery for one operation or the pending set.

        :param operation_id: Optional exact operation UUID.
        :return: Current recovery issue messages.
        """
        ...

    def adopt_location(
        self, location: storage_models.Location, **kwargs: Any
    ) -> manager_api.DigitalAssetIngestResult:
        """Replay an adoption request from durable request evidence.

        :param location: Existing object Location to adopt.
        :param kwargs: Retained ingest request controls.
        :return: Completed ingest result.
        """
        ...

    def get_store(self, store_ref: storage_models.StoreUUID) -> store_api.StoreAPI:
        """Resolve the source Store used by a replayable object request.

        :param store_ref: Stable source Store UUID.
        :return: Attached Store facade.
        """
        ...

    def ingest_store_object(
        self, store: store_api.StoreAPI, info: Any, **kwargs: Any
    ) -> manager_api.DigitalAssetIngestResult:
        """Replay ingestion of a version-checked Store object.

        :param store: Attached source Store facade.
        :param info: Fresh source object information.
        :param kwargs: Retained ingest request controls.
        :return: Completed ingest result.
        """
        ...


class IngestRecoveryService:
    """Reconcile published bytes and replay durable source requests.

    Example:
        >>> issues = IngestRecoveryService(manager, repository).recover()  # doctest: +SKIP
    """

    def __init__(
        self,
        host: IngestRecoveryHost,
        repository: DatabaseStorageMetadataRepository,
    ) -> None:
        """Retain the manager host and its matching journal repository.

        Example:
            >>> service = IngestRecoveryService(manager, repository)  # doctest: +SKIP
        """
        self.host = host
        self.repository = repository

    def recover(self, operation_id: UUID | None = None) -> tuple[str, ...]:
        """Recover pending publications and replace the host's issue snapshot.

        Example:
            >>> service.recover()  # doctest: +SKIP

        :param operation_id: Optional exact journal UUID; None processes every pending entry.
        :return: Current recovery issue messages, also installed on the host.
        """
        repository = self.repository
        issues: list[str] = []
        pending = list(repository.pending_ingests())
        if operation_id is not None:
            pending = [entry for entry in pending if entry[0] == operation_id]
            if not pending:
                entry = repository.ingest_journal_entry(operation_id)
                if entry is None:
                    issue = f"{operation_id}: ingest operation is not journalled"
                    self.host.ingest_recovery_issues = (issue,)
                    return self.host.ingest_recovery_issues
                if entry[0] == "failed":
                    pending = [(operation_id, entry[0], entry[1])]
        for current_operation_id, state, payload in pending:
            request = payload.get("request")
            if not isinstance(
                request,
                (
                    _StreamIngestRequest,
                    _IdentifiedStreamIngestRequest,
                    _StoreObjectIngestRequest,
                    _AdoptIngestRequest,
                ),
            ):
                error = manager_api.StorageManagementError(
                    "publication journal has an invalid ingest request."
                )
                repository.journal_failed(current_operation_id, error)
                issues.append(
                    f"{current_operation_id}: publication recovery failed: {error}"
                )
                continue
            if state == "started":
                error = manager_api.StorageManagementError(
                    "process stopped before Store publication began."
                )
                repository.journal_failed(
                    current_operation_id,
                    error,
                )
                issues.append(
                    f"{current_operation_id}: publication recovery failed: {error}"
                )
                continue
            asset_value = payload.get("asset_record")
            location = payload.get("location")
            mode = payload.get("replica_mode")
            if not isinstance(
                asset_value, manager_api.DigitalAssetRecord
            ) or not isinstance(location, storage_models.Location):
                repository.journal_failed(
                    current_operation_id,
                    manager_api.StorageManagementError(
                        "publication journal has incomplete recovery metadata."
                    ),
                )
                continue
            try:
                asset_record = self.host.get_digital_asset_record(
                    asset_value.digital_asset_id
                )
                info = self.host.stat(location)
                if info.size is None:
                    raise storage_errors.StorageIntegrityError(
                        "recovery cannot verify a publication with unknown size."
                    )
                observed = self.host._calculate_location_digests(
                    location,
                    tuple(digest.algorithm for digest in asset_record.digests),
                )
                self.host._require_same_identity(asset_record, info.size, observed)
                with self.host._lock:
                    existing = next(
                        (
                            record
                            for record in self.host._replicas.values()
                            if record.location == location
                            and record.state is not manager_api.ReplicaState.DELETED
                        ),
                        None,
                    )
                if (
                    existing is not None
                    and existing.digital_asset_id != asset_record.digital_asset_id
                ):
                    raise storage_errors.StoragePreconditionFailed(
                        "journalled Location is claimed by another Digital Asset."
                    )
                replica_created = existing is None
                replica_record = existing or self.host._add_replica(
                    manager_api.ReplicaDeclaration(
                        asset_record.digital_asset_id,
                        location,
                        (
                            mode
                            if isinstance(mode, manager_api.ReplicaMode)
                            else manager_api.ReplicaMode.ACTIVE
                        ),
                        manager_api.ReplicaObservation(
                            manager_api.ReplicaState.VERIFIED,
                            observed_size_bytes=info.size,
                            observed_digests=observed,
                            checked_at=datetime.now(UTC),
                        ),
                        placement_hints=payload.get("placement_hints"),
                    )
                )
                result = manager_api.DigitalAssetIngestResult(
                    current_operation_id,
                    asset_record,
                    replica_record,
                    bool(payload.get("asset_created")),
                    replica_created,
                    deduplicated=not bool(payload.get("asset_created")),
                    verified=True,
                )
                item_id = getattr(request, "item_id", None)
                if item_id is not None:
                    self.host.link_item_to_digital_asset(
                        item_id,
                        asset_record.digital_asset_id,
                        role=(getattr(request, "role", None) or "primary_payload"),
                    )
                with self.host._lock:
                    self.host._ingest_operations[current_operation_id] = (
                        _IngestOperation(request, result)
                    )
            except (
                storage_errors.StoreUnavailable,
                manager_api.StoreConfigurationNotFound,
                storage_errors.StorageTimeout,
            ) as error:
                issues.append(
                    f"{current_operation_id}: publication remains pending: "
                    f"{str(error) or type(error).__name__}"
                )
            except (
                storage_errors.StorageNotFound,
                storage_errors.StorageIntegrityError,
                storage_errors.StoragePreconditionFailed,
            ) as error:
                repository.journal_failed(current_operation_id, error)
                issues.append(
                    f"{current_operation_id}: publication recovery failed: "
                    f"{str(error) or type(error).__name__}"
                )
            except Exception as error:
                issues.append(
                    f"{current_operation_id}: publication recovery deferred: "
                    f"{str(error) or type(error).__name__}"
                )
        self.host.ingest_recovery_issues = tuple(issues)
        return self.host.ingest_recovery_issues

    def retry(self, operation_id: UUID) -> manager_api.DigitalAssetIngestResult:
        """Return, recover, or replay one durable ingest operation.

        Example:
            >>> result = service.retry(operation_id)  # doctest: +SKIP

        :param operation_id: Exact durable ingest UUID to return, recover, or replay.
        :return: Existing, recovered, or replayed ingest result.
        """
        repository = self.repository
        existing = self.host._ingest_operations.get(operation_id)
        if existing is not None:
            return existing.result
        entry = repository.ingest_journal_entry(operation_id)
        if entry is None:
            raise storage_errors.StoragePreconditionFailed(
                f"ingest operation {operation_id} is not journalled."
            )
        state, payload, last_error = entry
        request = payload.get("request")
        if state in {"publishing", "published"} or isinstance(
            payload.get("location"), storage_models.Location
        ):
            self.host.recover_pending_ingests(operation_id)
            recovered = self.host._ingest_operations.get(operation_id)
            if recovered is not None:
                return recovered.result
            refreshed = repository.ingest_journal_entry(operation_id)
            detail = (
                last_error
                if refreshed is None or refreshed[2] in (None, "")
                else refreshed[2]
            )
            raise storage_errors.StoragePreconditionFailed(
                "journalled publication could not be recovered{}".format(
                    "." if not detail else f": {detail}"
                )
            )
        if isinstance(request, _AdoptIngestRequest):
            return self.host.adopt_location(
                request.location,
                operation_id=operation_id,
                digital_asset_id=request.digital_asset_id,
                item_id=request.item_id,
                role=request.role,
                metadata=request.metadata,
                replica_mode=request.replica_mode,
                verify=request.verify,
            )
        if isinstance(request, _StoreObjectIngestRequest):
            source = self.host.get_store(request.source_location.store_ref)
            info = source.stat(request.source_location)
            if (
                request.source_version is not None
                and info.version != request.source_version
            ):
                raise storage_errors.StoragePreconditionFailed(
                    "ingest source changed after the original operation."
                )
            return self.host.ingest_store_object(
                source,
                info,
                operation_id=operation_id,
                item_id=request.item_id,
                role=request.role,
                metadata=request.metadata,
                placement_hints=request.placement_hints,
                preferred_store_ref=request.preferred_store_ref,
                replica_mode=request.replica_mode,
                verify=request.verify,
            )
        request_kind = type(request).__name__ if request is not None else "unknown"
        raise storage_errors.StoragePreconditionFailed(
            f"ingest operation {operation_id} used a non-replayable {request_kind} source; retry it "
            "through the original caller with the same operation UUID and "
            "source bytes."
        )


__all__ = ["IngestRecoveryHost", "IngestRecoveryService"]

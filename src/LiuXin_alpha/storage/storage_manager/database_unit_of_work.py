"""Concrete storage persistence SPI over the portable database repository."""

from __future__ import annotations

import dataclasses

from types import TracebackType
from uuid import uuid4

import LiuXin_alpha.storage.api as api

from LiuXin_alpha.storage.storage_manager.database_repository import (
    DatabaseStorageMetadataRepository,
    RepositoryRecordMapping,
)


def _revision() -> str:
    return f"d-{uuid4().hex}"


def _check_revision(current: str | None, expected: str | None) -> None:
    if expected is not None and current != expected:
        raise api.StoragePreconditionFailed(
            f"revision precondition failed: expected {expected!r}, "
            f"found {current!r}."
        )


class DatabaseDigitalAssetRepository(api.DigitalAssetRepositoryAPI):
    def __init__(self, repository: DatabaseStorageMetadataRepository) -> None:
        self._repository = repository

    def add(
        self,
        declaration: api.DigitalAssetDeclaration,
    ) -> api.DigitalAssetRecord:
        identifier = api.DigitalAssetID(
            self._repository.allocate_record_id("digital_asset")
        )
        record = api.DigitalAssetRecord(
            identifier,
            declaration.size_bytes,
            declaration.digests,
            declaration.metadata,
            declaration.replication_policy_id,
            declaration.backup_policy_id,
            _revision(),
        )
        self._repository.upsert_asset(record)
        return record

    def get(self, digital_asset_id):
        try:
            return self._repository.get_asset(digital_asset_id)
        except KeyError as error:
            raise api.DigitalAssetNotFound(
                f"Digital Asset {digital_asset_id} is not registered."
            ) from error

    def replace_metadata(
        self,
        digital_asset_id,
        metadata,
        *,
        if_revision=None,
    ):
        current = self.get(digital_asset_id)
        _check_revision(current.revision, if_revision)
        updated = dataclasses.replace(
            current,
            metadata=metadata,
            revision=_revision(),
        )
        self._repository.upsert_asset(updated)
        return updated

    def find_by_digest(self, digest, *, size_bytes=None):
        for record in self.iter_assets():
            if size_bytes is not None and record.size_bytes != size_bytes:
                continue
            if digest in record.digests:
                return record
        return None

    def iter_assets(self):
        records = self._repository._load_assets()
        return iter(records[key] for key in sorted(records))

    def remove(self, digital_asset_id, *, if_revision=None):
        current = self.get(digital_asset_id)
        _check_revision(current.revision, if_revision)
        self._repository.remove_asset(digital_asset_id)
        return True

    def upsert_record(self, record: api.DigitalAssetRecord) -> None:
        self._repository.upsert_asset(record)


class DatabaseReplicaRepository(api.ReplicaRepositoryAPI):
    def __init__(self, repository: DatabaseStorageMetadataRepository) -> None:
        self._repository = repository

    def add(self, declaration):
        identifier = api.ReplicaID(
            self._repository.allocate_record_id("replica")
        )
        record = api.ReplicaRecord(
            identifier,
            declaration.digital_asset_id,
            declaration.location,
            declaration.mode,
            declaration.observation,
            _revision(),
            declaration.placement_hints,
        )
        self._repository.upsert_replica(record)
        return record

    def get(self, replica_id):
        try:
            return self._repository.get_replica(replica_id)
        except KeyError as error:
            raise api.ReplicaNotFound(
                f"Replica {replica_id} is not registered."
            ) from error

    def update_observation(
        self,
        replica_id,
        observation,
        *,
        if_revision=None,
    ):
        current = self.get(replica_id)
        _check_revision(current.revision, if_revision)
        updated = dataclasses.replace(
            current,
            observation=observation,
            revision=_revision(),
        )
        self._repository.upsert_replica(updated)
        return updated

    def iter_replicas(
        self,
        *,
        digital_asset_id=None,
        store_ref=None,
        mode=None,
    ):
        records = self._repository._load_replicas()
        return iter(
            record
            for key in sorted(records)
            for record in (records[key],)
            if (
                digital_asset_id is None
                or record.digital_asset_id == digital_asset_id
            )
            and (store_ref is None or record.location.store_ref == store_ref)
            and (mode is None or record.mode is mode)
        )

    def remove(
        self,
        replica_id,
        *,
        retain_tombstone=True,
        if_revision=None,
    ):
        current = self.get(replica_id)
        _check_revision(current.revision, if_revision)
        if retain_tombstone:
            self._repository.upsert_replica(
                dataclasses.replace(
                    current,
                    observation=api.ReplicaObservation(
                        api.ReplicaState.DELETED,
                        checked_at=current.observation.checked_at,
                    ),
                    revision=_revision(),
                )
            )
        else:
            self._repository.remove_replica(replica_id)
        return True

    def upsert_record(self, record: api.ReplicaRecord) -> None:
        self._repository.upsert_replica(record)


class DatabaseCompositeRepository(api.CompositeDigitalAssetRepositoryAPI):
    def __init__(self, repository: DatabaseStorageMetadataRepository) -> None:
        self._repository = repository

    def add(self, declaration):
        identifier = api.CompositeDigitalAssetID(
            self._repository.allocate_record_id("composite")
        )
        record = api.CompositeDigitalAssetRecord(
            identifier,
            declaration.members,
            declaration.name,
            declaration.attributes,
            _revision(),
        )
        self._repository.upsert_composite(record)
        return record

    def get(self, composite_digital_asset_id):
        try:
            return self._repository.get_composite(composite_digital_asset_id)
        except KeyError as error:
            raise api.CompositeDigitalAssetNotFound(
                "Composite Digital Asset "
                f"{composite_digital_asset_id} is not registered."
            ) from error

    def replace(
        self,
        composite_digital_asset_id,
        declaration,
        *,
        if_revision=None,
    ):
        current = self.get(composite_digital_asset_id)
        _check_revision(current.revision, if_revision)
        record = api.CompositeDigitalAssetRecord(
            composite_digital_asset_id,
            declaration.members,
            declaration.name,
            declaration.attributes,
            _revision(),
        )
        self._repository.upsert_composite(record)
        return record

    def iter_composites(self):
        records = self._repository._load_composites()
        return iter(records[key] for key in sorted(records))

    def remove(self, composite_digital_asset_id, *, if_revision=None):
        current = self.get(composite_digital_asset_id)
        _check_revision(current.revision, if_revision)
        self._repository.remove_composite(composite_digital_asset_id)
        return True

    def upsert_record(self, record: api.CompositeDigitalAssetRecord) -> None:
        self._repository.upsert_composite(record)


class DatabaseDerivationRepository(api.DigitalAssetDerivationRepositoryAPI):
    def __init__(self, repository: DatabaseStorageMetadataRepository) -> None:
        self._repository = repository

    def add(self, declaration):
        identifier = api.DigitalAssetDerivationID(
            self._repository.allocate_record_id("derivation")
        )
        record = api.DigitalAssetDerivationRecord(
            identifier,
            declaration,
            _revision(),
        )
        self._repository.upsert_derivation(record)
        return record

    def get(self, digital_asset_derivation_id):
        try:
            return self._repository.get_derivation(
                digital_asset_derivation_id
            )
        except KeyError as error:
            raise api.DigitalAssetDerivationNotFound(
                f"Derivation {digital_asset_derivation_id} is not registered."
            ) from error

    def iter_derivations(
        self,
        *,
        result_digital_asset_id=None,
        source_digital_asset_id=None,
        source_composite_digital_asset_id=None,
        workflow_id=None,
        workflow_reference=None,
        exact_only=False,
    ):
        records = self._repository._load_derivations()
        return iter(
            record
            for key in sorted(records)
            for record in (records[key],)
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

    def remove(self, digital_asset_derivation_id, *, if_revision=None):
        current = self.get(digital_asset_derivation_id)
        _check_revision(current.revision, if_revision)
        self._repository.remove_derivation(digital_asset_derivation_id)
        return True

    def upsert_record(self, record: api.DigitalAssetDerivationRecord) -> None:
        self._repository.upsert_derivation(record)


class _RollbackRequested(Exception):
    pass


class DatabaseStorageUnitOfWork(api.StorageUnitOfWorkAPI):
    """Explicit commit/rollback boundary over portable database macros."""

    def __init__(self, factory: DatabaseStorageUnitOfWorkFactory) -> None:
        self._factory = factory
        self._transaction = None
        self._entered = False
        self._commit_requested = False
        self._rollback_requested = False

    @property
    def assets(self):
        return self._factory.assets

    @property
    def replicas(self):
        return self._factory.replicas

    @property
    def composites(self):
        return self._factory.composites

    @property
    def derivations(self):
        return self._factory.derivations

    def commit(self) -> None:
        if not self._entered:
            raise RuntimeError("unit of work is not active.")
        if self._rollback_requested:
            raise RuntimeError("unit of work was already marked for rollback.")
        self._commit_requested = True

    def rollback(self) -> None:
        if not self._entered:
            raise RuntimeError("unit of work is not active.")
        self._rollback_requested = True
        self._commit_requested = False

    def __enter__(self):
        if self._entered:
            raise RuntimeError("unit of work cannot be entered twice.")
        self._transaction = self._factory.repository.transaction()
        self._transaction.__enter__()
        self._entered = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        assert self._transaction is not None
        try:
            if exc_type is not None:
                self._transaction.__exit__(exc_type, exc, traceback)
            elif self._commit_requested and not self._rollback_requested:
                self._transaction.__exit__(None, None, None)
            else:
                marker = _RollbackRequested()
                self._transaction.__exit__(
                    _RollbackRequested,
                    marker,
                    None,
                )
        finally:
            self._entered = False


class DatabaseStorageUnitOfWorkFactory(api.StorageUnitOfWorkFactoryAPI):
    """Factory and stable repository-port owner for one database binding."""

    def __init__(self, repository: DatabaseStorageMetadataRepository) -> None:
        self.repository = repository
        self.assets = DatabaseDigitalAssetRepository(repository)
        self.replicas = DatabaseReplicaRepository(repository)
        self.composites = DatabaseCompositeRepository(repository)
        self.derivations = DatabaseDerivationRepository(repository)

    def begin(self) -> DatabaseStorageUnitOfWork:
        return DatabaseStorageUnitOfWork(self)

    def asset_mapping(self):
        return RepositoryRecordMapping(
            get_one=self.repository.get_asset,
            load_all=self.repository._load_assets,
            upsert=self.assets.upsert_record,
            remove=self.repository.remove_asset,
            key_of=lambda record: record.digital_asset_id,
        )

    def replica_mapping(self):
        return RepositoryRecordMapping(
            get_one=self.repository.get_replica,
            load_all=self.repository._load_replicas,
            upsert=self.replicas.upsert_record,
            remove=self.repository.remove_replica,
            key_of=lambda record: record.replica_id,
        )

    def composite_mapping(self):
        return RepositoryRecordMapping(
            get_one=self.repository.get_composite,
            load_all=self.repository._load_composites,
            upsert=self.composites.upsert_record,
            remove=self.repository.remove_composite,
            key_of=lambda record: record.composite_digital_asset_id,
        )

    def derivation_mapping(self):
        return RepositoryRecordMapping(
            get_one=self.repository.get_derivation,
            load_all=self.repository._load_derivations,
            upsert=self.derivations.upsert_record,
            remove=self.repository.remove_derivation,
            key_of=lambda record: record.digital_asset_derivation_id,
        )


__all__ = [
    "DatabaseCompositeRepository",
    "DatabaseDerivationRepository",
    "DatabaseDigitalAssetRepository",
    "DatabaseReplicaRepository",
    "DatabaseStorageUnitOfWork",
    "DatabaseStorageUnitOfWorkFactory",
]

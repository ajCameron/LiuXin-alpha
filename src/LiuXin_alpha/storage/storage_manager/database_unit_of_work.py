"""
Coordinate explicit database-backed storage metadata units of work.

Domain adapters live in ``database_domain_repositories``; this module owns only
transaction lifecycle, factory composition, and compatibility mapping creation.

The factory shares four repository adapters across fresh inactive units. Enter
opens the underlying context; commit/rollback request its exit outcome rather
than immediately completing the database transaction. Compatibility mappings
retain the lower record operations for repository-neutral manager orchestration.
"""

from __future__ import annotations

from contextlib import AbstractContextManager
from types import TracebackType
from typing import Any

from LiuXin_alpha.storage.api import persistence_api
from LiuXin_alpha.storage.api import storage_manager_api as manager_api
from LiuXin_alpha.storage.storage_manager.database_domain_repositories import (
    DatabaseCompositeRepository,
    DatabaseDerivationRepository,
    DatabaseDigitalAssetRepository,
    DatabaseReplicaRepository,
)
from LiuXin_alpha.storage.storage_manager.database_mappings import (
    RepositoryRecordMapping,
)
from LiuXin_alpha.storage.storage_manager.database_repository import (
    DatabaseStorageMetadataRepository,
)


class _RollbackRequested(Exception):
    """
    Carry an internal exception marker to the underlying transaction when normal context exit must
    roll back. The unit-of-work wrapper constructs and passes this object to __exit__; it does not
    raise the marker directly or expose it as a workflow failure.

    Example:
        >>> marker = _RollbackRequested()
        >>> isinstance(marker, Exception)
        True
    """


class DatabaseStorageUnitOfWork(persistence_api.StorageUnitOfWorkAPI):
    """
    Request commit or rollback through an explicitly entered macro transaction.

    begin creates this object inactive. Enter opens the transaction; commit and rollback only set
    flags controlling exit. A normal exit without commit passes a rollback marker, and any body
    exception takes precedence over commit intent. Repository properties return stable factory-owned
    ports without checking entry.

    Use a fresh unit per operation: leaving clears the entered flag but does not reset
    commit/rollback intent or discard the transaction reference. Re-entry after exit is possible and
    reuses those intent flags. No independent lock, connection, repository isolation, or physical
    Store rollback is provided.

    Example:
        >>> with factory.begin() as unit:  # doctest: +SKIP
        ...     record = unit.assets.add(size_bytes, digests)
        ...     unit.commit()
    """

    def __init__(self, factory: DatabaseStorageUnitOfWorkFactory) -> None:
        """
        Retain the factory and initialize inactive transaction/intent state without opening a
        transaction or validating the factory.

        Example:
            >>> unit = DatabaseStorageUnitOfWork(factory)  # doctest: +SKIP


        :param factory: Owner of the metadata repository and stable domain repository ports.
        :return: None after initializing transaction and request flags.
        """

        self._factory = factory
        self._transaction: AbstractContextManager[Any] | None = None
        self._entered = False
        self._commit_requested = False
        self._rollback_requested = False

    @property
    def assets(self) -> DatabaseDigitalAssetRepository:
        """
        Return the factory's retained Asset repository port without checking whether this unit is
        entered. All units from the factory share this object; property access does not start or
        commit a transaction.

        Example:
            >>> port = unit.assets  # doctest: +SKIP


        :return: The stable factory-owned Asset repository adapter.
        """

        return self._factory.assets

    @property
    def replicas(self) -> DatabaseReplicaRepository:
        """
        Return the factory's retained Replica repository port without checking whether this unit is
        entered. All units from the factory share this object; property access does not start or
        commit a transaction.

        Example:
            >>> port = unit.replicas  # doctest: +SKIP


        :return: The stable factory-owned Replica repository adapter.
        """

        return self._factory.replicas

    @property
    def composites(self) -> DatabaseCompositeRepository:
        """
        Return the factory's retained Composite repository port without checking whether this unit
        is entered. All units from the factory share this object; property access does not start or
        commit a transaction.

        Example:
            >>> port = unit.composites  # doctest: +SKIP


        :return: The stable factory-owned Composite repository adapter.
        """

        return self._factory.composites

    @property
    def derivations(self) -> DatabaseDerivationRepository:
        """
        Return the factory's retained derivation repository port without checking whether this unit
        is entered. All units from the factory share this object; property access does not start or
        commit a transaction.

        Example:
            >>> port = unit.derivations  # doctest: +SKIP


        :return: The stable factory-owned derivation repository adapter.
        """

        return self._factory.derivations

    def commit(self) -> None:
        """
        Require an active unit without a prior rollback request, then mark commit intent. Repeated
        requests are accepted. No database commit occurs until successful context exit; a later
        rollback request or body exception still prevents that commit.

        Example:
            >>> unit.commit()  # doctest: +SKIP


        :return: None after setting commit intent; invalid lifecycle state raises RuntimeError.
        """

        if not self._entered:
            raise RuntimeError("unit of work is not active.")
        if self._rollback_requested:
            raise RuntimeError("unit of work was already marked for rollback.")
        self._commit_requested = True

    def rollback(self) -> None:
        """
        Require an active unit, mark rollback intent, and clear any earlier commit request. Repeated
        rollback requests are accepted; the underlying transaction is not exited until context
        cleanup.

        Example:
            >>> unit.rollback()  # doctest: +SKIP


        :return: None after recording rollback intent; an inactive unit raises RuntimeError.
        """

        if not self._entered:
            raise RuntimeError("unit of work is not active.")
        self._rollback_requested = True
        self._commit_requested = False

    def __enter__(self) -> DatabaseStorageUnitOfWork:
        """
        Reject entry while already active, obtain and enter repository.transaction(), then set the
        entered flag and return self. Entry failure leaves the transaction reference retained
        without marking active. Prior commit/rollback flags are not reset on reuse after exit.

        Example:
            >>> active = unit.__enter__()  # doctest: +SKIP


        :return: This same unit after its underlying transaction enters successfully.
        """

        if self._entered:
            raise RuntimeError("unit of work cannot be entered twice.")
        transaction = self._factory.repository.transaction()
        transaction.__enter__()
        self._transaction = transaction
        self._entered = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """
        Forward the body outcome or requested transaction result, then clear the active flag.

        Assert that a transaction reference exists. Body exceptions are passed through; otherwise a
        commit request without rollback sends a normal exit, and all other cases send
        _RollbackRequested. Ignore the provider's suppression return value and return None, so body
        failures remain unsuppressed. Provider cleanup errors propagate, but the entered flag clears
        in finally. Intent flags and the transaction reference remain retained; repeated exit is not
        independently guarded.

        Example:
            >>> unit.__exit__(None, None, None)  # doctest: +SKIP


        :param exc_type: Body exception type, or None for normal exit.
        :param exc: Body exception instance forwarded on the exceptional path.
        :param traceback: Body traceback forwarded with that exception, or None.
        :return: None after provider cleanup, without suppressing a body exception.
        """

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


class DatabaseStorageUnitOfWorkFactory(persistence_api.StorageUnitOfWorkFactoryAPI):
    """
    Own one metadata repository and four stable domain adapters, supplying fresh inactive units of
    work. Construction does not validate or enter the database. Units and compatibility mappings
    share this binding; new objects do not imply separate connections or transaction isolation.

    Example:
        >>> factory = DatabaseStorageUnitOfWorkFactory(repository)  # doctest: +SKIP
    """

    def __init__(self, repository: DatabaseStorageMetadataRepository) -> None:
        """
        Retain the repository and create stable Asset, Replica, Composite, and derivation ports. The
        ports perform no reads or writes during construction; no transaction or cache is created
        here.

        Example:
            >>> factory = DatabaseStorageUnitOfWorkFactory(repository)  # doctest: +SKIP


        :param repository: Shared lower metadata adapter used by every port and unit of work.
        :return: None after constructing the four repository adapters.
        """

        self.repository = repository
        self.assets = DatabaseDigitalAssetRepository(repository)
        self.replicas = DatabaseReplicaRepository(repository)
        self.composites = DatabaseCompositeRepository(repository)
        self.derivations = DatabaseDerivationRepository(repository)

    def begin(self) -> DatabaseStorageUnitOfWork:
        """
        Return a new inactive unit bound to this factory. No portable transaction is obtained or
        entered until the unit's context entry, and its repository properties resolve to the
        factory's existing adapters.

        Example:
            >>> unit = factory.begin()  # doctest: +SKIP


        :return: A fresh DatabaseStorageUnitOfWork with no active transaction or outcome request.
        """

        return DatabaseStorageUnitOfWork(self)

    def asset_mapping(
        self,
    ) -> RepositoryRecordMapping[
        manager_api.DigitalAssetID, manager_api.DigitalAssetRecord
    ]:
        """
        Return a fresh Asset compatibility mapping over lower repository reads/deletes and the
        domain port's complete-record upsert. Assignment checks record.digital_asset_id against the
        key, but bypasses port add_from_declaration/remove revision handling. No state snapshot,
        transaction, or extra reference check is created.

        Example:
            >>> mapping = factory.asset_mapping()  # doctest: +SKIP


        :return: A new RepositoryRecordMapping bound to the existing Asset persistence adapters.
        """

        return RepositoryRecordMapping(
            get_one=self.repository.get_asset,
            load_all=self.repository._load_assets,
            upsert=self.assets.upsert_record,
            remove=self.repository.remove_asset,
            key_of=lambda record: record.digital_asset_id,
        )

    def replica_mapping(
        self,
    ) -> RepositoryRecordMapping[manager_api.ReplicaID, manager_api.ReplicaRecord]:
        """
        Return a fresh Replica compatibility mapping over lower repository reads/deletes and the
        domain port's complete-record upsert. Assignment checks record.replica_id against the key,
        but bypasses port add_from_declaration/remove revision handling. No state snapshot,
        transaction, or extra reference check is created.

        Example:
            >>> mapping = factory.replica_mapping()  # doctest: +SKIP


        :return: A new RepositoryRecordMapping bound to the existing Replica persistence adapters.
        """

        return RepositoryRecordMapping(
            get_one=self.repository.get_replica,
            load_all=self.repository._load_replicas,
            upsert=self.replicas.upsert_record,
            remove=self.repository.remove_replica,
            key_of=lambda record: record.replica_id,
        )

    def composite_mapping(
        self,
    ) -> RepositoryRecordMapping[
        manager_api.CompositeDigitalAssetID,
        manager_api.CompositeDigitalAssetRecord,
    ]:
        """
        Return a fresh Composite compatibility mapping over lower repository reads/deletes and the
        domain port's complete-record upsert. Assignment checks record.composite_digital_asset_id
        against the key, but bypasses port add_from_declaration/remove revision handling. No state
        snapshot, transaction, or extra reference check is created.

        Example:
            >>> mapping = factory.composite_mapping()  # doctest: +SKIP


        :return: A new RepositoryRecordMapping bound to the existing Composite persistence adapters.
        """

        return RepositoryRecordMapping(
            get_one=self.repository.get_composite,
            load_all=self.repository._load_composites,
            upsert=self.composites.upsert_record,
            remove=self.repository.remove_composite,
            key_of=lambda record: record.composite_digital_asset_id,
        )

    def derivation_mapping(
        self,
    ) -> RepositoryRecordMapping[
        manager_api.DigitalAssetDerivationID,
        manager_api.DigitalAssetDerivationRecord,
    ]:
        """
        Return a fresh derivation compatibility mapping over lower repository reads/deletes and the
        domain port's complete-record upsert. Assignment checks record.digital_asset_derivation_id
        against the key, but bypasses port add_from_declaration/remove revision handling. No state
        snapshot, transaction, or extra reference check is created.

        Example:
            >>> mapping = factory.derivation_mapping()  # doctest: +SKIP


        :return: A new RepositoryRecordMapping bound to the existing derivation persistence adapters.
        """

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

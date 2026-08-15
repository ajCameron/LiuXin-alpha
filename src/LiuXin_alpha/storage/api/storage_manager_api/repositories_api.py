"""
Persistence ports consumed by storage-manager implementations.
"""

from __future__ import annotations

from collections.abc import Iterator
from types import TracebackType
from typing import Protocol, runtime_checkable

from LiuXin_alpha.storage.api.models import Digest, StoreUUID
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    DigitalAssetDerivationDeclaration,
    DigitalAssetDerivationID,
    DigitalAssetDerivationRecord,
    CompositeDigitalAssetDeclaration,
    CompositeDigitalAssetID,
    CompositeDigitalAssetRecord,
    DigitalAssetDeclaration,
    DigitalAssetID,
    DigitalAssetMetadata,
    DigitalAssetRecord,
    ReplicaDeclaration,
    ReplicaID,
    ReplicaMode,
    ReplicaObservation,
    ReplicaRecord,
)


# Todo: DigitalAssetRepositry is a weird and unituite name for this concept
# Todo: Unless this is exposed as part of the StoreMnagerAPI, it really shouldn't be here
@runtime_checkable
class DigitalAssetRepositoryAPI(Protocol):
    """
    Persistence port for immutable Digital Asset records.

    Concrete adapters translate between these values and SQL rows, document
    records, or another persistence model.
    Manager operations never receive an ORM object through this contract.

    Example:
        >>> record = repository.add(declaration)  # doctest: +SKIP
        >>> same = repository.get(record.digital_asset_id)  # doctest: +SKIP
    """

    def add(self, declaration: DigitalAssetDeclaration) -> DigitalAssetRecord:
        """
        Persist a new byte identity and return its public record.

        Example:
            >>> record = repository.add(declaration)  # doctest: +SKIP


        :param declaration:
        :return:
        """
        ...

    def get(self, digital_asset_id: DigitalAssetID) -> DigitalAssetRecord:
        """
        Load one Asset or raise ``DigitalAssetNotFound``.

        Example:
            >>> record = repository.get(DigitalAssetID(7))  # doctest: +SKIP


        :param digital_asset_id:
        :return:
        """
        ...

    def replace_metadata(
        self,
        digital_asset_id: DigitalAssetID,
        metadata: DigitalAssetMetadata,
        *,
        if_revision: str | None = None,
    ) -> DigitalAssetRecord:
        """
        Replace descriptive metadata with optional race protection.

        Example:
            >>> record = repository.replace_metadata(  # doctest: +SKIP
            ...     DigitalAssetID(7), metadata, if_revision="v2",
            ... )


        :param digital_asset_id:
        :param metadata:
        :param if_revision:
        :return:
        """
        ...

    # Todo: This can, and should, be a top level method for the StorageManagerAPI
    def find_by_digest(
        self,
        digest: Digest,
        *,
        size_bytes: int | None = None,
    ) -> DigitalAssetRecord | None:
        """
        Find a byte-identity candidate without suppressing failures.

        Example:
            >>> match = repository.find_by_digest(digest, size_bytes=4)  # doctest: +SKIP


        :param digest:
        :param size_bytes:
        :return:
        """
        ...

    def iter_assets(self) -> Iterator["DigitalAssetRecord"]:
        """
        Iterate over persisted Digital Asset records.

        Example:
            >>> assets = list(repository.iter_assets())  # doctest: +SKIP

        :return:
        """
        ...

    def remove(
        self,
        digital_asset_id: DigitalAssetID,
        *,
        if_revision: str | None = None,
    ) -> bool:
        """
        Forget an Asset identity and report whether it existed.

        Example:
            >>> removed = repository.remove(DigitalAssetID(7))  # doctest: +SKIP


        :param digital_asset_id:
        :param if_revision:
        :return:
        """
        ...


@runtime_checkable
class ReplicaRepositoryAPI(Protocol):
    """
    Persistence port for Replica claims and observations.

    Example:
        >>> record = repository.add(declaration)  # doctest: +SKIP
    """

    def add(self, declaration: ReplicaDeclaration) -> ReplicaRecord:
        """
        Persist a new Replica claim.

        Example:
            >>> record = repository.add(declaration)  # doctest: +SKIP


        :param declaration:
        :return:
        """
        ...

    def get(self, replica_id: ReplicaID) -> ReplicaRecord:
        """
        Load one Replica or raise ``ReplicaNotFound``.

        Example:
            >>> record = repository.get(ReplicaID(12))  # doctest: +SKIP


        :param replica_id:
        :return:
        """
        ...

    def update_observation(
        self,
        replica_id: ReplicaID,
        observation: ReplicaObservation,
        *,
        if_revision: str | None = None,
    ) -> ReplicaRecord:
        """
        Record fresh physical evidence with optional race protection.

        Example:
            >>> record = repository.update_observation(  # doctest: +SKIP
            ...     ReplicaID(12), observation, if_revision="v3",
            ... )


        :param replica_id:
        :param observation:
        :param if_revision:
        :return:
        """
        ...

    def iter_replicas(
        self,
        *,
        digital_asset_id: DigitalAssetID | None = None,
        store_ref: StoreUUID | None = None,
        mode: ReplicaMode | None = None,
    ) -> Iterator[ReplicaRecord]:
        """
        Iterate over Replica records matching domain filters.

        Example:
            >>> replicas = list(repository.iter_replicas(  # doctest: +SKIP
            ...     digital_asset_id=DigitalAssetID(7),
            ... ))


        :param digital_asset_id:
        :param store_ref:
        :param mode:
        :return:
        """
        ...

    def remove(
        self,
        replica_id: ReplicaID,
        *,
        retain_tombstone: bool = True,
        if_revision: str | None = None,
    ) -> bool:
        """
        Forget or tombstone one Replica claim.

        Example:
            >>> removed = repository.remove(ReplicaID(12))  # doctest: +SKIP


        :param replica_id:
        :param retain_tombstone:
        :param if_revision:
        :return:
        """
        ...


@runtime_checkable
class CompositeDigitalAssetRepositoryAPI(Protocol):
    """
    Persistence port for logical Composite Digital Assets.

    Example:
        >>> record = repository.add(declaration)  # doctest: +SKIP
    """

    def add(
        self,
        declaration: CompositeDigitalAssetDeclaration,
    ) -> CompositeDigitalAssetRecord:
        """
        Persist a new Composite Asset and ordered memberships.

        Example:
            >>> record = repository.add(declaration)  # doctest: +SKIP


        :param declaration:
        :return:
        """
        ...

    def get(
        self,
        composite_digital_asset_id: CompositeDigitalAssetID,
    ) -> CompositeDigitalAssetRecord:
        """
        Load one Composite or raise ``CompositeDigitalAssetNotFound``.

        Example:
            >>> record = repository.get(CompositeDigitalAssetID(3))  # doctest: +SKIP


        :param composite_digital_asset_id:
        :return:
        """
        ...

    def replace(
        self,
        composite_digital_asset_id: CompositeDigitalAssetID,
        declaration: CompositeDigitalAssetDeclaration,
        *,
        if_revision: str | None = None,
    ) -> CompositeDigitalAssetRecord:
        """
        Replace metadata and membership with race protection.

        Example:
            >>> record = repository.replace(  # doctest: +SKIP
            ...     CompositeDigitalAssetID(3), declaration, if_revision="v1",
            ... )


        :param composite_digital_asset_id:
        :param declaration:
        :param if_revision:
        :return:
        """
        ...

    def iter_composites(self) -> Iterator[CompositeDigitalAssetRecord]:
        """
        Iterate over Composite Digital Asset records.

        Example:
            >>> composites = list(repository.iter_composites())  # doctest: +SKIP


        :return:
        """
        ...

    def remove(
        self,
        composite_digital_asset_id: CompositeDigitalAssetID,
        *,
        if_revision: str | None = None,
    ) -> bool:
        """
        Forget one Composite domain identity.

        Example:
            >>> removed = repository.remove(CompositeDigitalAssetID(3))  # doctest: +SKIP


        :param composite_digital_asset_id:
        :param if_revision:
        :return:
        """
        ...


@runtime_checkable
class DigitalAssetDerivationRepositoryAPI(Protocol):
    """
    Persistence port for derivation provenance and replay recipes.

    Example:
        >>> record = repository.add(declaration)  # doctest: +SKIP
    """

    def add(
        self,
        declaration: DigitalAssetDerivationDeclaration,
    ) -> DigitalAssetDerivationRecord:
        """
        Persist one validated derivation and return its public record.

        Example:
            >>> record = repository.add(declaration)  # doctest: +SKIP


        :param declaration:
        :return:
        """
        ...

    def get(
        self,
        digital_asset_derivation_id: DigitalAssetDerivationID,
    ) -> DigitalAssetDerivationRecord:
        """
        Load one derivation or raise ``DigitalAssetDerivationNotFound``.

        Example:
            >>> record = repository.get(  # doctest: +SKIP
            ...     DigitalAssetDerivationID(11),
            ... )


        :param digital_asset_derivation_id:
        :return:
        """
        ...

    # Todo: This should be a top level digital asset?
    def iter_derivations(
        self,
        *,
        result_digital_asset_id: DigitalAssetID | None = None,
        source_digital_asset_id: DigitalAssetID | None = None,
        source_composite_digital_asset_id: CompositeDigitalAssetID | None = None,
        exact_only: bool = False,
    ) -> Iterator[DigitalAssetDerivationRecord]:
        """
        Iterate over derivations matching the supplied filters.

        Example:
            >>> derivations = list(repository.iter_derivations(  # doctest: +SKIP
            ...     result_digital_asset_id=DigitalAssetID(8), exact_only=True,
            ... ))


        :param result_digital_asset_id:
        :param source_digital_asset_id:
        :param source_composite_digital_asset_id:
        :param exact_only:
        :return:
        """
        ...

    def remove(
        self,
        digital_asset_derivation_id: DigitalAssetDerivationID,
        *,
        if_revision: str | None = None,
    ) -> bool:
        """
        Forget one derivation record without deleting any Asset.

        Example:
            >>> removed = repository.remove(  # doctest: +SKIP
            ...     DigitalAssetDerivationID(11),
            ... )


        :param digital_asset_derivation_id:
        :param if_revision:
        :return:
        """
        ...


@runtime_checkable
class StorageUnitOfWorkAPI(Protocol):
    """
    One transaction over manager-owned durable storage metadata.

    This transaction does not pretend to include external Store publication.
    Ingest and replication use recoverable staged states or operation IDs to
    bridge that boundary.

    Example:
        >>> with unit_of_work as work:  # doctest: +SKIP
        ...     asset_record = work.assets.add(declaration)
        ...     work.commit()
    """

    # Todo: It is not super clear to me what this does and where it's used. It probably shouldn't be here.
    # Todo: This is feeling like database stuff which... shouldn't be here really... or perhaps should, and should
    #       be contained in a separate section
    @property
    def assets(self) -> DigitalAssetRepositoryAPI:
        """
        Return the Asset repository bound to this transaction.

        Example:
            >>> repository = unit_of_work.assets  # doctest: +SKIP


        :return:
        """
        ...

    @property
    def replicas(self) -> ReplicaRepositoryAPI:
        """
        Return the Replica repository bound to this transaction.

        Example:
            >>> repository = unit_of_work.replicas  # doctest: +SKIP


        :return:
        """
        ...

    @property
    def composites(self) -> CompositeDigitalAssetRepositoryAPI:
        """
        Return the Composite repository bound to this transaction.

        Example:
            >>> repository = unit_of_work.composites  # doctest: +SKIP


        :return:
        """
        ...

    @property
    def derivations(self) -> DigitalAssetDerivationRepositoryAPI:
        """
        Return the derivation repository bound to this transaction.

        Example:
            >>> repository = unit_of_work.derivations  # doctest: +SKIP


        :return:
        """
        ...

    def commit(self) -> None:
        """
        Atomically commit durable metadata changes.

        Example:
            >>> unit_of_work.commit()  # doctest: +SKIP


        :return:
        """
        ...

    def rollback(self) -> None:
        """
        Discard uncommitted metadata changes idempotently.

        Example:
            >>> unit_of_work.rollback()  # doctest: +SKIP


        :return:
        """
        ...

    def __enter__(self) -> StorageUnitOfWorkAPI:
        """
        Enter the metadata transaction.

        Example:
            >>> entered = unit_of_work.__enter__()  # doctest: +SKIP


        :return:
        """
        ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """
        Rollback unless the metadata transaction was committed.

        Example:
            >>> unit_of_work.__exit__(None, None, None)  # doctest: +SKIP


        :param exc_type:
        :param exc:
        :param traceback:
        :return:
        """
        ...


@runtime_checkable
class StorageUnitOfWorkFactoryAPI(Protocol):
    """
    Factory supplying a fresh metadata transaction per manager operation.

    Example:
        >>> work = factory.begin()  # doctest: +SKIP
    """

    def begin(self) -> StorageUnitOfWorkAPI:
        """
        Create one fresh, unentered metadata unit of work.

        Example:
            >>> work = factory.begin()  # doctest: +SKIP


        :return:
        """
        ...


__all__ = [
    "DigitalAssetDerivationRepositoryAPI",
    "CompositeDigitalAssetRepositoryAPI",
    "DigitalAssetRepositoryAPI",
    "ReplicaRepositoryAPI",
    "StorageUnitOfWorkAPI",
    "StorageUnitOfWorkFactoryAPI",
]

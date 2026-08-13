"""Persistence ports consumed by storage-manager implementations."""

from __future__ import annotations

from collections.abc import Iterator
from types import TracebackType
from typing import Protocol, runtime_checkable

from LiuXin_alpha.storage.api.models import Digest, StoreRef
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    CompositeDigitalAsset,
    CompositeDigitalAssetID,
    CompositeDigitalAssetSpec,
    DigitalAsset,
    DigitalAssetID,
    DigitalAssetMetadata,
    DigitalAssetSpec,
    Replica,
    ReplicaID,
    ReplicaMode,
    ReplicaObservation,
    ReplicaSpec,
)


@runtime_checkable
class DigitalAssetRepositoryAPI(Protocol):
    """Persistence port for immutable Digital Asset domain snapshots.

    Concrete adapters translate between these values and SQL rows, document
    records, or another persistence model. Manager operations never receive an
    ORM object through this contract.

    Example:
        >>> asset = repository.add(spec)  # doctest: +SKIP
        >>> same = repository.get(asset.digital_asset_id)  # doctest: +SKIP
    """

    def add(self, spec: DigitalAssetSpec) -> DigitalAsset:
        """Persist a new byte identity and return its domain snapshot.

        Example:
            >>> asset = repository.add(spec)  # doctest: +SKIP
        """
        ...

    def get(self, digital_asset_id: DigitalAssetID) -> DigitalAsset:
        """Load one Asset or raise ``DigitalAssetNotFound``.

        Example:
            >>> asset = repository.get(DigitalAssetID(7))  # doctest: +SKIP
        """
        ...

    def replace_metadata(
        self,
        digital_asset_id: DigitalAssetID,
        metadata: DigitalAssetMetadata,
        *,
        if_revision: str | None = None,
    ) -> DigitalAsset:
        """Replace descriptive metadata with optional race protection.

        Example:
            >>> asset = repository.replace_metadata(  # doctest: +SKIP
            ...     DigitalAssetID(7), metadata, if_revision="v2",
            ... )
        """
        ...

    def find_by_digest(
        self,
        digest: Digest,
        *,
        size_bytes: int | None = None,
    ) -> DigitalAsset | None:
        """Find a byte-identity candidate without suppressing failures.

        Example:
            >>> match = repository.find_by_digest(digest, size_bytes=4)  # doctest: +SKIP
        """
        ...

    def iter_assets(self) -> Iterator[DigitalAsset]:
        """Iterate over persisted Asset domain snapshots.

        Example:
            >>> assets = list(repository.iter_assets())  # doctest: +SKIP
        """
        ...

    def remove(
        self,
        digital_asset_id: DigitalAssetID,
        *,
        if_revision: str | None = None,
    ) -> bool:
        """Forget an Asset identity and report whether it existed.

        Example:
            >>> removed = repository.remove(DigitalAssetID(7))  # doctest: +SKIP
        """
        ...


@runtime_checkable
class ReplicaRepositoryAPI(Protocol):
    """Persistence port for Replica claims and observations.

    Example:
        >>> replica = repository.add(spec)  # doctest: +SKIP
    """

    def add(self, spec: ReplicaSpec) -> Replica:
        """Persist a new Replica claim.

        Example:
            >>> replica = repository.add(spec)  # doctest: +SKIP
        """
        ...

    def get(self, replica_id: ReplicaID) -> Replica:
        """Load one Replica or raise ``ReplicaNotFound``.

        Example:
            >>> replica = repository.get(ReplicaID(12))  # doctest: +SKIP
        """
        ...

    def update_observation(
        self,
        replica_id: ReplicaID,
        observation: ReplicaObservation,
        *,
        if_revision: str | None = None,
    ) -> Replica:
        """Record fresh physical evidence with optional race protection.

        Example:
            >>> replica = repository.update_observation(  # doctest: +SKIP
            ...     ReplicaID(12), observation, if_revision="v3",
            ... )
        """
        ...

    def iter_replicas(
        self,
        *,
        digital_asset_id: DigitalAssetID | None = None,
        store_ref: StoreRef | None = None,
        mode: ReplicaMode | None = None,
    ) -> Iterator[Replica]:
        """Iterate over Replica snapshots matching domain filters.

        Example:
            >>> replicas = list(repository.iter_replicas(  # doctest: +SKIP
            ...     digital_asset_id=DigitalAssetID(7),
            ... ))
        """
        ...

    def remove(
        self,
        replica_id: ReplicaID,
        *,
        retain_tombstone: bool = True,
        if_revision: str | None = None,
    ) -> bool:
        """Forget or tombstone one Replica claim.

        Example:
            >>> removed = repository.remove(ReplicaID(12))  # doctest: +SKIP
        """
        ...


@runtime_checkable
class CompositeAssetRepositoryAPI(Protocol):
    """Persistence port for logical Composite Digital Assets.

    Example:
        >>> composite = repository.add(spec)  # doctest: +SKIP
    """

    def add(self, spec: CompositeDigitalAssetSpec) -> CompositeDigitalAsset:
        """Persist a new Composite Asset and ordered memberships.

        Example:
            >>> composite = repository.add(spec)  # doctest: +SKIP
        """
        ...

    def get(
        self,
        composite_id: CompositeDigitalAssetID,
    ) -> CompositeDigitalAsset:
        """Load one Composite or raise ``CompositeAssetNotFound``.

        Example:
            >>> composite = repository.get(CompositeDigitalAssetID(3))  # doctest: +SKIP
        """
        ...

    def replace(
        self,
        composite_id: CompositeDigitalAssetID,
        spec: CompositeDigitalAssetSpec,
        *,
        if_revision: str | None = None,
    ) -> CompositeDigitalAsset:
        """Replace metadata and membership with race protection.

        Example:
            >>> composite = repository.replace(  # doctest: +SKIP
            ...     CompositeDigitalAssetID(3), spec, if_revision="v1",
            ... )
        """
        ...

    def iter_composites(self) -> Iterator[CompositeDigitalAsset]:
        """Iterate over Composite domain snapshots.

        Example:
            >>> composites = list(repository.iter_composites())  # doctest: +SKIP
        """
        ...

    def remove(
        self,
        composite_id: CompositeDigitalAssetID,
        *,
        if_revision: str | None = None,
    ) -> bool:
        """Forget one Composite domain identity.

        Example:
            >>> removed = repository.remove(CompositeDigitalAssetID(3))  # doctest: +SKIP
        """
        ...


@runtime_checkable
class StorageUnitOfWorkAPI(Protocol):
    """One transaction over manager-owned durable storage metadata.

    This transaction does not pretend to include external Store publication.
    Ingest and replication use recoverable staged states or operation IDs to
    bridge that boundary.

    Example:
        >>> with unit_of_work as work:  # doctest: +SKIP
        ...     asset = work.assets.add(spec)
        ...     work.commit()
    """

    @property
    def assets(self) -> DigitalAssetRepositoryAPI:
        """Return the Asset repository bound to this transaction.

        Example:
            >>> repository = unit_of_work.assets  # doctest: +SKIP
        """
        ...

    @property
    def replicas(self) -> ReplicaRepositoryAPI:
        """Return the Replica repository bound to this transaction.

        Example:
            >>> repository = unit_of_work.replicas  # doctest: +SKIP
        """
        ...

    @property
    def composites(self) -> CompositeAssetRepositoryAPI:
        """Return the Composite repository bound to this transaction.

        Example:
            >>> repository = unit_of_work.composites  # doctest: +SKIP
        """
        ...

    def commit(self) -> None:
        """Atomically commit durable metadata changes.

        Example:
            >>> unit_of_work.commit()  # doctest: +SKIP
        """
        ...

    def rollback(self) -> None:
        """Discard uncommitted metadata changes idempotently.

        Example:
            >>> unit_of_work.rollback()  # doctest: +SKIP
        """
        ...

    def __enter__(self) -> StorageUnitOfWorkAPI:
        """Enter the metadata transaction.

        Example:
            >>> entered = unit_of_work.__enter__()  # doctest: +SKIP
        """
        ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Rollback unless the metadata transaction was committed.

        Example:
            >>> unit_of_work.__exit__(None, None, None)  # doctest: +SKIP
        """
        ...


@runtime_checkable
class StorageUnitOfWorkFactoryAPI(Protocol):
    """Factory supplying a fresh metadata transaction per manager operation.

    Example:
        >>> work = factory.begin()  # doctest: +SKIP
    """

    def begin(self) -> StorageUnitOfWorkAPI:
        """Create one fresh, unentered metadata unit of work.

        Example:
            >>> work = factory.begin()  # doctest: +SKIP
        """
        ...


__all__ = [
    "CompositeAssetRepositoryAPI",
    "DigitalAssetRepositoryAPI",
    "ReplicaRepositoryAPI",
    "StorageUnitOfWorkAPI",
    "StorageUnitOfWorkFactoryAPI",
]

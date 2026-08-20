"""Implementation-facing persistence ports for storage-manager metadata.

These protocols are an SPI for durable manager implementations. Application
callers should use ``StorageManagerAPI`` instead of repositories or metadata
transactions directly.
"""

from __future__ import annotations

from collections.abc import Iterator
from types import TracebackType
from typing import Protocol, runtime_checkable

from LiuXin_alpha.storage.api.models import Digest, StoreUUID
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    CompositeDigitalAssetDeclaration,
    CompositeDigitalAssetID,
    CompositeDigitalAssetRecord,
    DigitalAssetDeclaration,
    DigitalAssetDerivationDeclaration,
    DigitalAssetDerivationID,
    DigitalAssetDerivationRecord,
    DigitalAssetID,
    DigitalAssetMetadata,
    DigitalAssetRecord,
    ReplicaDeclaration,
    ReplicaID,
    ReplicaMode,
    ReplicaObservation,
    ReplicaRecord,
)


@runtime_checkable
class DigitalAssetRepositoryAPI(Protocol):
    """Persistence port for immutable Digital Asset records.

    Example:
        >>> isinstance(repository, DigitalAssetRepositoryAPI)  # doctest: +SKIP
        True
    """

    def add(self, declaration: DigitalAssetDeclaration) -> DigitalAssetRecord:
        """Persist a declaration and return its assigned record.

        Example:
            >>> record = repository.add(declaration)  # doctest: +SKIP
        """
        ...

    def get(self, digital_asset_id: DigitalAssetID) -> DigitalAssetRecord:
        """Load one Asset record by manager identity.

        Example:
            >>> record = repository.get(DigitalAssetID(7))  # doctest: +SKIP
        """
        ...

    def replace_metadata(
        self,
        digital_asset_id: DigitalAssetID,
        metadata: DigitalAssetMetadata,
        *,
        if_revision: str | None = None,
    ) -> DigitalAssetRecord:
        """Replace descriptive metadata with optional revision checking.

        Example:
            >>> updated = repository.replace_metadata(  # doctest: +SKIP
            ...     DigitalAssetID(7), metadata, if_revision=record.revision,
            ... )
        """
        ...

    def find_by_digest(
        self,
        digest: Digest,
        *,
        size_bytes: int | None = None,
    ) -> DigitalAssetRecord | None:
        """Find an Asset with the supplied content digest and optional size.

        Example:
            >>> found = repository.find_by_digest(digest)  # doctest: +SKIP
        """
        ...

    def iter_assets(self) -> Iterator[DigitalAssetRecord]:
        """Iterate persisted Asset records.

        Example:
            >>> records = tuple(repository.iter_assets())  # doctest: +SKIP
        """
        ...

    def remove(
        self,
        digital_asset_id: DigitalAssetID,
        *,
        if_revision: str | None = None,
    ) -> bool:
        """Remove one Asset record subject to reference and revision checks.

        Example:
            >>> repository.remove(DigitalAssetID(7))  # doctest: +SKIP
            True
        """
        ...


@runtime_checkable
class ReplicaRepositoryAPI(Protocol):
    """Persistence port for Replica claims and observations.

    Example:
        >>> isinstance(repository, ReplicaRepositoryAPI)  # doctest: +SKIP
        True
    """

    def add(self, declaration: ReplicaDeclaration) -> ReplicaRecord:
        """Persist a complete Replica declaration, including placement hints.

        Example:
            >>> replica = repository.add(declaration)  # doctest: +SKIP
        """
        ...

    def get(self, replica_id: ReplicaID) -> ReplicaRecord:
        """Load one Replica record by manager identity.

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
    ) -> ReplicaRecord:
        """Replace the latest physical observation.

        Example:
            >>> updated = repository.update_observation(  # doctest: +SKIP
            ...     ReplicaID(12), observation,
            ... )
        """
        ...

    def iter_replicas(
        self,
        *,
        digital_asset_id: DigitalAssetID | None = None,
        store_ref: StoreUUID | None = None,
        mode: ReplicaMode | None = None,
    ) -> Iterator[ReplicaRecord]:
        """Iterate Replica records matching optional domain filters.

        Example:
            >>> replicas = tuple(repository.iter_replicas(  # doctest: +SKIP
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
        """Remove or tombstone one Replica claim.

        Example:
            >>> repository.remove(ReplicaID(12))  # doctest: +SKIP
            True
        """
        ...


@runtime_checkable
class CompositeDigitalAssetRepositoryAPI(Protocol):
    """Persistence port for logical Composite Digital Assets.

    Example:
        >>> isinstance(repository, CompositeDigitalAssetRepositoryAPI)  # doctest: +SKIP
        True
    """

    def add(
        self,
        declaration: CompositeDigitalAssetDeclaration,
    ) -> CompositeDigitalAssetRecord:
        """Persist a Composite declaration and return its record.

        Example:
            >>> composite = repository.add(declaration)  # doctest: +SKIP
        """
        ...

    def get(
        self,
        composite_digital_asset_id: CompositeDigitalAssetID,
    ) -> CompositeDigitalAssetRecord:
        """Load one Composite record by manager identity.

        Example:
            >>> composite = repository.get(  # doctest: +SKIP
            ...     CompositeDigitalAssetID(3),
            ... )
        """
        ...

    def replace(
        self,
        composite_digital_asset_id: CompositeDigitalAssetID,
        declaration: CompositeDigitalAssetDeclaration,
        *,
        if_revision: str | None = None,
    ) -> CompositeDigitalAssetRecord:
        """Replace one Composite declaration with revision checking.

        Example:
            >>> updated = repository.replace(  # doctest: +SKIP
            ...     CompositeDigitalAssetID(3), declaration,
            ... )
        """
        ...

    def iter_composites(self) -> Iterator[CompositeDigitalAssetRecord]:
        """Iterate persisted Composite records.

        Example:
            >>> composites = tuple(repository.iter_composites())  # doctest: +SKIP
        """
        ...

    def remove(
        self,
        composite_digital_asset_id: CompositeDigitalAssetID,
        *,
        if_revision: str | None = None,
    ) -> bool:
        """Remove one Composite subject to reference and revision checks.

        Example:
            >>> repository.remove(CompositeDigitalAssetID(3))  # doctest: +SKIP
            True
        """
        ...


@runtime_checkable
class DigitalAssetDerivationRepositoryAPI(Protocol):
    """Persistence port for provenance and replay recipes.

    Example:
        >>> isinstance(repository, DigitalAssetDerivationRepositoryAPI)  # doctest: +SKIP
        True
    """

    def add(
        self,
        declaration: DigitalAssetDerivationDeclaration,
    ) -> DigitalAssetDerivationRecord:
        """Persist one provenance edge and optional recreation recipe.

        Example:
            >>> derivation = repository.add(declaration)  # doctest: +SKIP
        """
        ...

    def get(
        self,
        digital_asset_derivation_id: DigitalAssetDerivationID,
    ) -> DigitalAssetDerivationRecord:
        """Load one derivation record by manager identity.

        Example:
            >>> derivation = repository.get(  # doctest: +SKIP
            ...     DigitalAssetDerivationID(4),
            ... )
        """
        ...

    def iter_derivations(
        self,
        *,
        result_digital_asset_id: DigitalAssetID | None = None,
        source_digital_asset_id: DigitalAssetID | None = None,
        source_composite_digital_asset_id: (
            CompositeDigitalAssetID | None
        ) = None,
        workflow_id: int | None = None,
        exact_only: bool = False,
    ) -> Iterator[DigitalAssetDerivationRecord]:
        """Iterate provenance edges matching optional graph filters.

        Example:
            >>> edges = tuple(repository.iter_derivations(  # doctest: +SKIP
            ...     result_digital_asset_id=DigitalAssetID(7),
            ... ))
        """
        ...

    def remove(
        self,
        digital_asset_derivation_id: DigitalAssetDerivationID,
        *,
        if_revision: str | None = None,
    ) -> bool:
        """Remove one derivation record with optional revision checking.

        Example:
            >>> repository.remove(DigitalAssetDerivationID(4))  # doctest: +SKIP
            True
        """
        ...


@runtime_checkable
class StorageUnitOfWorkAPI(Protocol):
    """One atomic transaction over manager-owned durable metadata.

    External Store publication is deliberately outside this transaction;
    operation IDs and staged Replica states bridge that boundary.

    Example:
        >>> with factory.begin() as unit_of_work:  # doctest: +SKIP
        ...     record = unit_of_work.assets.add(declaration)
        ...     unit_of_work.commit()
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
    def composites(self) -> CompositeDigitalAssetRepositoryAPI:
        """Return the Composite repository bound to this transaction.

        Example:
            >>> repository = unit_of_work.composites  # doctest: +SKIP
        """
        ...

    @property
    def derivations(self) -> DigitalAssetDerivationRepositoryAPI:
        """Return the derivation repository bound to this transaction.

        Example:
            >>> repository = unit_of_work.derivations  # doctest: +SKIP
        """
        ...

    def commit(self) -> None:
        """Commit all metadata changes made through this unit of work.

        Example:
            >>> unit_of_work.commit()  # doctest: +SKIP
        """
        ...

    def rollback(self) -> None:
        """Roll back all uncommitted metadata changes.

        Example:
            >>> unit_of_work.rollback()  # doctest: +SKIP
        """
        ...

    def __enter__(self) -> StorageUnitOfWorkAPI:
        """Enter the metadata transaction context.

        Example:
            >>> active = unit_of_work.__enter__()  # doctest: +SKIP
        """
        ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the transaction, rolling back an uncommitted failure.

        Example:
            >>> unit_of_work.__exit__(None, None, None)  # doctest: +SKIP
        """
        ...


@runtime_checkable
class StorageUnitOfWorkFactoryAPI(Protocol):
    """Supply one fresh metadata transaction per manager operation.

    Example:
        >>> unit_of_work = factory.begin()  # doctest: +SKIP
    """

    def begin(self) -> StorageUnitOfWorkAPI:
        """Open and return one fresh metadata transaction.

        Example:
            >>> unit_of_work = factory.begin()  # doctest: +SKIP
        """
        ...


__all__ = [
    "CompositeDigitalAssetRepositoryAPI",
    "DigitalAssetDerivationRepositoryAPI",
    "DigitalAssetRepositoryAPI",
    "ReplicaRepositoryAPI",
    "StorageUnitOfWorkAPI",
    "StorageUnitOfWorkFactoryAPI",
]

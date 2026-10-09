"""Build one complete durable metadata binding before a manager publishes it."""

from __future__ import annotations

import dataclasses
from collections.abc import MutableMapping
from typing import Any, cast
from uuid import UUID

from LiuXin_alpha.storage.api import storage_manager_api as manager_api
from LiuXin_alpha.storage.storage_manager.database_repository import (
    DatabaseStorageMetadataRepository,
)
from LiuXin_alpha.storage.storage_manager.database_unit_of_work import (
    DatabaseStorageUnitOfWorkFactory,
)
from LiuXin_alpha.storage.storage_manager.mixins._types import (
    _IngestOperation,
    _ItemTarget,
)
from LiuXin_alpha.storage.utils.migrations import StorageMigrationReport


@dataclasses.dataclass(frozen=True, slots=True)
class DurableMetadataBinding:
    """Contain every repository-backed manager view for exactly one database.

    Construction performs envelope migration and creates every adapter before returning. A manager
    can therefore install this value as one state transition instead of publishing a partially built
    set of views.

    Example:
        >>> binding = DurableMetadataBinding.build(database)  # doctest: +SKIP
    """

    repository: DatabaseStorageMetadataRepository
    unit_of_work_factory: DatabaseStorageUnitOfWorkFactory
    assets: MutableMapping[manager_api.DigitalAssetID, manager_api.DigitalAssetRecord]
    replicas: MutableMapping[manager_api.ReplicaID, manager_api.ReplicaRecord]
    composites: MutableMapping[
        manager_api.CompositeDigitalAssetID,
        manager_api.CompositeDigitalAssetRecord,
    ]
    derivations: MutableMapping[
        manager_api.DigitalAssetDerivationID,
        manager_api.DigitalAssetDerivationRecord,
    ]
    replication_policies: MutableMapping[
        manager_api.ReplicationPolicyID,
        manager_api.ReplicationPolicyRecord,
    ]
    backup_policies: MutableMapping[
        manager_api.BackupPolicyID, manager_api.BackupPolicyRecord
    ]
    item_targets: MutableMapping[tuple[manager_api.ItemID, str], _ItemTarget]
    ingest_operations: MutableMapping[UUID, _IngestOperation]
    replica_generation: int
    migration_report: StorageMigrationReport

    @classmethod
    def build(
        cls,
        db: Any,
        *,
        additional_types: tuple[type[Any], ...] = (),
        cache: Any | None = None,
        migration_report: StorageMigrationReport | None = None,
    ) -> DurableMetadataBinding:
        """Construct all views locally and return a complete immutable binding descriptor.

        Example:
            >>> binding = DurableMetadataBinding.build(database, cache=cache)  # doctest: +SKIP

        :param db: Supported durable storage catalogue.
        :param additional_types: Private journal value types accepted by the envelope decoder.
        :param cache: Optional borrowed shared metadata cache.
        :param migration_report: Existing schema report to augment with envelope migration count.
        :return: Complete database-owned repository, unit-of-work, mapping, and migration state.
        """

        repository = DatabaseStorageMetadataRepository(
            db,
            additional_types=additional_types,
            cache=cache,
        )
        envelopes_migrated = repository.migrate_envelopes()
        factory = DatabaseStorageUnitOfWorkFactory(repository)
        assets = factory.asset_mapping()
        replicas = factory.replica_mapping()
        composites = factory.composite_mapping()
        derivations = factory.derivation_mapping()
        return cls(
            repository=repository,
            unit_of_work_factory=factory,
            assets=assets,
            replicas=replicas,
            composites=composites,
            derivations=derivations,
            replication_policies=repository.replication_policy_records(),
            backup_policies=repository.backup_policy_records(),
            item_targets=repository.item_targets(),
            ingest_operations=cast(
                MutableMapping[UUID, _IngestOperation],
                cast(object, repository.ingest_operations()),
            ),
            replica_generation=len(replicas),
            migration_report=dataclasses.replace(
                migration_report or StorageMigrationReport(),
                envelope_rows_upgraded=envelopes_migrated,
            ),
        )


__all__ = ["DurableMetadataBinding"]

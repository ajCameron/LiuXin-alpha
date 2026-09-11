"""
Implement Asset catalogue declaration, lookup, metadata replacement, and forgetting.

Identity matching and reference checks use shared manager hooks. Mutations take the
manager lock and metadata transaction supplied by the composition; these methods
do not coordinate physical byte changes.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterator
from typing import override

import LiuXin_alpha.storage.api as api
from LiuXin_alpha.storage.storage_manager.mixins._state import _StorageManagerState


class DigitalAssetRegistryMixin(_StorageManagerState):
    """
    Manage expected byte identities, descriptive metadata, and policy references in shared manager
    repositories.

    Catalogue mutations use the manager lock and metadata-transaction hook. The transient hook
    supplies no durable transaction, while application composition can provide persistence. These
    methods do not read, publish, rename, or delete Store bytes.

    Example:
        >>> asset = manager.declare_digital_asset(declaration)  # doctest: +SKIP
    """

    @override
    def declare_digital_asset(
        self,
        declaration: api.DigitalAssetDeclaration,
    ) -> api.DigitalAssetRecord:
        """
        Validate policy references, then reuse a matching identity or allocate a new Asset record.

        Policy checks run before deduplication, including rejection of a RECREATE loss policy before
        exact derivation registration. Under the lock and metadata transaction, the shared lookup
        selects the first Asset by ID with equal size, at least one common digest algorithm, and no
        disagreement across common algorithms. Digest sets need not be identical.

        An existing match is returned unchanged: new metadata, extra digests, and policy choices are
        not merged. Otherwise a new ID and revision are allocated and the declaration values are
        retained in the stored record. Repository and transaction failures propagate.

        Example:
            >>> asset = manager.declare_digital_asset(declaration)  # doctest: +SKIP


        :param declaration: Expected identity and metadata plus optional policies that must already be registered.
        :return: Existing matching record or newly allocated Asset record, without creating a Replica.
        """

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
        """
        Look up the exact ID under the manager lock without probing physical storage.

        A mapping KeyError is translated to DigitalAssetNotFound with the original error chained.
        Other repository errors are not suppressed.

        Example:
            >>> asset = manager.get_digital_asset_record(asset_id)  # doctest: +SKIP


        :param digital_asset_id: Manager-assigned atomic Asset ID to resolve.
        :return: Retained Asset record for the requested key.
        """

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
        """
        Require the record and optional matching revision, then replace metadata and advance its
        revision.

        The operation runs under the lock and metadata-transaction hook. dataclasses.replace retains
        all other fields, including size, digests, and policy references, and record construction
        does not validate the replacement metadata type. Errors propagate rather than becoming an
        absent result.

        Example:
            >>> updated = manager.update_digital_asset_metadata(asset_id, metadata, if_revision=asset.revision)  # doctest: +SKIP


        :param digital_asset_id: Manager-assigned atomic Asset ID to resolve.
        :param metadata: Complete replacement metadata value, retained without merging or copying.
        :param if_revision: Expected current revision, or None to omit the optimistic precondition.
        :return: Newly stored Asset record with the replacement metadata and new revision.
        """

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
        """
        Capture records under the lock in ascending Asset-key order and return a tuple iterator.

        The sequence is fixed before return, but its record and nested-value references are not deep
        copies. Repository iteration or lookup failures propagate.

        Example:
            >>> assets = tuple(manager.iter_digital_asset_records())  # doctest: +SKIP


        :return: Iterator over the captured ordered Asset record references.
        """

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
        """
        Run the shared identity lookup under the lock for one digest and optional exact size.

        The default helper returns the first matching Asset in ID order. Missing candidates return
        None, while repository failures propagate; no Store lookup or byte verification is
        performed.

        Example:
            >>> candidate = manager.find_digital_asset_record_by_digest(digest)  # doctest: +SKIP


        :param digest: Expected algorithm/value pair supplied as the sole lookup digest.
        :param size_bytes: Exact expected byte count, or None to accept any recorded size.
        :return: First matching catalogue record from the shared lookup, or None.
        """

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
        """
        Remove an existing Asset record after revision and reference checks under the metadata
        lock/transaction.

        Absence returns False before checking the revision. With require_no_replicas enabled, every
        Replica claim counts, including deleted tombstones. Composite membership, derivation
        references, and direct Item links always prevent forgetting. Disabling the Replica check
        does not cascade-delete those claims or touch their Store bytes.

        The derivation helper includes result/source identities, recipe inputs, executors, and
        dependencies. Other reference or repository failures propagate, and persistence guarantees
        belong to the transaction implementation.

        Example:
            >>> removed = manager.forget_digital_asset(asset_id, require_no_replicas=True)  # doctest: +SKIP


        :param digital_asset_id: Manager-assigned atomic Asset ID to resolve.
        :param require_no_replicas: Whether any Replica record, in any state, prevents forgetting this Asset.
        :param if_revision: Expected current revision, or None to omit the optimistic precondition.
        :return: True after deleting the Asset record, False for an absent ID; remaining references can raise StoragePreconditionFailed.
        """

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


__all__ = ["DigitalAssetRegistryMixin"]

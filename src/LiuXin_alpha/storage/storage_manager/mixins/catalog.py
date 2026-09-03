"""
Digital Asset catalogue implementation for the storage manager.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterator
from typing import override

import LiuXin_alpha.storage.api as api
from LiuXin_alpha.storage.storage_manager.mixins._state import _StorageManagerState


class DigitalAssetRegistryMixin(_StorageManagerState):
    """
    Own manager metadata for content-addressed Digital Asset identities.

    Catalogue operations declare, find, update, and remove immutable content
    identities plus their mutable descriptive metadata and policy references.
    They do not publish or retrieve bytes; ingest and Replica components join
    those identities to physical Store locations.
    """

    @override
    def declare_digital_asset(
        self,
        declaration: api.DigitalAssetDeclaration,
    ) -> api.DigitalAssetRecord:
        """
        Declare a content identity, idempotently reusing an exact match.


        :param declaration:
        :return:
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
        Return one Digital Asset record or raise a typed domain error.


        :param digital_asset_id:
        :return:
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
        Replace metadata under an optimistic revision precondition.


        :param digital_asset_id:
        :param metadata:
        :param if_revision:
        :return:
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
        Iterate over a stable ID-ordered Asset snapshot.


        :return:
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
        Return the first stable-ID record matching digest and size.


        :param digest:
        :param size_bytes:
        :return:
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
        Forget an unreferenced Asset record without touching Store bytes.


        :param digital_asset_id:
        :param require_no_replicas:
        :param if_revision:
        :return:
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

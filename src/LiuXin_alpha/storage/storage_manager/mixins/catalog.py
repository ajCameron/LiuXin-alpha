"""
Implement Asset catalogue declaration, lookup, metadata replacement, and forgetting.

Identity matching and reference checks use shared manager hooks. Mutations take the
manager lock and metadata transaction supplied by the composition; these methods
do not coordinate physical byte changes.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterator
from typing import cast, override

from LiuXin_alpha.storage.api import errors as storage_errors
from LiuXin_alpha.storage.api import models as storage_models
from LiuXin_alpha.storage.api import storage_manager_api as manager_api
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
        declaration: manager_api.DigitalAssetDeclaration,
    ) -> manager_api.DigitalAssetRecord:
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
            if policy.loss_action is manager_api.DigitalAssetLossAction.RECREATE:
                raise manager_api.StoragePolicyUnsatisfied(
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
            digital_asset_id = manager_api.DigitalAssetID(
                self._allocate_metadata_id_locked("digital_asset")
            )
            record = manager_api.DigitalAssetRecord(
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
        digital_asset_id: manager_api.DigitalAssetID,
    ) -> manager_api.DigitalAssetRecord:
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
                raise manager_api.DigitalAssetNotFound(
                    f"Digital Asset {digital_asset_id} is not registered."
                ) from error

    @override
    def update_digital_asset_metadata(
        self,
        digital_asset_id: manager_api.DigitalAssetID,
        metadata: manager_api.DigitalAssetMetadata,
        *,
        if_revision: str | None = None,
    ) -> manager_api.DigitalAssetRecord:
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
    def set_digital_asset_name(
        self,
        digital_asset_id: manager_api.DigitalAssetID,
        name: str | None,
        *,
        if_revision: str | None = None,
    ) -> manager_api.DigitalAssetRecord:
        """Replace only the display-name field through the shared atomic metadata updater.

        :param digital_asset_id: Registered Asset identity to update.
        :param name: Replacement display name, or None to clear it.
        :param if_revision: Expected revision, or None for no precondition.
        :return: Updated Asset record with a new revision.
        """

        return self._update_digital_asset_metadata_field(
            digital_asset_id,
            field_name="name",
            value=name,
            if_revision=if_revision,
        )

    @override
    def set_digital_asset_media_type(
        self,
        digital_asset_id: manager_api.DigitalAssetID,
        media_type: str | None,
        *,
        if_revision: str | None = None,
    ) -> manager_api.DigitalAssetRecord:
        """Replace only the media-type field through the shared atomic metadata updater.

        :param digital_asset_id: Registered Asset identity to update.
        :param media_type: Replacement media type, or None to clear it.
        :param if_revision: Expected revision, or None for no precondition.
        :return: Updated Asset record with a new revision.
        """

        return self._update_digital_asset_metadata_field(
            digital_asset_id,
            field_name="media_type",
            value=media_type,
            if_revision=if_revision,
        )

    @override
    def set_digital_asset_original_name(
        self,
        digital_asset_id: manager_api.DigitalAssetID,
        original_name: str | None,
        *,
        if_revision: str | None = None,
    ) -> manager_api.DigitalAssetRecord:
        """Replace only the original-name field through the shared atomic metadata updater.

        :param digital_asset_id: Registered Asset identity to update.
        :param original_name: Replacement source filename, or None to clear it.
        :param if_revision: Expected revision, or None for no precondition.
        :return: Updated Asset record with a new revision.
        """

        return self._update_digital_asset_metadata_field(
            digital_asset_id,
            field_name="original_name",
            value=original_name,
            if_revision=if_revision,
        )

    @override
    def set_digital_asset_attributes(
        self,
        digital_asset_id: manager_api.DigitalAssetID,
        attributes: tuple[tuple[str, str], ...],
        *,
        if_revision: str | None = None,
    ) -> manager_api.DigitalAssetRecord:
        """Replace only extension attributes through the shared atomic metadata updater.

        :param digital_asset_id: Registered Asset identity to update.
        :param attributes: Complete replacement attribute sequence.
        :param if_revision: Expected revision, or None for no precondition.
        :return: Updated Asset record with a new revision.
        """

        return self._update_digital_asset_metadata_field(
            digital_asset_id,
            field_name="attributes",
            value=attributes,
            if_revision=if_revision,
        )

    def _update_digital_asset_metadata_field(
        self,
        digital_asset_id: manager_api.DigitalAssetID,
        *,
        field_name: str,
        value: object,
        if_revision: str | None,
    ) -> manager_api.DigitalAssetRecord:
        """
        Replace one named metadata field under the manager lock and revision precondition.

        Public callers supply one of the four DigitalAssetMetadata field names. Constructing the
        replacement metadata applies its ordinary blank-label and attribute-name validation before
        the Asset record is stored. Size, digests, policies, and the three unselected metadata
        fields remain unchanged. Transaction hooks control durable rollback.

        :param digital_asset_id: Registered Asset identity to update.
        :param field_name: DigitalAssetMetadata field selected by a public wrapper.
        :param value: Complete replacement value for that field.
        :param if_revision: Expected current Asset revision, or None for no precondition.
        :return: Updated Asset record with a newly allocated revision.
        """

        with self._lock, self._metadata_transaction():
            current = self._require_asset_locked(digital_asset_id)
            self._check_revision(current.revision, if_revision)
            if field_name == "name":
                metadata = dataclasses.replace(
                    current.metadata,
                    name=cast(str | None, value),
                )
            elif field_name == "media_type":
                metadata = dataclasses.replace(
                    current.metadata,
                    media_type=cast(str | None, value),
                )
            elif field_name == "original_name":
                metadata = dataclasses.replace(
                    current.metadata,
                    original_name=cast(str | None, value),
                )
            elif field_name == "attributes":
                metadata = dataclasses.replace(
                    current.metadata,
                    attributes=cast(tuple[tuple[str, str], ...], value),
                )
            else:
                raise AssertionError(f"unsupported metadata field: {field_name}")
            updated = dataclasses.replace(
                current,
                metadata=metadata,
                revision=self._new_revision_locked(),
            )
            self._assets[digital_asset_id] = updated
            return updated

    @override
    def iter_digital_asset_records(
        self,
        *,
        order_by: manager_api.DigitalAssetRecordOrder
        | str = manager_api.DigitalAssetRecordOrder.ID,
        descending: bool = False,
    ) -> Iterator[manager_api.DigitalAssetRecord]:
        """
        Capture records under the lock, apply the selected stable key, and return a tuple iterator.

        ID and size ordering use Asset ID as a tie breaker. Descriptive keys compare case-folded
        text, place missing values after present values in ascending order, and also use ID as a tie
        breaker. ``descending`` reverses the complete tuple key, including missing placement and ID
        ties. The sequence is fixed before return, but record references are not deep copies.

        Example:
            >>> assets = tuple(manager.iter_digital_asset_records())  # doctest: +SKIP


        :param order_by: Ordering enum or exact value string; invalid strings raise ValueError.
        :param descending: Whether to reverse the complete selected key ordering.
        :return: Iterator over the captured ordered Asset record references.
        """

        try:
            selected_order = manager_api.DigitalAssetRecordOrder(order_by)
        except ValueError as error:
            raise ValueError(
                "order_by must be 'id', 'size', 'name', 'media_type', or "
                "'original_name'."
            ) from error
        with self._lock:
            records = tuple(self._assets.values())
        if selected_order is manager_api.DigitalAssetRecordOrder.ID:
            records = tuple(
                sorted(
                    records,
                    key=lambda record: (record.digital_asset_id,),
                    reverse=descending,
                )
            )
        elif selected_order is manager_api.DigitalAssetRecordOrder.SIZE:
            records = tuple(
                sorted(
                    records,
                    key=lambda record: (
                        record.size_bytes,
                        record.digital_asset_id,
                    ),
                    reverse=descending,
                )
            )
        else:
            field_name = selected_order.value
            records = tuple(
                sorted(
                    records,
                    key=lambda record: (
                        getattr(record.metadata, field_name) is None,
                        (getattr(record.metadata, field_name) or "").casefold(),
                        record.digital_asset_id,
                    ),
                    reverse=descending,
                )
            )
        return iter(records)

    @override
    def find_digital_asset_record_by_digest(
        self,
        digest: storage_models.Digest,
        *,
        size_bytes: int | None = None,
    ) -> manager_api.DigitalAssetRecord | None:
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
        digital_asset_id: manager_api.DigitalAssetID,
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
                raise storage_errors.StoragePreconditionFailed(
                    "Digital Asset still has Replica claims."
                )
            if any(
                member.digital_asset_id == digital_asset_id
                for composite in self._composites.values()
                for member in composite.members
            ):
                raise storage_errors.StoragePreconditionFailed(
                    "Digital Asset is still a Composite member."
                )
            if self._asset_has_derivation_reference_locked(digital_asset_id):
                raise storage_errors.StoragePreconditionFailed(
                    "Digital Asset is still referenced by derivation provenance."
                )
            if any(
                kind == "digital_asset" and target_id == digital_asset_id
                for kind, target_id in self._item_targets.values()
            ):
                raise storage_errors.StoragePreconditionFailed(
                    "Digital Asset is still linked to an Item."
                )
            del self._assets[digital_asset_id]
            return True


__all__ = ["DigitalAssetRegistryMixin"]

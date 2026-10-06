"""
Implement Composite catalogue changes and required-member availability workflows.

Metadata mutations use shared locking and transaction hooks. Member resolution and
assessment read catalogue and Store evidence separately, preserve relationship
context, and do not assemble physical byte streams.
"""

from __future__ import annotations

import hashlib
import json

from collections.abc import Iterable, Iterator
from typing import override

import LiuXin_alpha.storage.api as api
from LiuXin_alpha.storage.storage_manager.mixins._state import _StorageManagerState


class CompositeDigitalAssetMixin(_StorageManagerState):
    """
    Manage Composite catalogue records and resolve their atomic membership relationships.

    Declaration/replacement validate referenced Assets before their metadata mutation. Replacement
    and forgetting support optional revision preconditions. Selection and assessment perform
    separate reads, preserve relationship context, and do not concatenate or materialize member
    bytes.

    Example:
        >>> composite = manager.declare_composite_digital_asset(declaration)  # doctest: +SKIP
    """

    @override
    def declare_composite_digital_asset(
        self,
        declaration: api.CompositeDigitalAssetDeclaration,
    ) -> api.CompositeDigitalAssetRecord:
        """
        Resolve every referenced Asset, then allocate and store a new Composite under the manager
        lock and metadata transaction.

        Member checks occur before the transaction and include optional members. Each call creates a
        new ID and revision without deduplicating an equivalent declaration. Supplied membership
        order and descriptive values are retained; no physical Replica check occurs.

        Example:
            >>> composite = manager.declare_composite_digital_asset(declaration)  # doctest: +SKIP


        :param declaration: Complete membership and metadata for a newly allocated Composite.
        :return: Newly stored Composite record; missing members and repository/transaction errors propagate.
        """

        for member in declaration.members:
            self.get_digital_asset_record(member.digital_asset_id)
        with self._lock, self._metadata_transaction():
            composite_id = api.CompositeDigitalAssetID(
                self._allocate_metadata_id_locked("composite")
            )
            record = api.CompositeDigitalAssetRecord(
                composite_id,
                declaration.members,
                declaration.name,
                declaration.attributes,
                self._new_revision_locked(),
            )
            self._composites[composite_id] = record
            return record

    @override
    def get_composite_digital_asset_record(
        self,
        composite_digital_asset_id: api.CompositeDigitalAssetID,
    ) -> api.CompositeDigitalAssetRecord:
        """
        Look up the exact Composite key under the manager lock.

        A mapping KeyError becomes CompositeDigitalAssetNotFound with the original error chained.
        Other repository errors propagate, and members are not resolved.

        Example:
            >>> composite = manager.get_composite_digital_asset_record(composite_id)  # doctest: +SKIP


        :param composite_digital_asset_id: Manager-assigned Composite identity to resolve.
        :return: Retained record for the requested Composite key.
        """

        with self._lock:
            try:
                return self._composites[composite_digital_asset_id]
            except KeyError as error:
                raise api.CompositeDigitalAssetNotFound(
                    f"Composite Digital Asset {composite_digital_asset_id} is not registered."
                ) from error

    @override
    def calculate_composite_digital_asset_digest(
        self,
        composite_digital_asset_id: api.CompositeDigitalAssetID,
        *,
        algorithm: str = "sha256",
    ) -> api.Digest:
        """
        Serialize the Composite's logical byte manifest canonically and hash it without reading bytes.

        Resolve the current record and every referenced atomic Asset. Members are serialized in the
        retained declaration order; each Asset digest set is sorted by algorithm/value so equivalent
        identities do not depend on tuple ordering. Compact sorted-key JSON with UTF-8 and
        ``ensure_ascii=False`` supplies deterministic text encoding. The schema marker permits a
        future incompatible manifest definition without silently reusing this digest namespace.

        Unsupported algorithms become StoreUnsupportedOperation and include runtime-supported
        hashlib names. Missing Composite/Asset records and malformed metadata propagate.

        :param composite_digital_asset_id: Registered Composite identity whose manifest is hashed.
        :param algorithm: Runtime-supported hashlib algorithm name, defaulting to sha256.
        :return: Digest over the version-one canonical manifest payload.
        """

        record = self.get_composite_digital_asset_record(
            composite_digital_asset_id
        )
        members: list[dict[str, object]] = []
        for membership in record.members:
            asset = self.get_digital_asset_record(membership.digital_asset_id)
            members.append(
                {
                    "sequence_number": membership.sequence_number,
                    "role": membership.role,
                    "logical_name": membership.logical_name,
                    "logical_path": membership.logical_path,
                    "title": membership.title,
                    "required": membership.required,
                    "asset": {
                        "size_bytes": asset.size_bytes,
                        "digests": [
                            {
                                "algorithm": digest.algorithm,
                                "value": digest.value,
                            }
                            for digest in sorted(
                                asset.digests,
                                key=lambda value: (
                                    value.algorithm,
                                    value.value,
                                ),
                            )
                        ],
                    },
                }
            )
        payload = json.dumps(
            {
                "schema": "liuxin-composite-digital-asset-v1",
                "members": members,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        try:
            hasher = hashlib.new(algorithm, payload)
        except ValueError as error:
            available = ", ".join(sorted(hashlib.algorithms_available))
            raise api.StoreUnsupportedOperation(
                f"digest algorithm is not supported: {algorithm!r}; "
                f"available algorithms: {available}"
            ) from error
        return api.Digest(algorithm, hasher.hexdigest())

    @override
    def find_composite_digital_asset_records_by_digest(
        self,
        digest: api.Digest,
    ) -> tuple[api.CompositeDigitalAssetRecord, ...]:
        """
        Scan the Composite catalogue and retain records whose canonical digest equals the target.

        Catalogue iteration supplies stable implementation order. Each calculation performs current
        Asset metadata lookups and uses the requested digest algorithm. This method has no index or
        cross-record snapshot; an unsupported algorithm or dangling member reference aborts the
        scan, potentially after earlier calculations but without mutating state.

        :param digest: Expected normalized algorithm/value pair for the canonical manifest.
        :return: Matching records in Composite iteration order, possibly empty.
        """

        return tuple(
            record
            for record in self.iter_composite_digital_asset_records()
            if self.calculate_composite_digital_asset_digest(
                record.composite_digital_asset_id,
                algorithm=digest.algorithm,
            )
            == digest
        )

    @override
    def set_composite_digital_asset_name(
        self,
        composite_digital_asset_id: api.CompositeDigitalAssetID,
        name: str | None,
        *,
        if_revision: str | None = None,
    ) -> api.CompositeDigitalAssetRecord:
        """
        Replace only the Composite display name under one metadata transaction.

        A supplied name must contain non-whitespace text; None clears it. Membership, attributes,
        and identity remain unchanged. Revision validation and replacement happen under the manager
        lock, preventing a read/modify/write race within this process.

        :param composite_digital_asset_id: Registered Composite identity to update.
        :param name: Nonblank replacement display name, or None to clear it.
        :param if_revision: Expected current revision, or None for no optimistic precondition.
        :return: Updated Composite record with a fresh revision.
        """

        if name is not None and not name.strip():
            raise ValueError("name must not be empty when supplied.")
        with self._lock, self._metadata_transaction():
            current = self._require_composite_locked(composite_digital_asset_id)
            self._check_revision(current.revision, if_revision)
            updated = api.CompositeDigitalAssetRecord(
                current.composite_digital_asset_id,
                current.members,
                name,
                current.attributes,
                self._new_revision_locked(),
            )
            self._composites[composite_digital_asset_id] = updated
            return updated

    @override
    def set_composite_digital_asset_attributes(
        self,
        composite_digital_asset_id: api.CompositeDigitalAssetID,
        attributes: tuple[tuple[str, str], ...],
        *,
        if_revision: str | None = None,
    ) -> api.CompositeDigitalAssetRecord:
        """
        Replace only Composite extension attributes under one metadata transaction.

        Attribute validation intentionally matches CompositeDigitalAssetRecord: pairs are retained
        without normalization or uniqueness checks. Membership and name remain unchanged, and the
        optional revision protects the complete record update.

        :param composite_digital_asset_id: Registered Composite identity to update.
        :param attributes: Complete ordered replacement attribute pairs.
        :param if_revision: Expected current revision, or None for no optimistic precondition.
        :return: Updated Composite record with a fresh revision.
        """

        with self._lock, self._metadata_transaction():
            current = self._require_composite_locked(composite_digital_asset_id)
            self._check_revision(current.revision, if_revision)
            updated = api.CompositeDigitalAssetRecord(
                current.composite_digital_asset_id,
                current.members,
                current.name,
                attributes,
                self._new_revision_locked(),
            )
            self._composites[composite_digital_asset_id] = updated
            return updated

    @override
    def replace_composite_digital_asset_member(
        self,
        composite_digital_asset_id: api.CompositeDigitalAssetID,
        membership: api.CompositeDigitalAssetMembership,
        *,
        if_revision: str | None = None,
    ) -> api.CompositeDigitalAssetRecord:
        """
        Replace one relationship selected by sequence number under the manager lock.

        The replacement Asset must already exist. The sequence number must occur in the current
        record; other relationships retain their original tuple order and values. Record
        construction revalidates contiguous positions. No Replica bytes are copied, removed, or
        checked, and metadata transaction behavior belongs to the manager composition.

        :param composite_digital_asset_id: Registered Composite identity to update.
        :param membership: Complete replacement relationship selecting an existing sequence number.
        :param if_revision: Expected current revision, or None for no optimistic precondition.
        :return: Updated Composite record with a fresh revision.
        """

        with self._lock, self._metadata_transaction():
            current = self._require_composite_locked(composite_digital_asset_id)
            self._check_revision(current.revision, if_revision)
            self._require_asset_locked(membership.digital_asset_id)
            if not any(
                member.sequence_number == membership.sequence_number
                for member in current.members
            ):
                raise ValueError(
                    "membership sequence number does not exist in the Composite."
                )
            members = tuple(
                membership
                if member.sequence_number == membership.sequence_number
                else member
                for member in current.members
            )
            updated = api.CompositeDigitalAssetRecord(
                current.composite_digital_asset_id,
                members,
                current.name,
                current.attributes,
                self._new_revision_locked(),
            )
            self._composites[composite_digital_asset_id] = updated
            return updated

    @override
    def replace_composite_digital_asset(
        self,
        composite_digital_asset_id: api.CompositeDigitalAssetID,
        declaration: api.CompositeDigitalAssetDeclaration,
        *,
        if_revision: str | None = None,
    ) -> api.CompositeDigitalAssetRecord:
        """
        Validate all replacement Asset references, then replace the Composite after optional
        revision checking.

        Member lookup precedes the locked metadata transaction and lookup of the Composite itself.
        The new record retains the requested Composite ID, replaces the complete
        membership/name/attributes, and receives a new revision. No byte changes occur; transaction
        guarantees depend on the manager composition.

        Example:
            >>> composite = manager.replace_composite_digital_asset(composite_id, declaration, if_revision=old.revision)  # doctest: +SKIP


        :param composite_digital_asset_id: Manager-assigned Composite identity to resolve.
        :param declaration: Complete replacement values, with all member Assets required to exist.
        :param if_revision: Expected current revision, or None to omit the optimistic precondition.
        :return: Newly stored replacement record, after member and revision checks succeed.
        """

        for member in declaration.members:
            self.get_digital_asset_record(member.digital_asset_id)
        with self._lock, self._metadata_transaction():
            current = self._require_composite_locked(composite_digital_asset_id)
            self._check_revision(current.revision, if_revision)
            record = api.CompositeDigitalAssetRecord(
                composite_digital_asset_id,
                declaration.members,
                declaration.name,
                declaration.attributes,
                self._new_revision_locked(),
            )
            self._composites[composite_digital_asset_id] = record
            return record

    @override
    def iter_composite_digital_asset_records(
        self,
    ) -> Iterator[api.CompositeDigitalAssetRecord]:
        """
        Capture records under the lock in ascending Composite-key order and return a tuple iterator.

        The sequence is stable after return, but retained records and nested values are not deep
        copies. Repository iteration and lookup failures propagate.

        Example:
            >>> composites = tuple(manager.iter_composite_digital_asset_records())  # doctest: +SKIP


        :return: Iterator over the captured ordered Composite record references.
        """

        with self._lock:
            records = tuple(self._composites[key] for key in sorted(self._composites))
        return iter(records)

    @override
    def forget_composite_digital_asset(
        self,
        composite_digital_asset_id: api.CompositeDigitalAssetID,
        *,
        require_unlinked: bool = True,
        if_revision: str | None = None,
    ) -> bool:
        """
        Remove one Composite record after optional revision and reference checks in the metadata
        transaction.

        Absence returns False before checking a revision. When require_unlinked is true, matching
        Item targets and Composite references in derivation sources raise StoragePreconditionFailed.
        False skips both reference checks. The operation does not remove Item links, provenance,
        member Assets, or Store bytes; repository constraints may still reject the deletion.

        Example:
            >>> removed = manager.forget_composite_digital_asset(composite_id, require_unlinked=True)  # doctest: +SKIP


        :param composite_digital_asset_id: Manager-assigned Composite identity to resolve.
        :param require_unlinked: Whether Item-target and derivation-source references must prevent deletion.
        :param if_revision: Expected current revision, or None to omit the optimistic precondition.
        :return: True after deleting the record, or False if already absent; failed preconditions and persistence errors propagate.
        """

        with self._lock, self._metadata_transaction():
            current = self._composites.get(composite_digital_asset_id)
            if current is None:
                return False
            self._check_revision(current.revision, if_revision)
            if require_unlinked:
                if any(
                    kind == "composite_digital_asset"
                    and target_id == composite_digital_asset_id
                    for kind, target_id in self._item_targets.values()
                ):
                    raise api.StoragePreconditionFailed(
                        "Composite Digital Asset is still linked to an Item."
                    )
                if any(
                    source.composite_digital_asset_id == composite_digital_asset_id
                    for record in self._derivations.values()
                    for source in record.declaration.sources
                ):
                    raise api.StoragePreconditionFailed(
                        "Composite Digital Asset is still derivation provenance."
                    )
            del self._composites[composite_digital_asset_id]
            return True

    @override
    def resolve_composite_digital_asset(
        self,
        composite_digital_asset_id: api.CompositeDigitalAssetID,
        *,
        preferred_store_ref: api.StoreUUID | None = None,
        require_verified: bool = False,
    ) -> api.CompositeDigitalAssetResolution:
        """
        Resolve each membership in stored order using the default ACTIVE atomic-selection mode.

        DigitalAssetNotFound and NoReadableReplica omit optional members and accumulate required
        member IDs. Processing continues after those failures; at the end, any missing required
        membership raises CompositeDigitalAssetIncomplete instead of returning a partial tuple.
        Other failures propagate immediately.

        Successful results retain each full membership relationship, including repeated Assets, and
        are not sorted by sequence number. Separate member resolutions do not form an atomic storage
        snapshot or perform fresh digest verification merely because require_verified is true.

        Example:
            >>> members = manager.resolve_composite_digital_asset(composite_id, preferred_store_ref=store_uuid)  # doctest: +SKIP


        :param composite_digital_asset_id: Manager-assigned Composite identity to resolve.
        :param preferred_store_ref: Optional Store UUID to prefer without excluding eligible copies elsewhere.
        :param require_verified: Whether selection requires a recorded VERIFIED state; this flag does not itself request fresh digest verification.
        :return: Aggregate Composite record and successful member resolutions in stored order.
        """

        record = self.get_composite_digital_asset_record(composite_digital_asset_id)
        resolved: list[api.CompositeDigitalAssetMemberResolution] = []
        missing: list[api.DigitalAssetID] = []
        for membership in record.members:
            try:
                resolution = self.resolve_digital_asset(
                    membership.digital_asset_id,
                    preferred_store_ref=preferred_store_ref,
                    require_verified=require_verified,
                )
            except (api.DigitalAssetNotFound, api.NoReadableReplica):
                if membership.required:
                    missing.append(membership.digital_asset_id)
                continue
            resolved.append(
                api.CompositeDigitalAssetMemberResolution(
                    membership,
                    resolution,
                )
            )
        if missing:
            raise api.CompositeDigitalAssetIncomplete(
                "required member Assets are unavailable: "
                + ", ".join(str(value) for value in missing)
            )
        return api.CompositeDigitalAssetResolution(record, tuple(resolved))

    @override
    def materialize_composite_digital_asset(
        self,
        composite_digital_asset_id: api.CompositeDigitalAssetID,
        *,
        preferred_store_ref: api.StoreUUID | None = None,
        source_modes: Iterable[api.ReplicaMode | str] = (api.ReplicaMode.ACTIVE,),
        cache_store_ref: api.StoreUUID | None = None,
        verify: bool = True,
    ) -> api.CompositeDigitalAssetResolution:
        """
        Materialize each relationship through the atomic Asset workflow and retain Composite context.

        Materialize memberships in stored order. Source modes are captured once so generators apply
        identically to every member. DigitalAssetNotFound and NoReadableReplica omit optional
        relationships and accumulate required IDs; other failures propagate immediately. When a
        cache destination is supplied, successful earlier member publications remain if a later
        member fails, because atomic materializations are independent transactions.

        Repeated Asset memberships independently call materialize_digital_asset. Its exact-cache
        reuse avoids duplicate publication while this result still contains every relationship.
        Verification follows the atomic workflow: no-cache selection can require recorded VERIFIED
        state, while new CACHE copies are inspected after publication.

        Example:
            >>> resolved = manager.materialize_composite_digital_asset(  # doctest: +SKIP
            ...     composite_id, cache_store_ref=cache_uuid,
            ... )

        :param composite_digital_asset_id: Registered Composite identity whose members are materialized.
        :param preferred_store_ref: Optional source Store preference forwarded for every atomic selection.
        :param source_modes: Ordered source modes or strings, captured once and searched for every member.
        :param cache_store_ref: Exact CACHE destination UUID for every member, or None for no-copy selection.
        :param verify: Whether reused/no-copy selections require recorded verification and new copies are verified.
        :return: Aggregate Composite resolution when every required relationship materializes.
        """

        record = self.get_composite_digital_asset_record(composite_digital_asset_id)
        selected_source_modes = tuple(source_modes)
        resolved: list[api.CompositeDigitalAssetMemberResolution] = []
        missing: list[api.DigitalAssetID] = []
        for membership in record.members:
            try:
                resolution = self.materialize_digital_asset(
                    membership.digital_asset_id,
                    preferred_store_ref=preferred_store_ref,
                    source_modes=selected_source_modes,
                    cache_store_ref=cache_store_ref,
                    verify=verify,
                )
            except (api.DigitalAssetNotFound, api.NoReadableReplica):
                if membership.required:
                    missing.append(membership.digital_asset_id)
                continue
            resolved.append(
                api.CompositeDigitalAssetMemberResolution(membership, resolution)
            )
        if missing:
            raise api.CompositeDigitalAssetIncomplete(
                "required member Assets could not be materialized: "
                + ", ".join(str(value) for value in missing)
            )
        return api.CompositeDigitalAssetResolution(record, tuple(resolved))

    @override
    def assess_composite_digital_asset(
        self,
        composite_digital_asset_id: api.CompositeDigitalAssetID,
    ) -> api.CompositeDigitalAssetAvailabilityAssessment:
        """
        Count required membership occurrences whose Assets exist and whose ACTIVE Replicas can be
        selected.

        Optional members are ignored entirely. Each required occurrence performs an Asset lookup,
        then default Replica selection without requiring recorded verification. DigitalAssetNotFound
        at the first lookup and NoReadableReplica during selection become diagnostics; other errors
        propagate.

        Missing Asset IDs are deduplicated in first-seen order while error messages remain per
        failure, and repeated memberships still contribute separately to counts. With no required
        members, the zero-count assessment is readable. This method does not recompute digests or
        update observations itself.

        Example:
            >>> assessment = manager.assess_composite_digital_asset(composite_id)  # doctest: +SKIP


        :param composite_digital_asset_id: Manager-assigned Composite identity to resolve.
        :return: Assessment of required-member presence and selection, with deduplicated missing IDs and ordered errors.
        """

        record = self.get_composite_digital_asset_record(composite_digital_asset_id)
        resolved = readable = 0
        missing: list[api.DigitalAssetID] = []
        errors: list[str] = []
        required_members = tuple(
            membership for membership in record.members if membership.required
        )
        for membership in required_members:
            try:
                self.get_digital_asset_record(membership.digital_asset_id)
                resolved += 1
            except api.DigitalAssetNotFound as error:
                missing.append(membership.digital_asset_id)
                errors.append(str(error))
                continue
            try:
                self.select_replica(membership.digital_asset_id)
                readable += 1
            except api.NoReadableReplica as error:
                missing.append(membership.digital_asset_id)
                errors.append(str(error))
        return api.CompositeDigitalAssetAvailabilityAssessment(
            composite_digital_asset_id,
            len(required_members),
            resolved,
            readable,
            tuple(dict.fromkeys(missing)),
            tuple(errors),
        )


__all__ = ["CompositeDigitalAssetMixin"]

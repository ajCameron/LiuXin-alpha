"""
Implement Replica ranking, Asset/Item resolution, and optional cache-copy selection.

Recorded state and current stat size guide selection without fresh hashing or byte
reads. Materialization delegates new CACHE publication to Replica lifecycle methods;
resolution values retain routing context without owning a reader or a storage lock.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import override

import LiuXin_alpha.storage.api as api
from LiuXin_alpha.storage.storage_manager.mixins._state import _StorageManagerState


class DigitalAssetRetrievalMixin(_StorageManagerState):
    """
    Select recorded copies using Store preference, state, and current stat size, and expose their
    routing context.

    Selection and resolution do not open readers, recalculate digests, or update observations.
    Materialization can delegate creation of a CACHE Replica, with publication and later
    verification governed by the separate replication workflow. No local-filesystem requirement is
    imposed here.

    Example:
        >>> selection = manager.resolve_digital_asset(asset_id)  # doctest: +SKIP
    """

    @override
    def select_replica(
        self,
        digital_asset_id: api.DigitalAssetID,
        *,
        preferred_store_ref: api.StoreUUID | None = None,
        mode: api.ReplicaMode = api.ReplicaMode.ACTIVE,
        require_verified: bool = False,
    ) -> api.ReplicaRecord:
        """
        Find the first eligible size-matching claim after ranking Store preference, recorded state,
        and Replica ID.

        The Asset is resolved first and candidate claims are captured for the requested mode. A
        preferred Store ranks ahead of state quality; otherwise VERIFIED, PRESENT, and UNVERIFIED
        rank in that order, then integer Replica ID breaks ties. The shared iterator compares mode
        by identity rather than coercing strings.

        Candidates outside those three states are skipped. require_verified additionally demands the
        VERIFIED enum singleton. Stat StorageError failures and size mismatches skip a candidate
        without changing its observation; other errors propagate. No digest comparison, fresh
        verification, open-read attempt, or version precondition is performed. Exhausting candidates
        raises NoReadableReplica, including when storage errors prevented selection.

        Example:
            >>> replica = manager.select_replica(asset_id, preferred_store_ref=store_uuid, require_verified=False)  # doctest: +SKIP


        :param digital_asset_id: Registered atomic Asset identity to select.
        :param preferred_store_ref: Optional Store UUID preferred over alternatives, without requiring an exact destination.
        :param mode: Operational Replica mode to select, defaulting to ACTIVE.
        :param require_verified: Whether a recorded VERIFIED state is required; selection does not itself recalculate digests.
        :return: First ranked candidate with acceptable recorded state and matching current stat size.
        """

        asset_record = self.get_digital_asset_record(digital_asset_id)
        candidates = list(
            self.iter_replica_records(
                digital_asset_id=digital_asset_id,
                mode=mode,
            )
        )
        state_rank = {
            api.ReplicaState.VERIFIED: 0,
            api.ReplicaState.PRESENT: 1,
            api.ReplicaState.UNVERIFIED: 2,
        }
        candidates.sort(
            key=lambda record: (
                record.location.store_ref != preferred_store_ref
                if preferred_store_ref is not None
                else False,
                state_rank.get(record.state, 99),
                int(record.replica_id),
            )
        )
        for record in candidates:
            if require_verified and record.state is not api.ReplicaState.VERIFIED:
                continue
            if record.state not in state_rank:
                continue
            try:
                info = self.stat(record.location)
            except api.StorageError:
                continue
            if info.size != asset_record.size_bytes:
                continue
            return record
        raise api.NoReadableReplica(
            f"Digital Asset {digital_asset_id} has no readable {mode.value} Replica."
        )

    @override
    def resolve_digital_asset(
        self,
        digital_asset_id: api.DigitalAssetID,
        *,
        preferred_store_ref: api.StoreUUID | None = None,
        mode: api.ReplicaMode = api.ReplicaMode.ACTIVE,
        require_verified: bool = False,
    ) -> api.DigitalAssetResolution:
        """
        Read the Asset record, select a Replica with the forwarded controls, and construct their
        resolution value.

        The default selector reads the Asset again. These reads are not one atomic snapshot; the
        resolution constructor checks only Asset-ID agreement. Selection errors propagate without
        byte reads or observation updates.

        Example:
            >>> selection = manager.resolve_digital_asset(asset_id, mode=ReplicaMode.ARCHIVE)  # doctest: +SKIP


        :param digital_asset_id: Registered atomic Asset identity to select.
        :param preferred_store_ref: Optional Store UUID preferred over alternatives, without requiring an exact destination.
        :param mode: Operational Replica mode to select, defaulting to ACTIVE.
        :param require_verified: Whether a recorded VERIFIED state is required; selection does not itself recalculate digests.
        :return: New resolution retaining the first Asset record and the selected Replica record.
        """

        return api.DigitalAssetResolution(
            self.get_digital_asset_record(digital_asset_id),
            self.select_replica(
                digital_asset_id,
                preferred_store_ref=preferred_store_ref,
                mode=mode,
                require_verified=require_verified,
            ),
        )

    @override
    def locate_replica(self, replica_id: api.ReplicaID) -> api.Location:
        """
        Return the Location from an exact Replica-record lookup without checking state, Store
        availability, or current bytes.

        Deleted or unhealthy claims still expose their retained Location. Unknown IDs raise
        ReplicaNotFound through get_replica_record.

        Example:
            >>> location = manager.locate_replica(replica_id)  # doctest: +SKIP


        :param replica_id: Registered Replica identity to project directly, without selection.
        :return: The exact Location retained by the resolved Replica record.
        """

        return self.get_replica_record(replica_id).location

    @override
    def materialize_digital_asset(
        self,
        digital_asset_id: api.DigitalAssetID,
        *,
        preferred_store_ref: api.StoreUUID | None = None,
        source_replica_id: api.ReplicaID | None = None,
        source_modes: Iterable[api.ReplicaMode | str] = (api.ReplicaMode.ACTIVE,),
        cache_store_ref: api.StoreUUID | None = None,
        verify: bool = True,
    ) -> api.DigitalAssetResolution:
        """
        Reuse a readable claim in the requested cache Store or select a source and optionally
        replicate it there.

        When a cache UUID is supplied, first resolve CACHE-mode claims preferring that Store and
        requiring recorded verification according to verify. Only NoReadableReplica is suppressed,
        and reuse requires the returned Location to match the requested Store exactly. A hit returns
        before validating source IDs or iterating source_modes.

        Otherwise select a source. Without a cache, verify requires recorded VERIFIED state and the
        source is returned without copying or hashing; it can be remote. With a cache, source
        selection does not require verified state, then replicate_digital_asset publishes a CACHE
        claim with the requested verification setting. That workflow can return an unhealthy record
        or raise after publication, and this method adds no rollback or final health check.

        Example:
            >>> selection = manager.materialize_digital_asset(asset_id, cache_store_ref=cache_uuid, verify=True)  # doctest: +SKIP


        :param digital_asset_id: Registered atomic Asset identity to make available through a selected copy.
        :param preferred_store_ref: Optional preference when searching source modes; an exact source ID takes precedence.
        :param source_replica_id: Exact source claim, or None to search the requested source modes.
        :param source_modes: Ordered source modes or enum-value strings, defaulting to ACTIVE only.
        :param cache_store_ref: Exact cache destination UUID, or None to return an eligible source without copying.
        :param verify: Whether a reused/no-copy selection must be recorded VERIFIED and a new cache copy is inspected after publication.
        :return: Resolution for an exact-cache hit, selected no-copy source, or resulting CACHE Replica.
        """

        if cache_store_ref is not None:
            try:
                cached = self.resolve_digital_asset(
                    digital_asset_id,
                    preferred_store_ref=cache_store_ref,
                    mode=api.ReplicaMode.CACHE,
                    require_verified=verify,
                )
            except api.NoReadableReplica:
                pass
            else:
                if cached.location.store_ref == cache_store_ref:
                    return cached

        source_record = self._select_materialization_source(
            digital_asset_id,
            preferred_store_ref=preferred_store_ref,
            source_replica_id=source_replica_id,
            source_modes=source_modes,
            require_verified=verify and cache_store_ref is None,
        )
        if cache_store_ref is None:
            return api.DigitalAssetResolution(
                self.get_digital_asset_record(digital_asset_id),
                source_record,
            )
        replica_record = self.replicate_digital_asset(
            digital_asset_id,
            destination_store_ref=cache_store_ref,
            source_replica_id=source_record.replica_id,
            mode=api.ReplicaMode.CACHE,
            verify=verify,
        )
        return api.DigitalAssetResolution(
            self.get_digital_asset_record(digital_asset_id),
            replica_record,
        )

    def _select_materialization_source(
        self,
        digital_asset_id: api.DigitalAssetID,
        *,
        preferred_store_ref: api.StoreUUID | None,
        source_replica_id: api.ReplicaID | None,
        source_modes: Iterable[api.ReplicaMode | str],
        require_verified: bool,
    ) -> api.ReplicaRecord:
        """
        Select an exact eligible Replica or search fully normalized source modes in caller order.

        An explicit ID is resolved first and must belong to the Asset, satisfy any VERIFIED-state
        requirement, and have a VERIFIED/PRESENT/UNVERIFIED state. Its stat StorageError becomes a
        chained NoReadableReplica; Asset lookup follows stat and the byte count must match. This
        branch ignores Store preference and does not consume or validate source_modes.

        Without an explicit ID, every source mode is materialized and converted to ReplicaMode
        before any selection. Empty input rejects; invalid later values also reject before an
        earlier valid mode is tried. Duplicate modes are removed in encounter order. Each mode
        delegates to select_replica, suppressing only NoReadableReplica before trying the next. No
        fresh digest verification occurs in either branch.

        Example:
            >>> source = manager._select_materialization_source(asset_id, preferred_store_ref=None, source_replica_id=None, source_modes=(ReplicaMode.ARCHIVE,), require_verified=False)  # doctest: +SKIP


        :param digital_asset_id: Expected owning Asset identity.
        :param preferred_store_ref: Store preference forwarded only during mode-based selection.
        :param source_replica_id: Exact claim to validate, or None to search source modes.
        :param source_modes: Ordered modes or enum-value strings, consumed eagerly only without an exact source ID.
        :param require_verified: Whether the selected record must carry the VERIFIED enum singleton.
        :return: Eligible source record; exhausted modes or ineligible explicit state/stat evidence raise NoReadableReplica.
        """

        if source_replica_id is not None:
            record = self.get_replica_record(source_replica_id)
            if record.digital_asset_id != digital_asset_id:
                raise api.StoragePreconditionFailed(
                    "source Replica belongs to another Digital Asset."
                )
            if require_verified and record.state is not api.ReplicaState.VERIFIED:
                raise api.NoReadableReplica(
                    f"Replica {source_replica_id} is not verified."
                )
            if record.state not in {
                api.ReplicaState.VERIFIED,
                api.ReplicaState.PRESENT,
                api.ReplicaState.UNVERIFIED,
            }:
                raise api.NoReadableReplica(
                    f"Replica {source_replica_id} is not currently readable."
                )
            try:
                info = self.stat(record.location)
            except api.StorageError as error:
                raise api.NoReadableReplica(
                    f"Replica {source_replica_id} is not currently readable."
                ) from error
            asset = self.get_digital_asset_record(digital_asset_id)
            if info.size != asset.size_bytes:
                raise api.NoReadableReplica(
                    f"Replica {source_replica_id} has the wrong size."
                )
            return record

        modes = tuple(
            mode if isinstance(mode, api.ReplicaMode) else api.ReplicaMode(mode)
            for mode in source_modes
        )
        if not modes:
            raise ValueError("source_modes must contain at least one Replica mode.")
        for mode in dict.fromkeys(modes):
            try:
                return self.select_replica(
                    digital_asset_id,
                    preferred_store_ref=preferred_store_ref,
                    mode=mode,
                    require_verified=require_verified,
                )
            except api.NoReadableReplica:
                continue
        rendered = ", ".join(mode.value for mode in dict.fromkeys(modes))
        raise api.NoReadableReplica(
            f"Digital Asset {digital_asset_id} has no readable Replica in "
            f"source modes: {rendered}."
        )

    @override
    def resolve_item_digital_asset(
        self,
        item_id: api.ItemID,
        *,
        role: str = "primary_payload",
        preferred_store_ref: api.StoreUUID | None = None,
        require_verified: bool = False,
    ) -> api.ItemDigitalAssetResolution:
        """
        Read an exact Item-role target under the lock, then resolve its Asset or Composite outside
        that lock.

        A missing target raises StorageManagementError without ID/role normalization. The
        digital_asset kind resolves an atomic target; every other stored kind follows the Composite
        branch. Both use the default ACTIVE selection mode and forward Store preference and
        recorded-verification requirements.

        Composite record lookup and member resolution occur separately, so later construction can
        reject inconsistent membership after concurrent changes. ItemDigitalAssetResolution
        validates the resulting target/relationship shape; this method adds no persistent read
        transaction, byte assembly, or exception suppression.

        Example:
            >>> selection = manager.resolve_item_digital_asset(item_id, role="primary_payload", require_verified=True)  # doctest: +SKIP


        :param item_id: Item identity whose stored role association is resolved.
        :param role: Exact role key, defaulting to primary_payload; no stripping or case normalization is implied.
        :param preferred_store_ref: Optional Store UUID preferred over alternatives, without requiring an exact destination.
        :param require_verified: Whether a recorded VERIFIED state is required; selection does not itself recalculate digests.
        :return: New Item-role resolution after all target/member lookups and result validation succeed.
        """

        with self._lock:
            target = self._item_targets.get((item_id, role))
        if target is None:
            raise api.StorageManagementError(
                f"Item {item_id} has no Digital Asset link for role {role!r}."
            )
        kind, target_id = target
        if kind == "digital_asset":
            return api.ItemDigitalAssetResolution(
                item_id,
                role,
                digital_asset_resolution=self.resolve_digital_asset(
                    api.DigitalAssetID(target_id),
                    preferred_store_ref=preferred_store_ref,
                    require_verified=require_verified,
                ),
            )
        composite_id = api.CompositeDigitalAssetID(target_id)
        record = self.get_composite_digital_asset_record(composite_id)
        members = self.resolve_composite_digital_asset(
            composite_id,
            preferred_store_ref=preferred_store_ref,
            require_verified=require_verified,
        )
        return api.ItemDigitalAssetResolution(
            item_id,
            role,
            composite_digital_asset_record=record,
            composite_member_resolutions=members,
        )


__all__ = ["DigitalAssetRetrievalMixin"]

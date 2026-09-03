"""
Replica selection and Digital Asset retrieval workflows.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import override

import LiuXin_alpha.storage.api as api
from LiuXin_alpha.storage.storage_manager.mixins._state import _StorageManagerState


class DigitalAssetRetrievalMixin(_StorageManagerState):
    """
    Select readable Replicas and expose Asset bytes to callers.

    Selection uses stable preference and observation rules, then delegates the
    actual read to the owning Store with version preconditions where available.
    Retrieval does not create content identities or silently repair unhealthy
    Replica metadata.
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
        Choose a currently readable Replica using stable preference rules.


        :param digital_asset_id:
        :param preferred_store_ref:
        :param mode:
        :param require_verified:
        :return:
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
        Pair a Digital Asset record with the selected readable Replica.


        :param digital_asset_id:
        :param preferred_store_ref:
        :param mode:
        :param require_verified:
        :return:
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
        Return the exact Location claimed by one Replica record.


        :param replica_id:
        :return:
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
        Return an existing readable copy or create one in the cache Store.

        Exact Replica selection permits materializing container members and
        unmanaged source bytes without pretending they are ACTIVE Replicas.


        :param digital_asset_id:
        :param preferred_store_ref:
        :param source_replica_id:
        :param source_modes:
        :param cache_store_ref:
        :param verify:
        :return:
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
        Select one readable source using exact identity or ordered modes.


        :param digital_asset_id:
        :param preferred_store_ref:
        :param source_replica_id:
        :param source_modes:
        :param require_verified:
        :return:
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
        Resolve one implementation-managed Item role link.


        :param item_id:
        :param role:
        :param preferred_store_ref:
        :param require_verified:
        :return:
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

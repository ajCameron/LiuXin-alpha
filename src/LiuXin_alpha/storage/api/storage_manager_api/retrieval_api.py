"""
Define Asset/Item selection, exact Location lookup, and cache materialization.

Retrieval returns domain selections and routing values rather than open readers.
Concrete implementations own ranking, observation limits, and publication behavior;
the default helpers only bind or delegate through this interface.
"""

import abc

from collections.abc import Iterable

from LiuXin_alpha.storage.api.models import Location, StoreUUID
from LiuXin_alpha.storage.api.storage_manager_api.location_factory import LocationFactory
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    DigitalAssetID,
    DigitalAssetResolution,
    ItemDigitalAssetResolution,
    ItemID,
    ReplicaRecord,
    ReplicaID,
    ReplicaMode,
)


class DigitalAssetRetrievalAPI(abc.ABC):
    """
    Resolve logical Assets and Item roles to recorded Replica selections and routing Locations.

    Resolution values retain expected identity and chosen-copy context without owning open readers.
    Selection establishes only the evidence checked by an implementation at that time.
    Materialization can return a source directly or publish a cache copy; neither the interface nor
    a Location implies local filesystem storage.

    Example:
        >>> selection = manager.resolve_digital_asset(asset_id, require_verified=True)  # doctest: +SKIP
    """

    @property
    def location_factory(self) -> LocationFactory:
        """
        Create a fresh catalogue-aware LocationFactory retaining this manager reference.

        The factory is not cached and construction adds no repository lookup, Store probe, or
        resource ownership. Its later calls delegate selection to the retained manager.

        Example:
            >>> location = manager.location_factory.from_id(asset_id)  # doctest: +SKIP


        :return: New LocationFactory bound to this manager.
        """

        return LocationFactory(self)

    @abc.abstractmethod
    def select_replica(
        self,
        digital_asset_id: DigitalAssetID,
        *,
        preferred_store_ref: StoreUUID | None = None,
        mode: ReplicaMode = ReplicaMode.ACTIVE,
        require_verified: bool = False,
    ) -> ReplicaRecord:
        """
        Choose an eligible Replica for the requested Asset and mode, applying an optional Store
        preference.

        A known Asset without a suitable selection raises NoReadableReplica. The preferred Store is
        a ranking preference rather than an exclusive filter. Requiring recorded verification does
        not itself request fresh hashing or an open reader.

        Example:
            >>> replica = manager.select_replica(asset_id, preferred_store_ref=store_uuid)  # doctest: +SKIP


        :param digital_asset_id: Registered atomic Asset identity to select.
        :param preferred_store_ref: Optional Store UUID preferred over alternatives, without requiring an exact destination.
        :param mode: Operational Replica mode to select, defaulting to ACTIVE.
        :param require_verified: Whether a recorded VERIFIED state is required; selection does not itself recalculate digests.
        :return: Selected Replica record, subject to the implementation's observation and ranking rules.
        """
        ...

    @abc.abstractmethod
    def resolve_digital_asset(
        self,
        digital_asset_id: DigitalAssetID,
        *,
        preferred_store_ref: StoreUUID | None = None,
        mode: ReplicaMode = ReplicaMode.ACTIVE,
        require_verified: bool = False,
    ) -> DigitalAssetResolution:
        """
        Pair the requested Asset record with one eligible Replica using the selection controls.

        The resulting value retains identity context for the chosen Location; it does not open
        storage or guarantee the copy remains available after selection.

        Example:
            >>> selection = manager.resolve_digital_asset(asset_id, require_verified=True)  # doctest: +SKIP


        :param digital_asset_id: Registered atomic Asset identity to select.
        :param preferred_store_ref: Optional Store UUID preferred over alternatives, without requiring an exact destination.
        :param mode: Operational Replica mode to select, defaulting to ACTIVE.
        :param require_verified: Whether a recorded VERIFIED state is required; selection does not itself recalculate digests.
        :return: Asset/Replica resolution value for the selected copy.
        """
        ...

    def locate_digital_asset(
        self,
        digital_asset_id: DigitalAssetID,
        *,
        preferred_store_ref: StoreUUID | None = None,
        mode: ReplicaMode = ReplicaMode.ACTIVE,
        require_verified: bool = False,
    ) -> "Location":
        """
        Delegate to resolve_digital_asset with every selection option, then project its Location.

        Dynamic overrides remain authoritative. This convenience method adds no validation,
        selection cache, exception translation, or reader ownership.

        Example:
            >>> location = manager.locate_digital_asset(asset_id, mode=ReplicaMode.ARCHIVE)  # doctest: +SKIP


        :param digital_asset_id: Registered atomic Asset identity to select.
        :param preferred_store_ref: Optional Store UUID preferred over alternatives, without requiring an exact destination.
        :param mode: Operational Replica mode to select, defaulting to ACTIVE.
        :param require_verified: Whether a recorded VERIFIED state is required; selection does not itself recalculate digests.
        :return: Location exposed by the returned Asset resolution; resolution errors propagate.
        """

        return self.resolve_digital_asset(
            digital_asset_id,
            preferred_store_ref=preferred_store_ref,
            mode=mode,
            require_verified=require_verified,
        ).location

    @abc.abstractmethod
    def locate_replica(self, replica_id: ReplicaID) -> "Location":
        """
        Project the concrete Location for an exact Replica identity.

        This direct lookup need not select a readable state or verify current bytes; callers
        requiring Asset-level selection should use resolve_digital_asset.

        Example:
            >>> location = manager.locate_replica(replica_id)  # doctest: +SKIP


        :param replica_id: Exact registered Replica identity whose claimed Location is requested.
        :return: Claimed Location for that Replica; unknown identities raise through the implementation.
        """
        ...

    @abc.abstractmethod
    def materialize_digital_asset(
        self,
        digital_asset_id: DigitalAssetID,
        *,
        preferred_store_ref: StoreUUID | None = None,
        source_replica_id: ReplicaID | None = None,
        source_modes: Iterable[ReplicaMode | str] = (ReplicaMode.ACTIVE,),
        cache_store_ref: StoreUUID | None = None,
        verify: bool = True,
    ) -> DigitalAssetResolution:
        """
        Reuse an eligible cache copy, return a selected source, or publish a new copy to a requested
        cache Store.

        Without a cache destination, returning the source does not make it local. Exact source
        selection permits archive or unmanaged claims; otherwise source_modes are considered in
        order and default to ACTIVE only. Reusing an existing requested-cache selection can avoid
        source selection entirely.

        Verification can require recorded state for reuse and inspect a newly published copy.
        Publication and later metadata/verification failures need not roll back bytes; callers
        should inspect the resulting Replica observation.

        Example:
            >>> selection = manager.materialize_digital_asset(asset_id, source_modes=(ReplicaMode.ARCHIVE,), cache_store_ref=cache_uuid)  # doctest: +SKIP


        :param digital_asset_id: Registered atomic Asset identity to make available through a selected copy.
        :param preferred_store_ref: Optional preference when searching source modes; an exact source ID takes precedence.
        :param source_replica_id: Exact source claim, or None to search the requested source modes.
        :param source_modes: Ordered source modes or enum-value strings, defaulting to ACTIVE only.
        :param cache_store_ref: Exact cache destination UUID, or None to return an eligible source without copying.
        :param verify: Whether a reused/no-copy selection must be recorded VERIFIED and a new cache copy is inspected after publication.
        :return: Resolution of a reused source/cache claim or a newly published cache claim.
        """
        ...

    @abc.abstractmethod
    def resolve_item_digital_asset(
        self,
        item_id: ItemID,
        *,
        role: str = "primary_payload",
        preferred_store_ref: StoreUUID | None = None,
        require_verified: bool = False,
    ) -> ItemDigitalAssetResolution:
        """
        Resolve an exact Item-role association to an atomic selection or a Composite with its member
        selections.

        Target lookup and member selection can fail separately. The result retains relationship
        metadata and routing choices without reading or assembling the complete payload.

        Example:
            >>> selection = manager.resolve_item_digital_asset(item_id, role="cover")  # doctest: +SKIP


        :param item_id: Item identity whose stored role association is resolved.
        :param role: Exact role key, defaulting to primary_payload; no stripping or case normalization is implied.
        :param preferred_store_ref: Optional Store UUID preferred over alternatives, without requiring an exact destination.
        :param require_verified: Whether a recorded VERIFIED state is required; selection does not itself recalculate digests.
        :return: Item-role resolution containing one atomic target or a Composite and its resolved members.
        """
        ...


__all__ = ["DigitalAssetRetrievalAPI"]

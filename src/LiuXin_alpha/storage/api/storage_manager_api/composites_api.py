"""Composite Digital Asset domain facade."""

import abc

from collections.abc import Iterator

from LiuXin_alpha.storage.api.models import StoreRef
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    CompositeAssetHealth,
    CompositeDigitalAsset,
    CompositeDigitalAssetID,
    CompositeDigitalAssetSpec,
    ResolvedCompositeMember,
)


class CompositeAssetAPI(abc.ABC):
    """Operations over ordered logical assemblies of atomic Assets.

    Composite Assets do not directly contain bytes or own Replicas. Resolution
    preserves each membership relationship and pairs it with a selected atomic
    Asset and Replica.

    Example:
        >>> members = manager.resolve_composite_digital_asset(  # doctest: +SKIP
        ...     CompositeDigitalAssetID(3),
        ... )
    """

    @abc.abstractmethod
    def declare_composite_digital_asset(
        self,
        spec: CompositeDigitalAssetSpec,
    ) -> CompositeDigitalAsset:
        """Register a new logical assembly and its ordered memberships.

        Example:
            >>> composite = manager.declare_composite_digital_asset(spec)  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def get_composite_digital_asset(
        self,
        composite_id: CompositeDigitalAssetID,
    ) -> CompositeDigitalAsset:
        """Return one Composite snapshot or raise ``CompositeAssetNotFound``.

        Example:
            >>> composite = manager.get_composite_digital_asset(  # doctest: +SKIP
            ...     CompositeDigitalAssetID(3),
            ... )
        """
        ...

    @abc.abstractmethod
    def replace_composite_digital_asset(
        self,
        composite_id: CompositeDigitalAssetID,
        spec: CompositeDigitalAssetSpec,
        *,
        if_revision: str | None = None,
    ) -> CompositeDigitalAsset:
        """Replace Composite metadata and membership atomically in metadata.

        Example:
            >>> composite = manager.replace_composite_digital_asset(  # doctest: +SKIP
            ...     CompositeDigitalAssetID(3), spec, if_revision="v2",
            ... )
        """
        ...

    @abc.abstractmethod
    def iter_composite_digital_assets(
        self,
    ) -> Iterator[CompositeDigitalAsset]:
        """Iterate over known Composite domain snapshots.

        Example:
            >>> composites = list(manager.iter_composite_digital_assets())  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def forget_composite_digital_asset(
        self,
        composite_id: CompositeDigitalAssetID,
        *,
        require_unlinked: bool = True,
        if_revision: str | None = None,
    ) -> bool:
        """Forget a Composite identity without deleting member Assets.

        Example:
            >>> forgotten = manager.forget_composite_digital_asset(  # doctest: +SKIP
            ...     CompositeDigitalAssetID(3), require_unlinked=True,
            ... )
        """
        ...

    @abc.abstractmethod
    def resolve_composite_digital_asset(
        self,
        composite_id: CompositeDigitalAssetID,
        *,
        preferred_store: StoreRef | None = None,
        require_verified: bool = False,
    ) -> tuple[ResolvedCompositeMember, ...]:
        """Resolve required members without discarding names, paths or roles.

        Required members without readable copies raise ``CompositeIncomplete``.

        Example:
            >>> resolved = manager.resolve_composite_digital_asset(  # doctest: +SKIP
            ...     CompositeDigitalAssetID(3), require_verified=True,
            ... )
        """
        ...

    @abc.abstractmethod
    def assess_composite_digital_asset(
        self,
        composite_id: CompositeDigitalAssetID,
    ) -> CompositeAssetHealth:
        """Assess membership completeness and current readability.

        Example:
            >>> health = manager.assess_composite_digital_asset(  # doctest: +SKIP
            ...     CompositeDigitalAssetID(3),
            ... )
        """
        ...


__all__ = ["CompositeAssetAPI"]

"""
Define Item-role associations to atomic or Composite Asset identities.

Links select at most one target for each exact Item-role pair. These metadata
operations do not select Replicas, publish bytes, or control Item lifecycle.
"""

from __future__ import annotations

import abc

from LiuXin_alpha.storage.api.storage_manager_api.models import (
    CompositeDigitalAssetID,
    DigitalAssetID,
    ItemID,
)


class ItemDigitalAssetLinkAPI(abc.ABC):
    """
    Set or remove the atomic/Composite target selected by an Item-role pair.

    One exact pair selects at most one target. Linking replaces the pair's previous target and
    changes association metadata rather than Item fields, target identity, or Replica bytes.
    Implementations own reference validation and persistence.

    Example:
        >>> manager.link_item_to_digital_asset(item_id, asset_id, role="cover")  # doctest: +SKIP
    """

    @abc.abstractmethod
    def link_item_to_digital_asset(
        self,
        item_id: "ItemID",
        digital_asset_id: "DigitalAssetID",
        *,
        role: str = "primary_payload",
    ) -> None:
        """
        Set an Item role to a known atomic Asset, replacing any previous target for that pair
        without requiring readable bytes.

        Example:
            >>> manager.link_item_to_digital_asset(item_id, asset_id, role="cover")  # doctest: +SKIP

        :param item_id: Library Item identity whose role association is updated.
        :param digital_asset_id: Registered atomic Asset selected by this role.
        :param role: Exact Item-role key, defaulting to primary_payload; spelling and whitespace are significant.

        :return: None after the association is stored.
        """
        ...

    @abc.abstractmethod
    def link_item_to_composite_digital_asset(
        self,
        item_id: ItemID,
        composite_digital_asset_id: CompositeDigitalAssetID,
        *,
        role: str = "primary_payload",
    ) -> None:
        """
        Set an Item role to a known Composite, replacing any previous target without resolving
        member availability.

        Example:
            >>> manager.link_item_to_composite_digital_asset(item_id, composite_id)  # doctest: +SKIP


        :param item_id: Library Item identity whose role association is updated.
        :param composite_digital_asset_id: Registered Composite selected by this role.
        :param role: Exact Item-role key, defaulting to primary_payload; spelling and whitespace are significant.
        :return: None after the association is stored.
        """
        ...

    @abc.abstractmethod
    def unlink_item_digital_asset(
        self,
        item_id: ItemID,
        *,
        role: str = "primary_payload",
    ) -> bool:
        """
        Remove one exact Item-role association without deleting the target Asset, Composite, or any
        physical bytes.

        Example:
            >>> removed = manager.unlink_item_digital_asset(item_id, role="cover")  # doctest: +SKIP


        :param item_id: Item identity identifying the association to remove.
        :param role: Exact Item-role key, defaulting to primary_payload; spelling and whitespace are significant.
        :return: True when an association existed and was removed, otherwise False.
        """
        ...


__all__ = ["ItemDigitalAssetLinkAPI"]

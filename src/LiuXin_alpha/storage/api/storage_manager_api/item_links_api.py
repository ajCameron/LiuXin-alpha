"""
Item-to-Digital-Asset association facade.
"""

import abc

from LiuXin_alpha.storage.api.storage_manager_api.models import (
    CompositeDigitalAssetID,
    DigitalAssetID,
    ItemID,
)


class ItemDigitalAssetLinkAPI(abc.ABC):
    """
    Create and remove the Item-role links used by asset retrieval.

    Example:
        >>> manager.link_item_to_digital_asset(  # doctest: +SKIP
        ...     ItemID(9), DigitalAssetID(7), role="cover",
        ... )
    """

    @abc.abstractmethod
    def link_item_to_digital_asset(
        self,
        item_id: ItemID,
        digital_asset_id: DigitalAssetID,
        *,
        role: str = "primary_payload",
    ) -> None:
        """
        Link one Item role to an atomic Digital Asset.

        Example:
            >>> manager.link_item_to_digital_asset(  # doctest: +SKIP
            ...     ItemID(9), DigitalAssetID(7), role="cover",
            ... )


        :param item_id:
        :param digital_asset_id:
        :param role:
        :return:
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
        Link one Item role to a Composite Digital Asset.

        Example:
            >>> manager.link_item_to_composite_digital_asset(  # doctest: +SKIP
            ...     ItemID(9), CompositeDigitalAssetID(3),
            ... )


        :param item_id:
        :param composite_digital_asset_id:
        :param role:
        :return:
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
        Remove an Item-role association and report whether it existed.

        Example:
            >>> manager.unlink_item_digital_asset(ItemID(9))  # doctest: +SKIP
            True


        :param item_id:
        :param role:
        :return:
        """
        ...


__all__ = ["ItemDigitalAssetLinkAPI"]

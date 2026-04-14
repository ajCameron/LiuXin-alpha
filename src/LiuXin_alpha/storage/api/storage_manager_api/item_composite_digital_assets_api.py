"""Item <-> composite digital asset link methods for the storage manager."""

from __future__ import annotations

import abc
from collections.abc import Iterator
from typing import TYPE_CHECKING, Union, Literal

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api.info_containers_api import CompositeDigitalAssetItemLinkRow
    from LiuXin_alpha.storage.storage_types import CompositeDigitalAssetID, CompositeDigitalAssetItemLinkID, ItemID, DigitalAssetID


class ItemCompositeDigitalAssetsManagerAPI(abc.ABC):
    """Access and update semantic links between items and composite digital assets."""

    @abc.abstractmethod
    def create_composite_digital_asset(
            self,
            composite_assets: list["DigitalAssetID"],
    ) -> "CompositeDigitalAssetID":
        """
        Create a new composite digital asset.

        :return:
        """

    @abc.abstractmethod
    def create_item_composite_digital_asset_link_from_row(
        self,
        link: "CompositeDigitalAssetItemLinkRow",
    ) -> "CompositeDigitalAssetItemLinkRow":
        """
        Write an item -> composite digital asset row out to the database.

        :param link:
        :return:
        """

    @abc.abstractmethod
    def create_item_composite_digital_asset_link(
            self,
            item_id: int,
            composite_asset_id: int,
            priority: Union[Literal["highest"], Literal["lowest"], int],
            link_type: str,
            origin: str,
            primary: bool = False
    ):
        """
        Link an item to a composite digital asset.

        :param item_id:
        :param composite_asset_id:
        :param priority:
        :param link_type:
        :param origin:
        :param primary:
        :return:
        """

    @abc.abstractmethod
    def get_item_composite_digital_asset_link_from_id(
        self,
        composite_digital_asset_item_link_id: "CompositeDigitalAssetItemLinkID",
    ) -> "CompositeDigitalAssetItemLinkRow":
        """
        Get the item -> composite digital asset link from the database by the link ID.

        :param composite_digital_asset_item_link_id:
        :return:
        """

    @abc.abstractmethod
    def update_item_composite_digital_asset_link(
        self,
        link: "CompositeDigitalAssetItemLinkRow",
    ) -> "CompositeDigitalAssetItemLinkRow":
        """
        Write an updated link row out to the database.

        :param link:
        :return:
        """

    @abc.abstractmethod
    def delete_item_composite_digital_asset_link(
        self,
        composite_digital_asset_item_link_id: "CompositeDigitalAssetItemLinkID",
    ) -> bool:
        """
        Delete a item -> composite digital asset link from the database by ID.

        :param composite_digital_asset_item_link_id:
        :return:
        """

    @abc.abstractmethod
    def iter_item_composite_digital_asset_links(
        self,
        item_id: "ItemID",
    ) -> Iterator["CompositeDigitalAssetItemLinkRow"]:
        """
        Iter over the item -> composite digital asset links from the database.

        :param item_id:
        :return:
        """

    @abc.abstractmethod
    def iter_composite_digital_asset_item_links(
        self,
        composite_digital_asset_id: "CompositeDigitalAssetID",
    ) -> Iterator["CompositeDigitalAssetItemLinkRow"]:
        """
        Iter over the composite digital asset -> item links from the database.

        :param composite_digital_asset_id:
        :return:
        """

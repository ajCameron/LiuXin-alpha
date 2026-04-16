"""Item <-> digital asset link methods for the storage manager."""

from __future__ import annotations

import abc
from collections.abc import Iterator
from typing import TYPE_CHECKING, Literal, Union



if TYPE_CHECKING:
    from LiuXin_alpha.storage.api.info_containers_api import DigitalAssetItemLinkRow
    from LiuXin_alpha.storage.storage_types import DigitalAssetID, DigitalAssetItemLinkID, ItemID


class ItemDigitalAssetsManagerAPI(abc.ABC):
    """Access and update semantic links between items and atomic digital assets."""

    @abc.abstractmethod
    def create_item_digital_asset_link_from_row(
        self,
        link: "DigitalAssetItemLinkRow",
    ) -> "DigitalAssetItemLinkRow":
        """
        Write an Item -> Digital Asset link from an instructional asset_replica row.

        You could also create the row, and then call [row].sync()
        :param link:
        :return:
        """

    @abc.abstractmethod
    def create_item_digital_asset_link(
            self,
            item_id: int,
            asset_id: DigitalAssetID,
            priority: Union[Literal["highest"], Literal["lowest"], int],
            link_type: str,
            origin: str,
            primary: bool = False
            ):
        """
        We're linking an item to an atomic digital asset.

        :param item_id:
        :param asset_id:
        :param priority:
        :param link_type:
        :param origin:
        :param primary:
        :return:
        """

    @abc.abstractmethod
    def get_item_digital_asset_link(
        self,
        digital_asset_item_link_id: "DigitalAssetItemLinkID",
    ) -> "DigitalAssetItemLinkRow":
        """
        Return the Item -> Digital Asset link from the link ID.

        :param digital_asset_item_link_id:
        :return:
        """

    @abc.abstractmethod
    def update_item_digital_asset_link(
        self,
        link: "DigitalAssetItemLinkRow",
    ) -> "DigitalAssetItemLinkRow":
        """
        Write changes to the digital asset out to the database.

        :param link:
        :return:
        """

    @abc.abstractmethod
    def delete_item_digital_asset_link(self, digital_asset_item_link_id: "DigitalAssetItemLinkID") -> bool:
        """
        Break a link between an item and a digital asset by the link id.

        :param digital_asset_item_link_id:
        :return:
        """

    @abc.abstractmethod
    def iter_item_digital_asset_links(self, item_id: "ItemID") -> Iterator["DigitalAssetItemLinkRow"]:
        """
        Iter over all the link rows between an item and a digital asset.

        :param item_id:
        :return:
        """

    @abc.abstractmethod
    def iter_digital_asset_item_links(self, digital_asset_id: "DigitalAssetID") -> Iterator["DigitalAssetItemLinkRow"]:
        """
        Iter over all the assets linked to a digitial item.

        :param digital_asset_id:
        :return:
        """

"""Item <-> digital asset link methods for the storage manager.

Examples:
    Find the atomic assets attached to an item::

        links = list(manager.iter_item_digital_asset_links(item_id=7))
"""

from __future__ import annotations

import abc
from collections.abc import Iterator
from typing import TYPE_CHECKING, Literal, Union



if TYPE_CHECKING:
    from LiuXin_alpha.storage.api.info_containers_api import DigitalAssetItemLinkRow
    from LiuXin_alpha.storage.storage_types import DigitalAssetID, DigitalAssetItemLinkID, ItemID


class ItemDigitalAssetsManagerAPI(abc.ABC):
    """Access and update semantic links between items and atomic digital assets.

    Examples:
        Link item ``7`` to asset ``42``::

            link = manager.create_item_digital_asset_link(
                item_id=7,
                asset_id=42,
                priority="highest",
                link_type="primary_payload",
                origin="import",
                primary=True,
            )
    """

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

        Examples:
            Persist an already populated row::

                link = manager.create_item_digital_asset_link_from_row(link_row)
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

        Examples:
            Attach a cover asset with explicit priority::

                link = manager.create_item_digital_asset_link(
                    7, 42, 10, "cover", "scanner", primary=False
                )
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

        Examples:
            Retrieve link ``5``::

                link = manager.get_item_digital_asset_link(5)
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

        Examples:
            Save changes to a link row::

                link["digital_asset_item_link_primary"] = 1
                link = manager.update_item_digital_asset_link(link)
        """

    @abc.abstractmethod
    def delete_item_digital_asset_link(self, digital_asset_item_link_id: "DigitalAssetItemLinkID") -> bool:
        """
        Break a link between an item and a digital asset by the link id.

        :param digital_asset_item_link_id:
        :return:

        Examples:
            Remove link ``5``::

                removed = manager.delete_item_digital_asset_link(5)
        """

    @abc.abstractmethod
    def iter_item_digital_asset_links(self, item_id: "ItemID") -> Iterator["DigitalAssetItemLinkRow"]:
        """
        Iterate over all the link rows between an item and a digital asset.

        :param item_id:
        :return:

        Examples:
            List every asset attached to item ``7``::

                links = list(manager.iter_item_digital_asset_links(7))
        """

    @abc.abstractmethod
    def iter_digital_asset_item_links(self, digital_asset_id: "DigitalAssetID") -> Iterator["DigitalAssetItemLinkRow"]:
        """
        Iterate over all item links for a digital asset.

        :param digital_asset_id:
        :return:

        Examples:
            Find every item using asset ``42``::

                links = list(manager.iter_digital_asset_item_links(42))
        """

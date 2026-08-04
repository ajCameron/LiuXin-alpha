"""Item <-> composite digital asset link methods for the storage manager.

Examples:
    Inspect the multipart assets attached to an item::

        links = list(manager.iter_item_composite_digital_asset_links(7))
"""

from __future__ import annotations

import abc
from collections.abc import Iterator
from typing import TYPE_CHECKING, Union, Literal

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api.info_containers_api import CompositeDigitalAssetItemLinkRow
    from LiuXin_alpha.storage.storage_types import CompositeDigitalAssetID, CompositeDigitalAssetItemLinkID, ItemID, DigitalAssetID


class ItemCompositeDigitalAssetsManagerAPI(abc.ABC):
    """Access and update semantic links between items and composite digital assets.

    Examples:
        Build a composite from ordered atomic asset ids::

            composite_id = manager.create_composite_digital_asset([41, 42])
    """

    @abc.abstractmethod
    def create_composite_digital_asset(
            self,
            composite_assets: list["DigitalAssetID"],
    ) -> "CompositeDigitalAssetID":
        """
        Create a new composite digital asset.

        :return:

        Examples:
            Create a two-part composite in playback order::

                composite_id = manager.create_composite_digital_asset([41, 42])
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

        Examples:
            Persist an already populated link row::

                link = manager.create_item_composite_digital_asset_link_from_row(link_row)
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

        Examples:
            Attach a multipart audiobook to an item::

                link = manager.create_item_composite_digital_asset_link(
                    7, 12, "highest", "primary_payload", "import", primary=True
                )
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

        Examples:
            Retrieve link ``8``::

                link = manager.get_item_composite_digital_asset_link_from_id(8)
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

        Examples:
            Save a changed link::

                link["composite_digital_asset_item_link_primary"] = 1
                link = manager.update_item_composite_digital_asset_link(link)
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

        Examples:
            Remove link ``8``::

                removed = manager.delete_item_composite_digital_asset_link(8)
        """

    @abc.abstractmethod
    def iter_item_composite_digital_asset_links(
        self,
        item_id: "ItemID",
    ) -> Iterator["CompositeDigitalAssetItemLinkRow"]:
        """
        Iterate over the item -> composite digital asset links from the database.

        :param item_id:
        :return:

        Examples:
            List the composites attached to item ``7``::

                links = list(manager.iter_item_composite_digital_asset_links(7))
        """

    @abc.abstractmethod
    def iter_composite_digital_asset_item_links(
        self,
        composite_digital_asset_id: "CompositeDigitalAssetID",
    ) -> Iterator["CompositeDigitalAssetItemLinkRow"]:
        """
        Iterate over the composite digital asset -> item links from the database.

        :param composite_digital_asset_id:
        :return:

        Examples:
            Find every item using composite ``12``::

                links = list(manager.iter_composite_digital_asset_item_links(12))
        """

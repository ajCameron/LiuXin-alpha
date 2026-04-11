"""Item <-> digital asset link methods for the storage manager."""

from __future__ import annotations

import abc
from collections.abc import Iterator
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api.info_containers_api import ItemDigitalAssetLinkRecord
    from LiuXin_alpha.storage.storage_types import DigitalAssetID, ItemDigitalAssetLinkID, ItemID


class ItemDigitalAssetsManagerAPI(abc.ABC):
    """Access and update semantic links between items and digital assets."""

    @abc.abstractmethod
    def create_item_digital_asset_link(
        self,
        link: "ItemDigitalAssetLinkRecord",
    ) -> "ItemDigitalAssetLinkRecord":
        ...

    @abc.abstractmethod
    def get_item_digital_asset_link(
        self,
        item_digital_asset_link_id: "ItemDigitalAssetLinkID",
    ) -> "ItemDigitalAssetLinkRecord":
        ...

    @abc.abstractmethod
    def update_item_digital_asset_link(
        self,
        link: "ItemDigitalAssetLinkRecord",
    ) -> "ItemDigitalAssetLinkRecord":
        ...

    @abc.abstractmethod
    def delete_item_digital_asset_link(self, item_digital_asset_link_id: "ItemDigitalAssetLinkID") -> bool:
        ...

    @abc.abstractmethod
    def iter_item_digital_asset_links(self, item_id: "ItemID") -> Iterator["ItemDigitalAssetLinkRecord"]:
        ...

    @abc.abstractmethod
    def iter_digital_asset_item_links(self, digital_asset_id: "DigitalAssetID") -> Iterator["ItemDigitalAssetLinkRecord"]:
        ...

"""Item <-> composite digital asset link methods for the storage manager."""

from __future__ import annotations

import abc
from collections.abc import Iterator
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api.info_containers_api import CompositeDigitalAssetItemLinkRow
    from LiuXin_alpha.storage.storage_types import CompositeDigitalAssetID, CompositeDigitalAssetItemLinkID, ItemID


class ItemCompositeDigitalAssetsManagerAPI(abc.ABC):
    """Access and update semantic links between items and composite digital assets."""

    @abc.abstractmethod
    def create_item_composite_digital_asset_link(
        self,
        link: "CompositeDigitalAssetItemLinkRow",
    ) -> "CompositeDigitalAssetItemLinkRow":
        ...

    @abc.abstractmethod
    def get_item_composite_digital_asset_link(
        self,
        composite_digital_asset_item_link_id: "CompositeDigitalAssetItemLinkID",
    ) -> "CompositeDigitalAssetItemLinkRow":
        ...

    @abc.abstractmethod
    def update_item_composite_digital_asset_link(
        self,
        link: "CompositeDigitalAssetItemLinkRow",
    ) -> "CompositeDigitalAssetItemLinkRow":
        ...

    @abc.abstractmethod
    def delete_item_composite_digital_asset_link(
        self,
        composite_digital_asset_item_link_id: "CompositeDigitalAssetItemLinkID",
    ) -> bool:
        ...

    @abc.abstractmethod
    def iter_item_composite_digital_asset_links(
        self,
        item_id: "ItemID",
    ) -> Iterator["CompositeDigitalAssetItemLinkRow"]:
        ...

    @abc.abstractmethod
    def iter_composite_digital_asset_item_links(
        self,
        composite_digital_asset_id: "CompositeDigitalAssetID",
    ) -> Iterator["CompositeDigitalAssetItemLinkRow"]:
        ...

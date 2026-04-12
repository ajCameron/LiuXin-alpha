"""Composite digital asset access/update methods for the storage manager."""

from __future__ import annotations

import abc
from collections.abc import Iterator
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api.info_containers_api import CompositeDigitalAssetRecord
    from LiuXin_alpha.storage.storage_types import CompositeDigitalAssetID


class CompositeDigitalAssetsManagerAPI(abc.ABC):
    """CRUD access for logical multipart composite digital assets."""

    @abc.abstractmethod
    def create_composite_digital_asset(
        self,
        composite_digital_asset: "CompositeDigitalAssetRecord",
    ) -> "CompositeDigitalAssetRecord":
        ...

    @abc.abstractmethod
    def get_composite_digital_asset(
        self,
        composite_digital_asset_id: "CompositeDigitalAssetID",
    ) -> "CompositeDigitalAssetRecord":
        ...

    @abc.abstractmethod
    def update_composite_digital_asset(
        self,
        composite_digital_asset: "CompositeDigitalAssetRecord",
    ) -> "CompositeDigitalAssetRecord":
        ...

    @abc.abstractmethod
    def delete_composite_digital_asset(self, composite_digital_asset_id: "CompositeDigitalAssetID") -> bool:
        ...

    @abc.abstractmethod
    def iter_composite_digital_assets(self) -> Iterator["CompositeDigitalAssetRecord"]:
        ...

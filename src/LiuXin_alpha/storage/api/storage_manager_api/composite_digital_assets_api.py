"""Composite digital asset access/update methods for the storage manager."""

from __future__ import annotations

import abc
from collections.abc import Iterator
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api.info_containers_api import CompositeDigitalAssetRow
    from LiuXin_alpha.storage.storage_types import CompositeDigitalAssetID


class CompositeDigitalAssetsDBManagerAPI(abc.ABC):
    """CRUD access for logical multipart composite digital assets."""

    @abc.abstractmethod
    def create_composite_digital_asset(
        self,
        composite_digital_asset: "CompositeDigitalAssetRow",
    ) -> "CompositeDigitalAssetRow":
        """
        Write a composite digital asset out to the database.

        :param composite_digital_asset:
        :return:
        """

    @abc.abstractmethod
    def get_composite_digital_asset(
        self,
        composite_digital_asset_id: "CompositeDigitalAssetID",
    ) -> "CompositeDigitalAssetRow":
        """
        Retrieve a composite digital asset out from the database.

        :param composite_digital_asset_id:
        :return:
        """

    @abc.abstractmethod
    def update_composite_digital_asset(
        self,
        composite_digital_asset: "CompositeDigitalAssetRow",
    ) -> "CompositeDigitalAssetRow":
        """
        Update a composite digital asset out to the database.

        :param composite_digital_asset:
        :return:
        """

    @abc.abstractmethod
    def delete_composite_digital_asset(self, composite_digital_asset_id: "CompositeDigitalAssetID") -> bool:
        """
        Remove a composite digital asset out from the database.

        :param composite_digital_asset_id:
        :return:
        """

    @abc.abstractmethod
    def iter_composite_digital_assets(self) -> Iterator["CompositeDigitalAssetRow"]:
        """
        Iter over the entire composite digital asset table.

        :return:
        """

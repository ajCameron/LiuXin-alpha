"""Composite digital asset access/update methods for the storage manager.

Examples:
    Iterate over multipart assets through a concrete manager::

        composites = list(manager.iter_composite_digital_assets())
"""

from __future__ import annotations

import abc
from collections.abc import Iterator
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api.info_containers_api import CompositeDigitalAssetRow
    from LiuXin_alpha.storage.storage_types import CompositeDigitalAssetID


class CompositeDigitalAssetsDBManagerAPI(abc.ABC):
    """CRUD access for logical multipart composite digital assets.

    Examples:
        Fetch an audiobook-like composite by id::

            composite = manager.get_composite_digital_asset(12)
    """

    @abc.abstractmethod
    def create_composite_digital_asset(
        self,
        composite_digital_asset: "CompositeDigitalAssetRow",
    ) -> "CompositeDigitalAssetRow":
        """
        Write a composite digital asset out to the database.

        :param composite_digital_asset:
        :return:

        Examples:
            Persist a newly constructed row::

                composite = manager.create_composite_digital_asset(row)
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

        Examples:
            Retrieve composite ``12``::

                composite = manager.get_composite_digital_asset(12)
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

        Examples:
            Save changes made to a row::

                composite = manager.update_composite_digital_asset(composite)
        """

    @abc.abstractmethod
    def delete_composite_digital_asset(self, composite_digital_asset_id: "CompositeDigitalAssetID") -> bool:
        """
        Remove a composite digital asset out from the database.

        :param composite_digital_asset_id:
        :return:

        Examples:
            Delete composite ``12``::

                removed = manager.delete_composite_digital_asset(12)
        """

    @abc.abstractmethod
    def iter_composite_digital_assets(self) -> Iterator["CompositeDigitalAssetRow"]:
        """
        Iterate over the entire composite digital asset table.

        :return:

        Examples:
            Take a snapshot of all composites::

                composites = list(manager.iter_composite_digital_assets())
        """

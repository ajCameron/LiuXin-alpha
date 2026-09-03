"""
Item-to-Asset link management for the storage manager.
"""

from __future__ import annotations

from typing import override

import LiuXin_alpha.storage.api as api
from LiuXin_alpha.storage.storage_manager.mixins._state import _StorageManagerState


class ItemDigitalAssetLinkMixin(_StorageManagerState):
    """
    Manage role-keyed references from library Items to stored content.

    Each ``(ItemID, role)`` identifies at most one atomic or Composite Digital
    Asset target.  This component validates targets and updates reference
    metadata only; Item lifecycle and the target Assets themselves belong to
    their respective catalogue components.
    """

    @override
    def link_item_to_digital_asset(
        self,
        item_id: api.ItemID,
        digital_asset_id: api.DigitalAssetID,
        *,
        role: str = "primary_payload",
    ) -> None:
        """
        Link one Item role to an atomic Digital Asset in reference state.


        :param item_id:
        :param digital_asset_id:
        :param role:
        :return:
        """

        self.get_digital_asset_record(digital_asset_id)
        self._set_item_target(item_id, role, "digital_asset", digital_asset_id)

    @override
    def link_item_to_composite_digital_asset(
        self,
        item_id: api.ItemID,
        composite_digital_asset_id: api.CompositeDigitalAssetID,
        *,
        role: str = "primary_payload",
    ) -> None:
        """
        Link one Item role to a Composite Digital Asset.


        :param item_id:
        :param composite_digital_asset_id:
        :param role:
        :return:
        """

        self.get_composite_digital_asset_record(composite_digital_asset_id)
        self._set_item_target(
            item_id,
            role,
            "composite_digital_asset",
            composite_digital_asset_id,
        )

    @override
    def unlink_item_digital_asset(
        self,
        item_id: api.ItemID,
        *,
        role: str = "primary_payload",
    ) -> bool:
        """
        Remove one manager-owned Item-to-Asset role link.


        :param item_id:
        :param role:
        :return:
        """

        with self._lock, self._metadata_transaction():
            return self._item_targets.pop((item_id, role), None) is not None


__all__ = ["ItemDigitalAssetLinkMixin"]

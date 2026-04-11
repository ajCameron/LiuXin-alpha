"""Composite digital asset membership methods for the storage manager."""

from __future__ import annotations

import abc
from collections.abc import Iterator
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api.info_containers_api import DigitalAssetCompositionMemberRecord
    from LiuXin_alpha.storage.storage_types import DigitalAssetCompositionID, DigitalAssetID


class DigitalAssetCompositionsManagerAPI(abc.ABC):
    """Access and update ordered membership inside composite digital assets."""

    @abc.abstractmethod
    def create_digital_asset_composition_member(
        self,
        member: "DigitalAssetCompositionMemberRecord",
    ) -> "DigitalAssetCompositionMemberRecord":
        ...

    @abc.abstractmethod
    def get_digital_asset_composition_member(
        self,
        digital_asset_composition_id: "DigitalAssetCompositionID",
    ) -> "DigitalAssetCompositionMemberRecord":
        ...

    @abc.abstractmethod
    def update_digital_asset_composition_member(
        self,
        member: "DigitalAssetCompositionMemberRecord",
    ) -> "DigitalAssetCompositionMemberRecord":
        ...

    @abc.abstractmethod
    def delete_digital_asset_composition_member(self, digital_asset_composition_id: "DigitalAssetCompositionID") -> bool:
        ...

    @abc.abstractmethod
    def iter_digital_asset_members(self, parent_digital_asset_id: "DigitalAssetID") -> Iterator["DigitalAssetCompositionMemberRecord"]:
        ...

    @abc.abstractmethod
    def iter_digital_asset_parents(self, member_digital_asset_id: "DigitalAssetID") -> Iterator["DigitalAssetCompositionMemberRecord"]:
        ...

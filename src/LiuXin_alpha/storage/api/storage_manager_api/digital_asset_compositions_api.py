"""Composite digital asset membership link methods for the storage manager."""

from __future__ import annotations

import abc
from collections.abc import Iterator
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api.info_containers_api import CompositeDigitalAssetMemberLinkRecord
    from LiuXin_alpha.storage.storage_types import CompositeDigitalAssetID, CompositeDigitalAssetMemberLinkID, DigitalAssetID


class CompositeDigitalAssetMembersManagerAPI(abc.ABC):
    """Access and update ordered membership links inside composite digital assets."""

    @abc.abstractmethod
    def create_composite_digital_asset_member_link(
        self,
        link: "CompositeDigitalAssetMemberLinkRecord",
    ) -> "CompositeDigitalAssetMemberLinkRecord":
        ...

    @abc.abstractmethod
    def get_composite_digital_asset_member_link(
        self,
        composite_digital_asset_member_link_id: "CompositeDigitalAssetMemberLinkID",
    ) -> "CompositeDigitalAssetMemberLinkRecord":
        ...

    @abc.abstractmethod
    def update_composite_digital_asset_member_link(
        self,
        link: "CompositeDigitalAssetMemberLinkRecord",
    ) -> "CompositeDigitalAssetMemberLinkRecord":
        ...

    @abc.abstractmethod
    def delete_composite_digital_asset_member_link(
        self,
        composite_digital_asset_member_link_id: "CompositeDigitalAssetMemberLinkID",
    ) -> bool:
        ...

    @abc.abstractmethod
    def iter_composite_digital_asset_members(
        self,
        composite_digital_asset_id: "CompositeDigitalAssetID",
    ) -> Iterator["CompositeDigitalAssetMemberLinkRecord"]:
        ...

    @abc.abstractmethod
    def iter_digital_asset_composites(
        self,
        digital_asset_id: "DigitalAssetID",
    ) -> Iterator["CompositeDigitalAssetMemberLinkRecord"]:
        ...

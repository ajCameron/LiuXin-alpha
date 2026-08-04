"""Composite digital asset membership link methods for the storage manager.

Examples:
    Resolve the ordered members of a composite::

        member_ids = list(manager.get_composite_digital_asset_items(12))
"""

from __future__ import annotations

import abc
from collections.abc import Iterator
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api.info_containers_api import CompositeDigitalAssetMemberLinkRow
    from LiuXin_alpha.storage.storage_types import (
        CompositeDigitalAssetID, CompositeDigitalAssetMemberLinkID, DigitalAssetID)


class CompositeDigitalAssetMembersManagerAPI(abc.ABC):
    """
    Access and update ordered membership links inside composite digital assets.

    Examples:
        Iterate over the membership rows for composite ``12``::

            links = list(manager.iter_composite_digital_asset_members_links(12))
    """

    @abc.abstractmethod
    def get_composite_digital_asset_items(
            self,
            composite_digital_asset_id: "CompositeDigitalAssetID") -> Iterator["DigitalAssetID"]:
        """
        Get all the ids from all the digital assets linked to the given composite digital asset ID.

        :param composite_digital_asset_id:
        :return:

        Examples:
            Preserve database ordering by consuming the returned iterator::

                member_ids = list(manager.get_composite_digital_asset_items(12))
        """

    @abc.abstractmethod
    def create_composite_digital_asset_member_link(
        self,
        link: "CompositeDigitalAssetMemberLinkRow",
    ) -> "CompositeDigitalAssetMemberLinkRow":
        """
        Write a composite digital asset member link out to the database.

        :param link:
        :return:

        Examples:
            Persist a member link row::

                link = manager.create_composite_digital_asset_member_link(link_row)
        """

    @abc.abstractmethod
    def get_composite_digital_asset_member_link(
        self,
        composite_digital_asset_member_link_id: "CompositeDigitalAssetMemberLinkID",
    ) -> "CompositeDigitalAssetMemberLinkRow":
        """
        Get the link row by id.

        :param composite_digital_asset_member_link_id:
        :return:

        Examples:
            Retrieve membership link ``9``::

                link = manager.get_composite_digital_asset_member_link(9)
        """

    @abc.abstractmethod
    def update_composite_digital_asset_member_link(
        self,
        link: "CompositeDigitalAssetMemberLinkRow",
    ) -> "CompositeDigitalAssetMemberLinkRow":
        """
        Update a link row by the row.

        :param link:
        :return:

        Examples:
            Save a changed member position::

                link["composite_digital_asset_digital_asset_link_sequence_number"] = 2
                link = manager.update_composite_digital_asset_member_link(link)
        """

    @abc.abstractmethod
    def delete_composite_digital_asset_member_link(
        self,
        composite_digital_asset_member_link_id: "CompositeDigitalAssetMemberLinkID",
    ) -> bool:
        """
        Delete a link row by id.

        :param composite_digital_asset_member_link_id:
        :return:

        Examples:
            Remove membership link ``9``::

                removed = manager.delete_composite_digital_asset_member_link(9)
        """

    @abc.abstractmethod
    def iter_composite_digital_asset_members_links(
        self,
        composite_digital_asset_id: "CompositeDigitalAssetID",
    ) -> Iterator["CompositeDigitalAssetMemberLinkRow"]:
        """
        Iterate over the link rows between the composite digital asset and its members.

        :param composite_digital_asset_id:
        :return:

        Examples:
            Load all membership rows for composite ``12``::

                links = list(manager.iter_composite_digital_asset_members_links(12))
        """

    @abc.abstractmethod
    def iter_digital_asset_composites(
        self,
        digital_asset_id: "DigitalAssetID",
    ) -> Iterator["CompositeDigitalAssetMemberLinkRow"]:
        """
        Iterate over the link rows between a digital asset and its composite parents.

        :param digital_asset_id:
        :return:

        Examples:
            Find every composite containing asset ``42``::

                parents = list(manager.iter_digital_asset_composites(42))
        """

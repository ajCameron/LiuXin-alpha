"""Item repository API."""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from ..common import EntityId, MetadataCandidate, MatchResult, RowMapping, WemiBundle
from .base import BaseRepositoryAPI


# Todo: Currently we're only reaching one level up the wemi stack - it might be better to be able to reach further

@runtime_checkable
class ItemRepositoryAPI(BaseRepositoryAPI, Protocol):
    """
    Storage and lookup API for Item-level metadata.
    """

    def list_for_manifestation(self, manifestation_id: EntityId) -> Sequence[RowMapping]:
        """
        Return items belonging to a manifestation.

        :param manifestation_id:
        :return:
        """

    def manifestation_for_item(self, item_id: EntityId) -> RowMapping | None:
        """
        Return the Manifestation owning an Item, if assigned.

        :param item_id:
        :return:
        """

    def get_metadata_bundle(self, item_id: EntityId) -> WemiBundle:
        """
        Return a coherent WEMI bundle rooted at an item.

        :param item_id:
        :return:
        """

    def match(self, manifestation_id: EntityId, candidate: MetadataCandidate) -> MatchResult:
        """
        Match a candidate item inside a manifestation context.

        :param manifestation_id:
        :param candidate:
        :return:
        """

    def match_or_create(self, manifestation_id: EntityId, candidate: MetadataCandidate) -> EntityId:
        """
        Return a matched item id, or create a new item.

        :param manifestation_id:
        :param candidate:
        :return:
        """

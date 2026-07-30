"""Item repository API."""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from ..common import EntityId, MetadataCandidate, MatchResult, RowMapping, WemiBundle
from .base import BaseRepositoryAPI


@runtime_checkable
class ItemRepositoryAPI(BaseRepositoryAPI, Protocol):
    """Storage and identity API for individual Items.

    An Item is one owned, observed, or managed copy of a Manifestation. It is
    the appropriate level for inventory codes, acquisition details, source
    paths, condition, and raw observed identifiers.

    Example::

        item_id = catalog.items.match_or_create(
            manifestation_id,
            MetadataCandidate({
                "inventory_code": "EBOOK-0042",
                "source": "manual import",
            }),
        )
        bundle = catalog.items.get_metadata_bundle(item_id)
        print(bundle.work["work_title"])
    """

    def list_for_manifestation(self, manifestation_id: EntityId) -> Sequence[RowMapping]:
        """
        Return items belonging to a manifestation.

        :param manifestation_id: Existing Manifestation ID.
        :return: Item mappings linked to that Manifestation, with relationship
            metadata.
        """

    def manifestation_for_item(self, item_id: EntityId) -> RowMapping | None:
        """
        Return the Manifestation owning an Item, if assigned.

        :param item_id: Existing Item ID.
        :return: Owning Manifestation mapping with relationship metadata, or
            ``None`` when the Item has not been assigned.
        """

    def get_metadata_bundle(self, item_id: EntityId) -> WemiBundle:
        """
        Return a coherent WEMI bundle rooted at an item.

        This is a convenience alias for
        ``catalog.retrieval.bundles.for_item(item_id)``.

        :param item_id: Existing Item ID.
        :return: Work-to-Item path plus attached agents, identifiers, titles,
            notes, and link records.
        """

    def match(self, manifestation_id: EntityId, candidate: MetadataCandidate) -> MatchResult:
        """
        Match a candidate item inside a manifestation context.

        :param manifestation_id: Existing Manifestation defining the scope.
        :param candidate: Item fields such as ``inventory_code``,
            ``source_path``, ``condition``, or ``acquired_date``.
        :return: Contextual match, no-match, ambiguity, or conflict decision.
        """

    def match_or_create(self, manifestation_id: EntityId, candidate: MetadataCandidate) -> EntityId:
        """
        Return a matched item id, or create a new item.

        The new Item is linked to ``manifestation_id`` transactionally.

        :param manifestation_id: Existing Manifestation to link.
        :param candidate: Item fields accepted by :meth:`create`.
        :return: Existing safely matched or newly created Item ID.
        """

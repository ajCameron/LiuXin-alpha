"""Repository API for raw identifiers observed on Items."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from ..common import EntityId, IdentifierCandidate, MatchResult, RowMapping
from .base import BaseRepositoryAPI


@runtime_checkable
class ItemIdentifierRepositoryAPI(BaseRepositoryAPI, Protocol):
    """Store and exactly resolve Item-scoped identifier observations.

    Use this repository for values observed on one acquired file/copy when they
    have not been curated into an entity-owned identifier. The optional
    ``item_id`` scope distinguishes identical values observed on different
    Items.

    Example::

        observation_id = catalog.item_identifiers.match_or_create(
            item_id,
            IdentifierCandidate("source-id", "vendor-record-42"),
        )
    """

    def match(
        self,
        candidate: IdentifierCandidate,
        *,
        item_id: EntityId | None = None,
    ) -> MatchResult:
        """
        Return an exact observed identifier decision.

        :param candidate: Identifier to normalize and compare.
        :param item_id: Optional owning Item ID.
        :return: Explained exact match or no-match result.
        """

        ...

    def exact(
        self,
        candidate_str: str,
        id_type: str,
        *,
        item_id: EntityId | None = None,
    ) -> MatchResult:
        """Return the exact observed decision for a value and scheme.

        :param candidate_str: Identifier value to match.
        :param id_type: Identifier scheme or supported alias.
        :param item_id: Optional owning Item ID.
        :return: Explained exact match or no-match result.
        """

        ...

    def match_or_create(
        self,
        item_id: EntityId,
        candidate: IdentifierCandidate,
    ) -> EntityId:
        """
        Reuse an exact observation on one Item or create it there.

        The same scheme/value on another Item is a distinct observation.

        :param item_id: Existing Item which owns the observation.
        :param candidate: Identifier observation to normalize and persist.
        :return: Existing or newly created observed identifier ID.
        """

        ...

    def list_for_item(self, item_id: EntityId) -> Sequence[RowMapping]:
        """
        Return identifier observations owned by one Item.

        :param item_id: Existing Item ID.
        :return: ID-ordered observed identifier rows.
        """

        ...


__all__ = ["ItemIdentifierRepositoryAPI"]

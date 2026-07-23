"""Repository API for raw identifiers observed on Items."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from ..common import EntityId, IdentifierCandidate, MatchResult, RowMapping
from .base import BaseRepositoryAPI


@runtime_checkable
class ItemIdentifierRepositoryAPI(BaseRepositoryAPI, Protocol):
    """Store and exactly resolve Item-scoped identifier observations."""

    def match(
        self,
        candidate: IdentifierCandidate,
        *,
        item_id: EntityId | None = None,
    ) -> MatchResult:
        """Return an exact observed identifier decision.

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
        :param id_type: Identifier scheme.
        :param item_id: Optional owning Item ID.
        :return: Explained exact match or no-match result.
        """

        ...

    def match_or_create(
        self,
        item_id: EntityId,
        candidate: IdentifierCandidate,
    ) -> EntityId:
        """Reuse an exact observation on one Item or create it there.

        :param item_id: Existing Item which owns the observation.
        :param candidate: Identifier observation to normalize and persist.
        :return: Existing or newly created observed identifier ID.
        """

        ...

    def list_for_item(self, item_id: EntityId) -> Sequence[RowMapping]:
        """Return identifier observations owned by one Item.

        :param item_id: Existing Item ID.
        :return: ID-ordered observed identifier rows.
        """

        ...


__all__ = ["ItemIdentifierRepositoryAPI"]

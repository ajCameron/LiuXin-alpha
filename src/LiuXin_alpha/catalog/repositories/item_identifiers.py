"""Repository for raw identifiers observed on catalog Items."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import ClassVar

from ..api.common import EntityId, IdentifierCandidate, MatchResult, RowMapping
from ..matching.item_identifier_matcher import ItemIdentifierMatcher
from ..matching.policy import normalise_identifier
from .base import BaseRepository


class ItemIdentifierRepository(BaseRepository):
    """Store and exactly resolve Item-scoped identifier observations."""

    table_name = "item_identifiers"
    id_column = "item_identifier_id"
    input_aliases: ClassVar[Mapping[str, str]] = {
        "id": "item_identifier_id",
        "item_id": "item_identifier_item_id",
        "identifier_type": "item_identifier_scheme",
        "scheme": "item_identifier_scheme",
        "value": "item_identifier_value",
        "source": "item_identifier_source",
    }

    def matcher(self) -> ItemIdentifierMatcher:
        """Return the exact observed identifier matcher.

        :return: Item identifier matcher bound to this repository.
        """

        return ItemIdentifierMatcher(self)

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

        return self.matcher().best(candidate, item_id=item_id)

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

        return self.matcher().exact(candidate_str, id_type, item_id=item_id)

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

        self._require_table_row("items", item_id)
        normalised = normalise_identifier(candidate)
        result = self.match(normalised, item_id=item_id)
        if result.is_match:
            assert result.entity_id is not None
            return result.entity_id
        return self.create(
            {
                "item_id": item_id,
                "scheme": normalised.identifier_type,
                "value": normalised.value,
                "source": normalised.source,
            }
        )

    def list_for_item(self, item_id: EntityId) -> Sequence[RowMapping]:
        """Return identifier observations owned by one Item.

        :param item_id: Existing Item ID.
        :return: ID-ordered observed identifier rows.
        """

        self._require_table_row("items", item_id)
        return tuple(
            row
            for row in self._all_rows()
            if row.get("item_identifier_item_id") == item_id
        )


__all__ = ["ItemIdentifierRepository"]

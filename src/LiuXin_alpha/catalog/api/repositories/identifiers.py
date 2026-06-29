"""
Identifier repository API.

Enables finding
"""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from ..common import EntityId, IdentifierCandidate, MatchResult, RowMapping, WemiLevel
from .base import BaseRepositoryAPI


@runtime_checkable
class IdentifierRepositoryAPI(BaseRepositoryAPI, Protocol):
    """
    Storage, resolution, and linking API for identifiers.
    """

    def normalise(self, candidate: IdentifierCandidate) -> IdentifierCandidate:
        """
        Return a normalised identifier candidate.

        :param candidate:
        :return:
        """

    # Todo: Note - an identifier can be linked to multiple things
    # Todo: This might be better, semantically, as something like .find.from_string - this could be a callable class
    def find(self, *, identifier_type: str, value: str) -> RowMapping | None:
        """
        Find an identifier by type and value.

        :param identifier_type:
        :param value:
        :return:
        """

    def match(self, candidate: IdentifierCandidate) -> MatchResult:
        """
        Match an identifier candidate to an existing identifier.

        :param candidate:
        :return:
        """

    def match_or_create(self, candidate: IdentifierCandidate) -> EntityId:
        """
        Return a matched identifier id, or create a new identifier.

        :param candidate:
        :return new_identifier_id:
        """

    def link_to_wemi(
        self,
        *,
        identifier_id: EntityId,
        level: WemiLevel,
        entity_id: EntityId,
        priority: int | None = None,
    ) -> None:
        """
        Link an identifier to a WEMI entity.

        :param identifier_id:
        :param level:
        :param entity_id:
        :param priority:
        :return:
        """

    def list_for_wemi(self, *, level: WemiLevel, entity_id: EntityId) -> Sequence[RowMapping]:
        """
        Return identifiers linked to a WEMI entity.

        :param level:
        :param entity_id:
        :return:
        """

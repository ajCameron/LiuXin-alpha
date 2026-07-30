"""Repository contract for curated identifiers owned by catalog entities."""

from __future__ import annotations

from typing import Mapping, Protocol, Sequence, runtime_checkable

from ..common import EntityId, IdentifierCandidate, MatchResult, RowMapping, WemiLevel
from .base import BaseRepositoryAPI


@runtime_checkable
class IdentifierRepositoryAPI(BaseRepositoryAPI, Protocol):
    """Normalize, resolve, and assign curated identifiers.

    These are logical identifiers owned by a Work, Expression, Manifestation,
    Item, or Agent. They differ from ``item_identifiers``, which record raw
    observations on a particular Item.

    Example::

        identifier_id = catalog.identifiers.match_or_create(
            IdentifierCandidate("isbn13", "978-0-14-143947-1")
        )
        assigned_id = catalog.identifiers.link_to_wemi(
            identifier_id=identifier_id,
            level="manifestation",
            entity_id=manifestation_id,
            priority=0,
        )
    """

    def normalise(self, candidate: IdentifierCandidate) -> IdentifierCandidate:
        """
        Return a normalised identifier candidate.

        Scheme aliases are canonicalized and scheme-specific punctuation is
        removed where appropriate.

        :param candidate: Raw scheme/value plus optional provenance.
        :return: New immutable candidate containing canonical scheme and
            ``normalised_value``.
        """

    # Todo: Note - an identifier can be linked to multiple things
    # Todo: This might be better, semantically, as something like .find.from_string - this could be a callable class
    def find(self, *, identifier_type: str, value: str) -> RowMapping | None:
        """
        Find an identifier by type and value.

        :param identifier_type: Scheme or supported scheme alias.
        :param value: Raw identifier string.
        :return: First stable-ID exact logical row, or ``None``.
        """

    def match(self, candidate: IdentifierCandidate) -> MatchResult:
        """
        Match an identifier candidate to an existing identifier.

        :param candidate: Identifier to normalize and compare.
        :return: Explained exact match or no-match result.
        """

    def match_or_create(self, candidate: IdentifierCandidate) -> EntityId:
        """
        Return a matched identifier id, or create a new identifier.

        This creates an initially unowned row. Assign it with
        :meth:`link_to_wemi` or :meth:`link_to_agent`.

        :param candidate: Identifier to normalize and persist when absent.
        :return: Existing or newly created logical identifier row ID.
        """

    def link_to_wemi(
        self,
        *,
        identifier_id: EntityId,
        level: WemiLevel,
        entity_id: EntityId,
        priority: int | None = None,
    ) -> EntityId:
        """
        Link an identifier to a WEMI entity.

        Identifier rows are owner-scoped. Assigning a row already owned
        elsewhere copies its logical value so the original owner is unchanged.

        :param identifier_id: Existing logical identifier row ID.
        :param level: WEMI owner level.
        :param entity_id: Existing owner ID at ``level``.
        :param priority: Optional order; ``0`` marks the primary identifier.
        :return: Assigned identifier row ID.
        """

    def link_to_agent(
        self,
        *,
        identifier_id: EntityId,
        agent_id: EntityId,
        priority: int | None = None,
    ) -> EntityId:
        """
        Assign an identifier to an Agent, copying an already-owned row.

        :param identifier_id: Existing logical identifier row ID.
        :param agent_id: Existing Agent owner ID.
        :param priority: Optional order; ``0`` marks the primary identifier.
        :return: Assigned identifier row ID.
        """

    def list_for_wemi(self, *, level: WemiLevel, entity_id: EntityId) -> Sequence[RowMapping]:
        """
        Return identifiers linked to a WEMI entity.

        :param level: WEMI owner level.
        :param entity_id: Existing owner ID.
        :return: Identifier rows in stable ID order.
        """

    def replace_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
        identifiers: Mapping[str, str],
    ) -> Mapping[str, EntityId]:
        """
        Replace the complete identifier mapping for a WEMI entity.

        Replacement is atomic. Schemes are normalized before duplicate checks,
        and identifiers omitted from the mapping are removed from this owner.

        :param level: WEMI owner level.
        :param entity_id: Existing owner ID.
        :param identifiers: Complete desired scheme-to-value mapping.
        :return: Assigned IDs keyed by normalized scheme.
        """

    def list_for_agent(self, agent_id: EntityId) -> Sequence[RowMapping]:
        """
        Return identifiers owned by an Agent.

        :param agent_id: Existing Agent ID.
        :return: Agent-owned identifier rows in stable ID order.
        """

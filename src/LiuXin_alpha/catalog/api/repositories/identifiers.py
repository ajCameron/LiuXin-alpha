"""Identifier repository API."""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from ..common import EntityId, IdentifierCandidate, MatchResult, RowMapping, WemiLevel
from .base import BaseRepositoryAPI


@runtime_checkable
class IdentifierRepositoryAPI(BaseRepositoryAPI, Protocol):
    """Storage, resolution, and linking API for identifiers."""

    def normalise(self, candidate: IdentifierCandidate) -> IdentifierCandidate:
        """Return a normalised identifier candidate."""

    def find(self, *, identifier_type: str, value: str) -> RowMapping | None:
        """Find an identifier by type and value."""

    def match(self, candidate: IdentifierCandidate) -> MatchResult:
        """Match an identifier candidate to an existing identifier."""

    def match_or_create(self, candidate: IdentifierCandidate) -> EntityId:
        """Return a matched identifier id, or create a new identifier."""

    def link_to_wemi(
        self,
        *,
        identifier_id: EntityId,
        level: WemiLevel,
        entity_id: EntityId,
        priority: int | None = None,
    ) -> None:
        """Link an identifier to a WEMI entity."""

    def list_for_wemi(self, *, level: WemiLevel, entity_id: EntityId) -> Sequence[RowMapping]:
        """Return identifiers linked to a WEMI entity."""

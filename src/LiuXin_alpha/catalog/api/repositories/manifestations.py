"""Manifestation repository API."""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from ..common import EntityId, MetadataCandidate, MatchResult, RowMapping
from .base import BaseRepositoryAPI


@runtime_checkable
class ManifestationRepositoryAPI(BaseRepositoryAPI, Protocol):
    """Storage and lookup API for Manifestation-level metadata."""

    def list_for_expression(self, expression_id: EntityId) -> Sequence[RowMapping]:
        """Return manifestations belonging to an expression."""

    def match(self, expression_id: EntityId, candidate: MetadataCandidate) -> MatchResult:
        """Match a candidate manifestation inside an expression context."""

    def match_or_create(self, expression_id: EntityId, candidate: MetadataCandidate) -> EntityId:
        """Return a matched manifestation id, or create a new manifestation."""

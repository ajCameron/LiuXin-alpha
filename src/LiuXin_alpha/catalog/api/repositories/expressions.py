"""Expression repository API."""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from ..common import EntityId, MetadataCandidate, MatchResult, RowMapping
from .base import BaseRepositoryAPI


@runtime_checkable
class ExpressionRepositoryAPI(BaseRepositoryAPI, Protocol):
    """Storage and lookup API for Expression-level metadata."""

    def list_for_work(self, work_id: EntityId) -> Sequence[RowMapping]:
        """Return expressions belonging to a work."""

    def match(self, work_id: EntityId, candidate: MetadataCandidate) -> MatchResult:
        """Match a candidate expression inside a work context."""

    def match_or_create(self, work_id: EntityId, candidate: MetadataCandidate) -> EntityId:
        """Return a matched expression id, or create a new expression."""

"""Work repository API."""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from ..common import EntityId, MetadataCandidate, MatchResult, RowMapping
from .base import BaseRepositoryAPI


@runtime_checkable
class WorkRepositoryAPI(BaseRepositoryAPI, Protocol):
    """Storage and lookup API for Work-level metadata."""

    def find_by_title(self, title: str, *, limit: int = 20) -> Sequence[RowMapping]:
        """Find works whose title data matches the supplied string."""

    def match(self, candidate: MetadataCandidate) -> MatchResult:
        """Match a candidate to an existing work if possible."""

    def match_or_create(self, candidate: MetadataCandidate) -> EntityId:
        """Return a matched work id, or create a new work."""

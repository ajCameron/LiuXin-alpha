"""Agent matching API."""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from ..common import MetadataCandidate, MatchResult


@runtime_checkable
class AgentMatcherAPI(Protocol):
    """Policy object for matching incoming metadata to agents."""

    def candidates(self, candidate: MetadataCandidate, *, limit: int = 20) -> Sequence[MatchResult]:
        """Return possible agent matches ordered by confidence."""

    def best(self, candidate: MetadataCandidate) -> MatchResult:
        """Return the best agent match, or a non-match result."""

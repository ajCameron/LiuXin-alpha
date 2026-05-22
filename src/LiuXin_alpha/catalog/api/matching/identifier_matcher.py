"""Identifier matching API."""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from ..common import IdentifierCandidate, MatchResult


@runtime_checkable
class IdentifierMatcherAPI(Protocol):
    """Policy object for matching identifiers."""

    def candidates(self, candidate: IdentifierCandidate, *, limit: int = 20) -> Sequence[MatchResult]:
        """Return possible identifier matches ordered by confidence."""

    def best(self, candidate: IdentifierCandidate) -> MatchResult:
        """Return the best identifier match, or a non-match result."""

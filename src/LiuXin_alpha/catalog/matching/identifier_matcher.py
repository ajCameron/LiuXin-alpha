"""IdentifierMatcher implementation scaffold."""

from __future__ import annotations

from typing import Any, Sequence

from ..api.common import DatabaseHandle, IdentifierCandidate, MatchResult


class IdentifierMatcher:
    """Match incoming identifier candidates to existing identifiers."""

    def __init__(self, db: DatabaseHandle, repositories: Any) -> None:
        self.db = db
        self.repositories = repositories

    def candidates(self, candidate: IdentifierCandidate, *, limit: int = 20) -> Sequence[MatchResult]:
        raise NotImplementedError("Move identifier candidate matching policy here from databases")

    def best(self, candidate: IdentifierCandidate) -> MatchResult:
        results = self.candidates(candidate, limit=1)
        if results:
            return results[0]
        return MatchResult(entity_id=None, confidence=0.0, reason="no identifier candidates")

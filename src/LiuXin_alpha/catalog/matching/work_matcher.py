"""WorkMatcher implementation scaffold."""

from __future__ import annotations

from typing import Any, Sequence

from ..api.common import DatabaseHandle, MatchResult, MetadataCandidate


class WorkMatcher:
    """Match incoming metadata candidates to existing works."""

    def __init__(self, db: DatabaseHandle, repositories: Any) -> None:
        self.db = db
        self.repositories = repositories

    def candidates(self, candidate: MetadataCandidate, *, limit: int = 20) -> Sequence[MatchResult]:
        raise NotImplementedError("Move work candidate matching policy here from databases")

    def best(self, candidate: MetadataCandidate) -> MatchResult:
        results = self.candidates(candidate, limit=1)
        if results:
            return results[0]
        return MatchResult(entity_id=None, confidence=0.0, reason="no work candidates")

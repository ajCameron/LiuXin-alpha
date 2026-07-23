"""Agent matching API."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from ..common import MetadataCandidate, MatchResult


@runtime_checkable
class AgentMatcherAPI(Protocol):
    """Match incoming metadata candidates to Agents without mutation."""

    def candidates(self, candidate: MetadataCandidate, *, limit: int = 20) -> Sequence[MatchResult]:
        """Return exact policy-qualified Agent candidates.

        :param candidate: Candidate Agent metadata and structured hints.
        :param limit: Maximum candidates to return.
        :return: Qualified candidates ordered by evidence and confidence.
        """

        ...

    def best(self, candidate: MetadataCandidate) -> MatchResult:
        """Return the final Agent identity decision.

        :param candidate: Candidate Agent metadata and structured hints.
        :return: Explained match, no-match, ambiguity, or conflict.
        """

        ...

    def exact(self, candidate_str: str) -> "MatchResult":
        """Apply the final policy to an Agent name string.

        :param candidate_str: Agent name to normalize and match.
        :return: Exact match, no-match, or ambiguity result.
        """

        ...

"""Agent matching API."""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from typing import Optional, TYPE_CHECKING

from ..common import MetadataCandidate, MatchResult


@runtime_checkable
class AgentMatcherAPI(Protocol):
    """
    Policy object for matching incoming metadata to agents.
    """

    def candidates(self, candidate: MetadataCandidate, *, limit: int = 20) -> Sequence[MatchResult]:
        """
        Return possible agent matches ordered by confidence.

        :param candidate:
        :param limit:
        :return:
        """

    def best(self, candidate: MetadataCandidate) -> MatchResult:
        """
        Return the best agent match, or a non-match result.

        :param candidate:
        :return:
        """

    def exact(self, candidate_str: str) -> Optional["MatchResult"]:
        """
        Exact match to an agent based on its text.

        Either matches, or does not. If it doesn't, then you get back a None.
        :param candidate_str:
        :return:
        """

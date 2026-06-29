"""Identifier matching API."""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.catalog.api.common import IdentifierCandidate, MatchResult


@runtime_checkable
class IdentifierMatcherAPI(Protocol):
    """
    Policy object for matching identifiers.
    """

    def candidates(self, candidate: "IdentifierCandidate", *, limit: int = 20) -> Sequence[MatchResult]:
        """
        Return possible identifier matches ordered by confidence.

        :param candidate:
        :param limit:
        :return:
        """

    def best(self, candidate: IdentifierCandidate) -> MatchResult:
        """
        Return the best identifier match, or a non-match result.

        :param candidate:
        :return:
        """

    def exact(self, candidate_str: str, id_type: str) -> MatchResult:
        """
        Either get an EXACT match based on the identifier, or get nothing.

        :param candidate_str:
        :param id_type:
        :return:
        """
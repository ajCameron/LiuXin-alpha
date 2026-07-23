"""Work matching API."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:

    from LiuXin_alpha.catalog.api.common import MetadataCandidate, MatchResult


@runtime_checkable
class WorkMatcherAPI(Protocol):
    """Match incoming metadata candidates to Works without mutation."""

    def candidates(self, candidate: MetadataCandidate, *, limit: int = 20) -> Sequence[MatchResult]:
        """Return policy-qualified Work candidates in deterministic order.

        :param candidate: Candidate Work metadata and structured hints.
        :param limit: Maximum candidates to return.
        :return: Qualified candidates ordered by evidence and confidence.
        """

        ...

    def best(self, candidate: MetadataCandidate) -> MatchResult:
        """Return the final Work identity decision.

        :param candidate: Candidate Work metadata and structured hints.
        :return: Explained match, no-match, ambiguity, or conflict.
        """

        ...

    def exact(self, cand_str: str) -> MatchResult:
        """Apply the final policy to a Work title string.

        :param cand_str: Work title to normalize and match.
        :return: Exact match, no-match, or ambiguity result.
        """

        ...

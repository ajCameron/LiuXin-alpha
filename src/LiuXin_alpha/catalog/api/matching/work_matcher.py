"""Work matching API."""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable, TYPE_CHECKING, Optional

if TYPE_CHECKING:

    from LiuXin_alpha.catalog.api.common import MetadataCandidate, MatchResult


# Todo: Might be worth an entire subclass for each, so you can do catalog.matching.work.by_string("some string")
@runtime_checkable
class WorkMatcherAPI(Protocol):
    """
    Policy object for matching incoming metadata to works.
    """

    def candidates(self, candidate: MetadataCandidate, *, limit: int = 20) -> Sequence[MatchResult]:
        """
        Return possible work matches ordered by confidence.

        :param candidate:
        :param limit:
        :return:
        """

    def best(self, candidate: MetadataCandidate) -> MatchResult:
        """
        Return the best work match, or a non-match result.

        :param candidate:
        :return:
        """

    def exact(self, cand_str: str) -> Optional[MatchResult]:
        """
        Return an exact match, string based, on the works.

        :param cand_str:
        :return:
        """

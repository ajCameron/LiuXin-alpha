"""Work matching API."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:

    from LiuXin_alpha.catalog.api.common import MetadataCandidate, MatchResult


@runtime_checkable
class WorkMatcherAPI(Protocol):
    """Match incoming metadata candidates to Works without mutation.

    Title evidence is normalized for Unicode, case, and whitespace. Structured
    hints such as identifiers can provide decisive evidence according to the
    Catalog's :class:`MatchingPolicy`.
    """

    def candidates(self, candidate: MetadataCandidate, *, limit: int = 20) -> Sequence[MatchResult]:
        """Return policy-qualified Work candidates in deterministic order.

        :param candidate: Candidate Work metadata and structured hints.
        :param limit: Maximum candidates to return.
        :return: Qualified candidate decisions ordered by confidence, evidence,
            and stable entity ID. This is not the final ambiguity decision.
        """

        ...

    def best(self, candidate: MetadataCandidate) -> MatchResult:
        """Return the final Work identity decision.

        :param candidate: Candidate Work metadata and structured hints.
        :return: Explained match, no-match, ambiguity, or conflict. On
            ambiguity, ``alternatives`` contains candidate Work IDs.
        """

        ...

    def exact(self, cand_str: str) -> MatchResult:
        """Apply the final policy to a Work title string.

        :param cand_str: Work title to normalize and match.
        :return: Exact normalized-title match, no-match, or ambiguity result.

        Example::

            result = catalog.matching.works.exact("Frankenstein")
        """

        ...

"""Identifier matching API."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from LiuXin_alpha.catalog.api.common import IdentifierCandidate, MatchResult


@runtime_checkable
class IdentifierMatcherAPI(Protocol):
    """Match identifiers after scheme-specific normalization."""

    def candidates(
        self,
        candidate: "IdentifierCandidate",
        *,
        limit: int = 20,
    ) -> Sequence[MatchResult]:
        """Return every exact stored copy of a logical identifier.

        :param candidate: Identifier to normalize and compare.
        :param limit: Maximum stored rows to return.
        :return: Exact identifier rows ordered by storage ID.
        """

        ...

    def best(self, candidate: IdentifierCandidate) -> MatchResult:
        """Return the first stored copy of an exact logical identifier.

        :param candidate: Identifier to normalize and compare.
        :return: Explained exact match or no-match result.
        """

        ...

    def exact(self, candidate_str: str, id_type: str) -> MatchResult:
        """Return the exact decision for an identifier value and scheme.

        :param candidate_str: Identifier value to match.
        :param id_type: Identifier scheme.
        :return: Explained exact match or no-match result.
        """

        ...

"""Exact-default matching API for catalog value entities."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from ..common import MatchResult, MetadataCandidate


@runtime_checkable
class ExactEntityMatcherAPI(Protocol):
    """Match a configured catalog entity exactly by default."""

    def candidates(
        self,
        candidate: MetadataCandidate,
        *,
        limit: int = 20,
        use_policy: bool = False,
    ) -> Sequence[MatchResult]:
        """Return exact candidates with approximate matching explicitly opt-in.

        :param candidate: Candidate entity metadata.
        :param limit: Maximum candidates to return.
        :param use_policy: Permit approximate matching after exact matching fails.
        :return: Deterministically ranked possible matches.
        """

        ...

    def best(
        self,
        candidate: MetadataCandidate,
        *,
        use_policy: bool = False,
    ) -> MatchResult:
        """Return the exact-default identity decision.

        :param candidate: Candidate entity metadata.
        :param use_policy: Permit approximate matching after exact matching fails.
        :return: Match, no-match, ambiguity, or conflict decision.
        """

        ...

    def exact(self, value: object, **scope: object) -> MatchResult:
        """Return the exact decision for a scalar identity value.

        :param value: Scalar identity value.
        :param scope: Optional public scope aliases.
        :return: Exact match, no-match, or ambiguity decision.
        """

        ...


__all__ = ["ExactEntityMatcherAPI"]

"""Repository API for exact-default catalog entities."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..common import EntityId, MatchResult, MetadataCandidate, RowMapping
from .base import BaseRepositoryAPI


@runtime_checkable
class ExactEntityRepositoryAPI(BaseRepositoryAPI, Protocol):
    """CRUD and exact-default matching for a catalog entity."""

    def match(
        self,
        candidate: MetadataCandidate,
        *,
        use_policy: bool = False,
    ) -> MatchResult:
        """Return an exact-default identity decision.

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

    def resolve(self, value: object, **scope: object) -> RowMapping | None:
        """Return one uniquely exact row.

        :param value: Scalar identity value.
        :param scope: Optional public scope aliases.
        :return: Matching row, or ``None`` when not uniquely matched.
        """

        ...

    def match_or_create(
        self,
        candidate: MetadataCandidate,
        *,
        use_policy: bool = False,
    ) -> EntityId:
        """Reuse a permitted match or create on a genuine non-match.

        :param candidate: Candidate entity metadata.
        :param use_policy: Permit explicitly requested approximate reuse.
        :return: Existing or newly created entity ID.
        """

        ...


__all__ = ["ExactEntityRepositoryAPI"]

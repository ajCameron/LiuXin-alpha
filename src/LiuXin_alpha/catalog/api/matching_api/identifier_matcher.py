"""Identifier matching API."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from LiuXin_alpha.catalog.api.common import IdentifierCandidate, MatchResult


@runtime_checkable
class IdentifierMatcherAPI(Protocol):
    """Match curated identifiers after scheme-specific normalization.

    An Identifier can be copied into several owner-scoped rows. Consequently
    ``candidates`` may return multiple storage rows for the same logical
    scheme/value, while ``best`` selects the first stable row.
    """

    def candidates(
        self,
        candidate: "IdentifierCandidate",
        *,
        limit: int = 20,
    ) -> Sequence[MatchResult]:
        """Return every exact stored copy of a logical identifier.

        :param candidate: Identifier to normalize and compare.
        :param limit: Maximum stored rows to return.
        :return: Explained exact matches ordered by storage ID.
        """

        ...

    def best(self, candidate: IdentifierCandidate) -> MatchResult:
        """Return the first stored copy of an exact logical identifier.

        :param candidate: Identifier to normalize and compare.
        :return: First stable exact row or a no-match result.
        """

        ...

    def exact(self, candidate_str: str, id_type: str) -> MatchResult:
        """Return the exact decision for an identifier value and scheme.

        :param candidate_str: Identifier value to match.
        :param id_type: Identifier scheme or supported alias.
        :return: Explained exact match or no-match result.

        Example::

            result = catalog.matching.identifiers.exact(
                "978-0-14-143947-1",
                "isbn13",
            )
        """

        ...

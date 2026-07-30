"""Matching API for raw identifiers observed on Items."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from ..common import IdentifierCandidate, MatchResult


@runtime_checkable
class ItemIdentifierMatcherAPI(Protocol):
    """Exactly match scheme-aware identifier observations on Items.

    Pass ``item_id`` to distinguish an observation on one Item from identical
    values observed on other copies.
    """

    def candidates(
        self,
        candidate: IdentifierCandidate,
        *,
        item_id: int | None = None,
        limit: int = 20,
    ) -> Sequence[MatchResult]:
        """Return exact observations, optionally scoped to one Item.

        :param candidate: Identifier to normalize and compare.
        :param item_id: Optional owning Item ID.
        :param limit: Maximum observations to return.
        :return: Exact observation decisions ordered by storage ID.
        """

        ...

    def best(
        self,
        candidate: IdentifierCandidate,
        *,
        item_id: int | None = None,
    ) -> MatchResult:
        """Return the first exact stored observation.

        :param candidate: Identifier to normalize and compare.
        :param item_id: Optional owning Item ID.
        :return: First stable exact observation or no-match result.
        """

        ...

    def exact(
        self,
        candidate_str: str,
        id_type: str,
        *,
        item_id: int | None = None,
    ) -> MatchResult:
        """Return the exact observed decision for a value and scheme.

        :param candidate_str: Identifier value to match.
        :param id_type: Identifier scheme or supported alias.
        :param item_id: Optional owning Item ID.
        :return: Explained exact match or no-match result.

        Example::

            result = catalog.matching.item_identifiers.exact(
                "vendor-record-42",
                "source-id",
                item_id=item_id,
            )
        """

        ...


__all__ = ["ItemIdentifierMatcherAPI"]

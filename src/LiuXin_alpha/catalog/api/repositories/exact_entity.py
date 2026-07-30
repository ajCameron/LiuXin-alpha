"""Repository API for exact-default catalog entities."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..common import EntityId, MatchResult, MetadataCandidate, RowMapping
from .base import BaseRepositoryAPI


@runtime_checkable
class ExactEntityRepositoryAPI(BaseRepositoryAPI, Protocol):
    """CRUD and conservative matching for reusable value entities.

    Tags, labels, genres, subjects, series, languages, ratings, comments,
    synopses, notes, and annotations share this shape. Exact normalized reuse is
    the default; approximate policy matching is opt-in.

    Example::

        tag_id = catalog.tags.match_or_create(
            MetadataCandidate({"value": "Gothic"})
        )
        result = catalog.matching.for_entity("tags").exact("gothic")
    """

    def match(
        self,
        candidate: MetadataCandidate,
        *,
        use_policy: bool = False,
    ) -> MatchResult:
        """
        Return an exact-default identity decision.

        :param candidate: Candidate entity metadata.
        :param use_policy: Permit configured approximate matching only after
            exact matching fails.
        :return: Match, no-match, ambiguity, or conflict decision.
        """

        ...

    def exact(self, value: object, **scope: object) -> MatchResult:
        """
        Return the exact decision for a scalar identity value.

        :param value: Scalar identity value.
        :param scope: Entity-specific scope aliases, for example an owning Item
            for annotations.
        :return: Exact match, no-match, or ambiguity decision.
        """

        ...

    def resolve(self, value: object, **scope: object) -> RowMapping | None:
        """
        Return one uniquely exact row.

        :param value: Scalar identity value.
        :param scope: Optional public scope aliases.
        :return: Matching row, or ``None`` for no match or ambiguity.
        """

        ...

    def match_or_create(
        self,
        candidate: MetadataCandidate,
        *,
        use_policy: bool = False,
    ) -> EntityId:
        """
        Reuse a permitted match or create on a genuine non-match.

        :param candidate: Candidate entity metadata.
        :param use_policy: Permit explicitly requested approximate reuse.
        :return: Existing or newly created entity ID.
        :raises CatalogMatchError: For ambiguity or conflicting evidence.
        """

        ...


__all__ = ["ExactEntityRepositoryAPI"]

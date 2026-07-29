"""Manifestation repository API."""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from LiuXin_alpha.catalog.api.common import EntityId, MetadataCandidate, MatchResult, RowMapping
from LiuXin_alpha.catalog.api.repositories.base import BaseRepositoryAPI


# Todo: Doc string examples of how this class can be used
@runtime_checkable
class ManifestationRepositoryAPI(BaseRepositoryAPI, Protocol):
    """
    Storage and lookup API for Manifestation-level metadata.
    """

    def list_for_expression(self, expression_id: EntityId) -> Sequence[RowMapping]:
        """
        Return manifestations belonging to an expression.

        :param expression_id:
        :return:
        """

    def list_expressions(self, manifestation_id: EntityId) -> Sequence[RowMapping]:
        """
        Return Expressions linked to a Manifestation.

        :param manifestation_id:
        :return:
        """

    def match(self, expression_id: EntityId, candidate: MetadataCandidate) -> MatchResult:
        """
        Match a candidate manifestation inside an expression context.

        :param expression_id:
        :param candidate:
        :return:
        """

    def match_or_create(self, expression_id: EntityId, candidate: MetadataCandidate) -> EntityId:
        """
        Return a matched manifestation id, or create a new manifestation.

        :param expression_id:
        :param candidate:
        :return:
        """

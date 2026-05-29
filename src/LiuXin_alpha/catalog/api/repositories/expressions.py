"""
Expression repository API.

This supports calls such as "catalog.expression.match" and so on.
"""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from LiuXin_alpha.catalog.api.common import EntityId, MetadataCandidate, MatchResult, RowMapping
from LiuXin_alpha.catalog.api.repositories.base import BaseRepositoryAPI


@runtime_checkable
class ExpressionRepositoryAPI(BaseRepositoryAPI, Protocol):
    """
    Storage and lookup API for Expression-level metadata.
    """

    def list_for_work(self, work_id: EntityId) -> Sequence[RowMapping]:
        """
        Return expressions belonging to a work.

        :param work_id:
        :return:
        """

    def match(self, work_id: EntityId, candidate: MetadataCandidate) -> MatchResult:
        """
        Match a candidate expression inside a work context.

        :param work_id:
        :param candidate:
        :return:
        """

    def match_or_create(self, work_id: EntityId, candidate: MetadataCandidate) -> EntityId:
        """
        Return a matched expression id, or create a new expression.

        :param work_id:
        :param candidate:
        :return:
        """

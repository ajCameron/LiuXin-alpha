"""Expression repository contract."""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from LiuXin_alpha.catalog.api.common import EntityId, MetadataCandidate, MatchResult, RowMapping
from LiuXin_alpha.catalog.api.repositories.base import BaseRepositoryAPI


@runtime_checkable
class ExpressionRepositoryAPI(BaseRepositoryAPI, Protocol):
    """Storage and identity API for a Work's Expressions.

    An Expression captures realization-specific metadata such as language,
    translation, narration, cut, or revision. Identity decisions are scoped to
    a Work, so ``match`` and ``match_or_create`` always require ``work_id``.

    Example::

        expression_id = catalog.expressions.match_or_create(
            work_id,
            MetadataCandidate({
                "language_id": english_id,
                "label": "1818 English text",
            }),
        )
    """

    def list_for_work(self, work_id: EntityId) -> Sequence[RowMapping]:
        """
        Return expressions belonging to a work.

        :param work_id: Existing Work ID.
        :return: Priority-ordered Expression mappings. Each mapping includes
            ``"_catalog_link"`` relationship metadata.
        """

    def list_works(self, expression_id: EntityId) -> Sequence[RowMapping]:
        """
        Return Works linked to an Expression.

        :param expression_id: Existing Expression ID.
        :return: Priority-ordered Work mappings with ``"_catalog_link"``
            relationship metadata.
        """

    def match(self, work_id: EntityId, candidate: MetadataCandidate) -> MatchResult:
        """
        Match a candidate expression inside a work context.

        :param work_id: Existing Work which defines the match scope.
        :param candidate: Expression fields such as ``language_id``, ``label``,
            ``year``, or ``title_override``.
        :return: Contextual match, no-match, ambiguity, or conflict decision.
        """

    def match_or_create(self, work_id: EntityId, candidate: MetadataCandidate) -> EntityId:
        """
        Return a matched expression id, or create a new expression.

        The new Expression is linked to ``work_id`` in the same transaction.
        Unresolved ambiguity/conflict raises rather than creating a duplicate.

        :param work_id: Existing Work which will own the relationship.
        :param candidate: Expression fields accepted by :meth:`create`.
        :return: Existing safely matched or newly created Expression ID.
        """

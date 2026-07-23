"""Repository for Expression-level metadata."""

from __future__ import annotations

from typing import ClassVar, Mapping, Sequence

from ..api.common import EntityId, MetadataCandidate, MatchResult, RowMapping
from .base import BaseRepository
from ..matching.policy import contextual_match, raise_for_unresolved


class ExpressionRepository(BaseRepository):
    """Store Expressions and traverse their Work relationships."""

    table_name = "expressions"
    id_column = "expression_id"
    input_aliases: ClassVar[Mapping[str, str]] = {
        "id": "expression_id",
        "type": "expression_type",
        "label": "expression_label",
        "year": "expression_year",
        "is_preferred": "expression_is_preferred",
        "language_id": "expression_language_id",
        "mode": "expression_mode",
        "title": "expression_title_override",
        "title_override": "expression_title_override",
        "subtitle": "expression_subtitle",
        "wordcount": "expression_wordcount",
        "status": "expression_status",
        "origin_note": "expression_origin_note",
    }

    def list_for_work(self, work_id: EntityId) -> Sequence[RowMapping]:
        """Return Expressions linked to a Work.

        :param work_id: Work whose Expressions should be returned.
        :return: Priority-ordered Expression rows with link metadata.
        """

        return self._linked_rows("works", work_id, self.table_name)

    def list_works(self, expression_id: EntityId) -> Sequence[RowMapping]:
        """Return Works linked to an Expression.

        :param expression_id: Expression whose Works should be returned.
        :return: Priority-ordered Work rows with link metadata.
        """

        return self._linked_rows(self.table_name, expression_id, "works")

    def match(self, work_id: EntityId, candidate: MetadataCandidate) -> MatchResult:
        """Match an Expression within one Work.

        :param work_id: Work context for the match.
        :param candidate: Candidate Expression metadata.
        :return: Explained match or non-match result.
        """

        return contextual_match(
            self,
            self.list_for_work(work_id),
            candidate,
            identity_fields=("expression_label", "expression_title_override"),
            corroborating_fields=(
                "expression_year",
                "expression_language_id",
                "expression_type",
                "expression_mode",
            ),
            subject=f"Expression in Work {work_id}",
            policy=self.matching_policy,
        )

    def match_or_create(self, work_id: EntityId, candidate: MetadataCandidate) -> EntityId:
        """Return a matched Expression or create and link one.

        :param work_id: Work which owns the new relationship.
        :param candidate: Candidate Expression metadata.
        :return: Existing or newly created Expression ID.
        """

        match = self.match(work_id, candidate)
        if match.is_match:
            assert match.entity_id is not None
            return match.entity_id
        raise_for_unresolved(match)
        data = dict(candidate.data)
        data.pop("work_id", None)
        data.pop("expression_work_id", None)
        expression_id = self.create(data)
        self._link("works", work_id, self.table_name, expression_id)
        return expression_id


__all__ = ["ExpressionRepository"]

"""
Implement Expression metadata storage and semantic repository operations.

The repository extends base CRUD with explicit parent-scoped matching and relationship reads.
Returned mappings are shallow row copies; changing them does not persist values.
Persistence uses portable database macros, with no lifetime ownership of the database.
"""

from __future__ import annotations

from typing import ClassVar, Mapping, Sequence

from ..api.common import EntityId, MetadataCandidate, MatchResult, RowMapping
from .base import BaseRepository
from ..matching.policy import contextual_match, raise_for_unresolved


class ExpressionRepository(BaseRepository):
    """
    Store realization metadata and traverse Work/Expression relationships.

    Use expressions and expression_id with aliases for label, title/title_override, language_id,
    year, type, and mode. Base create inserts an Expression without automatically linking a Work.
    Relationships may have multiple parents or children, represented by schema links with
    _catalog_link metadata on traversal. match and match_or_create require an explicit Work scope.
    Creation and the following link do not share an enclosing transaction in this repository.

    Example:
        >>> repository = catalog.expressions  # doctest: +SKIP
        >>> result = repository.match(work_id, candidate)  # doctest: +SKIP
    """

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
        """
        Read Expression rows linked from an existing Work.

        Require the source before resolving the link and fetching each destination. Each row
        includes _catalog_link with endpoint IDs, type, priority, and extra link fields. Preserve
        link order and duplicate destinations reached by distinct links. Portable SQL macros order
        by descending priority when present, then destination ID; the repository performs no
        additional sort. No result limit or enclosing read transaction is applied.

        Example:
            >>> rows = catalog.expressions.list_for_work(work_id)  # doctest: +SKIP


        :param work_id: Nonnegative non-boolean integer ID of the required Work.
        :return: Tuple of shallow row dictionaries with _catalog_link metadata, in macro link order.
        :raises CatalogNotFoundError: If the source or any linked destination is missing.
        :raises Exception: ID validation, schema-link resolution, macro reads, and row-conversion failures propagate.
        """

        return self._linked_rows("works", work_id, self.table_name)

    def list_works(self, expression_id: EntityId) -> Sequence[RowMapping]:
        """
        Read Work rows linked from an existing Expression.

        Require the source before resolving the link and fetching each destination. Each row
        includes _catalog_link with endpoint IDs, type, priority, and extra link fields. Preserve
        link order and duplicate destinations reached by distinct links. Portable SQL macros order
        by descending priority when present, then destination ID; the repository performs no
        additional sort. No result limit or enclosing read transaction is applied.

        Example:
            >>> rows = catalog.expressions.list_works(expression_id)  # doctest: +SKIP


        :param expression_id: Nonnegative non-boolean integer ID of the required Expression.
        :return: Tuple of shallow row dictionaries with _catalog_link metadata, in macro link order.
        :raises CatalogNotFoundError: If the source or any linked destination is missing.
        :raises Exception: ID validation, schema-link resolution, macro reads, and row-conversion failures propagate.
        """

        return self._linked_rows(self.table_name, expression_id, "works")

    def match(self, work_id: EntityId, candidate: MetadataCandidate) -> MatchResult:
        """
        Match Expression identity among rows belonging to one existing Work.

        Fetch the scoped rows before validating the candidate, then normalize aliases while ignoring
        unknown columns. The explicit parent argument selects scope; it is not inferred from
        candidate.data. Repository ID-column restrictions and alias collision checks still apply.
        Candidate source/hints do not add evidence.

        Identity fields are expression_label and expression_title_override, each with weight five.
        Supporting fields are expression_year, expression_language_id, expression_type, and
        expression_mode, each with weight one. Any exact identity comparison qualifies a row;
        approximate identity needs exact corroboration. Disagreements still reduce confidence, and
        the current bound/default policy decides final acceptance and ambiguity. Supporting fields
        alone cannot establish a match.

        The contextual helper returns match, no_match, or ambiguous; it does not construct a
        conflict decision. This method performs reads without persistence or an enclosing snapshot
        transaction.

        Example:
            >>> result = catalog.expressions.match(work_id, MetadataCandidate({"label": "English text", "language_id": english_id}))  # doctest: +SKIP


        :param work_id: Existing Work ID whose children form the entire match scope.
        :param candidate: MetadataCandidate containing Expression identity and optional supporting fields.
        :return: An explained match, no_match, or ambiguous decision over the scoped rows.
        :raises CatalogNotFoundError: If the parent or a required linked row is missing.
        :raises Exception: ID/candidate validation, input normalization, policy access, and database/scoring failures propagate.
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
        """
        Reuse a scoped Expression or create it under the explicit Work.

        Return the matched ID without updating existing data or rewriting its relationship.
        Translate ambiguous/conflict results into Catalog exceptions before any creation. On
        no_match, shallow-copy candidate.data, discard work_id and expression_work_id, create the
        Expression, then link it to the explicit Work. The input mapping itself is not changed. The
        link helper assigns its default priority for a new ordered relationship.

        Matching can ignore unknown fields that creation subsequently rejects. Candidate
        source/hints are not independently persisted or applied to attachments. Creation occurs
        before _link starts its own transaction. This method does not wrap both operations together:
        without an outer transaction, a link failure can leave a newly created, unlinked Expression.
        Matching and creation also have no uniqueness lock here.

        Example:
            >>> entity_id = catalog.expressions.match_or_create(work_id, MetadataCandidate({"label": "English text", "language_id": english_id}))  # doctest: +SKIP


        :param work_id: Existing Work defining both lookup scope and the new parent relationship.
        :param candidate: Expression candidate whose data supplies fields for insertion only when no match is found.
        :return: Existing selected Expression ID or newly created ID after the parent relationship is established.
        :raises CatalogAmbiguousMatchError: If scoped matching remains ambiguous.
        :raises CatalogMatchConflictError: If an overridden matcher supplies a conflict decision.
        :raises Exception: Parent lookup, candidate validation, matching, insertion, and relationship failures propagate.
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

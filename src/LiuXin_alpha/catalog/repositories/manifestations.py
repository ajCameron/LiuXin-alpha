"""
Implement Manifestation metadata storage and semantic repository operations.

The repository extends base CRUD with explicit parent-scoped matching and relationship reads.
Returned mappings are shallow row copies; changing them does not persist values.
Persistence uses portable database macros, with no lifetime ownership of the database.
"""

from __future__ import annotations

from typing import ClassVar, Mapping, Sequence

from ..api.common import EntityId, MetadataCandidate, MatchResult, RowMapping
from .base import BaseRepository
from ..matching.policy import contextual_match, raise_for_unresolved


class ManifestationRepository(BaseRepository):
    """
    Store edition or publication metadata and traverse Expression relationships.

    Use manifestations and manifestation_id with aliases such as edition_statement, pub_year,
    pub_date, carrier_type, format_detail, and subtitle. Base create inserts metadata without
    linking an Expression. Relationship traversal returns linked row copies with _catalog_link
    metadata. Matching requires an explicit Expression scope. Creating and then linking a
    Manifestation are separate operations unless a caller supplies an encompassing transaction.

    Example:
        >>> repository = catalog.manifestations  # doctest: +SKIP
        >>> result = repository.match(expression_id, candidate)  # doctest: +SKIP
    """

    table_name = "manifestations"
    id_column = "manifestation_id"
    input_aliases: ClassVar[Mapping[str, str]] = {
        "id": "manifestation_id",
        "subtitle": "manifestation_subtitle",
        "carrier_type": "manifestation_carrier_type",
        "format_detail": "manifestation_format_detail",
        "edition_statement": "manifestation_edition_statement",
        "pub_year": "manifestation_pub_year",
        "pub_date": "manifestation_pub_date",
        "page_count": "manifestation_page_count",
        "runtime_minutes": "manifestation_runtime_minutes",
        "region_code": "manifestation_region_code",
        "status": "manifestation_status",
        "note": "manifestation_note",
    }

    def list_for_expression(self, expression_id: EntityId) -> Sequence[RowMapping]:
        """
        Read Manifestation rows linked from an existing Expression.

        Require the source before resolving the link and fetching each destination. Each row
        includes _catalog_link with endpoint IDs, type, priority, and extra link fields. Preserve
        link order and duplicate destinations reached by distinct links. Portable SQL macros order
        by descending priority when present, then destination ID; the repository performs no
        additional sort. No result limit or enclosing read transaction is applied.

        Example:
            >>> rows = catalog.manifestations.list_for_expression(expression_id)  # doctest: +SKIP


        :param expression_id: Nonnegative non-boolean integer ID of the required Expression.
        :return: Tuple of shallow row dictionaries with _catalog_link metadata, in macro link order.
        :raises CatalogNotFoundError: If the source or any linked destination is missing.
        :raises Exception: ID validation, schema-link resolution, macro reads, and row-conversion failures propagate.
        """

        return self._linked_rows("expressions", expression_id, self.table_name)

    def list_expressions(self, manifestation_id: EntityId) -> Sequence[RowMapping]:
        """
        Read Expression rows linked from an existing Manifestation.

        Require the source before resolving the link and fetching each destination. Each row
        includes _catalog_link with endpoint IDs, type, priority, and extra link fields. Preserve
        link order and duplicate destinations reached by distinct links. Portable SQL macros order
        by descending priority when present, then destination ID; the repository performs no
        additional sort. No result limit or enclosing read transaction is applied.

        Example:
            >>> rows = catalog.manifestations.list_expressions(manifestation_id)  # doctest: +SKIP


        :param manifestation_id: Nonnegative non-boolean integer ID of the required Manifestation.
        :return: Tuple of shallow row dictionaries with _catalog_link metadata, in macro link order.
        :raises CatalogNotFoundError: If the source or any linked destination is missing.
        :raises Exception: ID validation, schema-link resolution, macro reads, and row-conversion failures propagate.
        """

        return self._linked_rows(self.table_name, manifestation_id, "expressions")

    def match(self, expression_id: EntityId, candidate: MetadataCandidate) -> MatchResult:
        """
        Match Manifestation identity among rows belonging to one existing Expression.

        Fetch the scoped rows before validating the candidate, then normalize aliases while ignoring
        unknown columns. The explicit parent argument selects scope; it is not inferred from
        candidate.data. Repository ID-column restrictions and alias collision checks still apply.
        Candidate source/hints do not add evidence.

        Identity fields are manifestation_edition_statement, manifestation_pub_date, and
        manifestation_subtitle, each with weight five. Supporting fields are manifestation_pub_year,
        manifestation_carrier_type, manifestation_format_detail, and manifestation_region_code, each
        with weight one. Any exact identity comparison qualifies a row; approximate identity needs
        exact corroboration. Disagreements still reduce confidence, and the current bound/default
        policy decides final acceptance and ambiguity. Supporting fields alone cannot establish a
        match.

        The contextual helper returns match, no_match, or ambiguous; it does not construct a
        conflict decision. This method performs reads without persistence or an enclosing snapshot
        transaction.

        Example:
            >>> result = catalog.manifestations.match(expression_id, MetadataCandidate({"edition_statement": "First edition", "pub_year": 1818}))  # doctest: +SKIP


        :param expression_id: Existing Expression ID whose children form the entire match scope.
        :param candidate: MetadataCandidate containing Manifestation identity and optional supporting fields.
        :return: An explained match, no_match, or ambiguous decision over the scoped rows.
        :raises CatalogNotFoundError: If the parent or a required linked row is missing.
        :raises Exception: ID/candidate validation, input normalization, policy access, and database/scoring failures propagate.
        """

        return contextual_match(
            self,
            self.list_for_expression(expression_id),
            candidate,
            identity_fields=(
                "manifestation_edition_statement",
                "manifestation_pub_date",
                "manifestation_subtitle",
            ),
            corroborating_fields=(
                "manifestation_pub_year",
                "manifestation_carrier_type",
                "manifestation_format_detail",
                "manifestation_region_code",
            ),
            subject=f"Manifestation in Expression {expression_id}",
            policy=self.matching_policy,
        )

    def match_or_create(self, expression_id: EntityId, candidate: MetadataCandidate) -> EntityId:
        """
        Reuse a scoped Manifestation or create it under the explicit Expression.

        Return the matched ID without updating existing data or rewriting its relationship.
        Translate ambiguous/conflict results into Catalog exceptions before any creation. On
        no_match, shallow-copy candidate.data, discard expression_id and
        manifestation_expression_id, create the Manifestation, then link it to the explicit
        Expression. The input mapping itself is not changed. The link helper assigns its default
        priority for a new ordered relationship.

        Matching can ignore unknown fields that creation subsequently rejects. Candidate
        source/hints are not independently persisted or applied to attachments. Creation occurs
        before _link starts its own transaction. This method does not wrap both operations together:
        without an outer transaction, a link failure can leave a newly created, unlinked
        Manifestation. Matching and creation also have no uniqueness lock here.

        Example:
            >>> entity_id = catalog.manifestations.match_or_create(expression_id, MetadataCandidate({"edition_statement": "First edition", "pub_year": 1818}))  # doctest: +SKIP


        :param expression_id: Existing Expression defining both lookup scope and the new parent relationship.
        :param candidate: Manifestation candidate whose data supplies fields for insertion only when no match is found.
        :return: Existing selected Manifestation ID or newly created ID after the parent relationship is established.
        :raises CatalogAmbiguousMatchError: If scoped matching remains ambiguous.
        :raises CatalogMatchConflictError: If an overridden matcher supplies a conflict decision.
        :raises Exception: Parent lookup, candidate validation, matching, insertion, and relationship failures propagate.
        """

        match = self.match(expression_id, candidate)
        if match.is_match:
            assert match.entity_id is not None
            return match.entity_id
        raise_for_unresolved(match)
        data = dict(candidate.data)
        data.pop("expression_id", None)
        data.pop("manifestation_expression_id", None)
        manifestation_id = self.create(data)
        self._link("expressions", expression_id, self.table_name, manifestation_id)
        return manifestation_id


__all__ = ["ManifestationRepository"]

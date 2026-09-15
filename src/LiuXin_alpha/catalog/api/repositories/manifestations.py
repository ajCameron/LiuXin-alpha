"""
Declare the protocol for Manifestation metadata storage and semantic repository operations.

The repository extends base CRUD with explicit parent-scoped matching and relationship reads.
Returned mappings are shallow row copies; changing them does not persist values.
The protocol adds no runtime implementation or transaction guarantee.
"""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from LiuXin_alpha.catalog.api.common import EntityId, MetadataCandidate, MatchResult, RowMapping
from LiuXin_alpha.catalog.api.repositories.base import BaseRepositoryAPI


@runtime_checkable
class ManifestationRepositoryAPI(BaseRepositoryAPI, Protocol):
    """
    Store edition or publication metadata and traverse Expression relationships.

    Use manifestations and manifestation_id with aliases such as edition_statement, pub_year,
    pub_date, carrier_type, format_detail, and subtitle. Base create inserts metadata without
    linking an Expression. Relationship traversal returns linked row copies with _catalog_link
    metadata. Matching requires an explicit Expression scope. Creating and then linking a
    Manifestation are separate operations unless a caller supplies an encompassing transaction.

    This runtime-checkable protocol describes the concrete repository contract; method presence does
    not validate signatures, database capabilities, or policy.

    Example:
        >>> repository = catalog.manifestations  # doctest: +SKIP
        >>> result = repository.match(expression_id, candidate)  # doctest: +SKIP
    """

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

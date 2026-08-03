"""Manifestation repository API."""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from LiuXin_alpha.catalog.api.common import EntityId, MetadataCandidate, MatchResult, RowMapping
from LiuXin_alpha.catalog.api.repositories.base import BaseRepositoryAPI


@runtime_checkable
class ManifestationRepositoryAPI(BaseRepositoryAPI, Protocol):
    """Storage and identity API for an Expression's Manifestations.

    A Manifestation is an edition or publication embodiment: carrier, format,
    edition statement, publication date, pagination, region, and similar
    edition-level facts. Match/create is scoped to an Expression.

    Example::

        manifestation_id = catalog.manifestations.match_or_create(
            expression_id,
            MetadataCandidate({
                "edition_statement": "Penguin Classics",
                "pub_year": 2003,
                "carrier_type": "ebook",
            }),
        )
    """

    def list_for_expression(self, expression_id: EntityId) -> Sequence[RowMapping]:
        """
        Return manifestations belonging to an expression.

        :param expression_id: Existing Expression ID.
        :return: Priority-ordered Manifestation mappings with
            ``"_catalog_link"`` relationship metadata.
        """

    def list_expressions(self, manifestation_id: EntityId) -> Sequence[RowMapping]:
        """
        Return Expressions linked to a Manifestation.

        :param manifestation_id: Existing Manifestation ID.
        :return: Priority-ordered Expression mappings with relationship
            metadata.
        """

    def match(self, expression_id: EntityId, candidate: MetadataCandidate) -> MatchResult:
        """
        Match a candidate manifestation inside an expression context.

        :param expression_id: Existing Expression defining the match scope.
        :param candidate: Manifestation fields such as ``edition_statement``,
            ``pub_year``, ``carrier_type``, or ``format_detail``.
        :return: Contextual match, no-match, ambiguity, or conflict decision.
        """

    def match_or_create(self, expression_id: EntityId, candidate: MetadataCandidate) -> EntityId:
        """
        Return a matched manifestation id, or create a new manifestation.

        The new Manifestation is linked to ``expression_id`` transactionally.

        :param expression_id: Existing Expression to link.
        :param candidate: Manifestation fields accepted by :meth:`create`.
        :return: Existing safely matched or newly created Manifestation ID.
        """

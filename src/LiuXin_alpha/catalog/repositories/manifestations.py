"""Repository for Manifestation-level metadata."""

from __future__ import annotations

from typing import ClassVar, Mapping, Sequence

from ..api.common import EntityId, MetadataCandidate, MatchResult, RowMapping
from .base import BaseRepository
from ..matching.policy import contextual_match, raise_for_unresolved


class ManifestationRepository(BaseRepository):
    """Store Manifestations and traverse Expression relationships."""

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
        """Return Manifestations linked to an Expression."""

        return self._linked_rows("expressions", expression_id, self.table_name)

    def list_expressions(self, manifestation_id: EntityId) -> Sequence[RowMapping]:
        """Return Expressions linked to a Manifestation."""

        return self._linked_rows(self.table_name, manifestation_id, "expressions")

    def match(self, expression_id: EntityId, candidate: MetadataCandidate) -> MatchResult:
        """Match a Manifestation within one Expression."""

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
        """Return a matched Manifestation or create and link one."""

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

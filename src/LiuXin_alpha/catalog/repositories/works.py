"""Repository for Work-level metadata."""

from __future__ import annotations

from typing import ClassVar, Mapping, Sequence

from ..api.common import EntityId, MetadataCandidate, MatchResult, RowMapping
from .base import BaseRepository, normalise_text


class WorkRepository(BaseRepository):
    """Store, find, and match FRBR Works."""

    table_name = "works"
    id_column = "work_id"
    input_aliases: ClassVar[Mapping[str, str]] = {
        "id": "work_id",
        "title": "work_title",
        "canonical_title": "work_canonical_title",
        "sort_title": "work_sort_title",
        "creator_sort": "work_creator_sort",
        "type": "work_type",
        "medium": "work_medium",
        "original_language_id": "work_original_language_id",
        "original_year": "work_original_year",
        "original_date": "work_original_date",
        "original_copyright_date": "work_original_copyright_date",
        "wikipedia_link": "work_wikipedia_link",
        "is_fiction": "work_is_fiction",
        "audience": "work_audience",
        "completion_status": "work_completion_status",
        "discovery_note": "work_discovery_note",
    }

    def find_by_title(self, title: str, *, limit: int = 20) -> Sequence[RowMapping]:
        """Find Works by normalized preferred or canonical title.

        :param title: Human-readable title to match.
        :param limit: Maximum matches to return.
        :return: ID-ordered matching Works.
        """

        if not isinstance(title, str):
            raise TypeError("title must be a string")
        if not isinstance(limit, int) or isinstance(limit, bool):
            raise TypeError("limit must be an integer")
        if limit < 0:
            raise ValueError("limit cannot be negative")
        wanted = normalise_text(title)
        if not wanted or limit == 0:
            return ()
        return tuple(
            row
            for row in self._all_rows()
            if any(
                value is not None and normalise_text(value) == wanted
                for value in (
                    row.get("work_title"),
                    row.get("work_canonical_title"),
                )
            )
        )[:limit]

    def match(self, candidate: MetadataCandidate) -> MatchResult:
        """Return the best Work match for a metadata candidate.

        :param candidate: Candidate Work metadata.
        :return: Explained match or non-match result.
        """

        from ..matching.work_matcher import WorkMatcher

        return WorkMatcher(
            self.db,
            self.repositories,
            self.matching_policy,
        ).best(candidate)

    def match_or_create(self, candidate: MetadataCandidate) -> EntityId:
        """Return a matched Work ID, or create the Work.

        :param candidate: Candidate Work metadata.
        :return: Existing or newly created Work ID.
        """

        match = self.match(candidate)
        if match.is_match:
            assert match.entity_id is not None
            return match.entity_id
        from ..matching.policy import raise_for_unresolved

        raise_for_unresolved(match)
        return self.create(candidate.data)


__all__ = ["WorkRepository"]

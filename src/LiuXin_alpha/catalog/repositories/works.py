"""
Implement Work metadata storage and semantic repository operations.

The repository extends base CRUD with title lookup and global Work matching.
Returned mappings are shallow row copies; changing them does not persist values.
Persistence uses portable database macros, with no lifetime ownership of the database.
"""

from __future__ import annotations

from typing import ClassVar, Mapping, Sequence

from ..api.common import EntityId, MetadataCandidate, MatchResult, RowMapping
from .base import BaseRepository, normalise_text


class WorkRepository(BaseRepository):
    """
    Store and locate Works before Expression, edition, and copy distinctions.

    Base CRUD uses the works table and work_id, with aliases including title, canonical_title,
    sort_title, creator_sort, medium, and original_year. CRUD performs no automatic matching or
    child creation. find_by_title uses punctuation-preserving normalization on preferred/canonical
    titles; match uses the separate WorkMatcher and requires a bound repository group.
    match_or_create preserves existing metadata on reuse and does not persist candidate hints as
    identifiers or Agent credits.

    Example:
        >>> repository = catalog.works  # doctest: +SKIP
        >>> rows = repository.find_by_title("Frankenstein")  # doctest: +SKIP
    """

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
        """
        Find Works by normalized preferred or canonical title in ID order.

        Validate title and limit before returning early for blank normalized text or limit zero.
        Comparison uses NFKC, case-folding, and collapsed whitespace while preserving punctuation.
        It considers work_title and work_canonical_title, not work_sort_title; a stored non-None
        value is converted to text for comparison. Nonempty lookup materializes all Work rows and
        matching rows before slicing. This direct lookup does not apply fuzzy matching, confidence,
        or ambiguity policy.

        Example:
            >>> repository = WorkRepository(None)
            >>> repository._all_rows = lambda: (
            ...     {"work_id": 2, "work_canonical_title": "  Ａ & B!  "},
            ...     {"work_id": 3, "work_sort_title": "A & B!"},
            ... )
            >>> tuple(row["work_id"] for row in repository.find_by_title("a & b!"))
            (2,)
            >>> repository.find_by_title("a and b")
            ()


        :param title: Preferred or canonical title string to normalize and compare.
        :param limit: Nonnegative integer result cap; booleans are rejected.
        :return: Tuple of matching Work row copies in macro-provided ID order.
        :raises TypeError: If title is not a string or limit is not a non-boolean integer.
        :raises ValueError: If limit is negative.
        :raises Exception: Database reads and row/text conversion failures propagate.
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
        """
        Build a WorkMatcher with current repository dependencies and return its best decision.

        Read the bound repository group and current matching policy for each call; a standalone
        repository must be bound before using this method. Matching uses the group's
        Work/identifier/supporting repositories, which need not be this instance if composition was
        customized. No matcher is cached and no data is written.

        Identifier hints take the terminal ownership path, which can return a match below ordinary
        confidence acceptance. Otherwise descriptive title/evidence matching applies acceptance and
        ambiguity rules. Unknown fields may be ignored during matching even though later creation
        rejects them.

        Example:
            >>> result = catalog.works.match(MetadataCandidate({"title": "Frankenstein"}))  # doctest: +SKIP


        :param candidate: MetadataCandidate with Work fields and optional structured matching hints.
        :return: Explained match, no_match, ambiguous, or conflict decision from WorkMatcher.best.
        :raises RuntimeError: If no repository group is bound.
        :raises Exception: Candidate, hint, schema, and matching/read failures propagate.
        """

        from ..matching.work_matcher import WorkMatcher

        return WorkMatcher(
            self.db,
            self.repositories,
            self.matching_policy,
        ).best(candidate)

    def match_or_create(self, candidate: MetadataCandidate) -> EntityId:
        """
        Reuse a selected Work, or create from candidate data after an unresolved-result check.

        Return a matched ID immediately without updating that Work's metadata or provenance.
        Translate ambiguous/conflict results into Catalog match errors. For the normal no_match
        result, pass candidate.data unchanged to create; matching hints and source do not become
        stored identifiers, credits, or provenance. Unknown fields tolerated by matching can
        therefore fail creation.

        Matching and insertion are not enclosed in one transaction or lock here. Concurrent callers
        can race unless an outer operation/backend constraint provides stronger guarantees. This
        convenience does not ensure uniqueness by itself.

        Example:
            >>> work_id = catalog.works.match_or_create(MetadataCandidate({"title": "Frankenstein"}))  # doctest: +SKIP


        :param candidate: Work candidate whose data is inserted only when matching finds no reusable identity.
        :return: Existing selected Work ID or the ID returned by create.
        :raises CatalogAmbiguousMatchError: If matching leaves several plausible Works.
        :raises CatalogMatchConflictError: If matching reports a conflict.
        :raises Exception: Binding, matching, input validation, and insertion failures propagate.
        """

        match = self.match(candidate)
        if match.is_match:
            assert match.entity_id is not None
            return match.entity_id
        from ..matching.policy import raise_for_unresolved

        raise_for_unresolved(match)
        return self.create(candidate.data)


__all__ = ["WorkRepository"]

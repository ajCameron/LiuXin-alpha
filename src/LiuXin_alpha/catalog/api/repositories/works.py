"""
Declare the protocol for Work metadata storage and semantic repository operations.

The repository extends base CRUD with title lookup and global Work matching.
Returned mappings are shallow row copies; changing them does not persist values.
The protocol adds no runtime implementation or transaction guarantee.
"""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from ..common import EntityId, MetadataCandidate, MatchResult, RowMapping
from .base import BaseRepositoryAPI


@runtime_checkable
class WorkRepositoryAPI(BaseRepositoryAPI, Protocol):
    """
    Store and locate Works before Expression, edition, and copy distinctions.

    Base CRUD uses the works table and work_id, with aliases including title, canonical_title,
    sort_title, creator_sort, medium, and original_year. CRUD performs no automatic matching or
    child creation. find_by_title uses punctuation-preserving normalization on preferred/canonical
    titles; match uses the separate WorkMatcher and requires a bound repository group.
    match_or_create preserves existing metadata on reuse and does not persist candidate hints as
    identifiers or Agent credits.

    This runtime-checkable protocol describes the concrete repository contract; method presence does
    not validate signatures, database capabilities, or policy.

    Example:
        >>> repository = catalog.works  # doctest: +SKIP
        >>> rows = repository.find_by_title("Frankenstein")  # doctest: +SKIP
    """

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
            >>> rows = catalog.works.find_by_title("Frankenstein", limit=5)  # doctest: +SKIP


        :param title: Preferred or canonical title string to normalize and compare.
        :param limit: Nonnegative integer result cap; booleans are rejected.
        :return: Tuple of matching Work row copies in macro-provided ID order.
        :raises TypeError: If title is not a string or limit is not a non-boolean integer.
        :raises ValueError: If limit is negative.
        :raises Exception: Database reads and row/text conversion failures propagate.
        """

    # Todo: Regex titles match?
    # Todo: Find by creators, with roll

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

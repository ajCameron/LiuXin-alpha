"""
Declare normalized matching over curated identifier storage rows.

The protocol exposes a limited candidate list, deterministic first-copy selection,
and a value/scheme convenience call. Multiple storage rows for one logical value
are expected; owner ambiguity belongs to Work/Agent matching. Protocol membership
checks structure without validating implementations, inputs, or database readiness.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from LiuXin_alpha.catalog.api.common import IdentifierCandidate, MatchResult


@runtime_checkable
class IdentifierMatcherAPI(Protocol):
    """
    Describe normalized curated-identifier lookup with deterministic storage-copy selection.

    candidates may return several rows for the same scheme/value across owners. best and exact
    select the first stable copy rather than report ownership ambiguity. These are read-only
    identifier-row decisions, not proof of a unique bibliographic entity. Runtime protocol checks do
    not validate signatures or the behavior of the supplied implementation.

    Example:
        >>> matcher: IdentifierMatcherAPI = catalog.matching.identifiers  # doctest: +SKIP
        >>> result = matcher.exact("10.1000/abc", "doi")  # doctest: +SKIP
    """

    def candidates(
        self,
        candidate: "IdentifierCandidate",
        *,
        limit: int = 20,
    ) -> Sequence[MatchResult]:
        """
        Find normalized scheme/value matches across curated identifier storage rows.

        Validate the integer limit before normalizing the incoming candidate, then read all
        identifier rows. Normalize each stored scheme/value, skipping stored entries whose
        normalization raises TypeError or ValueError. Match canonical scheme and comparison value,
        with no owner-level or owner-ID restriction and no existence check on referenced
        bibliographic entities.

        Each matching row supplies confidence one and two decisive identifier evidence items. Retain
        the original row, sort by storage ID with -1 as the key for a false-valued ID, and only then
        slice. Zero still normalizes and scans. Duplicate logical values/storage copies are not
        collapsed. Row IDs are used directly without an integer/positivity check; malformed IDs can
        affect result shape or sorting, and a missing ID key raises.

        Example:
            >>> matches = catalog.matching.identifiers.candidates(  # doctest: +SKIP
            ...     IdentifierCandidate("DOI", "doi:10.1000/abc"), limit=10,
            ... )


        :param candidate: IdentifierCandidate whose scheme and value or normalized override should be matched.
        :param limit: Nonnegative integer cap after the full scan/sort; booleans are rejected.
        :return: A tuple of storage-row results in ID order, normally exact match decisions for well-formed rows.
        :raises TypeError: If limit is not an integer or is a boolean, or incoming identifier fields are invalid.
        :raises ValueError: If limit is negative or incoming identifier normalization fails.
        :raises KeyError: If a matching stored row lacks entity_identifier_id.
        :raises Exception: Repository access and other malformed-row/normalization failures propagate.
        """

        ...

    def best(self, candidate: IdentifierCandidate) -> MatchResult:
        """
        Select the first stored row matching a logical curated identifier.

        Delegate to candidates with limit one, which still scans and sorts every matching storage
        copy. Multiple owners or copies do not produce ambiguity here; Work and Agent matchers
        resolve bibliographic ownership separately. Return the first result unchanged, or a new
        zero-confidence no_match when no candidate remains. No matching-policy thresholds or entity
        writes apply.

        Example:
            >>> result = catalog.matching.identifiers.best(  # doctest: +SKIP
            ...     IdentifierCandidate("isbn13", "978-0-306-40615-7"),
            ... )


        :param candidate: Logical identifier to normalize and compare across all curated storage copies.
        :return: First matching storage-row result, or a no_match result when there are no candidates.
        :raises Exception: Incoming normalization, repository, and row-processing failures propagate from candidates.
        """

        ...

    def exact(self, candidate_str: str, id_type: str) -> MatchResult:
        """
        Build an identifier candidate from a value and scheme, then select its first copy.

        The concrete implementation calls best with a new IdentifierCandidate. Shared normalization
        validates the strings and interprets supported scheme aliases; no candidate source, hints,
        or normalized override are supplied. This matches normalized identifier text rather than
        literal byte spelling.

        Example:
            >>> result = catalog.matching.identifiers.exact("978-0-306-40615-7", "ISBN-13")  # doctest: +SKIP


        :param candidate_str: Original identifier value string to normalize and match.
        :param id_type: Identifier scheme string or supported alias.
        :return: The first matching stored-copy result, or no_match; duplicate copies are not reported as ambiguous.
        :raises Exception: Identifier validation and delegated matching failures propagate.
        """

        ...

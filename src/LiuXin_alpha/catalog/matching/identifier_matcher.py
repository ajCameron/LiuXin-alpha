"""
Match normalized curated identifier values across their stored copies.

IdentifierMatcher compares canonical schemes and scheme-specific values, then
orders matching rows by storage ID. It intentionally selects one stable stored
copy instead of interpreting multiple bibliographic owners as ambiguity. Work and
Agent matchers handle that ownership decision. Incoming invalid identifiers raise;
selected normalization failures in stored rows are skipped. No entity is written.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from ..api.common import (
    DatabaseHandle,
    IdentifierCandidate,
    MatchEvidence,
    MatchResult,
)
from .policy import normalise_identifier


class IdentifierMatcher:
    """
    Find stored copies of a normalized curated identifier without resolving owners.

    Reads use the identifier repository in the retained group. Stored raw scheme and value columns
    are normalized on each scan; other normalized/provenance columns are not used as comparison
    inputs. Multiple matching copies remain visible in candidates, and best/exact select the first
    by storage ID without ambiguity or confidence-threshold checks.

    Example:
        >>> from types import SimpleNamespace
        >>> rows = (
        ...     {"entity_identifier_id": 5, "entity_identifier_scheme": "local", "entity_identifier_value": "42"},
        ...     {"entity_identifier_id": 2, "entity_identifier_scheme": "local", "entity_identifier_value": "42"},
        ... )
        >>> repository = SimpleNamespace(_all_rows=lambda: rows)
        >>> matcher = IdentifierMatcher(None, SimpleNamespace(identifiers=repository))
        >>> matcher.exact("42", "local").entity_id
        2


    :ivar db: Borrowed database context retained without opening, closing, or direct query use.
    :ivar repositories: Bound group exposing the curated identifiers repository and its row snapshots.
    """

    def __init__(self, db: DatabaseHandle, repositories: Any) -> None:
        """
        Retain the database context and repository group without validating capabilities.

        Objects are assigned by reference. Construction does not query rows, copy repository state,
        or take ownership of the database lifetime.

        Example:
            >>> marker = object()
            >>> matcher = IdentifierMatcher(marker, marker)
            >>> matcher.db is matcher.repositories
            True


        :param db: Borrowed database context stored for the matcher.
        :param repositories: Group expected to expose an identifiers repository when matching starts.
        :return: None after assigning db and repositories.
        """

        self.db = db
        self.repositories = repositories

    def candidates(
        self,
        candidate: IdentifierCandidate,
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

        if not isinstance(limit, int) or isinstance(limit, bool):
            raise TypeError("limit must be an integer")
        if limit < 0:
            raise ValueError("limit cannot be negative")
        normalised = normalise_identifier(candidate)
        repository = self.repositories.identifiers
        results: list[MatchResult] = []
        for row in repository._all_rows():
            try:
                stored = normalise_identifier(
                    IdentifierCandidate(
                        str(row.get("entity_identifier_scheme") or ""),
                        str(row.get("entity_identifier_value") or ""),
                    )
                )
            except (TypeError, ValueError):
                continue
            if (
                stored.identifier_type != normalised.identifier_type
                or stored.normalised_value != normalised.normalised_value
            ):
                continue
            evidence = (
                MatchEvidence(
                    "entity_identifier_scheme",
                    "identifier",
                    1.0,
                    1.0,
                    "exact normalized identifier scheme",
                    normalised.identifier_type,
                    stored.identifier_type,
                    decisive=True,
                ),
                MatchEvidence(
                    "entity_identifier_value",
                    "identifier",
                    1.0,
                    1.0,
                    "exact scheme-specific identifier value",
                    normalised.normalised_value,
                    stored.normalised_value,
                    decisive=True,
                ),
            )
            results.append(
                MatchResult(
                    row["entity_identifier_id"],
                    1.0,
                    "exact normalized identifier",
                    ("entity_identifier_scheme", "entity_identifier_value"),
                    row,
                    evidence=evidence,
                )
            )
        results.sort(key=lambda result: result.entity_id or -1)
        return tuple(results[:limit])

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

        results = self.candidates(candidate, limit=1)
        if results:
            return results[0]
        return MatchResult(
            entity_id=None,
            confidence=0.0,
            reason="no identifier with the same normalized scheme and value",
            decision="no_match",
        )

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

        return self.best(IdentifierCandidate(id_type, candidate_str))


__all__ = ["IdentifierMatcher"]

"""Scheme-aware Identifier identity matching."""

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
    """Match identifier candidates after scheme-specific normalization.

    :param db: Catalog database handle.
    :param repositories: Bound catalog repository group.
    """

    def __init__(self, db: DatabaseHandle, repositories: Any) -> None:
        """Store the database and repository group.

        :param db: Catalog database handle.
        :param repositories: Bound catalog repository group.
        :return: None.
        """

        self.db = db
        self.repositories = repositories

    def candidates(
        self,
        candidate: IdentifierCandidate,
        *,
        limit: int = 20,
    ) -> Sequence[MatchResult]:
        """Return every exact stored copy of a logical identifier.

        :param candidate: Identifier to normalize and compare.
        :param limit: Maximum stored rows to return.
        :return: Exact identifier rows ordered by storage ID.
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
        """Return the first stored copy of an exact logical identifier.

        Storage copies are deliberately deterministic rather than ambiguous;
        bibliographic ownership ambiguity is handled by Work and Agent matchers.

        :param candidate: Identifier to normalize and compare.
        :return: Explained exact match or no-match result.
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
        """Return the exact decision for an identifier value and scheme.

        :param candidate_str: Identifier value to match.
        :param id_type: Identifier scheme.
        :return: Explained exact match or no-match result.
        """

        return self.best(IdentifierCandidate(id_type, candidate_str))


__all__ = ["IdentifierMatcher"]

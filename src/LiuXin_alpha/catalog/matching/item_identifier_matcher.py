"""Exact matching for raw identifiers observed on catalog Items."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from ..api.common import IdentifierCandidate, MatchEvidence, MatchResult
from .policy import normalise_identifier


class ItemIdentifierMatcher:
    """Match observed Item identifiers after scheme-specific normalization.

    :param repository: Bound observed Item identifier repository.
    """

    def __init__(self, repository: Any) -> None:
        """Store the observed identifier repository.

        :param repository: Bound observed Item identifier repository.
        :return: None.
        """

        self.repository = repository

    def candidates(
        self,
        candidate: IdentifierCandidate,
        *,
        item_id: int | None = None,
        limit: int = 20,
    ) -> Sequence[MatchResult]:
        """Return exact observed identifier rows, optionally scoped to an Item.

        :param candidate: Identifier to normalize and compare.
        :param item_id: Optional owning Item ID.
        :param limit: Maximum observations to return.
        :return: Exact observations ordered by storage ID.
        """

        if limit < 0:
            raise ValueError("limit cannot be negative")
        normalised = normalise_identifier(candidate)
        results: list[MatchResult] = []
        for row in self.repository._all_rows():
            if item_id is not None and row.get("item_identifier_item_id") != item_id:
                continue
            try:
                stored = normalise_identifier(
                    IdentifierCandidate(
                        str(row.get("item_identifier_scheme") or ""),
                        str(row.get("item_identifier_value") or ""),
                    )
                )
            except (TypeError, ValueError):
                continue
            if (
                stored.identifier_type != normalised.identifier_type
                or stored.normalised_value != normalised.normalised_value
            ):
                continue
            row_id = row.get("item_identifier_id")
            if not isinstance(row_id, int):
                continue
            evidence = (
                MatchEvidence(
                    "item_identifier_scheme",
                    "identifier",
                    1.0,
                    1.0,
                    "exact normalized observed identifier scheme",
                    normalised.identifier_type,
                    stored.identifier_type,
                    decisive=True,
                ),
                MatchEvidence(
                    "item_identifier_value",
                    "identifier",
                    1.0,
                    1.0,
                    "exact scheme-specific observed identifier value",
                    normalised.normalised_value,
                    stored.normalised_value,
                    decisive=True,
                ),
            )
            results.append(
                MatchResult(
                    row_id,
                    1.0,
                    "exact normalized observed Item identifier",
                    ("item_identifier_scheme", "item_identifier_value"),
                    row,
                    evidence=evidence,
                )
            )
        results.sort(key=lambda result: result.entity_id or -1)
        return tuple(results[:limit])

    def best(
        self,
        candidate: IdentifierCandidate,
        *,
        item_id: int | None = None,
    ) -> MatchResult:
        """Return the first exact stored observation.

        :param candidate: Identifier to normalize and compare.
        :param item_id: Optional owning Item ID.
        :return: Explained exact match or no-match result.
        """

        results = self.candidates(candidate, item_id=item_id, limit=1)
        if results:
            return results[0]
        return MatchResult(
            None,
            0.0,
            "no observed Item identifier with the same scheme and value",
            decision="no_match",
        )

    def exact(
        self,
        candidate_str: str,
        id_type: str,
        *,
        item_id: int | None = None,
    ) -> MatchResult:
        """Return the exact observed decision for a value and scheme.

        :param candidate_str: Identifier value to match.
        :param id_type: Identifier scheme.
        :param item_id: Optional owning Item ID.
        :return: Explained exact match or no-match result.
        """

        return self.best(
            IdentifierCandidate(id_type, candidate_str),
            item_id=item_id,
        )


__all__ = ["ItemIdentifierMatcher"]

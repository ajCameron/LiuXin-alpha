"""
Match normalized identifier observations on Items with an optional owner filter.

Each observation remains a separate stored row even when its scheme/value equals
another Item's observation. The matcher reads raw stored scheme/value columns,
normalizes them, and selects by observation ID after optional direct Item equality
filtering. It does not resolve copy identity, report repeated values as ambiguity,
validate Item existence, or persist any changes.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from ..api.common import IdentifierCandidate, MatchEvidence, MatchResult
from .policy import normalise_identifier


class ItemIdentifierMatcher:
    """
    Find exact identifier observations, retaining their per-Item storage identity.

    The repository supplies all observation rows. Optional item_id narrows reads by equality, while
    best/exact otherwise choose the first global stored ID. Equal values are not deduplicated and do
    not cause an ambiguous result. No MatchingPolicy thresholds or owner validation are involved.

    Example:
        >>> from types import SimpleNamespace
        >>> rows = (
        ...     {"item_identifier_id": 5, "item_identifier_item_id": 1,
        ...      "item_identifier_scheme": "local", "item_identifier_value": "42"},
        ...     {"item_identifier_id": 2, "item_identifier_item_id": 2,
        ...      "item_identifier_scheme": "local", "item_identifier_value": "42"},
        ... )
        >>> matcher = ItemIdentifierMatcher(SimpleNamespace(_all_rows=lambda: rows))
        >>> matcher.exact("42", "local").entity_id, matcher.exact("42", "local", item_id=1).entity_id
        (2, 5)


    :ivar repository: Retained observed-identifier repository exposing _all_rows snapshots.
    """

    def __init__(self, repository: Any) -> None:
        """
        Retain the observed-identifier repository without validation or a row read.

        Store the supplied object by reference. Matching later requires its row snapshot interface;
        construction does not alter repository configuration or assume database-lifetime ownership.

        Example:
            >>> repository = object()
            >>> ItemIdentifierMatcher(repository).repository is repository
            True


        :param repository: Owner supplying observed identifier rows when matching is requested.
        :return: None after retaining the repository reference.
        """

        self.repository = repository

    def candidates(
        self,
        candidate: IdentifierCandidate,
        *,
        item_id: int | None = None,
        limit: int = 20,
    ) -> Sequence[MatchResult]:
        """
        Find exact normalized identifier observations, optionally restricted to one Item.

        The concrete matcher checks only whether limit is negative before normalizing the incoming
        candidate and reading all rows. A non-None item_id filters by direct equality before
        stored-value normalization; it is not validated or checked against an existing Item. Skip
        stored normalization TypeError/ValueError and rows with non-integer observation IDs.
        Python's integer check still admits booleans and imposes no positivity.

        Matching scheme/value pairs produce confidence-one results with two decisive evidence items
        and their original row references. Sort by observation ID, using -1 for a false-valued ID,
        then slice. Zero still normalizes and scans; bool limits act as zero/one, while an
        unsuitable slice bound can fail after the work. Equal values on different Items remain
        separate observations.

        Example:
            >>> observations = catalog.matching.item_identifiers.candidates(  # doctest: +SKIP
            ...     IdentifierCandidate("source-id", "record-42"), item_id=item_id, limit=10,
            ... )


        :param candidate: IdentifierCandidate to normalize before comparing stored observation scheme/value pairs.
        :param item_id: Optional Item equality filter; None searches all Items and no owner existence check is performed.
        :param limit: Result cap after sorting; use a nonnegative integer, although the implementation also accepts bool.
        :return: A tuple of exact observation results ordered by stored observation ID.
        :raises ValueError: If limit is negative or incoming identifier normalization fails.
        :raises TypeError: If limit cannot be compared/sliced or incoming identifier fields have unsupported types.
        :raises Exception: Repository access and other malformed input/row failures propagate.
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
        """
        Select the first exact observed identifier within the optional Item scope.

        The concrete implementation calls candidates with limit one, so all eligible observations
        are still read, compared, and sorted. It reports no ambiguity for repeated observations or
        equal values across Items. Supply item_id when ownership matters; otherwise the first global
        observation wins. No confidence policy or write is applied.

        Example:
            >>> result = catalog.matching.item_identifiers.best(  # doctest: +SKIP
            ...     IdentifierCandidate("source-id", "record-42"), item_id=item_id,
            ... )


        :param candidate: Logical observed identifier to normalize and match.
        :param item_id: Optional direct-equality owner filter, with None selecting across all Items.
        :return: The first exact observation result, or a new zero-confidence no_match when none exist.
        :raises Exception: Incoming normalization, repository access, and row-processing failures propagate.
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
        """
        Wrap a value/scheme pair and select its first observation in the requested scope.

        Construct an IdentifierCandidate without source, hints, or a normalized override, then
        forward it and item_id to best. Shared normalization validates strings and handles scheme
        aliases. Scope is passed unchanged; this method does not establish the existence or
        uniqueness of an Item.

        Example:
            >>> result = catalog.matching.item_identifiers.exact(  # doctest: +SKIP
            ...     "record-42", "source-id", item_id=item_id,
            ... )


        :param candidate_str: Original observation value string to normalize and compare.
        :param id_type: Identifier scheme string or supported alias.
        :param item_id: Optional Item equality filter passed unchanged to best; None searches globally.
        :return: First exact stored observation or no_match, without an ambiguity outcome for repeated values.
        :raises Exception: Identifier validation and delegated matching failures propagate.
        """

        return self.best(
            IdentifierCandidate(id_type, candidate_str),
            item_id=item_id,
        )


__all__ = ["ItemIdentifierMatcher"]

"""
Declare exact observed-identifier lookup with optional Item scoping.

The API returns observation-row decisions, preserving separate records for equal
values found on different Items. best/exact choose the first ordered observation;
they do not infer a unique Item or turn duplicates into ambiguity. Implementations
provide the read behavior; runtime protocol checks verify structural presence only.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from ..common import IdentifierCandidate, MatchResult


@runtime_checkable
class ItemIdentifierMatcherAPI(Protocol):
    """
    Describe normalized observation matching within one Item or across all Items.

    Pass item_id to distinguish an observation on a selected copy from the same logical value
    elsewhere. Candidate lists preserve individual storage rows; best/exact return the first match
    without resolving global ownership ambiguity. Matching is read-only and runtime protocol
    membership does not verify signatures, Item existence, or result semantics.

    Example:
        >>> matcher: ItemIdentifierMatcherAPI = catalog.matching.item_identifiers  # doctest: +SKIP
        >>> result = matcher.exact("record-42", "source-id", item_id=item_id)  # doctest: +SKIP
    """

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

        ...

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

        ...

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

        ...


__all__ = ["ItemIdentifierMatcherAPI"]

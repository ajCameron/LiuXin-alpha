"""
Declare Work candidate-list, final-decision, and title-only matching operations.

WorkMatcherAPI is a runtime-checkable structural protocol. The implementation
lives in catalog.matching.work_matcher and distinguishes descriptive acceptance
from terminal identifier ownership. Its exact parameter has a different keyword
name from this protocol; positional title calls work across both interfaces.
Protocol annotations do not execute matching or validate repository capabilities.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:

    from LiuXin_alpha.catalog.api.common import MetadataCandidate, MatchResult


@runtime_checkable
class WorkMatcherAPI(Protocol):
    """
    Describe read-only Work identity decisions from metadata and structured hints.

    The concrete matcher normalizes Unicode, case, whitespace, and punctuation. Approximate
    descriptive titles need corroboration; identifier owners use a separate terminal path whose
    matches bypass final confidence acceptance. candidates is a limited display of possibilities,
    while best retains ambiguity/conflict decisions. Runtime protocol checks establish structural
    presence, not matching behavior or signature agreement.

    exact names its parameter cand_str here and candidate_str in the concrete WorkMatcher. Use
    positional title arguments across that boundary.

    Example:
        >>> matcher: WorkMatcherAPI = catalog.matching.works  # doctest: +SKIP
        >>> result = matcher.exact("Frankenstein")  # doctest: +SKIP
    """

    def candidates(self, candidate: MetadataCandidate, *, limit: int = 20) -> Sequence[MatchResult]:
        """
        Collect possible Work matches and apply a display limit after ranking.

        The concrete matcher evaluates identifier ownership first, otherwise scanning descriptive
        rows. Decisive evidence ranks ahead of descending confidence, then ascending entity ID with
        -1 used for a false-valued ID. Acceptance and ambiguity are not resolved by this method.
        Identifier ambiguity or conflict is suppressed as an empty candidate tuple; use best to
        distinguish it.

        A limit of zero still performs normalization and matching before returning empty. Candidate
        records retain their stored row mappings and can have confidence below acceptance. No
        catalogue entity is created or changed.

        Example:
            >>> candidates = catalog.matching.works.candidates(candidate, limit=5)  # doctest: +SKIP


        :param candidate: MetadataCandidate containing Work fields and supported structured hints.
        :param limit: Nonnegative integer result cap applied after evaluation; booleans are rejected.
        :return: Ranked possible matches, returned as a tuple by the concrete implementation.
        :raises TypeError: If limit is not an integer, is a boolean, or candidate is not a MetadataCandidate.
        :raises ValueError: If limit is negative or a supplied identifier fails normalization.
        :raises Exception: Repository access, input/hint parsing, and evidence failures propagate.
        """

        ...

    def best(self, candidate: MetadataCandidate) -> MatchResult:
        """
        Return the complete Work decision with identifier ownership handled first.

        The concrete matcher returns any terminal identifier result directly, including a match
        whose weighted confidence is below the ordinary acceptance threshold. Identifier-specific
        contradiction checks can instead produce ambiguity or conflict. Only descriptive-only
        candidate sets undergo common acceptance and ambiguity selection. Read the decision rather
        than deriving permission to create from confidence or a missing ID alone.

        Example:
            >>> result = catalog.matching.works.best(candidate)  # doctest: +SKIP
            >>> requires_review = result.requires_resolution  # doctest: +SKIP


        :param candidate: MetadataCandidate carrying Work fields and optional identifier/other supported hints.
        :return: A match, no_match, ambiguous, or conflict result; selected records retain the stored row mapping.
        :raises TypeError: If candidate is not a MetadataCandidate.
        :raises Exception: Repository, input/hint normalization, and evidence failures propagate.
        """

        ...

    def exact(self, cand_str: str) -> MatchResult:
        """
        Apply final matching policy to one normalized Work title string.

        This convenience supplies only the title, without identifier or supporting hints. Matching
        is Unicode/case/punctuation tolerant, not byte equality; empty normalized text does not
        establish identity. Duplicate qualifying rows can produce ambiguity. The protocol names its
        argument cand_str, while the concrete method names it candidate_str; use a positional
        argument when calling across both contracts.

        Example:
            >>> result = catalog.matching.works.exact("Frankenstein")  # doctest: +SKIP


        :param cand_str: Work title supplied as a string; positional calls are portable across the protocol and implementation.
        :return: A normalized exact match, no_match, or ambiguous result for the string-only input.
        :raises TypeError: If the supplied title/name is not a string.
        :raises Exception: Repository and delegated matching failures propagate.
        """

        ...

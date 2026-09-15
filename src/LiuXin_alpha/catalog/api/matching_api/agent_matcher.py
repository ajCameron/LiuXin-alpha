"""
Declare Agent candidate-list, final-decision, and name-only matching operations.

The concrete matcher distinguishes exact normalized descriptive names/aliases from
identifier-backed owner checks. Candidate display limits do not resolve ambiguity
or retain terminal conflicts; best returns the full decision. This protocol supplies
no matching implementation or runtime behavioral/signature validation.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from ..common import MetadataCandidate, MatchResult


@runtime_checkable
class AgentMatcherAPI(Protocol):
    """
    Describe read-only Agent identity decisions from names, aliases, and identifiers.

    Descriptive-only comparisons require exact normalized name or alias evidence; there is no
    approximate name-only policy switch. Identifier ownership can tolerate a nonexact name above the
    configured contradiction cutoff, while incompatible supplied types produce conflict. Terminal
    identifier matches bypass final confidence acceptance. Runtime protocol membership checks
    structure rather than these behavioral rules.

    Example:
        >>> matcher: AgentMatcherAPI = catalog.matching.agents  # doctest: +SKIP
        >>> result = matcher.exact("Mary Shelley")  # doctest: +SKIP
    """

    def candidates(self, candidate: MetadataCandidate, *, limit: int = 20) -> Sequence[MatchResult]:
        """
        Collect possible Agent matches and apply a display limit after ranking.

        The concrete matcher evaluates identifier ownership first, otherwise scanning descriptive
        rows. Decisive evidence ranks ahead of descending confidence, then ascending entity ID with
        -1 used for a false-valued ID. Acceptance and ambiguity are not resolved by this method.
        Identifier ambiguity or conflict is suppressed as an empty candidate tuple; use best to
        distinguish it.

        A limit of zero still performs normalization and matching before returning empty. Candidate
        records retain their stored row mappings and can have confidence below acceptance. No
        catalogue entity is created or changed.

        Example:
            >>> candidates = catalog.matching.agents.candidates(candidate, limit=5)  # doctest: +SKIP


        :param candidate: MetadataCandidate containing Agent fields and supported structured hints.
        :param limit: Nonnegative integer result cap applied after evaluation; booleans are rejected.
        :return: Ranked possible matches, returned as a tuple by the concrete implementation.
        :raises TypeError: If limit is not an integer, is a boolean, or candidate is not a MetadataCandidate.
        :raises ValueError: If limit is negative or a supplied identifier fails normalization.
        :raises Exception: Repository access, input/hint parsing, and evidence failures propagate.
        """

        ...

    def best(self, candidate: MetadataCandidate) -> MatchResult:
        """
        Return the complete Agent decision with identifier ownership handled first.

        The concrete matcher returns any terminal identifier result directly, including a match
        whose weighted confidence is below the ordinary acceptance threshold. Identifier-specific
        contradiction checks can instead produce ambiguity or conflict. Only descriptive-only
        candidate sets undergo common acceptance and ambiguity selection. Read the decision rather
        than deriving permission to create from confidence or a missing ID alone.

        Example:
            >>> result = catalog.matching.agents.best(candidate)  # doctest: +SKIP
            >>> requires_review = result.requires_resolution  # doctest: +SKIP


        :param candidate: MetadataCandidate carrying Agent fields and optional identifier/other supported hints.
        :return: A match, no_match, ambiguous, or conflict result; selected records retain the stored row mapping.
        :raises TypeError: If candidate is not a MetadataCandidate.
        :raises Exception: Repository, input/hint normalization, and evidence failures propagate.
        """

        ...

    def exact(self, candidate_str: str) -> "MatchResult":
        """
        Apply final matching policy to one normalized Agent name string.

        This convenience supplies only the name, without identifier or supporting hints. Matching is
        Unicode/case/punctuation tolerant, not byte equality; empty normalized text does not
        establish identity. Duplicate qualifying rows can produce ambiguity. The concrete
        implementation wraps the string in a MetadataCandidate and calls best.

        Example:
            >>> result = catalog.matching.agents.exact("Mary Shelley")  # doctest: +SKIP


        :param candidate_str: Agent name supplied as a string; positional calls are portable across the protocol and implementation.
        :return: A normalized exact match, no_match, or ambiguous result for the string-only input.
        :raises TypeError: If the supplied title/name is not a string.
        :raises Exception: Repository and delegated matching failures propagate.
        """

        ...

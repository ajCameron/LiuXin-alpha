"""
Declare the common exact-default matcher contract for Catalog value entities.

The protocol exposes ranked candidates, final candidate decisions, and scoped
scalar exact lookup. Approximate fallback is explicit and cannot override a
terminal exact-stage decision. The concrete implementation and its comparison
specification live in catalog.matching.exact_matcher; repository owners separately
decide whether a result can be reused or a new row can be created.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from ..common import MatchResult, MetadataCandidate


@runtime_checkable
class ExactEntityMatcherAPI(Protocol):
    """
    Describe read-only exact-default decisions for configured Catalog value entities.

    Approximate comparison requires explicit use_policy=True and is available only for
    specifications with a policy field. candidates is a ranked display selection; best preserves
    no-match, ambiguity, and conflict decisions. Scalar exact lookup uses its supplied scope and
    scalar fields, independently of candidate required-identity fields. Repository reuse/creation
    policy remains separate, including for non-reusable comments and annotations.

    This runtime-checkable protocol describes structure without implementing matching or validating
    method signatures and behavior.

    Example:
        >>> matcher: ExactEntityMatcherAPI = catalog.matching.tags  # doctest: +SKIP
        >>> result = matcher.exact("Gothic")  # doctest: +SKIP
    """

    def candidates(
        self,
        candidate: MetadataCandidate,
        *,
        limit: int = 20,
        use_policy: bool = False,
    ) -> Sequence[MatchResult]:
        """
        List exact candidates, or opt-in approximate candidates after a nonterminal miss.

        Exact matches take precedence. Missing required scope/identity, absent identity values, and
        a detected conflict are terminal outcomes: they return an empty sequence and block approximate
        fallback. Use best to distinguish these outcomes. Approximate candidates pass the text
        cutoff but are not filtered by the final acceptance threshold or resolved for ambiguity
        here.

        The concrete matcher sorts by descending confidence and then ID, using -1 for false-valued
        IDs, before applying the limit. Zero still performs matching and returns an empty tuple; it
        is not a query short circuit. No rows are written.

        Example:
            >>> candidates = catalog.matching.tags.candidates(  # doctest: +SKIP
            ...     MetadataCandidate({"name": "Gothic"}), limit=5,
            ... )


        :param candidate: Proposed metadata; concrete repository alias normalization ignores unknown columns and drops the entity ID.
        :param limit: Maximum results after matching and sorting; use a nonnegative integer.
        :param use_policy: Enable approximate fallback only after an exact miss without a terminal decision.
        :return: Ranked possible matches; the concrete implementation returns a tuple and suppresses terminal decisions.
        :raises ValueError: If limit compares below zero.
        :raises TypeError: If limit cannot be compared or used as a slice bound; booleans currently act as zero/one.
        :raises Exception: Repository input, row retrieval, and comparison failures propagate.
        """

        ...

    def best(
        self,
        candidate: MetadataCandidate,
        *,
        use_policy: bool = False,
    ) -> MatchResult:
        """
        Return the complete exact-default decision, optionally trying approximate fallback.

        Missing required input and conflicting exact fields return their terminal decision
        immediately. Exact candidates pass through the common acceptance and ambiguity selection.
        Only an exact miss without a terminal decision can invoke approximate matching when
        use_policy is enabled. All eligible rows participate; there is no candidate display limit
        and no persistence step.

        Example:
            >>> result = catalog.matching.tags.best(  # doctest: +SKIP
            ...     MetadataCandidate({"name": "Gothic"}), use_policy=False,
            ... )
            >>> needs_choice = result.requires_resolution  # doctest: +SKIP


        :param candidate: Candidate fields interpreted by the configured entity repository and identity specification.
        :param use_policy: Permit approximate matching after a nonterminal exact miss; defaults to False.
        :return: A match, no_match, ambiguous, or conflict decision with the available explanation/evidence.
        :raises Exception: Repository input, row retrieval, and evidence/decision failures propagate.
        """

        ...

    def exact(self, value: object, **scope: object) -> MatchResult:
        """
        Match one scalar against any configured scalar field within the requested scope.

        Normalize scope aliases strictly, apply omitted scope defaults, and return no_match when
        required scope is incomplete. The concrete matcher compares every eligible row using exact
        Unicode/whitespace normalization and per-field case folding. Any matching scalar field
        qualifies a row; full candidate required-identity checks are not applied on this path. Known
        input fields outside the configured scope do not become additional filters.

        Matches have confidence one and decisive evidence. Common selection resolves duplicates as
        ambiguity. This operation neither tries approximate matching nor writes data, including for
        non-reusable entity types.

        Example:
            >>> result = catalog.matching.genres.exact("Gothic", parent_id=parent_id)  # doctest: +SKIP


        :param value: Scalar identity value compared against every declared scalar field.
        :param scope: Repository field aliases or columns constraining configured scope, such as parent_id or item_id.
        :return: An exact match, no_match, or ambiguous result; this scalar path does not construct conflict decisions.
        :raises Exception: Strict repository normalization, row retrieval, and comparison failures propagate.
        """

        ...


__all__ = ["ExactEntityMatcherAPI"]

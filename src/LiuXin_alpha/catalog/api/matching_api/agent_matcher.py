"""Agent matching API."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from ..common import MetadataCandidate, MatchResult


@runtime_checkable
class AgentMatcherAPI(Protocol):
    """Match Agent names/aliases without mutating the Catalog.

    Exact canonical-name or alias evidence is preferred. Approximate matching
    is used only when enabled by the configured policy.
    """

    def candidates(self, candidate: MetadataCandidate, *, limit: int = 20) -> Sequence[MatchResult]:
        """Return exact policy-qualified Agent candidates.

        :param candidate: Candidate Agent metadata and structured hints.
        :param limit: Maximum candidates to return.
        :return: Qualified Agent decisions ordered deterministically.
        """

        ...

    def best(self, candidate: MetadataCandidate) -> MatchResult:
        """Return the final Agent identity decision.

        :param candidate: Candidate Agent metadata and structured hints.
        :return: Explained match, no-match, ambiguity, or conflict.
        """

        ...

    def exact(self, candidate_str: str) -> "MatchResult":
        """Apply the final policy to an Agent name string.

        :param candidate_str: Agent name to normalize and match.
        :return: Exact normalized-name match, no-match, or ambiguity result.

        Example::

            result = catalog.matching.agents.exact("Mary Shelley")
        """

        ...

"""Read-only identity-decision contracts for Catalog entities.

Matching never writes. Each matcher returns :class:`MatchResult`, preserving
the difference between no match, ambiguity, and conflict::

    decision = catalog.matching.works.best(candidate)
    if decision.is_match:
        use(decision.entity_id)
    elif decision.requires_resolution:
        ask_user(decision.alternatives, decision.evidence)

Repository ``match_or_create`` methods build on these decisions and create only
for a genuine no-match.
"""

# Todo: Worth thinking about where file and storage related db ops should live...

from __future__ import annotations

from typing import Protocol, runtime_checkable

from LiuXin_alpha.catalog.api.matching_api.agent_matcher import AgentMatcherAPI
from LiuXin_alpha.catalog.api.matching_api.exact_entity_matcher import (
    ExactEntityMatcherAPI,
)
from LiuXin_alpha.catalog.api.matching_api.identifier_matcher import IdentifierMatcherAPI
from LiuXin_alpha.catalog.api.matching_api.item_identifier_matcher import (
    ItemIdentifierMatcherAPI,
)
from LiuXin_alpha.catalog.api.matching_api.work_matcher import WorkMatcherAPI


@runtime_checkable
class CatalogMatchingAPI(Protocol):
    """Grouped, mutation-free matching API exposed by Catalog.

    Works, Agents, curated Identifiers, and observed Item identifiers have
    specialized matchers. Reusable value entities share exact-default matchers.

    Example::

        decision = catalog.matching.works.best(candidate)
        exact_tag = catalog.matching.tags.exact("gothic")
    """

    works: WorkMatcherAPI
    agents: AgentMatcherAPI
    identifiers: IdentifierMatcherAPI
    item_identifiers: ItemIdentifierMatcherAPI
    tags: ExactEntityMatcherAPI
    labels: ExactEntityMatcherAPI
    genres: ExactEntityMatcherAPI
    subjects: ExactEntityMatcherAPI
    series: ExactEntityMatcherAPI
    languages: ExactEntityMatcherAPI
    ratings: ExactEntityMatcherAPI
    comments: ExactEntityMatcherAPI
    synopses: ExactEntityMatcherAPI
    notes: ExactEntityMatcherAPI
    annotations: ExactEntityMatcherAPI

    def for_entity(self, entity_name: str) -> ExactEntityMatcherAPI:
        """Return an exact-default matcher by entity or table name.

        :param entity_name: Singular/plural public entity name or table name,
            for example ``"tag"`` or ``"languages"``.
        :return: Configured exact-default matcher.
        :raises KeyError: If no exact-default matcher is registered.

        Example::

            result = catalog.matching.for_entity("tags").exact("gothic")
        """

        ...


__all__ = [
    "AgentMatcherAPI",
    "CatalogMatchingAPI",
    "ExactEntityMatcherAPI",
    "IdentifierMatcherAPI",
    "ItemIdentifierMatcherAPI",
    "WorkMatcherAPI",
]

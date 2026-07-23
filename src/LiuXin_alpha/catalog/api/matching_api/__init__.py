"""
Matching API contracts for catalog entities.
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
    """
    Grouped matching API exposed by the catalog facade.
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

        :param entity_name: Singular or plural entity name.
        :return: Configured exact-default matcher.
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

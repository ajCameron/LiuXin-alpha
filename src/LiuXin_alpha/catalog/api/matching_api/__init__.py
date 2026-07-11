"""
Matching API contracts for catalog entities.
"""

# Todo: Worth thinking about where file and storage related db ops should live...

from __future__ import annotations

from typing import Protocol, runtime_checkable

from LiuXin_alpha.catalog.api.matching_api.agent_matcher import AgentMatcherAPI
from LiuXin_alpha.catalog.api.matching_api.identifier_matcher import IdentifierMatcherAPI
from LiuXin_alpha.catalog.api.matching_api.work_matcher import WorkMatcherAPI


@runtime_checkable
class CatalogMatchingAPI(Protocol):
    """
    Grouped matching API exposed by the catalog facade.
    """

    works: WorkMatcherAPI
    agents: AgentMatcherAPI
    identifiers: IdentifierMatcherAPI


__all__ = [
    "AgentMatcherAPI",
    "CatalogMatchingAPI",
    "IdentifierMatcherAPI",
    "WorkMatcherAPI",
]

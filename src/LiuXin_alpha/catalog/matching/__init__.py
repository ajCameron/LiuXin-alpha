"""Matching implementations for the catalog layer."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..api.common import DatabaseHandle
from .agent_matcher import AgentMatcher
from .identifier_matcher import IdentifierMatcher
from .work_matcher import WorkMatcher


@dataclass(slots=True)
class CatalogMatching:
    """Grouped matchers exposed by `Catalog.matching`."""

    db: DatabaseHandle
    repositories: Any
    works: WorkMatcher = field(init=False)
    agents: AgentMatcher = field(init=False)
    identifiers: IdentifierMatcher = field(init=False)

    def __post_init__(self) -> None:
        self.works = WorkMatcher(self.db, self.repositories)
        self.agents = AgentMatcher(self.db, self.repositories)
        self.identifiers = IdentifierMatcher(self.db, self.repositories)


__all__ = ["AgentMatcher", "CatalogMatching", "IdentifierMatcher", "WorkMatcher"]

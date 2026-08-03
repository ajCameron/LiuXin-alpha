"""Retrieval implementations for bundles and projections."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..api.common import DatabaseHandle
from .bundles import BundleRetriever
from .graph import WemiGraphRetriever
from .hierarchy import HierarchyRetriever
from .projections import ProjectionService


@dataclass(slots=True)
class CatalogRetrieval:
    """Grouped retrieval services exposed by `Catalog.retrieval`."""

    db: DatabaseHandle
    repositories: Any
    bundles: BundleRetriever = field(init=False)
    graph: WemiGraphRetriever = field(init=False)
    hierarchy: HierarchyRetriever = field(init=False)
    projections: ProjectionService = field(init=False)

    def __post_init__(self) -> None:
        self.bundles = BundleRetriever(self.db, self.repositories)
        self.graph = WemiGraphRetriever(self.repositories)
        self.hierarchy = HierarchyRetriever(self.repositories)
        self.projections = ProjectionService(self.db, self.repositories)


__all__ = [
    "BundleRetriever",
    "CatalogRetrieval",
    "HierarchyRetriever",
    "ProjectionService",
    "WemiGraphRetriever",
]

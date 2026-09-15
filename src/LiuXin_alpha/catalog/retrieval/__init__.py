"""
Compose and export Catalog bundle, graph, hierarchy and projection services.
"""

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
    """
    Construct four retrieval services sharing one database and repository group.

    The slotted dataclass accepts db and repositories. Service fields are
    initialized internally without database queries or capability validation.

    Example:
        >>> db, repositories = object(), object()
        >>> retrieval = CatalogRetrieval(db, repositories)
        >>> retrieval.bundles.repositories is repositories
        True
    """

    db: DatabaseHandle
    repositories: Any
    bundles: BundleRetriever = field(init=False)
    graph: WemiGraphRetriever = field(init=False)
    hierarchy: HierarchyRetriever = field(init=False)
    projections: ProjectionService = field(init=False)

    def __post_init__(self) -> None:
        """
        Create the four services using the retained constructor arguments.

        Calling this hook manually again replaces all four service objects.

        Example:
            Dataclass construction invokes this hook; callers normally use the
            resulting ``retrieval.bundles`` and other service attributes.


        :return: None; assigns bundles, graph, hierarchy and projections.
        """

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

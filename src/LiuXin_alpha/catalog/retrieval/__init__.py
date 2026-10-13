"""
Compose Catalog bundle, graph, hierarchy, metadata and projection services.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..api.common import DatabaseHandle
from .bundles import BundleRetriever
from .graph import WemiGraphRetriever
from .hierarchy import HierarchyRetriever
from .metadata import MetadataRetriever
from .projections import ProjectionService


@dataclass(slots=True)
class CatalogRetrieval:
    """
    Construct five retrieval services sharing one database and repository group.

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
    metadata: MetadataRetriever = field(init=False)
    projections: ProjectionService = field(init=False)

    def __post_init__(self) -> None:
        """
        Create the five services using the retained constructor arguments.

        Calling this hook manually again replaces all four service objects.

        Example:
            Dataclass construction invokes this hook; callers normally use the
            resulting ``retrieval.bundles`` and other service attributes.


        :return: None; assigns bundles, graph, hierarchy, metadata and projections.
        """

        self.bundles = BundleRetriever(self.db, self.repositories)
        self.graph = WemiGraphRetriever(self.repositories)
        self.hierarchy = HierarchyRetriever(self.repositories)
        self.metadata = MetadataRetriever(self.db)
        self.projections = ProjectionService(self.db, self.repositories)


__all__ = [
    "BundleRetriever",
    "CatalogRetrieval",
    "HierarchyRetriever",
    "MetadataRetriever",
    "ProjectionService",
    "WemiGraphRetriever",
]

"""
Export contracts for grouped, display-neutral Catalog retrieval.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from .bundles import BundleRetrieverAPI
from .graph import WemiGraphRetrieverAPI
from .hierarchy import HierarchyRetrieverAPI
from .projections import ProjectionAPI


@runtime_checkable
class CatalogRetrievalAPI(Protocol):
    """
    Describe the four retrieval services exposed by a Catalog.

    This runtime-checkable protocol specifies service attributes; it neither
    constructs services nor verifies their signatures at runtime.

    Example:
        Annotate a consumer with ``CatalogRetrievalAPI`` to access bundles, graph,
        hierarchy and projections through their respective contracts.
    """

    bundles: BundleRetrieverAPI
    graph: WemiGraphRetrieverAPI
    hierarchy: HierarchyRetrieverAPI
    projections: ProjectionAPI


__all__ = [
    "BundleRetrieverAPI",
    "CatalogRetrievalAPI",
    "HierarchyRetrieverAPI",
    "ProjectionAPI",
    "WemiGraphRetrieverAPI",
]

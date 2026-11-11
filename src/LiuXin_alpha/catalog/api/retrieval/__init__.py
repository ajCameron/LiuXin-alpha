"""
Export contracts for grouped, display-neutral Catalog retrieval.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from .bundles import BundleRetrieverAPI
from .graph import WemiGraphRetrieverAPI
from .hierarchy import HierarchyRetrieverAPI
from .inherited import InheritedMetadataRetrieverAPI
from .metadata import MetadataRetrieverAPI
from .projections import ProjectionAPI


@runtime_checkable
class CatalogRetrievalAPI(Protocol):
    """
    Describe the six retrieval services exposed by a Catalog.

    This runtime-checkable protocol specifies service attributes; it neither
    constructs services nor verifies their signatures at runtime.

    Example:
        Annotate a consumer with ``CatalogRetrievalAPI`` to access bundles, graph,
        hierarchy, metadata, projections and inheritance through their contracts.
    """

    bundles: BundleRetrieverAPI
    graph: WemiGraphRetrieverAPI
    hierarchy: HierarchyRetrieverAPI
    metadata: MetadataRetrieverAPI
    projections: ProjectionAPI
    inherited: InheritedMetadataRetrieverAPI


__all__ = [
    "BundleRetrieverAPI",
    "CatalogRetrievalAPI",
    "HierarchyRetrieverAPI",
    "InheritedMetadataRetrieverAPI",
    "MetadataRetrieverAPI",
    "ProjectionAPI",
    "WemiGraphRetrieverAPI",
]

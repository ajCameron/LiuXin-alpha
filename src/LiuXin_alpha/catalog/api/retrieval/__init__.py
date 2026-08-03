"""Coherent, display-neutral Catalog read models.

Retrieval differs from repositories: a repository reads one entity family,
whereas retrieval deliberately combines several repositories into a WEMI slice
or semantic projection.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from .bundles import BundleRetrieverAPI
from .graph import WemiGraphRetrieverAPI
from .hierarchy import HierarchyRetrieverAPI
from .projections import ProjectionAPI


@runtime_checkable
class CatalogRetrievalAPI(Protocol):
    """Grouped retrieval API exposed by the Catalog facade.

    Example::

        bundle = catalog.retrieval.bundles.for_item(item_id)
        summary = catalog.retrieval.projections.item_summary(item_id)
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

"""Retrieval API contracts for catalog projections and bundles."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from .bundles import BundleRetrieverAPI
from .projections import ProjectionAPI


@runtime_checkable
class CatalogRetrievalAPI(Protocol):
    """
    Grouped retrieval API exposed by the catalog facade.
    """

    bundles: BundleRetrieverAPI
    projections: ProjectionAPI


__all__ = ["BundleRetrieverAPI", "CatalogRetrievalAPI", "ProjectionAPI"]

"""
Facade API for the catalog layer.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from .matching import CatalogMatchingAPI
from .mutations import CatalogMutationsAPI
from .repositories import CatalogRepositoriesAPI
from .retrieval import CatalogRetrievalAPI


@runtime_checkable
class CatalogAPI(Protocol):
    """
    Top-level catalog facade.

    API shape mirrors `LiuXin_alpha.catalog` module shape: repositories, matching,
    retrieval, and mutations are separate areas behind one convenience object.
    """

    repositories: CatalogRepositoriesAPI
    matching: CatalogMatchingAPI
    retrieval: CatalogRetrievalAPI
    mutations: CatalogMutationsAPI

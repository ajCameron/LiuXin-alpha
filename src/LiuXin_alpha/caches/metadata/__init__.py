"""Expose native metadata services over Catalog and an optional loaded cache.

Use ``CatalogCache(catalog, cache=cache)`` or uncached ``MetadataAPI(catalog)``.
The package owns coordinated reads, search, shared records, custom fields and
writes; Catalog's public protocols remain under ``catalog.api``.
"""

from __future__ import annotations

from LiuXin_alpha.caches.metadata.cache import CatalogCache
from LiuXin_alpha.caches.metadata.service import MetadataAPI

__all__ = ["CatalogCache", "MetadataAPI"]

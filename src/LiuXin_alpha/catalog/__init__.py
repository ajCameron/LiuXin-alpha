"""
Define the catalog package boundary without assembling the concrete catalog.

Separation of concerns: Catalog owns metadata-aware retrieval, identity decisions,
semantic validation, and public persistence mutations.

Contracts live under ``LiuXin_alpha.catalog.api``. Applications that need the
implementation import ``Catalog`` from ``LiuXin_alpha.catalog.catalog`` explicitly.
This keeps contract imports independent from repositories, matching, metadata
containers, caches, and database implementations.

See ``dev-docs/04 - Seperation of Concerns.md`` and
``dev-docs/catalog-api-usage.md``.
"""

from __future__ import annotations

__all__: list[str] = []

"""
Define the cache package boundary without assembling cache implementations.

Separation of concerns: caches accelerate reads and reconcile delegated Catalog
writes; they do not own bibliographic policy or application lifecycle.

Contracts live under ``LiuXin_alpha.caches.api``; the Catalog-independent read facade
lives in ``LiuXin_alpha.caches.read_cache``; the writable facade and constructor live
in ``LiuXin_alpha.caches.cache``; bundled backends and registration helpers live under
``LiuXin_alpha.caches.cache_plugins``. Import those owners explicitly so a contract
import does not initialize every bundled backend or pull database implementations into
the caller.

See ``dev-docs/04 - Seperation of Concerns.md`` and
``dev-docs/catalog-cache-boundary.md``.
"""

from __future__ import annotations

__all__: list[str] = []

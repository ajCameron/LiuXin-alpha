"""
Export the NumPy-oriented storage backend and its generic plugin entry name.

Both names come from the canonical storage_cache module. NumPy availability
and the require_numpy constructor option govern vectorized support; importing
this package does not itself instantiate or load a database cache.
"""

from __future__ import annotations

from LiuXin_alpha.caches.cache_plugins.numpy_vectorized.storage_cache import (
    NumpyVectorizedStorageCache,
    StorageCache,
)

__all__ = [
    "NumpyVectorizedStorageCache",
    "StorageCache",
]

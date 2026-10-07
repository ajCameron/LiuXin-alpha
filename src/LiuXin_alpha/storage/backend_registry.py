"""
Compatibility exports for the storage backend registry.

The registry implementation and construction adapters live in
``LiuXin_alpha.storage.utils.backend_registry``.  This module preserves the
established public import path without keeping implementation-heavy helpers at
the storage package root.
"""

from LiuXin_alpha.storage.utils.backend_registry import (
    DEFAULT_BACKEND_REGISTRY,
    BackendBuilder,
    StorageBackendDescriptor,
    StorageBackendRegistry,
    StoreConstructionContext,
    normalize_backend_kind,
)

__all__ = [
    "BackendBuilder",
    "DEFAULT_BACKEND_REGISTRY",
    "StorageBackendDescriptor",
    "StorageBackendRegistry",
    "StoreConstructionContext",
    "normalize_backend_kind",
]

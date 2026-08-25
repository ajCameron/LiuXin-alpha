"""Construction of configured Stores through the canonical backend registry."""

from __future__ import annotations

from LiuXin_alpha.storage.api import StoreAPI, StoreConfiguration
from LiuXin_alpha.storage.backend_registry import (
    DEFAULT_BACKEND_REGISTRY,
    StorageBackendRegistry,
    StoreConstructionContext,
)


def build_store(
    configuration: StoreConfiguration,
    *,
    context: StoreConstructionContext | None = None,
    registry: StorageBackendRegistry = DEFAULT_BACKEND_REGISTRY,
) -> StoreAPI:
    """Construct a Store while keeping credentials in runtime-only context.

    Ordinary backends need only their durable ``StoreConfiguration``. S3 can
    receive an injected client, and encrypted wrappers receive their inner
    Store resolver and key provider through ``StoreConstructionContext``.
    """

    return registry.build(configuration, context=context)


__all__ = [
    "DEFAULT_BACKEND_REGISTRY",
    "StorageBackendRegistry",
    "StoreConstructionContext",
    "build_store",
]

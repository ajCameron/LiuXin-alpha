"""
Delegate configured Store construction to the canonical or an injected registry.

The compatibility exports retain the default registry, registry type, and runtime
construction context at this import location. Backend selection, dependency
checks, and constructor effects belong to the selected registry and builder.
"""

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
    """
    Pass a Store configuration and runtime context to the selected registry.

    The default registry resolves backend aliases, checks Asset-backed view restrictions, and
    invokes its registered builder. S3 clients, encryption providers, and Store/backing-path
    resolvers can be supplied through context. This wrapper adds no persistence, credential
    filtering, startup call, or error translation; backend construction can still access local
    resources. A custom registry controls its own validation and construction behavior.

    Example:
        >>> store = build_store(configuration, context=context)  # doctest: +SKIP


    :param configuration: Configured Store intent passed unchanged to registry.build.
    :param context: Optional runtime dependencies passed through unchanged; the default registry creates a context when absent.
    :param registry: Registry whose build method owns construction; defaults to the shared mutable canonical registry.
    :return: The object returned by registry.build; lookup, dependency, and constructor failures propagate.
    """

    return registry.build(configuration, context=context)


__all__ = [
    "DEFAULT_BACKEND_REGISTRY",
    "StorageBackendRegistry",
    "StoreConstructionContext",
    "build_store",
]

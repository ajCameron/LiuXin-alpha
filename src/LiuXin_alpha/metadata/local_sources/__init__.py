"""
Local metadata-source plugin package.

This package hosts metadata integrations backed by local datasets instead of
live network scraping.
"""

from __future__ import annotations

from importlib import import_module
from types import ModuleType

# Keep this list explicit so callers can introspect local sources without
# scanning the filesystem.
KNOWN_LOCAL_SOURCE_MODULES: tuple[str, ...] = ("isfdb",)


def iter_known_local_source_modules() -> tuple[str, ...]:
    """
    Return the known local-source module names in deterministic order.
    """
    return KNOWN_LOCAL_SOURCE_MODULES


def import_local_source_module(module_name: str) -> ModuleType:
    """
    Import and return a local-source module by short name.

    Raises:
        ValueError: if module_name is empty.
        ModuleNotFoundError: if the module has not been ported yet.
    """
    name = str(module_name or "").strip()
    if not name:
        raise ValueError("module_name must be a non-empty string.")
    return import_module(f"{__name__}.{name}")


__all__ = [
    "KNOWN_LOCAL_SOURCE_MODULES",
    "import_local_source_module",
    "iter_known_local_source_modules",
]

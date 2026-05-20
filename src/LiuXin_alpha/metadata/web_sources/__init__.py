"""
Web metadata-source plugin package.

This package hosts online metadata/cover source integrations (Amazon, Google,
OpenLibrary, etc.). Most concrete source modules are being ported in stages;
this module provides a stable, typed package surface in the meantime.
"""

from __future__ import annotations

from importlib import import_module
from types import ModuleType

# Keep this list explicit so callers can introspect available/expected source
# modules without scanning the filesystem.
KNOWN_WEB_SOURCE_MODULES: tuple[str, ...] = (
    "amazon",
    "base",
    "big_book_search",
    "cli",
    "covers",
    "douban",
    "edelweiss",
    "google",
    "google_images",
    "identify",
    "internet_archive",
    "isbndb",
    "kdl",
    "library_of_congress",
    "library_thing",
    "openlibrary",
    "overdrive",
    "ozon",
    "prefs",
    "worker",
    "xisbn",
)


def iter_known_web_source_modules() -> tuple[str, ...]:
    """
    Return the known web-source module names in deterministic order.
    """
    return KNOWN_WEB_SOURCE_MODULES


def import_web_source_module(module_name: str) -> ModuleType:
    """
    Import and return a web-source module by short name.

    Raises:
        ValueError: if module_name is empty.
        ModuleNotFoundError: if the module has not been ported yet.
    """
    name = str(module_name or "").strip()
    if not name:
        raise ValueError("module_name must be a non-empty string.")
    return import_module(f"{__name__}.{name}")


__all__ = [
    "KNOWN_WEB_SOURCE_MODULES",
    "import_web_source_module",
    "iter_known_web_source_modules",
]

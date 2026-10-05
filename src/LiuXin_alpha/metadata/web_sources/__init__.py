"""
Expose deterministic discovery and explicit imports for staged online metadata-source modules.

The module makes ordering, fallback, ownership and optional-integration behavior
explicit for callers.

Example:
    Exercise   init   with the owning regression module::

        python -m pytest -q tests/metadata/web_sources/test_web_sources_amazon.py
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
    "wikidata",
    "xisbn",
)


def iter_known_web_source_modules() -> tuple[str, ...]:
    """
    Return known staged web-source module names in stable declaration order.

    Example:
        Exercise iter known web source modules with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_amazon.py


    :return: The normalized row, metadata object or value described above.
    """
    return KNOWN_WEB_SOURCE_MODULES


def import_web_source_module(module_name: str) -> ModuleType:
    """
    Import one web-source module by short name and propagate genuine import failures.

    Example:
        Exercise import web source module with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_amazon.py


    :param module_name: Short source-module name below the current package.
    :return: The normalized row, metadata object or value described above.
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

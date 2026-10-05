"""
Expose deterministic discovery and explicit imports for metadata sources backed by local datasets.

The module makes ordering, fallback, ownership and optional-integration behavior
explicit for callers.

Example:
    Exercise   init   with the owning regression module::

        python -m pytest -q tests/metadata/local_sources/test_local_sources_isfdb.py
"""

from __future__ import annotations

from importlib import import_module
from types import ModuleType

# Keep this list explicit so callers can introspect local sources without
# scanning the filesystem.
KNOWN_LOCAL_SOURCE_MODULES: tuple[str, ...] = ("isfdb",)


def iter_known_local_source_modules() -> tuple[str, ...]:
    """
    Return explicitly supported local-source module names in stable declaration order.

    Example:
        Exercise iter known local source modules with the owning regression module::

            python -m pytest -q tests/metadata/local_sources/test_local_sources_isfdb.py


    :return: The normalized row, metadata object or value described above.
    """
    return KNOWN_LOCAL_SOURCE_MODULES


def import_local_source_module(module_name: str) -> ModuleType:
    """
    Import one local-source module by short name and propagate genuine import failures.

    Example:
        Exercise import local source module with the owning regression module::

            python -m pytest -q tests/metadata/local_sources/test_local_sources_isfdb.py


    :param module_name: Short source-module name below the current package.
    :return: The normalized row, metadata object or value described above.
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

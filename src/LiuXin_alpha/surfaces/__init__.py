"""
Expose selected surface subpackages lazily without eagerly loading unrelated application front ends.

The sorted __all__ advertises the explicit lazy-name set. First attribute access
imports and caches the selected submodule; importing other submodules directly
remains possible even when their names are not advertised here. Imports retain
their own normal side effects and errors. Directory listing advertises lazy names
without importing them.
"""

from __future__ import annotations

from importlib import import_module


_LAZY_SUBMODULES = {
    "acquisition",
    "api",
    "api_readonly",
    "catalog",
    "categories",
    "cli",
    "images",
    "metadata_facets",
    "opds",
    "opds_readonly",
    "read_model",
    "renderers",
    "tags_icons",
    "terminal",
    "thumbnail_cache",
    "tkinter_gui",
    "web_calibre_readonly",
    "web_readonly",
    "web_readwrite",
}

__all__ = sorted(_LAZY_SUBMODULES)


def __getattr__(name: str):
    """
    Import an advertised submodule on demand and cache the module in this package's globals.

    This hook normally runs only for missing attributes. Failed imports propagate
    before the cache assignment, so a later attribute access can attempt import again.

    Example:
        >>> __getattr__("categories").__name__
        'LiuXin_alpha.surfaces.categories'


    :param name: Exact advertised submodule name, without case or whitespace normalization.
    :return: Imported submodule object, also stored as a package attribute.
    :raises AttributeError: If the requested name is not in the lazy-name set.
    """
    if name not in _LAZY_SUBMODULES:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module = import_module(f"{__name__}.{name}")
    globals()[name] = module
    return module


def __dir__() -> list[str]:
    """
    List current globals and advertised lazy submodules without loading any missing modules.

    Example:
        >>> "terminal" in __dir__()
        True


    :return: Sorted deduplicated names, including non-exported globals and all advertised lazy names.
    """
    return sorted(set(globals()) | _LAZY_SUBMODULES)

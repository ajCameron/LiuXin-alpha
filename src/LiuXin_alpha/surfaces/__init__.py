"""Top-level surface package.

Keep package import side effects minimal so submodules such as
``LiuXin_alpha.surfaces.field_metadata`` can be imported directly without
pulling in unrelated front ends.
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
    "field_metadata",
    "images",
    "metadata_facets",
    "opds",
    "opds_readonly",
    "read_model",
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
    if name not in _LAZY_SUBMODULES:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module = import_module(f"{__name__}.{name}")
    globals()[name] = module
    return module


def __dir__() -> list[str]:
    return sorted(set(globals()) | _LAZY_SUBMODULES)

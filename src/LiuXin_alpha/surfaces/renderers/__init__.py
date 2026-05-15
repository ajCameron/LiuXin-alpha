"""Renderer helpers used by surface modules.

Keep this package lazy so importing ``LiuXin_alpha.surfaces`` does not pull in
metadata, database, or front-end dependencies.
"""

from __future__ import annotations

from importlib import import_module


_LAZY_SUBMODULES = {
    "metadata",
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

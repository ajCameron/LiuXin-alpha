"""
Storage subsystem public entry points.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

from LiuXin_alpha.storage.store_manager import (
    StorageBootstrapIssue,
    StorageBootstrapReport,
    StoreManager,
    StorageManager,
)

__all__ = [
    "StorageManager",
    "StoreManager",
    "StorageBootstrapIssue",
    "StorageBootstrapReport",
    "reconcile",
]


def __getattr__(name: str) -> Any:
    if name != "reconcile":
        raise AttributeError("module {!r} has no attribute {!r}".format(__name__, name))
    module = import_module("LiuXin_alpha.storage.reconcile")
    globals()[name] = module
    return module


def __dir__() -> list[str]:
    return sorted(list(globals().keys()) + __all__)

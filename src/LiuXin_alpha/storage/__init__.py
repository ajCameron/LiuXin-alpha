"""Storage subsystem public entry points.

Storage has a strict three-part shape:
- `StorageManager` orchestrates and chooses stores
- `StoreContainer` wraps one configured store plus optional DB state
- `StorePlugin` talks to one physical backend only

The subsystem should expose `Location` objects for concrete file access and keep
replica/library semantics out of raw plugins.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

from LiuXin_alpha.storage.store_container import StoreContainer
from LiuXin_alpha.storage.errors import (
    CalibreLikeImplicitOverwriteError,
    FlatStoreImplicitOverwriteError,
    ManagedDriveImplicitOverwriteError,
    SqliteBlobImplicitOverwriteError,
    SquashfsBuildImplicitOverwriteError,
    StorageError,
    StorageImplicitOverwriteError,
    StorageWriteError,
)
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
    "StoreContainer",
    "StorageBootstrapReport",
    "StorageError",
    "StorageWriteError",
    "StorageImplicitOverwriteError",
    "ManagedDriveImplicitOverwriteError",
    "CalibreLikeImplicitOverwriteError",
    "FlatStoreImplicitOverwriteError",
    "SqliteBlobImplicitOverwriteError",
    "SquashfsBuildImplicitOverwriteError",
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

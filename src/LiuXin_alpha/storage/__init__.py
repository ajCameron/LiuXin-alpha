"""
Expose storage packages and selected implementations through lazy package lookup.

API, ingest, reconcile, utilities, registries, managers, workflows, and legacy
write-error classes are imported only when their names are requested through this
package hook. Successful resolutions are cached in module globals. Individual
implementation imports retain their own dependencies and may fail independently.

The package-level StorageError resolves to the legacy storage.errors class;
storage.api.StorageError is the separate shared contract hierarchy.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any


_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "DEFAULT_BACKEND_REGISTRY": (
        "LiuXin_alpha.storage.backend_registry",
        "DEFAULT_BACKEND_REGISTRY",
    ),
    "StorageBackendDescriptor": (
        "LiuXin_alpha.storage.backend_registry",
        "StorageBackendDescriptor",
    ),
    "StorageBackendRegistry": (
        "LiuXin_alpha.storage.backend_registry",
        "StorageBackendRegistry",
    ),
    "StoreConstructionContext": (
        "LiuXin_alpha.storage.backend_registry",
        "StoreConstructionContext",
    ),
    "build_store": (
        "LiuXin_alpha.storage.store_factory",
        "build_store",
    ),
    "InMemoryStorageManager": (
        "LiuXin_alpha.storage.storage_manager",
        "InMemoryStorageManager",
    ),
    "TransientStorageManager": (
        "LiuXin_alpha.storage.storage_manager",
        "TransientStorageManager",
    ),
    "StorageManager": ("LiuXin_alpha.storage.store_manager", "StorageManager"),
    "StoreManager": ("LiuXin_alpha.storage.store_manager", "StoreManager"),
    "StorageBootstrapIssue": (
        "LiuXin_alpha.storage.store_manager",
        "StorageBootstrapIssue",
    ),
    "StorageBootstrapReport": (
        "LiuXin_alpha.storage.store_manager",
        "StorageBootstrapReport",
    ),
    "StoreContainer": ("LiuXin_alpha.storage.store_container", "StoreContainer"),
    "BackupArtifactRegistry": (
        "LiuXin_alpha.storage.backup",
        "BackupArtifactRegistry",
    ),
    "BackupWorkflowRepository": (
        "LiuXin_alpha.storage.backup",
        "BackupWorkflowRepository",
    ),
    "PlannedBackupPack": ("LiuXin_alpha.storage.backup", "PlannedBackupPack"),
    "RegisteredBackupArtifact": (
        "LiuXin_alpha.storage.backup",
        "RegisteredBackupArtifact",
    ),
    "SquashfsBackupWorkflow": (
        "LiuXin_alpha.storage.backup",
        "SquashfsBackupWorkflow",
    ),
    "StoreBackupPlanner": ("LiuXin_alpha.storage.backup", "StoreBackupPlanner"),
    "SealedArtifactWorkflow": (
        "LiuXin_alpha.storage.workflows",
        "SealedArtifactWorkflow",
    ),
    "SquashfsDriveIngestWorkflow": (
        "LiuXin_alpha.storage.ingest",
        "SquashfsDriveIngestWorkflow",
    ),
    "SquashfsDriveIngestReport": (
        "LiuXin_alpha.storage.ingest",
        "SquashfsDriveIngestReport",
    ),
    "ingest_squashfs_drive": (
        "LiuXin_alpha.storage.ingest",
        "ingest_squashfs_drive",
    ),
    "ConsoleReporter": ("LiuXin_alpha.storage.backup", "ConsoleReporter"),
    "ExistingDriveSquashfsPrototype": (
        "LiuXin_alpha.storage.backup",
        "ExistingDriveSquashfsPrototype",
    ),
    "IndexedStoreRun": ("LiuXin_alpha.storage.backup", "IndexedStoreRun"),
    "PackExecutionRun": ("LiuXin_alpha.storage.backup", "PackExecutionRun"),
    "PrototypeRunResult": ("LiuXin_alpha.storage.backup", "PrototypeRunResult"),
    "StorageError": ("LiuXin_alpha.storage.errors", "StorageError"),
    "StorageWriteError": ("LiuXin_alpha.storage.errors", "StorageWriteError"),
    "StorageImplicitOverwriteError": (
        "LiuXin_alpha.storage.errors",
        "StorageImplicitOverwriteError",
    ),
    "ManagedDriveImplicitOverwriteError": (
        "LiuXin_alpha.storage.errors",
        "ManagedDriveImplicitOverwriteError",
    ),
    "CalibreLikeImplicitOverwriteError": (
        "LiuXin_alpha.storage.errors",
        "CalibreLikeImplicitOverwriteError",
    ),
    "FlatStoreImplicitOverwriteError": (
        "LiuXin_alpha.storage.errors",
        "FlatStoreImplicitOverwriteError",
    ),
    "SqliteBlobImplicitOverwriteError": (
        "LiuXin_alpha.storage.errors",
        "SqliteBlobImplicitOverwriteError",
    ),
    "SquashfsBuildImplicitOverwriteError": (
        "LiuXin_alpha.storage.errors",
        "SquashfsBuildImplicitOverwriteError",
    ),
    "RarBuildImplicitOverwriteError": (
        "LiuXin_alpha.storage.errors",
        "RarBuildImplicitOverwriteError",
    ),
}


__all__ = ["api", "ingest", "reconcile", "utils", *_LAZY_EXPORTS]


def __getattr__(name: str) -> Any:
    """
    Import a supported subpackage or mapped implementation attribute and cache its resolved value.

    Unknown names raise AttributeError chained from the missing export-map key. Import or attribute
    failures propagate before caching. A cached attribute normally bypasses this hook on later
    package access; direct calls to the hook perform resolution again.

    Example:
        >>> __getattr__("api").__name__
        'LiuXin_alpha.storage.api'


    :param name: Requested package attribute name from the supported lazy exports.
    :return: Resolved module or exported object, also stored in this package's globals.
    """
    if name == "api":
        value: Any = import_module("LiuXin_alpha.storage.api")
    elif name == "ingest":
        value = import_module("LiuXin_alpha.storage.ingest")
    elif name == "utils":
        value = import_module("LiuXin_alpha.storage.utils")
    elif name == "reconcile":
        value = import_module("LiuXin_alpha.storage.reconcile")
    else:
        try:
            module_name, attribute_name = _LAZY_EXPORTS[name]
        except KeyError as exc:
            raise AttributeError(
                f"module {__name__!r} has no attribute {name!r}"
            ) from exc
        value = getattr(import_module(module_name), attribute_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """
    List current module globals and declared lazy exports in sorted, duplicate-free order.

    Listing names does not import the corresponding implementation modules or prove they can be
    loaded.

    Example:
        >>> "StorageManager" in __dir__()
        True


    :return: Sorted list of eager/current global and advertised export names.
    """
    return sorted({*globals(), *__all__})

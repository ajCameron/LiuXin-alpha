"""
Retain filesystem integration checks for configured Store and container composition.

Tests create real temporary local files and verify public result types, UUID
ownership, and read-only policy. They complement the memory-driver contract suite
without requiring a live remote backend. Isolated import checks keep the package
root independent of implementations and exercise storage/ingest dependency order.

Example:
    >>> test_configured_store_surface_uses_opaque_locations_and_file_results(tmp_path)  # doctest: +SKIP
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from LiuXin_alpha.storage.api import (
    EnumerationCompleteness,
    Location,
    StoreAPI,
    StoreInvalidLocation,
    StoreReadOnly,
)
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive import (
    OnDiskUnmanagedStorageBackend,
)
from LiuXin_alpha.storage.store_container import StoreContainer
from LiuXin_alpha.storage.stores import FilesystemStore


def test_store_container_binds_new_store_and_configuration(tmp_path: Path) -> None:
    """
    Bind a real filesystem Store into a StoreContainer while retaining Store/configuration identity.

    Start the temporary destination and assert availability and writable status; this tests local
    composition rather than remote health.

    Example:
        >>> test_store_container_binds_new_store_and_configuration(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for actual local Store roots and bytes.
    :return: None after the stated regression assertions pass.
    """
    store = FilesystemStore(tmp_path / "managed", name="managed")
    container = StoreContainer.from_store(store)

    assert container.store is store
    assert container.configuration is store.configuration
    assert container.startup().available is True
    assert container.status().writable is True


def test_configured_store_surface_uses_opaque_locations_and_file_results(
    tmp_path: Path,
) -> None:
    """
    Write and read real filesystem bytes through Store conveniences and returned FileInfo
    identifiers.

    Assert the public Store/Location types, persisted key, byte size, and complete enumeration
    declaration.

    Example:
        >>> test_configured_store_surface_uses_opaque_locations_and_file_results(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for actual local Store roots and bytes.
    :return: None after the stated regression assertions pass.
    """
    store = FilesystemStore(tmp_path / "managed")
    stored = store.store_bytes(b"payload", location="books/one.epub")

    assert isinstance(store, StoreAPI)
    assert isinstance(stored.location, Location)
    assert stored.location.key == "books/one.epub"
    assert store.read_file(stored) == b"payload"
    assert store.stat_file(stored).size == 7
    assert store.capabilities.enumeration is EnumerationCompleteness.COMPLETE


def test_store_identity_prevents_cross_store_location_confusion(tmp_path: Path) -> None:
    """
    Reject a Location produced by one filesystem Store when read through another Store.

    The source bytes are real, and the assertion concerns UUID ownership before cross-Store access.

    Example:
        >>> test_store_identity_prevents_cross_store_location_confusion(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for actual local Store roots and bytes.
    :return: None after the stated regression assertions pass.
    """
    first = FilesystemStore(tmp_path / "first")
    second = FilesystemStore(tmp_path / "second")
    location = first.store_bytes(b"one", location="one.bin").location

    with pytest.raises(StoreInvalidLocation):
        second.read_file(location)


def test_read_only_store_reports_policy_before_backend_mutation(tmp_path: Path) -> None:
    """
    Read a real unmanaged source file and reject a convenience write under the Store read-only
    policy.

    The test checks the typed refusal; it does not inject a failing backend write or audit every
    filesystem effect.

    Example:
        >>> test_read_only_store_reports_policy_before_backend_mutation(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for actual local Store roots and bytes.
    :return: None after the stated regression assertions pass.
    """
    root = tmp_path / "source"
    root.mkdir()
    (root / "book.epub").write_bytes(b"book")
    store = OnDiskUnmanagedStorageBackend(root)

    assert store.read_file("book.epub") == b"book"
    with pytest.raises(StoreReadOnly):
        store.store_bytes(b"replacement", location="book.epub")


def _check_storage_imports(source: str) -> None:
    """
    Execute storage import assertions in a fresh interpreter with checkout sources.

    Isolation prevents this module's integration imports from masking dependency
    cycles or eagerly populated package attributes. Inherit the test environment
    so any configuration writes remain in its temporary directories.

    Example:
        _check_storage_imports("import LiuXin_alpha.storage")


    :param source: Python statements containing the import contract assertions.
    :return: None after the child exits successfully within the timeout.
    """
    source_root = Path(__file__).resolve().parents[3] / "src"
    setup = f"import sys\nsys.path.insert(0, {str(source_root)!r})\n"
    completed = subprocess.run(
        [sys.executable, "-I", "-c", setup + source],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


@pytest.mark.parametrize(
    "package",
    (
        "LiuXin_alpha.storage",
        "LiuXin_alpha.storage.utils",
        "LiuXin_alpha.storage.store_backend_plugins.squashfs_readonly",
    ),
)
def test_storage_namespaces_leave_implementations_unloaded(package: str) -> None:
    """
    Import the storage namespace without loading children or offering class aliases.

    Checking in a fresh interpreter catches eager imports even when unrelated
    tests have already loaded the storage manager and its dependencies.

    Example:
        test_storage_namespaces_leave_implementations_unloaded("LiuXin_alpha.storage")


    :param package: Storage namespace imported before any of its implementation modules.
    :return: None after package isolation and absent implementation exports are verified.
    """
    _check_storage_imports(
        f"import importlib\npackage = {package!r}\nstorage = importlib.import_module(package)\n"
        + """
assert not [name for name in sys.modules if name.startswith(package + '.')]
for name in ('StorageManager', 'StoreContainer', 'StorageError', 'SealedArtifactWorkflow'):
    assert not hasattr(storage, name), name
    assert name not in dir(storage), name
"""
    )


@pytest.mark.parametrize(
    "first_module",
    (
        "LiuXin_alpha.storage.api",
        "LiuXin_alpha.storage.utils.driver",
        "LiuXin_alpha.storage.utils.workflow",
        "LiuXin_alpha.storage.store_manager",
        "LiuXin_alpha.storage.reconcile",
        "LiuXin_alpha.storage.ingest",
        "LiuXin_alpha.ingest.remote_html",
    ),
)
def test_storage_owners_import_independently(first_module: str) -> None:
    """
    Load storage owners after each historical storage/ingest dependency entry point.

    Real subpackage imports must work through Python's package machinery, while
    loading an owner must not republish its classes at the storage root.

    Example:
        test_storage_owners_import_independently("LiuXin_alpha.storage.api")


    :param first_module: Module imported first in an otherwise fresh interpreter.
    :return: None after owner imports and subpackage identities are verified.
    """
    _check_storage_imports(
        f"import importlib\nimportlib.import_module({first_module!r})\n"
        + """
import LiuXin_alpha.storage as storage
from LiuXin_alpha.storage import api, ingest, reconcile, utils
from LiuXin_alpha.storage.backend_registry import StorageBackendRegistry
from LiuXin_alpha.storage.store_manager import StorageManager
from LiuXin_alpha.storage.store_container import StoreContainer
from LiuXin_alpha.storage.workflows.sealed_artifact_workflow import SealedArtifactWorkflow
from LiuXin_alpha.storage.backup import StoreBackupPlanner
from LiuXin_alpha.storage.errors import StorageError
for module in (api, ingest, reconcile, utils):
    assert module is importlib.import_module(module.__name__)
for name in ('StorageManager', 'StoreContainer', 'StorageError', 'SealedArtifactWorkflow'):
    assert not hasattr(storage, name), name
"""
    )

"""Verify Core's per-library owner provides complete startup and teardown."""

from __future__ import annotations

import threading

import pytest

from LiuXin_alpha.core.factory import create_core
from LiuXin_alpha.core.library_services import (
    LibraryServices,
    LibraryServicesCloseError,
)
from LiuXin_alpha.core.services import CoreServices
from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.storage.durable_manager import StorageManager


def test_library_services_closes_in_reverse_order_and_is_idempotent(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """Close Core services, storage, then database exactly once."""

    calls: list[str] = []
    target_database: list[Database | None] = [None]
    original_core_close = CoreServices.close
    original_storage_close = StorageManager.close
    original_database_close = Database.close

    def close_core(self) -> None:
        calls.append("core")
        original_core_close(self)

    def close_storage(self) -> None:
        calls.append("storage")
        original_storage_close(self)

    def close_database(self) -> None:
        if self is target_database[0]:
            calls.append("database")
        original_database_close(self)

    monkeypatch.setattr(CoreServices, "close", close_core)
    monkeypatch.setattr(StorageManager, "close", close_storage)
    monkeypatch.setattr(Database, "close", close_database)

    services = LibraryServices.open(
        database_path=tmp_path / "library.sqlite",
        create=True,
        enable_maintenance=False,
    )
    target_database[0] = services.database
    services.close()
    services.close()

    assert calls == ["core", "storage", "database"]


def test_library_services_rolls_back_database_and_storage_after_failed_open(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """A strict storage bootstrap failure closes every resource opened earlier."""

    calls: list[str] = []
    expected_database_path = str(tmp_path / "failed.sqlite")
    original_storage_close = StorageManager.close
    original_database_close = Database.close

    def fail_load(self, *args, **kwargs):
        del self, args, kwargs
        raise RuntimeError("bootstrap failed")

    def close_storage(self) -> None:
        calls.append("storage")
        original_storage_close(self)

    def close_database(self) -> None:
        if getattr(self, "metadata", {}).get("database_path") == expected_database_path:
            calls.append("database")
        original_database_close(self)

    monkeypatch.setattr(StorageManager, "load_from_database", fail_load)
    monkeypatch.setattr(StorageManager, "close", close_storage)
    monkeypatch.setattr(Database, "close", close_database)

    with pytest.raises(RuntimeError, match="bootstrap failed"):
        LibraryServices.open(
            database_path=tmp_path / "failed.sqlite",
            create=True,
            strict_storage_manager_bootstrap=True,
            enable_maintenance=False,
        )

    assert calls == ["storage", "database"]


def test_library_services_continues_teardown_after_close_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """A failing Store-manager close cannot prevent database cleanup."""

    calls: list[str] = []
    target_database: list[Database | None] = [None]
    original_database_close = Database.close

    def fail_storage_close(self) -> None:
        del self
        calls.append("storage")
        raise RuntimeError("store close failed")

    def close_database(self) -> None:
        if self is target_database[0]:
            calls.append("database")
        original_database_close(self)

    monkeypatch.setattr(StorageManager, "close", fail_storage_close)
    monkeypatch.setattr(Database, "close", close_database)
    services = LibraryServices.open(
        database_path=tmp_path / "close-failure.sqlite",
        create=True,
        enable_maintenance=False,
    )
    target_database[0] = services.database

    with pytest.raises(LibraryServicesCloseError, match="store close failed"):
        services.close()

    assert calls == ["storage", "database"]
    assert services.database.storage is None


def test_library_services_owns_and_stops_maintenance_thread(tmp_path) -> None:
    """An enabled maintenance engine is alive while open and absent after close."""

    services = LibraryServices.open(
        database_path=tmp_path / "maintained.sqlite",
        create=True,
        enable_maintenance=True,
    )
    engine = services.maintenance.maintainer
    assert engine.is_alive()

    services.close()

    assert not engine.is_alive()
    assert not any(
        thread.name == "liuxin-maintenance-engine" and thread.is_alive()
        for thread in threading.enumerate()
    )


def test_create_core_shutdown_closes_its_complete_owned_graph(tmp_path) -> None:
    """The standard Core factory transfers its owner to runtime shutdown."""

    runtime = create_core(
        database_path=tmp_path / "core-owned.sqlite",
        create=True,
        backup=False,
        enable_maintenance=False,
    )
    database = runtime.library.database
    storage = runtime.library.storage

    runtime.shutdown()

    assert storage._closed is True
    assert database.storage is None
    assert database.conn is None


def test_library_services_borrows_an_injected_database(tmp_path) -> None:
    """Closing an owner detaches owned storage but leaves an injected database open."""

    database = Database(
        metadata={"database_path": str(tmp_path / "borrowed.sqlite")},
        create=True,
        backup=False,
    )
    try:
        services = LibraryServices.open(
            database=database,
            enable_maintenance=False,
        )
        storage = services.storage

        services.close()

        assert storage is not None
        assert storage._closed is True
        assert database.storage is None
        assert database.conn is not None
    finally:
        database.close()

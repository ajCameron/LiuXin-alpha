"""
Unified high-level library facade.

This class is an intentionally small first-pass wrapper around:
- `Database` (schema + rows + metadata APIs)
- `StorageManager` (store orchestration + file retrieval)
- storage reconcile helpers (disk->DB registration)
"""

from __future__ import annotations

import pathlib

from collections.abc import Iterator
from typing import Optional

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.metadata.api import MetadataContainerAPI
from LiuXin_alpha.storage.api import SingleFileAPI, StoreAPI
from LiuXin_alpha.storage.reconcile import (
    UnmanagedDiskRegistrationReport,
    register_existing_disk_as_unmanaged_store,
)
from LiuXin_alpha.storage.store_manager import StorageBootstrapReport, StorageManager


class Library:
    """
    Unified access point for database + storage flows.

    This is a compatibility-friendly facade, not a replacement for lower-level
    APIs. Advanced callers can still access `library.database` directly.
    """

    def __init__(
        self,
        *,
        database: Optional[Database] = None,
        database_path: str | pathlib.Path | None = None,
        db_type: str = "SQLite",
        create: bool = False,
        backup: bool = False,
        enable_storage_manager: bool = True,
        strict_storage_manager_bootstrap: bool = False,
        storage_startup_on_add: bool = False,
        close_database_on_close: Optional[bool] = None,
    ) -> None:
        if database is None and database_path is None:
            raise ValueError("Provide either `database` or `database_path`.")
        if database is not None and database_path is not None:
            raise ValueError("Provide only one of `database` or `database_path`.")

        if database is None:
            path = pathlib.Path(database_path).expanduser()
            if create:
                path.parent.mkdir(parents=True, exist_ok=True)
            metadata = {"database_path": str(path)}
            database = Database(
                metadata=metadata,
                db_type=db_type,
                create=create,
                backup=backup,
                enable_storage_manager=enable_storage_manager,
                strict_storage_manager_bootstrap=strict_storage_manager_bootstrap,
                storage_startup_on_add=storage_startup_on_add,
            )
            self._owns_database = True
        else:
            self._owns_database = False

        self._database = database
        self._closed = False

        if close_database_on_close is None:
            close_database_on_close = self._owns_database
        self._close_database_on_close = bool(close_database_on_close)

    @property
    def database(self) -> Database:
        return self._database

    @property
    def db(self) -> Database:
        return self._database

    @property
    def storage(self) -> StorageManager:
        storage = getattr(self._database, "storage", None)
        if storage is None:
            raise RuntimeError("Storage manager is not enabled for this library instance.")
        return storage

    @property
    def storage_bootstrap_report(self) -> Optional[StorageBootstrapReport]:
        return getattr(self._database, "storage_bootstrap_report", None)

    def refresh_storage(
        self,
        *,
        startup_on_add: bool = False,
        include_offline: bool = False,
        clear_existing: bool = True,
        strict: bool = False,
    ) -> StorageBootstrapReport:
        return self._database.bootstrap_storage_manager(
            startup_on_add=startup_on_add,
            include_offline=include_offline,
            clear_existing=clear_existing,
            strict=strict,
        )

    def get_store(self, store_identifier: str) -> StoreAPI:
        return self.storage.get_store(store_identifier)

    def iter_stores(self) -> Iterator[StoreAPI]:
        return self.storage.iter_stores()

    def add_file(
        self,
        file_bytes: bytes,
        metadata: Optional[MetadataContainerAPI] = None,
        *,
        preferred_store: Optional[str] = None,
    ) -> SingleFileAPI:
        return self.storage.add_file(file_bytes=file_bytes, metadata=metadata, preferred_store=preferred_store)

    def retrieve_file(
        self,
        file_url: Optional[str] = None,
        metadata: Optional[MetadataContainerAPI] = None,
        *,
        preferred_store: Optional[str] = None,
    ) -> SingleFileAPI:
        return self.storage.retrieve_file(file_url=file_url, metadata=metadata, preferred_store=preferred_store)

    def delete_file(
        self,
        file_url: Optional[str] = None,
        metadata: Optional[MetadataContainerAPI] = None,
        file_container: Optional[SingleFileAPI] = None,
    ) -> bool:
        return self.storage.delete_file(file_url=file_url, metadata=metadata, file_container=file_container)

    def iter_files(self) -> Iterator[SingleFileAPI]:
        return self.storage.iter()

    def register_unmanaged_disk(
        self,
        disk_path: str | pathlib.Path,
        *,
        store_name: Optional[str] = None,
        ebook_extensions: Optional[tuple[str, ...] | list[str] | set[str]] = None,
        source_label: str = "on_disk_unmanaged_import",
        compute_hash: bool = True,
        follow_symlinks: bool = False,
        attach_store_links: bool = True,
        refresh_storage_manager: bool = True,
    ) -> UnmanagedDiskRegistrationReport:
        return register_existing_disk_as_unmanaged_store(
            self._database,
            disk_path=disk_path,
            store_name=store_name,
            ebook_extensions=ebook_extensions,
            source_label=source_label,
            compute_hash=compute_hash,
            follow_symlinks=follow_symlinks,
            attach_store_links=attach_store_links,
            refresh_storage_manager=refresh_storage_manager,
        )

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._close_database_on_close:
            self._database.close()

    def __enter__(self) -> "Library":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.close()
        return False


__all__ = ["Library"]

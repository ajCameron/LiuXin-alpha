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
from typing import Any, Mapping, Optional

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.metadata.api import MetadataContainerAPI
from LiuXin_alpha.storage.api import SingleFileAPI, StoreAPI
from LiuXin_alpha.storage.reconcile import (
    SquashfsArchivePublishReport,
    SquashfsDesignationReport,
    UnmanagedDiskRegistrationReport,
    designate_files_for_squashfs_store,
    ensure_open_squashfs_store,
    publish_open_squashfs_store,
    publish_squashfs_archive_from_file_ids,
    register_existing_disk_as_unmanaged_store,
    register_rclone_http_readonly_store_files,
    register_wget_html_readonly_store_files,
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

    @staticmethod
    def _row_to_plain_dict(row: Row | Mapping[str, Any]) -> dict[str, Any]:
        if isinstance(row, Mapping):
            return dict(row)
        return dict(getattr(row, "row_dict", {}) or {})

    def _find_existing_store_row(self, *, root_uri: str, store_name: str) -> Row | None:
        root_token = str(root_uri).strip()
        name_token = str(store_name).strip()

        if root_token:
            rows = self._database.search("stores", "store_root_uri", root_token)
            if rows:
                return rows[0]
        if name_token:
            rows = self._database.search("stores", "store_name", name_token)
            if rows:
                return rows[0]
        return None

    def find_existing_store(self, *, root_uri: str, store_name: str) -> dict[str, Any] | None:
        row = self._find_existing_store_row(root_uri=root_uri, store_name=store_name)
        if row is None:
            return None
        return self._row_to_plain_dict(row)

    def save_store_row(self, *, store_payload: Mapping[str, Any]) -> dict[str, Any]:
        payload = dict(store_payload or {})
        table_columns = set(self._database.get_column_headings("stores"))
        row_dict = {key: value for key, value in payload.items() if key in table_columns and value is not None}
        if not row_dict:
            raise ValueError("Store payload did not contain any writable `stores` columns.")

        existing = self._find_existing_store_row(
            root_uri=str(row_dict.get("store_root_uri", "") or ""),
            store_name=str(row_dict.get("store_name", "") or ""),
        )
        if existing is not None:
            changed = False
            for key, value in row_dict.items():
                if key not in existing.allowed_columns:
                    continue
                if existing[key] != value:
                    existing[key] = value
                    changed = True
            if changed:
                existing.sync()
            return self._row_to_plain_dict(existing)

        row = Row.from_idless_row_dict(self._database, row_dict=row_dict, table="stores")
        return self._row_to_plain_dict(row)

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

    def register_rclone_http_store(
        self,
        remote_url: str,
        *,
        store_name: Optional[str] = None,
        max_http_requests_per_hour: float | None = None,
        apply_rclone_tpslimit: bool = True,
        rclone_tpslimit_burst: int = 1,
        enforce_global_rate_limit: bool = True,
        rclone_exe: str = "rclone",
        rclone_args: Optional[tuple[str, ...] | list[str]] = None,
        timeout_s: float | None = 60.0,
        ebook_extensions: Optional[tuple[str, ...] | list[str] | set[str]] = None,
        source_label: str = "rclone_http_import",
        capture_hashes: bool = False,
        attach_store_links: bool = True,
        refresh_storage_manager: bool = True,
    ) -> UnmanagedDiskRegistrationReport:
        return register_rclone_http_readonly_store_files(
            self._database,
            remote_url=remote_url,
            store_name=store_name,
            max_http_requests_per_hour=max_http_requests_per_hour,
            apply_rclone_tpslimit=apply_rclone_tpslimit,
            rclone_tpslimit_burst=rclone_tpslimit_burst,
            enforce_global_rate_limit=enforce_global_rate_limit,
            rclone_exe=rclone_exe,
            rclone_args=rclone_args,
            timeout_s=timeout_s,
            ebook_extensions=ebook_extensions,
            source_label=source_label,
            capture_hashes=capture_hashes,
            attach_store_links=attach_store_links,
            refresh_storage_manager=refresh_storage_manager,
        )

    def register_wget_html_store(
        self,
        remote_url: str,
        *,
        store_name: Optional[str] = None,
        max_http_requests_per_hour: float | None = None,
        wget_exe: str = "wget",
        wget_args: Optional[tuple[str, ...] | list[str]] = None,
        timeout_s: float | None = 300.0,
        recurse: bool = True,
        max_depth: int | None = None,
        no_parent: bool = True,
        span_hosts: bool = False,
        respect_robots: bool = True,
        user_agent: str | None = None,
        no_verbose: bool = True,
        ebook_extensions: Optional[tuple[str, ...] | list[str] | set[str]] = None,
        source_label: str = "wget_html_import",
        attach_store_links: bool = True,
        refresh_storage_manager: bool = True,
    ) -> UnmanagedDiskRegistrationReport:
        return register_wget_html_readonly_store_files(
            self._database,
            remote_url=remote_url,
            store_name=store_name,
            max_http_requests_per_hour=max_http_requests_per_hour,
            wget_exe=wget_exe,
            wget_args=wget_args,
            timeout_s=timeout_s,
            recurse=recurse,
            max_depth=max_depth,
            no_parent=no_parent,
            span_hosts=span_hosts,
            respect_robots=respect_robots,
            user_agent=user_agent,
            no_verbose=no_verbose,
            ebook_extensions=ebook_extensions,
            source_label=source_label,
            attach_store_links=attach_store_links,
            refresh_storage_manager=refresh_storage_manager,
        )

    def ensure_open_squashfs_store(
        self,
        *,
        archive_path: str | pathlib.Path,
        store_name: Optional[str] = None,
    ):
        return ensure_open_squashfs_store(
            self._database,
            archive_path=archive_path,
            store_name=store_name,
        )

    def designate_files_for_squashfs_store(
        self,
        *,
        store_id: int,
        designations,
        replace_existing: bool = False,
    ) -> SquashfsDesignationReport:
        return designate_files_for_squashfs_store(
            self._database,
            store_id=store_id,
            designations=designations,
            replace_existing=replace_existing,
        )

    def publish_open_squashfs_store(
        self,
        *,
        store_id: int,
        output_archive: Optional[str | pathlib.Path] = None,
        compression: str = "zstd",
        deterministic: bool = False,
        force: bool = False,
        duplicate_verified_files: bool = True,
        strict: bool = False,
        refresh_storage_manager: bool = True,
    ) -> SquashfsArchivePublishReport:
        return publish_open_squashfs_store(
            self._database,
            store_id=store_id,
            output_archive=output_archive,
            compression=compression,
            deterministic=deterministic,
            force=force,
            duplicate_verified_files=duplicate_verified_files,
            strict=strict,
            refresh_storage_manager=refresh_storage_manager,
        )

    def publish_squashfs_archive_from_file_ids(
        self,
        *,
        file_ids,
        archive_path: str | pathlib.Path,
        store_name: Optional[str] = None,
        compression: str = "zstd",
        deterministic: bool = False,
        force: bool = False,
        strict: bool = False,
        refresh_storage_manager: bool = True,
    ) -> SquashfsArchivePublishReport:
        return publish_squashfs_archive_from_file_ids(
            self._database,
            file_ids=file_ids,
            archive_path=archive_path,
            store_name=store_name,
            compression=compression,
            deterministic=deterministic,
            force=force,
            strict=strict,
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

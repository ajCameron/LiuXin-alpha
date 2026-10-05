"""
Expose the unified high-level library facade.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise library through a consuming regression::

        python -m pytest -q tests/library/test_unified_library.py
"""

from __future__ import annotations

import pathlib
from collections.abc import Iterable, Iterator
from typing import Any, BinaryIO, Mapping, Optional
from uuid import UUID

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.ingest.models import (
    RemoteHtmlRegistrationReport,
    StoreIngestObjectCheckpoint,
    StoreIngestReport,
)
from LiuXin_alpha.ingest.remote_html import (
    register_native_html_readonly_store_files,
    register_wget_html_readonly_store_files,
)
from LiuXin_alpha.ingest.stores import adopt_store as adopt_configured_store
from LiuXin_alpha.ingest.stores import ingest_store as ingest_configured_store
from LiuXin_alpha.metadata.containers import ItemMetadata, ItemMetadataHydrator
from LiuXin_alpha.storage.api import (
    Digest,
    DigitalAssetID,
    DigitalAssetRecord,
    Location,
    ReplicaID,
    ReplicaMode,
    ReplicaRecord,
    ReplicaRemovalReport,
    StoreAPI,
    StoreConfiguration,
    StoreUUID,
)
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
)
from LiuXin_alpha.storage.store_manager import StorageBootstrapReport, StorageManager


class Library:
    """
    Unified access point for database + storage flows.

    Example:
        Exercise Library through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    def __init__(
        self,
        *,
        database: Optional[Database] = None,
        database_path: str | pathlib.Path | None = None,
        db_type: str = "SQLite",
        database_metadata: Mapping[str, Any] | None = None,
        create: bool = False,
        backup: bool = False,
        enable_storage_manager: bool = True,
        strict_storage_manager_bootstrap: bool = False,
        storage_startup_on_add: bool = False,
        enable_maintenance: bool = True,
        repair_bootstrap_rows: bool = True,
        close_database_on_close: Optional[bool] = None,
    ) -> None:
        """
        Initialize and validate the library state.

        Example:
            Exercise Library.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param database: Value supplied for database under the utility contract.
        :param database_path: Value supplied for database path under the utility contract.
        :param db_type: Value supplied for db type under the utility contract.
        :param database_metadata: Value supplied for database metadata under the utility
            contract.
        :param create: Value supplied for create under the utility contract.
        :param backup: Value supplied for backup under the utility contract.
        :param enable_storage_manager: Value supplied for enable storage manager under the
            utility contract.
        :param strict_storage_manager_bootstrap: Value supplied for strict storage manager
            bootstrap under the utility contract.
        :param storage_startup_on_add: Value supplied for storage startup on add under the
            utility contract.
        :param enable_maintenance: Value supplied for enable maintenance under the utility
            contract.
        :param repair_bootstrap_rows: Value supplied for repair bootstrap rows under the
            utility contract.
        :param close_database_on_close: Value supplied for close database on close under the
            utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        if database is None and database_path is None:
            raise ValueError("Provide either `database` or `database_path`.")
        if database is not None and database_path is not None:
            raise ValueError("Provide only one of `database` or `database_path`.")

        if database is None:
            assert database_path is not None
            server_database = str(db_type).strip().casefold() in {
                "postgres",
                "postgresql",
                "pg",
            }
            if server_database:
                target = str(database_path)
                metadata = {
                    "database_path": target,
                    **dict(database_metadata or {}),
                }
                if target.startswith(("postgres://", "postgresql://")):
                    metadata["postgres_url"] = target
                else:
                    metadata["postgres_service"] = target
            else:
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
                enable_maintenance=enable_maintenance,
                repair_bootstrap_rows=repair_bootstrap_rows,
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
        """
        Perform the database operation under explicit file-format and conversion rules.

        Example:
            Exercise Library.database through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._database

    @property
    def db(self) -> Database:
        """
        Perform the db operation under explicit file-format and conversion rules.

        Example:
            Exercise Library.db through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._database

    @property
    def storage(self) -> StorageManager:
        """
        Perform the storage operation under explicit file-format and conversion rules.

        Example:
            Exercise Library.storage through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        storage = getattr(self._database, "storage", None)
        if storage is None:
            raise RuntimeError("Storage manager is not enabled for this library instance.")
        return storage

    @property
    def storage_bootstrap_report(self) -> Optional[StorageBootstrapReport]:
        """
        Perform the storage bootstrap report operation under explicit file-format and conversion rules.

        Example:
            Exercise Library.storage bootstrap report through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return getattr(self._database, "storage_bootstrap_report", None)

    @staticmethod
    def _row_to_plain_dict(row: Row | Mapping[str, Any]) -> dict[str, Any]:
        """
        Perform the row to plain dict operation under explicit file-format and conversion rules.

        Example:
            Exercise Library. row to plain dict through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param row: Value supplied for row under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if isinstance(row, Mapping):
            return dict(row)
        return dict(getattr(row, "row_dict", {}) or {})

    def _sample_plain_rows(
        self,
        rows: list[Row] | tuple[Row, ...] | list[Mapping[str, Any]] | tuple[Mapping[str, Any], ...],
        *,
        limit: int,
    ) -> list[dict[str, Any]]:
        """
        Perform the sample plain rows operation under explicit file-format and conversion rules.

        Example:
            Exercise Library. sample plain rows through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param rows: Value supplied for rows under the utility contract.
        :param limit: Value supplied for limit under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        capped = max(0, int(limit))
        if capped <= 0:
            return []
        samples: list[dict[str, Any]] = []
        for row in list(rows)[:capped]:
            samples.append(self._row_to_plain_dict(row))
        return samples

    def _find_existing_store_row(self, *, root_uri: str, store_name: str) -> Row | None:
        """
        Find existing store row under the format's safety and compatibility rules.

        Example:
            Exercise Library. find existing store row through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param root_uri: Value supplied for root uri under the utility contract.
        :param store_name: Value supplied for store name under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Find existing store under the format's safety and compatibility rules.

        Example:
            Exercise Library.find existing store through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param root_uri: Value supplied for root uri under the utility contract.
        :param store_name: Value supplied for store name under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        row = self._find_existing_store_row(root_uri=root_uri, store_name=store_name)
        if row is None:
            return None
        return self._row_to_plain_dict(row)

    def get_row(self, *, table: str, row_id: int) -> dict[str, Any] | None:
        """
        Return row under the format's safety and compatibility rules.

        Example:
            Exercise Library.get row through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param table: Value supplied for table under the utility contract.
        :param row_id: Value supplied for row id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        target_table = str(table).strip()
        if not target_table:
            raise ValueError("`table` is required.")
        target_row_id = int(row_id)
        row = self._database.get_row_from_id(target_table, target_row_id)
        if row is None:
            return None
        return self._row_to_plain_dict(row)

    def get_item_metadata(
        self,
        *,
        item_id: int | None = None,
        source_row: Mapping[str, Any] | Row | None = None,
    ) -> ItemMetadata:
        """
        Return one concrete item metadata bundle.

        Example:
            Exercise Library.get item metadata through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :param source_row: Value supplied for source row under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        hydrator = ItemMetadataHydrator(self._database)
        if item_id is not None:
            return hydrator.from_item_id(int(item_id))
        if source_row is not None:
            return hydrator.from_source_row(source_row)
        raise ValueError("Provide either `item_id` or `source_row`.")

    def update_row_fields(
        self,
        *,
        table: str,
        row_id: int,
        updates: Mapping[str, Any],
    ) -> dict[str, Any]:
        """
        Perform the update row fields operation under explicit file-format and conversion rules.

        Example:
            Exercise Library.update row fields through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param table: Value supplied for table under the utility contract.
        :param row_id: Value supplied for row id under the utility contract.
        :param updates: Value supplied for updates under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        target_table = str(table).strip()
        if not target_table:
            raise ValueError("`table` is required.")

        row = self._database.get_row_from_id(target_table, int(row_id))
        if row is None:
            raise ValueError("No row found in {} for id {}.".format(target_table, int(row_id)))

        payload = dict(updates or {})
        if not payload:
            raise ValueError("`updates` must contain at least one column.")

        id_column = str(self._database.driver_wrapper.get_id_column(target_table))
        allowed_columns = set(getattr(row, "allowed_columns", ()) or ())

        clean_updates: dict[str, Any] = {}
        for raw_key, value in payload.items():
            key = str(raw_key).strip()
            if not key:
                continue
            if key == id_column:
                raise ValueError("Cannot update id column {!r} for table {!r}.".format(id_column, target_table))
            if key not in allowed_columns:
                raise ValueError("Unknown column {!r} for table {!r}.".format(key, target_table))
            clean_updates[key] = value

        if not clean_updates:
            raise ValueError("No writable updates were provided for table {!r}.".format(target_table))

        changed = False
        for key, value in clean_updates.items():
            if row[key] != value:
                row[key] = value
                changed = True
        if changed:
            row.sync()
        return self._row_to_plain_dict(row)

    def describe_row_delete_impact(self, *, table: str, row_id: int, sample_limit: int = 3) -> dict[str, Any]:
        """
        Perform the describe row delete impact operation under explicit file-format and conversion rules.

        Example:
            Exercise Library.describe row delete impact through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param table: Value supplied for table under the utility contract.
        :param row_id: Value supplied for row id under the utility contract.
        :param sample_limit: Value supplied for sample limit under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        target_table = str(table).strip()
        if not target_table:
            raise ValueError("`table` is required.")

        row = self._database.get_row_from_id(target_table, int(row_id))
        if row is None:
            raise ValueError("No row found in {} for id {}.".format(target_table, int(row_id)))

        max_samples = max(0, int(sample_limit))
        id_column = str(self._database.driver_wrapper.get_id_column(target_table))
        row_value = row[id_column]

        interlinked_counts: list[dict[str, Any]] = []
        interlinked_tables: set[str] = set()
        for linked_table in sorted(self._database.driver_wrapper.get_interlinked_tables(target_table)):
            if linked_table == target_table:
                continue
            try:
                rows = self._database.get_interlinked_rows(target_row=row, secondary_table=linked_table)
            except Exception:
                continue
            count = len(rows or [])
            if count <= 0:
                continue
            interlinked_tables.add(linked_table)
            interlinked_counts.append(
                {
                    "table": linked_table,
                    "count": count,
                    "sample_rows": self._sample_plain_rows(rows or [], limit=max_samples),
                }
            )

        reference_candidates: list[tuple[str, str]] = []
        seen_reference_candidates: set[tuple[str, str]] = set()
        for candidate_table in sorted(self._database.get_tables()):
            if candidate_table in interlinked_tables:
                continue
            try:
                candidate_columns = list(self._database.get_column_headings(candidate_table))
            except Exception:
                continue
            for candidate_column in candidate_columns:
                if candidate_table == target_table and candidate_column == id_column:
                    continue
                normalized = str(candidate_column).strip()
                if normalized == id_column or normalized.endswith("_" + id_column):
                    key = (candidate_table, normalized)
                    if key in seen_reference_candidates:
                        continue
                    seen_reference_candidates.add(key)
                    reference_candidates.append(key)

        try:
            parent_column = self._database.driver_wrapper.get_parent_column(target_table)
        except Exception:
            parent_column = None
        if parent_column:
            parent_key = (target_table, str(parent_column))
            if parent_key not in seen_reference_candidates and str(parent_column) != id_column:
                seen_reference_candidates.add(parent_key)
                reference_candidates.append(parent_key)

        reference_counts: list[dict[str, Any]] = []
        for candidate_table, candidate_column in reference_candidates:
            try:
                rows = self._database.search(candidate_table, candidate_column, row_value)
            except Exception:
                continue
            count = len(rows or [])
            if count <= 0:
                continue
            reference_counts.append(
                {
                    "table": candidate_table,
                    "column": candidate_column,
                    "count": count,
                    "sample_rows": self._sample_plain_rows(rows or [], limit=max_samples),
                }
            )

        interlinked_counts.sort(key=lambda item: (-int(item["count"]), str(item["table"])))
        reference_counts.sort(
            key=lambda item: (-int(item["count"]), str(item["table"]), str(item["column"]))
        )

        return {
            "table": target_table,
            "row_id": int(row_id),
            "id_column": id_column,
            "row": self._row_to_plain_dict(row),
            "sample_limit": max_samples,
            "interlinked_counts": interlinked_counts,
            "reference_counts": reference_counts,
            "interlinked_total": sum(int(item["count"]) for item in interlinked_counts),
            "reference_total": sum(int(item["count"]) for item in reference_counts),
            "warning": (
                "Delete may fail or cascade depending on schema constraints."
                if interlinked_counts or reference_counts
                else ""
            ),
        }

    def delete_row(self, *, table: str, row_id: int) -> dict[str, Any]:
        """
        Perform the delete row operation under explicit file-format and conversion rules.

        Example:
            Exercise Library.delete row through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param table: Value supplied for table under the utility contract.
        :param row_id: Value supplied for row id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        target_table = str(table).strip()
        if not target_table:
            raise ValueError("`table` is required.")

        row = self._database.get_row_from_id(target_table, int(row_id))
        if row is None:
            raise ValueError("No row found in {} for id {}.".format(target_table, int(row_id)))

        deleted = self._row_to_plain_dict(row)
        self._database.delete(row)
        return deleted

    def save_store_row(self, *, store_payload: Mapping[str, Any]) -> dict[str, Any]:
        """
        Perform the save store row operation under explicit file-format and conversion rules.

        Example:
            Exercise Library.save store row through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param store_payload: Value supplied for store payload under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Perform the refresh storage operation under explicit file-format and conversion rules.

        Example:
            Exercise Library.refresh storage through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param startup_on_add: Value supplied for startup on add under the utility contract.
        :param include_offline: Value supplied for include offline under the utility
            contract.
        :param clear_existing: Value supplied for clear existing under the utility contract.
        :param strict: Value supplied for strict under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._database.bootstrap_storage_manager(
            startup_on_add=startup_on_add,
            include_offline=include_offline,
            clear_existing=clear_existing,
            strict=strict,
        )

    def get_store(
        self,
        store: StoreUUID | StoreConfiguration | StoreAPI,
    ) -> StoreAPI:
        """
        Return one configured Store by stable UUID or Store value.

        Example:
            Exercise Library.get store through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param store: Value supplied for store under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        return self.storage.get_store(self._store_ref(store))

    def iter_stores(self) -> Iterator[StoreAPI]:
        """
        Iterate live Store facades, not persistence containers.

        Example:
            Exercise Library.iter stores through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        return self.storage.iter_stores()

    def add_file(
        self,
        file_bytes: bytes,
        metadata: Optional[Any] = None,
        *,
        store: StoreUUID | StoreConfiguration | StoreAPI | None = None,
        name: str | None = None,
        media_type: str | None = None,
        original_name: str | None = None,
        verify: bool = True,
    ) -> DigitalAssetRecord:
        """
        Store bytes transactionally and return their logical Asset record.

        Example:
            Exercise Library.add file through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param file_bytes: Value supplied for file bytes under the utility contract.
        :param metadata: Value supplied for metadata under the utility contract.
        :param store: Value supplied for store under the utility contract.
        :param name: Field, file, function or resource name addressed by the operation.
        :param media_type: Value supplied for media type under the utility contract.
        :param original_name: Value supplied for original name under the utility contract.
        :param verify: Value supplied for verify under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        return self.storage.store_bytes(
            file_bytes,
            metadata=metadata,
            store=store,
            name=name,
            media_type=media_type,
            original_name=original_name,
            verify=verify,
        )

    def ingest_store(
        self,
        source: StoreUUID | StoreConfiguration | StoreAPI,
        *,
        destination: StoreUUID | StoreConfiguration | StoreAPI | None = None,
        prefix: str | Location | None = None,
        extensions: Iterable[str] | None = None,
        metadata: Any = None,
        placement_hints: Any = None,
        inspect: bool = True,
        replica_mode: ReplicaMode | str = ReplicaMode.ACTIVE,
        verify: bool = True,
        continue_on_error: bool = True,
        cursor: str | None = None,
        snapshot_token: str | None = None,
        page_size: int | None = None,
        max_files: int | None = None,
        workers: int | None = 1,
        object_staging_directory: str | pathlib.Path | None = None,
        resume_checkpoints: Iterable[StoreIngestObjectCheckpoint] = (),
    ) -> StoreIngestReport:
        """
        Copy files from any enumerable Store into managed storage.

        Example:
            Exercise Library.ingest store through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param source: Value supplied for source under the utility contract.
        :param destination: Value supplied for destination under the utility contract.
        :param prefix: Text prepended to the formatted or selected result.
        :param extensions: Value supplied for extensions under the utility contract.
        :param metadata: Value supplied for metadata under the utility contract.
        :param placement_hints: Value supplied for placement hints under the utility
            contract.
        :param inspect: Value supplied for inspect under the utility contract.
        :param replica_mode: Value supplied for replica mode under the utility contract.
        :param verify: Value supplied for verify under the utility contract.
        :param continue_on_error: Value supplied for continue on error under the utility
            contract.
        :param cursor: Value supplied for cursor under the utility contract.
        :param snapshot_token: Value supplied for snapshot token under the utility contract.
        :param page_size: Value supplied for page size under the utility contract.
        :param max_files: Value supplied for max files under the utility contract.
        :param workers: Value supplied for workers under the utility contract.
        :param object_staging_directory: Value supplied for object staging directory under
            the utility contract.
        :param resume_checkpoints: Value supplied for resume checkpoints under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        return ingest_configured_store(
            self.storage,
            source,
            destination=destination,
            prefix=prefix,
            extensions=extensions,
            metadata=metadata,
            placement_hints=placement_hints,
            inspect=inspect,
            replica_mode=replica_mode,
            verify=verify,
            continue_on_error=continue_on_error,
            cursor=cursor,
            snapshot_token=snapshot_token,
            page_size=page_size,
            max_files=max_files,
            workers=workers,
            object_staging_directory=object_staging_directory,
            resume_checkpoints=resume_checkpoints,
        )

    def adopt_store(
        self,
        source: StoreUUID | StoreConfiguration | StoreAPI,
        *,
        prefix: str | Location | None = None,
        extensions: Iterable[str] | None = None,
        metadata: Any = None,
        inspect: bool = True,
        replica_mode: ReplicaMode | str = ReplicaMode.UNMANAGED,
        verify: bool = False,
        continue_on_error: bool = True,
        cursor: str | None = None,
        snapshot_token: str | None = None,
        page_size: int | None = None,
        max_files: int | None = None,
        workers: int | None = 1,
    ) -> StoreIngestReport:
        """
        Register files already present in an attached managed Store.

        Example:
            Exercise Library.adopt store through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param source: Value supplied for source under the utility contract.
        :param prefix: Text prepended to the formatted or selected result.
        :param extensions: Value supplied for extensions under the utility contract.
        :param metadata: Value supplied for metadata under the utility contract.
        :param inspect: Value supplied for inspect under the utility contract.
        :param replica_mode: Value supplied for replica mode under the utility contract.
        :param verify: Value supplied for verify under the utility contract.
        :param continue_on_error: Value supplied for continue on error under the utility
            contract.
        :param cursor: Value supplied for cursor under the utility contract.
        :param snapshot_token: Value supplied for snapshot token under the utility contract.
        :param page_size: Value supplied for page size under the utility contract.
        :param max_files: Value supplied for max files under the utility contract.
        :param workers: Value supplied for workers under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        return adopt_configured_store(
            self.storage,
            source,
            prefix=prefix,
            extensions=extensions,
            metadata=metadata,
            inspect=inspect,
            replica_mode=replica_mode,
            verify=verify,
            continue_on_error=continue_on_error,
            cursor=cursor,
            snapshot_token=snapshot_token,
            page_size=page_size,
            max_files=max_files,
            workers=workers,
        )

    def open_file(
        self,
        identifier: int | DigitalAssetRecord | Digest | str,
        *,
        store: StoreUUID | StoreConfiguration | StoreAPI | None = None,
        verified: bool = False,
        offset: int = 0,
        length: int | None = None,
    ) -> BinaryIO:
        """
        Open an Asset by ID or digest as a read-only binary stream.

        Example:
            Exercise Library.open file through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param identifier: Value supplied for identifier under the utility contract.
        :param store: Value supplied for store under the utility contract.
        :param verified: Value supplied for verified under the utility contract.
        :param offset: Value supplied for offset under the utility contract.
        :param length: Value supplied for length under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        return self.storage.open_file(
            identifier,
            store=store,
            verified=verified,
            offset=offset,
            length=length,
        )

    def read_file(
        self,
        identifier: int | DigitalAssetRecord | Digest | str,
        *,
        store: StoreUUID | StoreConfiguration | StoreAPI | None = None,
        verified: bool = False,
        offset: int = 0,
        length: int | None = None,
    ) -> bytes:
        """
        Read an Asset by ID or digest fully into memory.

        Example:
            Exercise Library.read file through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param identifier: Value supplied for identifier under the utility contract.
        :param store: Value supplied for store under the utility contract.
        :param verified: Value supplied for verified under the utility contract.
        :param offset: Value supplied for offset under the utility contract.
        :param length: Value supplied for length under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        return self.storage.read_file(
            identifier,
            store=store,
            verified=verified,
            offset=offset,
            length=length,
        )

    def retrieve_file(
        self,
        identifier: int | DigitalAssetRecord | Digest | str,
        *,
        store: StoreUUID | StoreConfiguration | StoreAPI | None = None,
        verified: bool = False,
        offset: int = 0,
        length: int | None = None,
    ) -> BinaryIO:
        """
        Return the same read-only stream as :meth:`open_file`.

        Example:
            Exercise Library.retrieve file through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param identifier: Value supplied for identifier under the utility contract.
        :param store: Value supplied for store under the utility contract.
        :param verified: Value supplied for verified under the utility contract.
        :param offset: Value supplied for offset under the utility contract.
        :param length: Value supplied for length under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        return self.open_file(
            identifier,
            store=store,
            verified=verified,
            offset=offset,
            length=length,
        )

    def locate_file(
        self,
        asset: int | DigitalAssetRecord,
        *,
        store: StoreUUID | StoreConfiguration | StoreAPI | None = None,
        mode: ReplicaMode = ReplicaMode.ACTIVE,
        verified: bool = False,
    ) -> Location:
        """
        Resolve a logical Asset to its selected concrete Location.

        Example:
            Exercise Library.locate file through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param asset: Value supplied for asset under the utility contract.
        :param store: Value supplied for store under the utility contract.
        :param mode: Open or adapter mode controlling read/write behavior.
        :param verified: Value supplied for verified under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        asset_id = (
            asset.digital_asset_id
            if isinstance(asset, DigitalAssetRecord)
            else DigitalAssetID(int(asset))
        )
        return self.storage.locate_digital_asset(
            asset_id,
            preferred_store_ref=(None if store is None else self._store_ref(store)),
            mode=mode,
            require_verified=verified,
        )

    def open_location(
        self,
        location: Location,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> BinaryIO:
        """
        Open one exact routed Location without catalogue selection.

        Example:
            Exercise Library.open location through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param location: Value supplied for location under the utility contract.
        :param offset: Value supplied for offset under the utility contract.
        :param length: Value supplied for length under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        return self.storage.get(location, offset=offset, length=length)

    def read_location(
        self,
        location: Location,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> bytes:
        """
        Read one exact routed Location fully into memory.

        Example:
            Exercise Library.read location through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param location: Value supplied for location under the utility contract.
        :param offset: Value supplied for offset under the utility contract.
        :param length: Value supplied for length under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        return self.storage.read_bytes(location, offset=offset, length=length)

    def delete_file(
        self,
        replica: ReplicaID | ReplicaRecord | int,
        *,
        delete_bytes: bool = True,
        retain_tombstone: bool = True,
    ) -> ReplicaRemovalReport:
        """
        Remove one exact Replica; never guess which copies an Asset means.

        Example:
            Exercise Library.delete file through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param replica: Value supplied for replica under the utility contract.
        :param delete_bytes: Value supplied for delete bytes under the utility contract.
        :param retain_tombstone: Value supplied for retain tombstone under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        replica_id = (
            replica.replica_id
            if isinstance(replica, ReplicaRecord)
            else ReplicaID(int(replica))
        )
        return self.storage.remove_replica(
            replica_id,
            delete_bytes=delete_bytes,
            retain_tombstone=retain_tombstone,
        )

    def iter_files(self) -> Iterator[DigitalAssetRecord]:
        """
        Iterate logical Asset records rather than path-like locations.

        Example:
            Exercise Library.iter files through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        return self.storage.iter_digital_asset_records()

    @staticmethod
    def _store_ref(
        store: StoreUUID | StoreConfiguration | StoreAPI,
    ) -> StoreUUID:
        """
        Perform the store ref operation under explicit file-format and conversion rules.

        Example:
            Exercise Library. store ref through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param store: Value supplied for store under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if isinstance(store, UUID):
            return store
        if isinstance(store, StoreConfiguration):
            return store.store_uuid
        store_ref = getattr(store, "store_ref", None)
        if isinstance(store_ref, UUID):
            return store_ref
        raise TypeError("store must be a Store UUID, configuration, or Store facade.")

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
        """
        Perform the register unmanaged disk operation under explicit file-format and conversion rules.

        Example:
            Exercise Library.register unmanaged disk through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param disk_path: Value supplied for disk path under the utility contract.
        :param store_name: Value supplied for store name under the utility contract.
        :param ebook_extensions: Value supplied for ebook extensions under the utility
            contract.
        :param source_label: Value supplied for source label under the utility contract.
        :param compute_hash: Value supplied for compute hash under the utility contract.
        :param follow_symlinks: Value supplied for follow symlinks under the utility
            contract.
        :param attach_store_links: Value supplied for attach store links under the utility
            contract.
        :param refresh_storage_manager: Value supplied for refresh storage manager under the
            utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Perform the register rclone http store operation under explicit file-format and conversion rules.

        Example:
            Exercise Library.register rclone http store through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param remote_url: Value supplied for remote url under the utility contract.
        :param store_name: Value supplied for store name under the utility contract.
        :param max_http_requests_per_hour: Value supplied for max http requests per hour
            under the utility contract.
        :param apply_rclone_tpslimit: Value supplied for apply rclone tpslimit under the
            utility contract.
        :param rclone_tpslimit_burst: Value supplied for rclone tpslimit burst under the
            utility contract.
        :param enforce_global_rate_limit: Value supplied for enforce global rate limit under
            the utility contract.
        :param rclone_exe: Value supplied for rclone exe under the utility contract.
        :param rclone_args: Value supplied for rclone args under the utility contract.
        :param timeout_s: Value supplied for timeout s under the utility contract.
        :param ebook_extensions: Value supplied for ebook extensions under the utility
            contract.
        :param source_label: Value supplied for source label under the utility contract.
        :param capture_hashes: Value supplied for capture hashes under the utility contract.
        :param attach_store_links: Value supplied for attach store links under the utility
            contract.
        :param refresh_storage_manager: Value supplied for refresh storage manager under the
            utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
    ) -> RemoteHtmlRegistrationReport:
        """
        Perform the register wget html store operation under explicit file-format and conversion rules.

        Example:
            Exercise Library.register wget html store through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param remote_url: Value supplied for remote url under the utility contract.
        :param store_name: Value supplied for store name under the utility contract.
        :param max_http_requests_per_hour: Value supplied for max http requests per hour
            under the utility contract.
        :param wget_exe: Value supplied for wget exe under the utility contract.
        :param wget_args: Value supplied for wget args under the utility contract.
        :param timeout_s: Value supplied for timeout s under the utility contract.
        :param recurse: Value supplied for recurse under the utility contract.
        :param max_depth: Value supplied for max depth under the utility contract.
        :param no_parent: Value supplied for no parent under the utility contract.
        :param span_hosts: Value supplied for span hosts under the utility contract.
        :param respect_robots: Value supplied for respect robots under the utility contract.
        :param user_agent: Value supplied for user agent under the utility contract.
        :param no_verbose: Value supplied for no verbose under the utility contract.
        :param ebook_extensions: Value supplied for ebook extensions under the utility
            contract.
        :param source_label: Value supplied for source label under the utility contract.
        :param attach_store_links: Value supplied for attach store links under the utility
            contract.
        :param refresh_storage_manager: Value supplied for refresh storage manager under the
            utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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

    def register_native_html_store(
        self,
        remote_url: str,
        *,
        store_name: Optional[str] = None,
        max_http_requests_per_hour: float | None = None,
        timeout_s: float | None = 30.0,
        recurse: bool = True,
        max_depth: int | None = None,
        no_parent: bool = True,
        span_hosts: bool = False,
        respect_robots: bool = True,
        user_agent: str | None = None,
        max_html_bytes: int = 2_000_000,
        ebook_extensions: Optional[tuple[str, ...] | list[str] | set[str]] = None,
        source_label: str = "native_html_import",
        attach_store_links: bool = True,
        refresh_storage_manager: bool = True,
    ) -> RemoteHtmlRegistrationReport:
        """
        Perform the register native html store operation under explicit file-format and conversion rules.

        Example:
            Exercise Library.register native html store through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param remote_url: Value supplied for remote url under the utility contract.
        :param store_name: Value supplied for store name under the utility contract.
        :param max_http_requests_per_hour: Value supplied for max http requests per hour
            under the utility contract.
        :param timeout_s: Value supplied for timeout s under the utility contract.
        :param recurse: Value supplied for recurse under the utility contract.
        :param max_depth: Value supplied for max depth under the utility contract.
        :param no_parent: Value supplied for no parent under the utility contract.
        :param span_hosts: Value supplied for span hosts under the utility contract.
        :param respect_robots: Value supplied for respect robots under the utility contract.
        :param user_agent: Value supplied for user agent under the utility contract.
        :param max_html_bytes: Value supplied for max html bytes under the utility contract.
        :param ebook_extensions: Value supplied for ebook extensions under the utility
            contract.
        :param source_label: Value supplied for source label under the utility contract.
        :param attach_store_links: Value supplied for attach store links under the utility
            contract.
        :param refresh_storage_manager: Value supplied for refresh storage manager under the
            utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return register_native_html_readonly_store_files(
            self._database,
            remote_url=remote_url,
            store_name=store_name,
            max_http_requests_per_hour=max_http_requests_per_hour,
            timeout_s=timeout_s,
            recurse=recurse,
            max_depth=max_depth,
            no_parent=no_parent,
            span_hosts=span_hosts,
            respect_robots=respect_robots,
            user_agent=user_agent,
            max_html_bytes=max_html_bytes,
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
        """
        Perform the ensure open squashfs store operation under explicit file-format and conversion rules.

        Example:
            Exercise Library.ensure open squashfs store through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param archive_path: Value supplied for archive path under the utility contract.
        :param store_name: Value supplied for store name under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Perform the designate files for squashfs store operation under explicit file-format and conversion rules.

        Example:
            Exercise Library.designate files for squashfs store through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param store_id: Value supplied for store id under the utility contract.
        :param designations: Value supplied for designations under the utility contract.
        :param replace_existing: Value supplied for replace existing under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Perform the publish open squashfs store operation under explicit file-format and conversion rules.

        Example:
            Exercise Library.publish open squashfs store through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param store_id: Value supplied for store id under the utility contract.
        :param output_archive: Value supplied for output archive under the utility contract.
        :param compression: Value supplied for compression under the utility contract.
        :param deterministic: Value supplied for deterministic under the utility contract.
        :param force: Value supplied for force under the utility contract.
        :param duplicate_verified_files: Value supplied for duplicate verified files under
            the utility contract.
        :param strict: Value supplied for strict under the utility contract.
        :param refresh_storage_manager: Value supplied for refresh storage manager under the
            utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Perform the publish squashfs archive from file ids operation under explicit file-format and conversion rules.

        Example:
            Exercise Library.publish squashfs archive from file ids through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param file_ids: Value supplied for file ids under the utility contract.
        :param archive_path: Value supplied for archive path under the utility contract.
        :param store_name: Value supplied for store name under the utility contract.
        :param compression: Value supplied for compression under the utility contract.
        :param deterministic: Value supplied for deterministic under the utility contract.
        :param force: Value supplied for force under the utility contract.
        :param strict: Value supplied for strict under the utility contract.
        :param refresh_storage_manager: Value supplied for refresh storage manager under the
            utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Perform the close operation under explicit file-format and conversion rules.

        Example:
            Exercise Library.close through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self._closed:
            return
        self._closed = True
        if self._close_database_on_close:
            self._database.close()

    def __enter__(self) -> "Library":
        """
        Implement the conversion resource's enter lifecycle operation.

        Example:
            Exercise Library.  enter   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        """
        Implement the conversion resource's exit lifecycle operation.

        Example:
            Exercise Library.  exit   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param exc_type: Value supplied for exc type under the utility contract.
        :param exc: Value supplied for exc under the utility contract.
        :param tb: Value supplied for tb under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.close()
        return False


__all__ = ["Library"]

"""Concrete storage manager implementation.

`StorageManager` is the orchestration layer of storage. It owns configured
`StoreContainer` objects, chooses which container should service an operation,
and returns `Location` handles to callers. It should stay out of raw backend
mechanics and let each `StorePlugin` deal with physical media directly.
"""

from __future__ import annotations

import dataclasses
import importlib
import json
import pathlib

from collections.abc import Iterable, Iterator, Mapping
from typing import TYPE_CHECKING, Any, Optional

from LiuXin_alpha.metadata.api import MetadataContainerAPI
from LiuXin_alpha.storage.api import (
    StoreContainerAPI,
    StoreLocationMixinAPI,
    StorageManagerAPI,
    StorePluginAPI,
    StoreSpec,
)
from LiuXin_alpha.storage.store_container import StoreContainer
from LiuXin_alpha.storage.store_spec_utils import store_spec_from_row

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api import DatabaseAPI
    from LiuXin_alpha.storage.storage_types import StoreRef


@dataclasses.dataclass(slots=True)
class StorageBootstrapIssue:
    """Details for one row that could not be loaded into the storage manager."""

    store_id: Optional[int]
    store_name: Optional[str]
    reason: str


@dataclasses.dataclass(slots=True)
class StorageBootstrapReport:
    """Aggregate results of loading stores from database rows."""

    discovered_rows: int = 0
    loaded_stores: int = 0
    skipped_rows: int = 0
    failed_rows: int = 0
    issues: list[StorageBootstrapIssue] = dataclasses.field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.failed_rows == 0


class StorageManager(StorageManagerAPI):
    """Default in-process storage manager.

    The manager owns store containers, not raw plugins. Lookups accept a store id,
    uuid, name, or URL. File-facing operations return `Location` objects.
    """

    _STORE_KIND_ALIASES: Mapping[str, str] = {
        "on_disk_existing_managed_drive": "on_disk_existing_managed_drive",
        "on_disk_existing_managed": "on_disk_existing_managed_drive",
        "on_disk_managed": "on_disk_existing_managed_drive",
        "filesystem": "on_disk_existing_managed_drive",
        "on_disk_existing_unmanaged_drive": "on_disk_existing_unmanaged_drive",
        "on_disk_existing_unmanaged": "on_disk_existing_unmanaged_drive",
        "on_disk_unmanaged": "on_disk_existing_unmanaged_drive",
        "on_disk_calibre_like": "on_disk_calibre_like",
        "on_disk_flat": "on_disk_flat",
        "flat_hash_disk": "on_disk_flat",
        "flat_hash_store": "on_disk_flat",
        "calibre_like": "on_disk_calibre_like",
        "single_file_sqlite": "single_file_sqlite",
        "sqlite_single_file": "single_file_sqlite",
        "sqlite_blob_store": "single_file_sqlite",
        "single_file_blob_store": "single_file_sqlite",
        "squashfs_readonly": "squashfs_readonly",
        "squashfs_build": "squashfs_build",
        "squashfs_builder": "squashfs_build",
        "squashfs_backup": "squashfs_build",
        "squashfs_ro": "squashfs_readonly",
        "squashfs_archive": "squashfs_readonly",
        "archive_squashfs": "squashfs_readonly",
        "rclone_http_readonly": "rclone_http_readonly",
        "http_ro": "rclone_http_readonly",
        "rclone_http_ro": "rclone_http_readonly",
        "wget_html_readonly": "wget_html_readonly",
        "wget_http_ro": "wget_html_readonly",
        "http_spider_ro": "wget_html_readonly",
        "native_html_readonly": "native_html_readonly",
        "native_http_ro": "native_html_readonly",
        "http_native_ro": "native_html_readonly",
        "ftp_readonly": "ftp_readonly",
        "ftp_ro": "ftp_readonly",
        "ftps_readonly": "ftp_readonly",
        "ftps_ro": "ftp_readonly",
    }

    _STORE_BACKEND_IMPORTS: Mapping[str, tuple[str, str]] = {
        "on_disk_existing_managed_drive": (
            "LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_managed_drive",
            "OnDiskExistingManagedStorageBackend",
        ),
        "on_disk_existing_unmanaged_drive": (
            "LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive",
            "OnDiskUnmanagedStorageBackend",
        ),
        "on_disk_calibre_like": (
            "LiuXin_alpha.storage.store_backend_plugins.on_disk_calibre_like",
            "OnDiskCalibreLikeStorageBackend",
        ),
        "on_disk_flat": (
            "LiuXin_alpha.storage.store_backend_plugins.on_disk_flat",
            "OnDiskFlatStorageBackend",
        ),
        "single_file_sqlite": (
            "LiuXin_alpha.storage.store_backend_plugins.single_file_sqlite",
            "SingleFileSqliteStorageBackend",
        ),
        "squashfs_readonly": (
            "LiuXin_alpha.storage.store_backend_plugins.squashfs_readonly",
            "SquashfsReadOnlyStorageBackend",
        ),
        "squashfs_build": (
            "LiuXin_alpha.storage.store_backend_plugins.squashfs_build",
            "SquashfsBuildStorageBackend",
        ),
        "rclone_http_readonly": (
            "LiuXin_alpha.storage.store_backend_plugins.rclone_http_readonly",
            "RcloneHttpReadOnlyStorageBackend",
        ),
        "wget_html_readonly": (
            "LiuXin_alpha.storage.store_backend_plugins.wget_html_readonly",
            "WgetHtmlReadOnlyStorageBackend",
        ),
        "native_html_readonly": (
            "LiuXin_alpha.storage.store_backend_plugins.native_html_readonly",
            "NativeHtmlReadOnlyStorageBackend",
        ),
        "ftp_readonly": (
            "LiuXin_alpha.storage.store_backend_plugins.ftp_readonly",
            "FtpReadOnlyStorageBackend",
        ),
    }

    def __init__(
        self,
        stores: Optional[Iterable[StoreContainerAPI | StorePluginAPI]] = None,
        *,
        db: "DatabaseAPI | None" = None,
        startup_on_add: bool = True,
    ) -> None:
        super().__init__(db=db)
        self._store_containers: list[StoreContainerAPI] = []
        self._containers_by_uuid: dict[str, StoreContainerAPI] = {}
        self._containers_by_name: dict[str, StoreContainerAPI] = {}
        self._containers_by_url: dict[str, StoreContainerAPI] = {}
        self._store_ids: dict[int, str] = {}
        self._default_store_key: Optional[str] = None
        self._startup_on_add = bool(startup_on_add)

        if stores is not None:
            for store in stores:
                if isinstance(store, StoreContainerAPI):
                    self.register_store_container(store)
                elif isinstance(store, StorePluginAPI):
                    self.register_store_container(StoreContainer.from_plugin(store, db=db))
                else:
                    raise TypeError("stores must contain StoreContainerAPI or StorePluginAPI instances.")

    @classmethod
    def from_database(
        cls,
        db: "DatabaseAPI",
        *,
        startup_on_add: bool = False,
        include_offline: bool = False,
        clear_existing: bool = True,
    ) -> tuple["StorageManager", StorageBootstrapReport]:
        manager = cls(db=db, startup_on_add=startup_on_add)
        report = manager.load_from_database(
            db,
            include_offline=include_offline,
            clear_existing=clear_existing,
        )
        return manager, report

    # ------------------------------------------------------------------
    # Store/container orchestration
    # ------------------------------------------------------------------

    def create_store_plugin(self, store_spec: StoreSpec) -> StorePluginAPI:
        backend_cls = self._resolve_backend_cls(store_spec)
        if backend_cls is None:
            raise ValueError(
                "Unsupported store kind/protocol: {!r} / {!r}".format(
                    store_spec.store_kind,
                    store_spec.store_access_protocol,
                )
            )

        root_uri = store_spec.store_root_uri or store_spec.store_url
        if not root_uri:
            raise ValueError("StoreSpec must provide store_root_uri or store_url.")

        kwargs: dict[str, Any] = {
            "url": str(root_uri),
            "name": store_spec.store_name,
            "uuid": store_spec.store_uuid,
        }

        if backend_cls.__name__ == "OnDiskCalibreLikeStorageBackend":
            kwargs["database"] = self.db
            if store_spec.store_id is not None:
                kwargs["store_id"] = int(store_spec.store_id)
        elif backend_cls.__name__ == "RcloneHttpReadOnlyStorageBackend":
            options = self._build_rclone_options_from_row(store_spec)
            if options is not None:
                kwargs["options"] = options
        elif backend_cls.__name__ == "WgetHtmlReadOnlyStorageBackend":
            options = self._build_wget_options_from_row(store_spec)
            if options is not None:
                kwargs["options"] = options
        elif backend_cls.__name__ == "NativeHtmlReadOnlyStorageBackend":
            options = self._build_native_html_options_from_row(store_spec)
            if options is not None:
                kwargs["options"] = options

        plugin = backend_cls(**kwargs)
        if not isinstance(plugin, StorePluginAPI):
            raise TypeError(
                "Backend class {!r} is not yet migrated to StorePluginAPI.".format(backend_cls.__name__)
            )
        return plugin

    def build_store_container(self, store_spec: StoreSpec) -> StoreContainerAPI:
        plugin = self.create_store_plugin(store_spec)
        store_container = StoreContainer(_plugin=plugin, _spec=store_spec, _db=self.db)
        self.register_store_container(store_container)
        if store_spec.store_id is not None:
            self.bind_store_id(int(store_spec.store_id), store_container.store_name)
        return store_container

    def register_store_container(self, store_container: StoreContainerAPI) -> bool:
        key_name = str(store_container.store_name)
        key_url = str(store_container.store_url)
        key_uuid = str(store_container.store_uuid) if store_container.store_uuid is not None else None

        self._check_duplicate_key("name", key_name, store_container)
        self._check_duplicate_key("url", key_url, store_container)
        if key_uuid is not None:
            self._check_duplicate_key("uuid", key_uuid, store_container)

        self._store_containers.append(store_container)
        self._containers_by_name[key_name] = store_container
        self._containers_by_url[key_url] = store_container
        if key_uuid is not None:
            self._containers_by_uuid[key_uuid] = store_container

        if self._default_store_key is None:
            self._default_store_key = key_name

        if self._startup_on_add:
            store_container.startup()
        return True

    def unregister_store_container(self, store_ref: "StoreRef", *, delete_from_db: bool = False) -> bool:
        try:
            store_container = self.get_store_container(store_ref)
        except KeyError:
            return False

        self._store_containers = [st for st in self._store_containers if st is not store_container]
        self._containers_by_name.pop(str(store_container.store_name), None)
        self._containers_by_url.pop(str(store_container.store_url), None)
        if store_container.store_uuid is not None:
            self._containers_by_uuid.pop(str(store_container.store_uuid), None)

        dead_ids = [store_id for store_id, identifier in self._store_ids.items() if identifier == str(store_ref)]
        for store_id in dead_ids:
            self._store_ids.pop(store_id, None)

        if delete_from_db:
            try:
                store_container.delete_from_db()
            except NotImplementedError:
                pass

        if not self._store_containers:
            self._default_store_key = None
            return True

        if self._default_store_key in (
            str(store_ref),
            str(store_container.store_name),
            str(store_container.store_uuid),
            str(store_container.store_url),
        ):
            self._default_store_key = str(self._store_containers[0].store_name)
        return True

    def get_store_container(self, store_ref: "StoreRef") -> StoreContainerAPI:
        identifier = str(store_ref)
        matches: list[StoreContainerAPI] = []
        seen_ids: set[int] = set()

        for lookup in (
            self._containers_by_uuid.get(identifier),
            self._containers_by_name.get(identifier),
            self._containers_by_url.get(identifier),
        ):
            if lookup is None:
                continue
            obj_id = id(lookup)
            if obj_id in seen_ids:
                continue
            seen_ids.add(obj_id)
            matches.append(lookup)

        if not matches:
            raise KeyError("Unknown store identifier: {!r}".format(store_ref))
        if len(matches) > 1:
            raise KeyError("Ambiguous store identifier {!r}; matches multiple stores.".format(store_ref))
        return matches[0]

    def iter_store_containers(self) -> Iterator[StoreContainerAPI]:
        return iter(tuple(self._store_containers))

    def bind_store_id(self, store_id: int, store_ref: "StoreRef") -> None:
        self._store_ids[int(store_id)] = str(store_ref)

    def set_default_store(self, store_ref: "StoreRef") -> None:
        store_container = self.get_store_container(store_ref)
        self._default_store_key = str(store_container.store_name)

    def get_default_store_container(self) -> StoreContainerAPI:
        if self._default_store_key is None:
            raise RuntimeError("No default store is configured.")
        return self.get_store_container(self._default_store_key)

    def close(self) -> None:
        for store_container in tuple(self._store_containers):
            try:
                store_container.close()
            except Exception:
                continue

    # ------------------------------------------------------------------
    # File/location orchestration
    # ------------------------------------------------------------------

    def store_bytes(
        self,
        file_bytes: bytes,
        metadata: Optional[MetadataContainerAPI] = None,
        *,
        preferred_store: "StoreRef | None" = None,
    ) -> StoreLocationMixinAPI:
        if not self._store_containers:
            raise RuntimeError("No stores are registered with this StorageManager.")

        errors: list[str] = []
        for store_container in self._candidate_store_containers(
            preferred_store=preferred_store,
            metadata=metadata,
            file_url=None,
        ):
            try:
                return store_container.write_bytes(file_bytes=file_bytes, metadata=metadata)
            except (PermissionError, NotImplementedError) as exc:
                errors.append("{}: {}".format(store_container.store_name, exc))
                continue
            except Exception as exc:  # pragma: no cover
                errors.append("{}: {!r}".format(store_container.store_name, exc))
                continue

        raise RuntimeError(
            "No writable store accepted the file. Errors: {}".format("; ".join(errors) if errors else "<none>")
        )

    def locate_file(
        self,
        file_url: Optional[str] = None,
        metadata: Optional[MetadataContainerAPI] = None,
        *,
        preferred_store: "StoreRef | None" = None,
    ) -> StoreLocationMixinAPI:
        resolved_url = file_url or self._metadata_file_url(metadata)
        if resolved_url is None:
            raise ValueError("locate_file requires file_url or metadata containing one.")

        checked_any = False
        for store_container in self._candidate_store_containers(
            preferred_store=preferred_store,
            metadata=metadata,
            file_url=resolved_url,
        ):
            checked_any = True
            try:
                if not store_container.exists(resolved_url):
                    continue
                return store_container.locate(resolved_url)
            except Exception:
                continue

        if not checked_any:
            raise RuntimeError("No stores are registered with this StorageManager.")
        raise FileNotFoundError("File could not be resolved from any store: {!r}".format(resolved_url))

    def locate_folder(
        self,
        folder_key: str,
        *,
        preferred_store: "StoreRef | None" = None,
    ) -> StoreLocationMixinAPI:
        if not self._store_containers:
            raise RuntimeError("No stores are registered with this StorageManager.")

        for store_container in self._candidate_store_containers(preferred_store=preferred_store, metadata=None, file_url=None):
            try:
                return store_container.location(folder_key)
            except NotImplementedError:
                continue
            except Exception:
                continue

        raise NotImplementedError("No registered store can provide folder/location handles.")

    def delete_location(
        self,
        file_url: Optional[str] = None,
        metadata: Optional[MetadataContainerAPI] = None,
        location: Optional[StoreLocationMixinAPI] = None,
    ) -> bool:
        resolved_url = file_url
        preferred_store: "StoreRef | None" = None

        if location is not None:
            resolved_url = resolved_url or location.file_url
            store_name = getattr(getattr(location, "store", None), "name", None)
            if store_name is not None:
                preferred_store = str(store_name)

        if resolved_url is None:
            resolved_url = self._metadata_file_url(metadata)
        if resolved_url is None:
            raise ValueError("delete_location requires file_url, metadata, or location.")

        for store_container in self._candidate_store_containers(
            preferred_store=preferred_store,
            metadata=metadata,
            file_url=resolved_url,
        ):
            try:
                if not store_container.exists(resolved_url):
                    continue
                return bool(store_container.delete(resolved_url))
            except Exception:
                continue

        return False

    def iter_locations(self) -> Iterator[StoreLocationMixinAPI]:
        for store_container in self._store_containers:
            try:
                yield from store_container.iter_locations()
            except Exception:
                continue

    # ------------------------------------------------------------------
    # Database bootstrap / specs
    # ------------------------------------------------------------------

    def get_store_spec_from_db(self, store_id: int) -> StoreSpec:
        if self.db is None:
            raise RuntimeError("StorageManager is not bound to a database.")
        if "stores" not in set(self.db.get_tables()):
            raise KeyError("Database does not expose a 'stores' table.")

        row = self.db.get_row_from_id("stores", int(store_id))
        if row is None:
            raise KeyError("Unknown store id: {!r}".format(store_id))

        return store_spec_from_row(row, fallback_store_id=int(store_id))

    def load_from_database(
        self,
        db: "DatabaseAPI",
        *,
        include_offline: bool = False,
        clear_existing: bool = True,
    ) -> StorageBootstrapReport:
        report = StorageBootstrapReport()

        tables = set(db.get_tables())
        if "stores" not in tables:
            return report

        if clear_existing:
            self._clear_registry()

        rows = db.get_all_rows("stores", iterator_return=False)
        if rows is None:
            return report

        for row in rows:
            report.discovered_rows += 1
            store_id = self._row_store_id(row)
            store_name = self._coerce_optional_str(self._row_get(row, "store_name"))

            online_status = self._coerce_optional_str(self._row_get(row, "store_online_status"))
            if (not include_offline) and online_status is not None and online_status.lower() == "offline":
                report.skipped_rows += 1
                report.issues.append(StorageBootstrapIssue(store_id=store_id, store_name=store_name, reason="store marked offline"))
                continue

            try:
                store_spec = self.get_store_spec_from_db(int(store_id)) if store_id is not None else None
            except Exception as exc:
                report.failed_rows += 1
                report.issues.append(StorageBootstrapIssue(store_id=store_id, store_name=store_name, reason="failed to load store spec: {!r}".format(exc)))
                continue

            if store_spec is None or not (store_spec.store_root_uri or store_spec.store_url):
                report.skipped_rows += 1
                report.issues.append(StorageBootstrapIssue(store_id=store_id, store_name=store_name, reason="store_root_uri is missing"))
                continue

            try:
                self.build_store_container(store_spec)
                report.loaded_stores += 1
            except Exception as exc:
                report.failed_rows += 1
                report.issues.append(StorageBootstrapIssue(store_id=store_id, store_name=store_name, reason="failed to load store: {!r}".format(exc)))

        return report

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _clear_registry(self) -> None:
        self._store_containers.clear()
        self._containers_by_uuid.clear()
        self._containers_by_name.clear()
        self._containers_by_url.clear()
        self._store_ids.clear()
        self._default_store_key = None

    @staticmethod
    def _row_get(row: Any, column: str) -> Any:
        try:
            return row[column]
        except Exception:
            pass
        if isinstance(row, Mapping):
            return row.get(column)
        return getattr(row, column, None)

    def _row_store_id(self, row: Any) -> Optional[int]:
        candidate = getattr(row, "row_id", None)
        if candidate is None:
            candidate = self._row_get(row, "store_id")
        return self._to_int(candidate)

    @staticmethod
    def _coerce_optional_str(value: Any) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _coerce_optional_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_int(value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_boolish(value: Any, default: bool = False) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        text = str(value).strip().lower()
        if text in {"1", "true", "yes", "y", "on"}:
            return True
        if text in {"0", "false", "no", "n", "off"}:
            return False
        return default

    def _build_rclone_options_from_row(self, row: Any):
        raw_policy = self._row_get(row, "store_policy_json")
        if raw_policy in (None, ""):
            return None
        try:
            policy = json.loads(str(raw_policy))
        except Exception:
            return None
        if not isinstance(policy, Mapping):
            return None

        rclone_policy: Mapping[str, Any]
        nested = policy.get("rclone")
        if isinstance(nested, Mapping):
            rclone_policy = nested
        else:
            rclone_policy = policy

        option_kwargs: dict[str, Any] = {}
        if "rclone_exe" in rclone_policy:
            exe = self._coerce_optional_str(rclone_policy.get("rclone_exe"))
            if exe:
                option_kwargs["rclone_exe"] = exe
        if "rclone_args" in rclone_policy:
            args = rclone_policy.get("rclone_args")
            if isinstance(args, (list, tuple)):
                option_kwargs["rclone_args"] = tuple(str(x) for x in args)
        if "timeout_s" in rclone_policy:
            option_kwargs["timeout_s"] = self._coerce_optional_float(rclone_policy.get("timeout_s"))
        if "max_http_requests_per_hour" in rclone_policy:
            option_kwargs["max_http_requests_per_hour"] = self._coerce_optional_float(rclone_policy.get("max_http_requests_per_hour"))
        if "apply_rclone_tpslimit" in rclone_policy:
            option_kwargs["apply_rclone_tpslimit"] = self._to_boolish(rclone_policy.get("apply_rclone_tpslimit"), default=True)
        if "rclone_tpslimit_burst" in rclone_policy:
            burst = self._to_int(rclone_policy.get("rclone_tpslimit_burst"))
            if burst is not None:
                option_kwargs["rclone_tpslimit_burst"] = max(1, burst)
        if "enforce_global_rate_limit" in rclone_policy:
            option_kwargs["enforce_global_rate_limit"] = self._to_boolish(rclone_policy.get("enforce_global_rate_limit"), default=True)
        if not option_kwargs:
            return None
        from LiuXin_alpha.storage.store_backend_plugins.rclone_http_readonly import RcloneBackendOptions
        return RcloneBackendOptions(**option_kwargs)

    def _build_wget_options_from_row(self, row: Any):
        raw_policy = self._row_get(row, "store_policy_json")
        if raw_policy in (None, ""):
            return None
        try:
            policy = json.loads(str(raw_policy))
        except Exception:
            return None
        if not isinstance(policy, Mapping):
            return None
        wget_policy: Mapping[str, Any] = policy.get("wget") if isinstance(policy.get("wget"), Mapping) else policy
        option_kwargs: dict[str, Any] = {}
        if "wget_exe" in wget_policy:
            exe = self._coerce_optional_str(wget_policy.get("wget_exe"))
            if exe:
                option_kwargs["wget_exe"] = exe
        if "wget_args" in wget_policy:
            args = wget_policy.get("wget_args")
            if isinstance(args, (list, tuple)):
                option_kwargs["wget_args"] = tuple(str(x) for x in args)
        if "timeout_s" in wget_policy:
            option_kwargs["timeout_s"] = self._coerce_optional_float(wget_policy.get("timeout_s"))
        if "max_http_requests_per_hour" in wget_policy:
            option_kwargs["max_http_requests_per_hour"] = self._coerce_optional_float(wget_policy.get("max_http_requests_per_hour"))
        if "recurse" in wget_policy:
            option_kwargs["recurse"] = self._to_boolish(wget_policy.get("recurse"), default=True)
        if "max_depth" in wget_policy:
            option_kwargs["max_depth"] = self._to_int(wget_policy.get("max_depth"))
        if "no_parent" in wget_policy:
            option_kwargs["no_parent"] = self._to_boolish(wget_policy.get("no_parent"), default=True)
        if "span_hosts" in wget_policy:
            option_kwargs["span_hosts"] = self._to_boolish(wget_policy.get("span_hosts"), default=False)
        if "respect_robots" in wget_policy:
            option_kwargs["respect_robots"] = self._to_boolish(wget_policy.get("respect_robots"), default=True)
        if "user_agent" in wget_policy:
            option_kwargs["user_agent"] = self._coerce_optional_str(wget_policy.get("user_agent"))
        if "max_html_bytes" in wget_policy:
            max_html_bytes = self._to_int(wget_policy.get("max_html_bytes"))
            if max_html_bytes is not None:
                option_kwargs["max_html_bytes"] = max(1024, max_html_bytes)
        if not option_kwargs:
            return None
        from LiuXin_alpha.storage.store_backend_plugins.wget_html_readonly import WgetBackendOptions
        return WgetBackendOptions(**option_kwargs)

    def _build_native_html_options_from_row(self, row: Any):
        raw_policy = self._row_get(row, "store_policy_json")
        if raw_policy in (None, ""):
            return None
        try:
            policy = json.loads(str(raw_policy))
        except Exception:
            return None
        if not isinstance(policy, Mapping):
            return None
        native_policy: Mapping[str, Any] = policy.get("native_html") if isinstance(policy.get("native_html"), Mapping) else policy
        option_kwargs: dict[str, Any] = {}
        if "timeout_s" in native_policy:
            option_kwargs["timeout_s"] = self._coerce_optional_float(native_policy.get("timeout_s"))
        if "max_http_requests_per_hour" in native_policy:
            option_kwargs["max_http_requests_per_hour"] = self._coerce_optional_float(native_policy.get("max_http_requests_per_hour"))
        if "recurse" in native_policy:
            option_kwargs["recurse"] = self._to_boolish(native_policy.get("recurse"), default=True)
        if "max_depth" in native_policy:
            option_kwargs["max_depth"] = self._to_int(native_policy.get("max_depth"))
        if "no_parent" in native_policy:
            option_kwargs["no_parent"] = self._to_boolish(native_policy.get("no_parent"), default=True)
        if "span_hosts" in native_policy:
            option_kwargs["span_hosts"] = self._to_boolish(native_policy.get("span_hosts"), default=False)
        if "respect_robots" in native_policy:
            option_kwargs["respect_robots"] = self._to_boolish(native_policy.get("respect_robots"), default=True)
        if "user_agent" in native_policy:
            option_kwargs["user_agent"] = self._coerce_optional_str(native_policy.get("user_agent"))
        if "max_html_bytes" in native_policy:
            max_html_bytes = self._to_int(native_policy.get("max_html_bytes"))
            if max_html_bytes is not None:
                option_kwargs["max_html_bytes"] = max(1024, max_html_bytes)
        if not option_kwargs:
            return None
        from LiuXin_alpha.storage.store_backend_plugins.native_html_readonly import NativeHtmlBackendOptions
        return NativeHtmlBackendOptions(**option_kwargs)

    def _resolve_backend_cls(self, row: Any) -> Optional[type[Any]]:
        kind_raw = self._coerce_optional_str(self._row_get(row, "store_kind"))
        protocol_raw = self._coerce_optional_str(self._row_get(row, "store_access_protocol"))
        is_read_only = self._to_boolish(self._row_get(row, "store_is_read_only"), default=False)

        normalized_kind = kind_raw.lower() if kind_raw else None
        normalized_protocol = protocol_raw.lower() if protocol_raw else None

        alias = self._STORE_KIND_ALIASES.get(normalized_kind) if normalized_kind is not None else None
        if alias is None:
            if normalized_protocol in {"http", "https", "rclone"}:
                alias = "rclone_http_readonly"
            elif normalized_protocol == "wget":
                alias = "wget_html_readonly"
            elif normalized_protocol in {"native", "native_html"}:
                alias = "native_html_readonly"
            elif normalized_protocol == "squashfs":
                alias = "squashfs_readonly"
            elif normalized_protocol in {"sqlite", "sqlite3"}:
                alias = "single_file_sqlite"
            elif normalized_protocol in {"file", "nfs", "smb"}:
                alias = "on_disk_existing_unmanaged_drive" if is_read_only else "on_disk_existing_managed_drive"
            elif normalized_protocol in {"ftp", "ftps"}:
                alias = "ftp_readonly"
        if alias is None:
            return None
        spec = self._STORE_BACKEND_IMPORTS.get(alias)
        if spec is None:
            return None
        module_name, class_name = spec
        module = importlib.import_module(module_name)
        return getattr(module, class_name, None)

    def _check_duplicate_key(self, key: str, value: str, new_store_container: StoreContainerAPI) -> None:
        if key == "name":
            existing = self._containers_by_name.get(value)
        elif key == "url":
            existing = self._containers_by_url.get(value)
        elif key == "uuid":
            existing = self._containers_by_uuid.get(value)
        else:  # pragma: no cover
            raise ValueError("Unsupported key type: {!r}".format(key))
        if existing is not None and existing is not new_store_container:
            raise ValueError("Duplicate store {}: {!r}".format(key, value))

    def _candidate_store_containers(
        self,
        *,
        preferred_store: "StoreRef | None",
        metadata: Optional[MetadataContainerAPI],
        file_url: Optional[str],
    ) -> list[StoreContainerAPI]:
        store_containers: list[StoreContainerAPI] = []
        seen: set[int] = set()

        def _append(candidate: StoreContainerAPI) -> None:
            marker = id(candidate)
            if marker in seen:
                return
            seen.add(marker)
            store_containers.append(candidate)

        if preferred_store is not None:
            _append(self.get_store_container(preferred_store))

        for identifier in self._metadata_store_identifiers(metadata):
            try:
                _append(self.get_store_container(identifier))
            except KeyError:
                continue

        if file_url:
            for store_container in self._store_containers:
                if self._file_url_belongs_to_store(file_url=file_url, store_container=store_container):
                    _append(store_container)

        if preferred_store is None and self._default_store_key:
            try:
                _append(self.get_store_container(self._default_store_key))
            except KeyError:
                pass

        for store_container in self._store_containers:
            _append(store_container)
        return store_containers

    def _metadata_sources(self, metadata: Optional[MetadataContainerAPI]) -> list[Any]:
        if metadata is None:
            return []
        sources: list[Any] = [metadata]
        hints_fn = getattr(metadata, "storage_hints", None)
        if callable(hints_fn):
            try:
                hints = hints_fn()
            except Exception:
                hints = None
            if hints is not None:
                sources.append(hints)
                extra = self._get_value(hints, "extra")
                if isinstance(extra, Mapping):
                    sources.append(extra)
        if isinstance(metadata, Mapping):
            extra = metadata.get("extra")
            if isinstance(extra, Mapping):
                sources.append(extra)
        return sources

    def _get_metadata_value(self, metadata: Optional[MetadataContainerAPI], *keys: str) -> Any:
        for source in self._metadata_sources(metadata):
            for key in keys:
                value = self._get_value(source, key)
                if value is not None:
                    return value
        return None

    @staticmethod
    def _get_value(source: Any, key: str) -> Any:
        if isinstance(source, Mapping):
            return source.get(key)
        return getattr(source, key, None)

    def _metadata_file_url(self, metadata: Optional[MetadataContainerAPI]) -> Optional[str]:
        value = self._get_metadata_value(metadata, "file_url", "url", "file_path", "path", "file_storage_key", "storage_key")
        if value is None:
            file_row = self._get_metadata_value(metadata, "file_row")
            if file_row is not None:
                value = self._get_value(file_row, "file_url")
                if value is None:
                    value = self._get_value(file_row, "file_path")
                if value is None:
                    value = self._get_value(file_row, "file_storage_key")
        return None if value is None else str(value)

    def _metadata_store_identifiers(self, metadata: Optional[MetadataContainerAPI]) -> Iterator[str]:
        for key in (
            "preferred_store",
            "store",
            "store_name",
            "store_uuid",
            "store_url",
            "file_store",
            "file_store_name",
            "file_store_uuid",
            "file_store_url",
        ):
            value = self._get_metadata_value(metadata, key)
            if value is None:
                continue
            if isinstance(value, (list, tuple, set)):
                for one in value:
                    yield str(one)
                continue
            yield str(value)
        numeric_id = self._get_metadata_value(metadata, "file_store_id", "store_id")
        if numeric_id is None:
            return
        try:
            yield self._store_ids[int(numeric_id)]
        except Exception:
            return

    @staticmethod
    def _file_url_belongs_to_store(file_url: str, store_container: StoreContainerAPI) -> bool:
        store_url = str(store_container.store_url)
        if file_url == store_url:
            return True
        if file_url.startswith(store_url.rstrip("/") + "/"):
            return True
        try:
            f_path = pathlib.Path(file_url).expanduser().resolve(strict=False)
            s_path = pathlib.Path(store_url).expanduser().resolve(strict=False)
            return f_path.is_relative_to(s_path)
        except Exception:
            return False


StoreManager = StorageManager

__all__ = [
    "StorageBootstrapIssue",
    "StorageBootstrapReport",
    "StorageManager",
    "StoreManager",
]

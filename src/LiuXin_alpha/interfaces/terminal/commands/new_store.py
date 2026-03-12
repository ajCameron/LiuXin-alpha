"""Interactive wizard command for adding new storage backends."""

from __future__ import annotations

import time

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from LiuXin_alpha.interfaces.terminal.commands.base import TerminalCommandAPI
from LiuXin_alpha.library.library import Library
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name


@dataclass(frozen=True)
class _StoreKindPreset:
    kind: str
    label: str
    access_protocol: str
    read_only_default: bool
    location_type: str  # dir | file | remote
    supports_folders: bool
    supports_hierarchical_list: bool
    supports_random_read: bool
    supports_random_write: bool
    supports_delete: bool
    supports_checksums: bool
    supports_immutable_objects: bool


_STORE_KIND_PRESETS: tuple[_StoreKindPreset, ...] = (
    _StoreKindPreset(
        kind="on_disk_existing_managed_drive",
        label="Managed local folder (read/write)",
        access_protocol="file",
        read_only_default=False,
        location_type="dir",
        supports_folders=True,
        supports_hierarchical_list=True,
        supports_random_read=True,
        supports_random_write=True,
        supports_delete=True,
        supports_checksums=True,
        supports_immutable_objects=False,
    ),
    _StoreKindPreset(
        kind="on_disk_existing_unmanaged_drive",
        label="Existing unmanaged local folder (read-only)",
        access_protocol="file",
        read_only_default=True,
        location_type="dir",
        supports_folders=True,
        supports_hierarchical_list=True,
        supports_random_read=True,
        supports_random_write=False,
        supports_delete=False,
        supports_checksums=True,
        supports_immutable_objects=False,
    ),
    _StoreKindPreset(
        kind="on_disk_calibre_like",
        label="Managed calibre-like local folder (read/write)",
        access_protocol="file",
        read_only_default=False,
        location_type="dir",
        supports_folders=True,
        supports_hierarchical_list=True,
        supports_random_read=True,
        supports_random_write=True,
        supports_delete=True,
        supports_checksums=True,
        supports_immutable_objects=False,
    ),
    _StoreKindPreset(
        kind="single_file_sqlite",
        label="Single-file SQLite blob store",
        access_protocol="sqlite",
        read_only_default=False,
        location_type="file",
        supports_folders=False,
        supports_hierarchical_list=False,
        supports_random_read=True,
        supports_random_write=True,
        supports_delete=True,
        supports_checksums=True,
        supports_immutable_objects=False,
    ),
    _StoreKindPreset(
        kind="squashfs_readonly",
        label="Read-only SquashFS archive",
        access_protocol="squashfs",
        read_only_default=True,
        location_type="file",
        supports_folders=True,
        supports_hierarchical_list=True,
        supports_random_read=True,
        supports_random_write=False,
        supports_delete=False,
        supports_checksums=True,
        supports_immutable_objects=True,
    ),
    _StoreKindPreset(
        kind="rclone_http_readonly",
        label="Rclone HTTP remote (read-only)",
        access_protocol="rclone",
        read_only_default=True,
        location_type="remote",
        supports_folders=True,
        supports_hierarchical_list=True,
        supports_random_read=True,
        supports_random_write=False,
        supports_delete=False,
        supports_checksums=True,
        supports_immutable_objects=False,
    ),
    _StoreKindPreset(
        kind="wget_html_readonly",
        label="Wget HTML spider remote (read-only)",
        access_protocol="wget",
        read_only_default=True,
        location_type="remote",
        supports_folders=True,
        supports_hierarchical_list=True,
        supports_random_read=True,
        supports_random_write=False,
        supports_delete=False,
        supports_checksums=False,
        supports_immutable_objects=False,
    ),
)


class NewStoreWizardCommand(TerminalCommandAPI):
    """Create/update a store row interactively from inside the terminal browser."""

    group = "add"
    name = "store"
    aliases = ("new-store", "new_store", "add-store", "add_store")
    summary = "Interactive wizard to create or update a store row."
    usage = "add store"

    def execute(self, browser, args: list[str]) -> bool:
        if args:
            raise ValueError("Usage: {}".format(self.usage))

        if "stores" not in set(browser.db.get_tables()):
            raise ValueError("Database schema does not contain `stores` table.")

        browser.emit("New store wizard")
        browser.emit("----------------")

        preset = self._prompt_store_kind(browser)
        root_uri = self._prompt_root_uri(browser, preset)

        default_name = safe_path_to_name(root_uri) or preset.kind
        store_name = browser.prompt_text("Store name", default=default_name).strip() or default_name

        read_only = browser.prompt_yes_no(
            "Read-only store?",
            default=bool(preset.read_only_default),
        )
        online = browser.prompt_yes_no("Mark store online?", default=True)

        row = self._create_or_update_store_row(
            browser,
            preset=preset,
            root_uri=root_uri,
            store_name=store_name,
            read_only=bool(read_only),
            online=bool(online),
        )

        store_id = self._store_row_id(row)
        browser.emit(
            "Store saved: id={} name={!r} kind={} root_uri={}".format(
                store_id,
                row["store_name"],
                row["store_kind"],
                row["store_root_uri"],
            )
        )

        refresh = browser.prompt_yes_no(
            "Refresh storage manager now?",
            default=True,
        )
        if refresh:
            report = self._refresh_storage_manager(browser)
            browser.emit(
                "Storage bootstrap: discovered={} loaded={} skipped={} failed={}".format(
                    self._bootstrap_report_field(report, "discovered_rows", 0),
                    self._bootstrap_report_field(report, "loaded_stores", 0),
                    self._bootstrap_report_field(report, "skipped_rows", 0),
                    self._bootstrap_report_field(report, "failed_rows", 0),
                )
            )

        return True

    @staticmethod
    def _bootstrap_report_field(report: object, key: str, default=0):
        if isinstance(report, dict):
            return report.get(key, default)
        return getattr(report, key, default)

    @staticmethod
    def _refresh_storage_manager(browser):
        if hasattr(browser, "supports_core_commands") and bool(browser.supports_core_commands()):
            return browser.execute_core_command(
                "invoke",
                payload={
                    "target": "library",
                    "method": "refresh_storage",
                    "kwargs": {"clear_existing": True},
                },
            )
        return NewStoreWizardCommand._local_library(browser).refresh_storage(clear_existing=True)

    def _prompt_store_kind(self, browser) -> _StoreKindPreset:
        browser.emit("Available store kinds:")
        for idx, preset in enumerate(_STORE_KIND_PRESETS, start=1):
            browser.emit("  {}. {} ({})".format(idx, preset.label, preset.kind))

        default_idx = 1
        selection = browser.prompt_text(
            "Store kind (number or id)",
            default=str(default_idx),
        ).strip()
        chosen = self._resolve_store_kind_selection(selection)
        if chosen is None:
            raise ValueError("Unknown store kind selection: {!r}".format(selection))
        return chosen

    def _resolve_store_kind_selection(self, raw: str) -> Optional[_StoreKindPreset]:
        text = str(raw).strip()
        if not text:
            return None
        try:
            idx = int(text)
            if 1 <= idx <= len(_STORE_KIND_PRESETS):
                return _STORE_KIND_PRESETS[idx - 1]
        except Exception:
            pass
        lowered = text.lower()
        for preset in _STORE_KIND_PRESETS:
            if preset.kind == lowered:
                return preset
        return None

    def _prompt_root_uri(self, browser, preset: _StoreKindPreset) -> str:
        prompt = "Store root URI/path"
        if preset.location_type == "remote":
            raw = browser.prompt_text(prompt, default="").strip()
            if not raw:
                raise ValueError("Store root URI cannot be empty.")
            return raw

        raw = browser.prompt_text(prompt, default="").strip()
        if not raw:
            raise ValueError("Store root path cannot be empty.")

        path = Path(raw).expanduser()
        if preset.location_type == "dir":
            if path.exists() and not path.is_dir():
                raise ValueError("Path exists but is not a directory: {!r}".format(str(path)))
            if not path.exists():
                create_default = preset.kind != "on_disk_existing_unmanaged_drive"
                create_it = browser.prompt_yes_no(
                    "Directory does not exist. Create it?",
                    default=create_default,
                )
                if not create_it:
                    raise ValueError("Directory does not exist: {!r}".format(str(path)))
                path.mkdir(parents=True, exist_ok=True)
            return str(path.resolve())

        if preset.location_type == "file":
            parent = path.parent
            if not parent.exists():
                create_parent = browser.prompt_yes_no(
                    "Parent directory does not exist. Create it?",
                    default=True,
                )
                if not create_parent:
                    raise ValueError("Parent directory does not exist: {!r}".format(str(parent)))
                parent.mkdir(parents=True, exist_ok=True)

            if preset.kind == "squashfs_readonly":
                if not path.exists() or not path.is_file():
                    raise ValueError(
                        "SquashFS store requires an existing archive file: {!r}".format(str(path))
                    )
            return str(path.resolve())

        raise ValueError("Unsupported store location type: {!r}".format(preset.location_type))

    def _create_or_update_store_row(
        self,
        browser,
        *,
        preset: _StoreKindPreset,
        root_uri: str,
        store_name: str,
        read_only: bool,
        online: bool,
    ):
        now_epk = int(time.time() * 1000)
        updates = self._build_store_payload(
            preset=preset,
            root_uri=root_uri,
            store_name=store_name,
            read_only=read_only,
            online=online,
            now_epk=now_epk,
        )

        existing = self._find_existing_store(browser, root_uri=root_uri, store_name=store_name)
        if existing is not None:
            browser.emit(
                "Existing store found: id={} name={!r} kind={} root_uri={}".format(
                    self._store_row_id(existing),
                    existing["store_name"],
                    existing["store_kind"],
                    existing["store_root_uri"],
                )
            )
            update_existing = browser.prompt_yes_no("Update existing row?", default=True)
            if not update_existing:
                raise ValueError("Store wizard canceled: existing row not updated.")

        return self._save_store_row(browser, store_payload=updates)

    def _build_store_payload(
        self,
        *,
        preset: _StoreKindPreset,
        root_uri: str,
        store_name: str,
        read_only: bool,
        online: bool,
        now_epk: int,
    ) -> dict[str, Any]:
        supports_random_write = bool(preset.supports_random_write and not read_only)
        supports_delete = bool(preset.supports_delete and not read_only)

        payload: dict[str, Any] = {
            "store_name": store_name,
            "store_kind": preset.kind,
            "store_access_protocol": preset.access_protocol,
            "store_root_uri": root_uri,
            "store_is_read_only": int(bool(read_only)),
            "store_online_status": "online" if online else "offline",
            "store_supports_folders": int(bool(preset.supports_folders)),
            "store_supports_hierarchical_list": int(bool(preset.supports_hierarchical_list)),
            "store_supports_random_read": int(bool(preset.supports_random_read)),
            "store_supports_random_write": int(supports_random_write),
            "store_supports_delete": int(supports_delete),
            "store_supports_checksums": int(bool(preset.supports_checksums)),
            "store_supports_immutable_objects": int(bool(preset.supports_immutable_objects)),
            "store_modified_timestamp_ep_k": int(now_epk),
            "store_created_timestamp_ep_k": int(now_epk),
        }
        return payload

    @staticmethod
    def _local_library(browser) -> Library:
        return Library(database=browser.db, close_database_on_close=False)

    def _find_existing_store(self, browser, *, root_uri: str, store_name: str):
        if hasattr(browser, "supports_core_queries") and bool(browser.supports_core_queries()):
            return browser.execute_core_query(
                "invoke",
                payload={
                    "target": "library",
                    "method": "find_existing_store",
                    "kwargs": {
                        "root_uri": root_uri,
                        "store_name": store_name,
                    },
                },
            )
        return self._local_library(browser).find_existing_store(root_uri=root_uri, store_name=store_name)

    def _save_store_row(self, browser, *, store_payload: dict[str, Any]):
        if hasattr(browser, "supports_core_commands") and bool(browser.supports_core_commands()):
            return browser.execute_core_command(
                "invoke",
                payload={
                    "target": "library",
                    "method": "save_store_row",
                    "kwargs": {"store_payload": dict(store_payload)},
                },
            )
        return self._local_library(browser).save_store_row(store_payload=store_payload)

    @staticmethod
    def _store_row_id(store_row) -> Optional[int]:
        for key in ("store_id",):
            try:
                value = store_row[key]
                if value is None:
                    continue
                return int(value)
            except Exception:
                continue
        try:
            if store_row.row_id is not None:
                return int(store_row.row_id)
        except Exception:
            pass
        return None

"""Concrete managed wrapper for one configured store plugin.

A `StoreContainer` is deliberately boring: one configured store spec, one raw
plugin instance, optional database binding, and cached health/probe state. It
should not absorb orchestration or raw-media logic.
"""

from __future__ import annotations

import dataclasses

from typing import TYPE_CHECKING, Optional

from LiuXin_alpha.databases import Row
from LiuXin_alpha.storage.api.info_containers_api import StoreSpec, StoreStatus
from LiuXin_alpha.storage.api.store_container_api import StoreContainerAPI
from LiuXin_alpha.storage.api.store_plugin_api import StorePluginAPI
from LiuXin_alpha.storage.store_spec_utils import store_spec_from_row, store_spec_to_row_dict

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database import DatabaseAPI


@dataclasses.dataclass(slots=True)
class StoreContainer(StoreContainerAPI):
    _plugin: StorePluginAPI
    _spec: StoreSpec
    _db: "DatabaseAPI | None" = None
    _status_cache: Optional[StoreStatus] = None

    @property
    def plugin(self) -> StorePluginAPI:
        return self._plugin

    @property
    def spec(self) -> StoreSpec:
        return self._spec

    @property
    def db(self) -> "DatabaseAPI | None":
        return self._db

    def startup(self) -> StoreStatus:
        self._status_cache = self.plugin.startup()
        return self._status_cache

    def probe(self) -> StoreStatus:
        self._status_cache = self.plugin.self_test()
        return self._status_cache

    def status(self, *, refresh: bool = False) -> StoreStatus:
        if refresh or self._status_cache is None:
            self._status_cache = self.plugin.status()
        return self._status_cache

    def reload_spec_from_db(self) -> StoreSpec:
        if self.db is None or self.spec.store_id is None:
            raise RuntimeError("StoreContainer is not bound to a database-backed store row.")
        row = self.db.get_row_from_id("stores", int(self.spec.store_id))
        if row is None:
            raise KeyError("Unknown store id: {!r}".format(self.spec.store_id))
        self._spec = store_spec_from_row(row, fallback_store_id=int(self.spec.store_id))
        return self._spec

    def save_spec_to_db(self) -> StoreSpec:
        if self.db is None:
            raise RuntimeError("StoreContainer is not bound to a database.")
        allowed_columns = set(self.db.get_column_headings("stores"))
        row_dict = store_spec_to_row_dict(self.spec, allowed_columns=allowed_columns)
        if not row_dict:
            raise ValueError("StoreSpec did not yield any writable `stores` columns.")

        if self.spec.store_id is None:
            row = Row.from_idless_row_dict(self.db, row_dict=row_dict, table="stores")
            self._spec = store_spec_from_row(row)
            return self._spec

        row = self.db.get_row_from_id("stores", int(self.spec.store_id))
        if row is None:
            raise KeyError("Unknown store id: {!r}".format(self.spec.store_id))
        for key, value in row_dict.items():
            if key not in row.allowed_columns:
                continue
            if row[key] != value:
                row[key] = value
        row.sync()
        self._spec = store_spec_from_row(row, fallback_store_id=int(self.spec.store_id))
        return self._spec

    def delete_from_db(self) -> bool:
        if self.db is None or self.spec.store_id is None:
            raise RuntimeError("StoreContainer is not bound to a database-backed store row.")
        row = self.db.get_row_from_id("stores", int(self.spec.store_id))
        if row is None:
            return False
        self.db.delete(row)
        return True

    @classmethod
    def from_plugin(
        cls,
        plugin: StorePluginAPI,
        *,
        db: "DatabaseAPI | None" = None,
        store_id: int | None = None,
    ) -> "StoreContainer":
        plugin_kind = plugin.plugin_kind
        role: str | None = None
        if plugin_kind == "OnDiskFlatStorageBackend":
            role = "cache"
        elif plugin_kind == "SquashfsBuildStorageBackend":
            role = "backup"
        elif plugin_kind == "SquashfsReadOnlyStorageBackend":
            role = "archive"
        elif plugin_kind in {
            "OnDiskExistingManagedStorageBackend",
            "OnDiskCalibreLikeStorageBackend",
            "OnDiskUnmanagedStorageBackend",
        }:
            role = "live"

        spec = StoreSpec(
            store_id=store_id,
            store_uuid=plugin.uuid,
            store_name=plugin.name,
            store_kind=plugin.plugin_kind,
            store_url=plugin.url,
            store_root_uri=plugin.url,
            store_operational_role=role,
        )
        return cls(_plugin=plugin, _spec=spec, _db=db)

"""Concrete managed wrapper for one configured store plugin."""

from __future__ import annotations

import dataclasses

from typing import TYPE_CHECKING, Optional

from LiuXin_alpha.storage.api.info_containers_api import StoreSpec, StoreStatus
from LiuXin_alpha.storage.api.store_container_api import StoreContainerAPI
from LiuXin_alpha.storage.api.store_plugin_api import StorePluginAPI

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
        raise NotImplementedError("StoreContainer.reload_spec_from_db() needs store-row loader semantics.")

    def save_spec_to_db(self) -> StoreSpec:
        if self.db is None:
            raise RuntimeError("StoreContainer is not bound to a database.")
        raise NotImplementedError("StoreContainer.save_spec_to_db() needs store-row write semantics.")

    def delete_from_db(self) -> bool:
        if self.db is None or self.spec.store_id is None:
            raise RuntimeError("StoreContainer is not bound to a database-backed store row.")
        raise NotImplementedError("StoreContainer.delete_from_db() needs store-row delete semantics.")

    @classmethod
    def from_plugin(
        cls,
        plugin: StorePluginAPI,
        *,
        db: "DatabaseAPI | None" = None,
        store_id: int | None = None,
    ) -> "StoreContainer":
        spec = StoreSpec(
            store_id=store_id,
            store_uuid=plugin.uuid,
            store_name=plugin.name,
            store_kind=plugin.plugin_kind,
            store_url=plugin.url,
            store_root_uri=plugin.url,
        )
        return cls(_plugin=plugin, _spec=spec, _db=db)

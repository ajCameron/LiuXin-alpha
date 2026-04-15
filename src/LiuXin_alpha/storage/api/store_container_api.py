"""Managed store container API.

A store container represents exactly one configured store. It owns one raw
plugin plus the DB/spec/status state around that plugin, but it should not grow
into an orchestrator or into a second backend API.
"""

from __future__ import annotations

import abc
from collections.abc import Iterator
from typing import TYPE_CHECKING, Optional

from LiuXin_alpha.storage.api.location_api import StoreLocationMixinAPI
from LiuXin_alpha.storage.single_file import SingleFileStatus

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database import DatabaseAPI
    from LiuXin_alpha.storage.api.info_containers_api import StoreSpec, StoreStatus
    from LiuXin_alpha.storage.api.store_plugin_api import StorePluginAPI


class StoreContainerAPI(abc.ABC):
    """Managed wrapper around one and only one configured plugin."""

    @property
    @abc.abstractmethod
    def plugin(self) -> "StorePluginAPI":
        ...

    @property
    @abc.abstractmethod
    def spec(self) -> "StoreSpec":
        ...

    @property
    @abc.abstractmethod
    def db(self) -> "DatabaseAPI | None":
        ...

    @property
    def store_id(self) -> Optional[int]:
        return self.spec.store_id

    @property
    def store_uuid(self) -> Optional[str]:
        return self.spec.store_uuid

    @property
    def store_name(self) -> str:
        return self.spec.store_name

    @property
    def store_url(self) -> str:
        return self.spec.store_url

    @abc.abstractmethod
    def startup(self) -> "StoreStatus":
        ...

    @abc.abstractmethod
    def probe(self) -> "StoreStatus":
        ...

    @abc.abstractmethod
    def status(self, *, refresh: bool = False) -> "StoreStatus":
        ...

    def close(self) -> None:
        self.plugin.close()

    def location(self, *tokens: str) -> StoreLocationMixinAPI:
        return self.plugin.location(*tokens)

    def locate(self, file_identifier: str | StoreLocationMixinAPI) -> StoreLocationMixinAPI:
        return self.plugin.locate(file_identifier)

    def exists(self, file_identifier: str | StoreLocationMixinAPI) -> bool:
        return self.plugin.exists(file_identifier)

    def file_size(self, file_identifier: str | StoreLocationMixinAPI) -> int | None:
        return self.plugin.file_size(file_identifier)

    def stat(self, file_identifier: str | StoreLocationMixinAPI) -> SingleFileStatus:
        return self.plugin.stat(file_identifier)

    def iter_locations(self) -> Iterator[StoreLocationMixinAPI]:
        return self.plugin.iter_locations()

    def write_bytes(self, file_bytes: bytes, *, metadata=None, location: str | None = None) -> StoreLocationMixinAPI:
        return self.plugin.write_bytes(file_bytes=file_bytes, metadata=metadata, location=location)

    def copy_within_store(
        self,
        src_location: str | StoreLocationMixinAPI,
        dst_location: str | StoreLocationMixinAPI,
    ) -> StoreLocationMixinAPI:
        return self.plugin.copy_within_plugin(src_location, dst_location)

    def delete(self, file_identifier: str | StoreLocationMixinAPI) -> bool:
        return self.plugin.delete(file_identifier)

    def update_bytes(
        self,
        file_identifier: str | StoreLocationMixinAPI,
        file_bytes: bytes,
        *,
        append: bool = False,
    ) -> bool:
        return self.plugin.update_bytes(file_identifier, file_bytes=file_bytes, append=append)

    @abc.abstractmethod
    def reload_spec_from_db(self) -> "StoreSpec":
        ...

    @abc.abstractmethod
    def save_spec_to_db(self) -> "StoreSpec":
        ...

    @abc.abstractmethod
    def delete_from_db(self) -> bool:
        ...

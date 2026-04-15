"""Preferred API contract for one raw storage plugin.

For now this extends the legacy `StoreAPI` surface while tightening the naming
around raw file/location operations. The long-term intent is that plugin code
should target this interface, while store containers and the storage manager own
all database/orchestration concerns.
"""

from __future__ import annotations

from collections.abc import Iterator

from LiuXin_alpha.storage.api.location_api import StoreLocationMixinAPI
from LiuXin_alpha.storage.api.store_api import StoreAPI
from LiuXin_alpha.storage.single_file import SingleFileStatus


class StorePluginAPI(StoreAPI):
    """Preferred raw-plugin naming layered on top of the legacy store API."""

    @property
    def plugin_kind(self) -> str:
        return type(self).__name__

    def close(self) -> None:
        return None

    def locate(self, file_identifier: str | StoreLocationMixinAPI) -> StoreLocationMixinAPI:
        return self.get_file(file_identifier)

    def exists(self, file_identifier: str | StoreLocationMixinAPI) -> bool:
        return self.file_exists(file_identifier)

    def stat(self, file_identifier: str | StoreLocationMixinAPI) -> SingleFileStatus:
        return self.get_file_status(file_identifier)

    def iter_locations(self) -> Iterator[StoreLocationMixinAPI]:
        return self.true_files()

    def write_bytes(
        self,
        file_bytes: bytes,
        *,
        metadata=None,
        location: str | None = None,
    ) -> StoreLocationMixinAPI:
        return self.add_file(file_bytes=file_bytes, metadata=metadata, url=location)

    def copy_within_store(
        self,
        src_location: str | StoreLocationMixinAPI,
        dst_location: str | StoreLocationMixinAPI,
    ) -> StoreLocationMixinAPI:
        src_key = src_location.as_store_key() if isinstance(src_location, StoreLocationMixinAPI) else str(src_location)
        dst_key = dst_location.as_store_key() if isinstance(dst_location, StoreLocationMixinAPI) else str(dst_location)
        return self.dupe_file_in_store(src_key, dst_key)

    def delete(self, file_identifier: str | StoreLocationMixinAPI) -> bool:
        return self.delete_file(file_identifier)

    def update_bytes(
        self,
        file_identifier: str | StoreLocationMixinAPI,
        file_bytes: bytes,
        *,
        append: bool = False,
    ) -> bool:
        key = file_identifier.as_store_key() if isinstance(file_identifier, StoreLocationMixinAPI) else str(file_identifier)
        return self.update_file(key, file_bytes=file_bytes, append=append)

from __future__ import annotations

import abc
from typing import Optional

from LiuXin_alpha.storage.api.location_api import StoreLocationMixinAPI


class StoreBackendAddFilesAPI(abc.ABC):
    """Add or duplicate files within a store."""

    def add_file(
        self,
        file_bytes: bytes,
        *,
        metadata=None,
        url: Optional[str] = None,
    ) -> StoreLocationMixinAPI:
        raise PermissionError("This store does not support file addition.")

    def put_replica(
        self,
        file_bytes: bytes,
        *,
        storage_key: str | None = None,
        metadata=None,
        add_sidecar_opf: bool = False,
    ) -> StoreLocationMixinAPI:
        if storage_key is not None:
            return self.add_file(file_bytes=file_bytes, metadata=metadata, url=storage_key)
        return self.add_file(file_bytes=file_bytes, metadata=metadata)

    def dupe_file_in_store(self, src_file_url: str, dst_file_url: str) -> StoreLocationMixinAPI:
        raise PermissionError("This store does not support in-store file duplication.")

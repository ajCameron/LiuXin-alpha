"""Compatibility methods for adding files to a raw store.

Examples:
    Add bytes to a writable legacy backend::

        location = backend.add_file(b"payload")
"""

from __future__ import annotations

import abc
from typing import Optional

from LiuXin_alpha.storage.api.location_api import StoreLocationMixinAPI


class StoreBackendAddFilesAPI(abc.ABC):
    """Add or duplicate files within a store.

    Examples:
        Store a replica under an explicit storage key::

            location = backend.put_replica(b"payload", storage_key="books/a.epub")
    """

    def add_file(
        self,
        file_bytes: bytes,
        *,
        metadata=None,
        url: Optional[str] = None,
    ) -> StoreLocationMixinAPI:
        """Add bytes to an optional backend-relative URL.

        Examples:
            Request an explicit destination when supported::

                location = backend.add_file(b"hello", url="notes/hello.txt")
        """
        raise PermissionError("This store does not support file addition.")

    def put_replica(
        self,
        file_bytes: bytes,
        *,
        storage_key: str | None = None,
        metadata=None,
        add_sidecar_opf: bool = False,
    ) -> StoreLocationMixinAPI:
        """Add replica bytes under an optional storage key.

        Examples:
            Let the backend select a safe implicit location::

                location = backend.put_replica(b"payload")
        """
        if storage_key is not None:
            return self.add_file(file_bytes=file_bytes, metadata=metadata, url=storage_key)
        return self.add_file(file_bytes=file_bytes, metadata=metadata)

    def dupe_file_in_store(self, src_file_url: str, dst_file_url: str) -> StoreLocationMixinAPI:
        """Duplicate a file within one store when supported.

        Examples:
            Copy an existing object to a new storage key::

                copy = backend.dupe_file_in_store("a.epub", "copies/a.epub")
        """
        raise PermissionError("This store does not support in-store file duplication.")

"""Location/file orchestration methods for the storage manager.

These methods are intentionally phrased in terms of `Location` handles and
manager-level routing. They should not expose backend-specific implementation
details or replica bookkeeping directly.
"""

from __future__ import annotations

import abc
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api.location_api import StoreLocationMixinAPI
    from LiuXin_alpha.storage.storage_types import StoreRef


class StoreFileOrchestrationAPI(abc.ABC):
    """Manager methods for choosing a store and returning Locations."""

    @abc.abstractmethod
    def store_bytes(
        self,
        file_bytes: bytes,
        metadata: Any = None,
        *,
        preferred_store: "StoreRef | None" = None,
    ) -> "StoreLocationMixinAPI":
        ...

    def add_file(
        self,
        file_bytes: bytes,
        metadata: "MetadataContainerAPI | None" = None,
        *,
        preferred_store: "StoreRef | None" = None,
    ) -> "StoreLocationMixinAPI":
        return self.store_bytes(file_bytes=file_bytes, metadata=metadata, preferred_store=preferred_store)

    @abc.abstractmethod
    def locate_file(
        self,
        file_url: str | None = None,
        metadata: Any = None,
        *,
        preferred_store: "StoreRef | None" = None,
    ) -> "StoreLocationMixinAPI":
        ...

    def retrieve_file(
        self,
        file_url: str | None = None,
        metadata: "MetadataContainerAPI | None" = None,
        *,
        preferred_store: "StoreRef | None" = None,
    ) -> "StoreLocationMixinAPI":
        return self.locate_file(file_url=file_url, metadata=metadata, preferred_store=preferred_store)

    @abc.abstractmethod
    def locate_folder(
        self,
        folder_key: str,
        *,
        preferred_store: "StoreRef | None" = None,
    ) -> "StoreLocationMixinAPI":
        ...

    @abc.abstractmethod
    def delete_location(
        self,
        file_url: str | None = None,
        metadata: Any = None,
        location: "StoreLocationMixinAPI | None" = None,
    ) -> bool:
        ...

    def delete_file(
        self,
        file_url: str | None = None,
        metadata: "MetadataContainerAPI | None" = None,
        file_container: "StoreLocationMixinAPI | None" = None,
    ) -> bool:
        return self.delete_location(file_url=file_url, metadata=metadata, location=file_container)

    @abc.abstractmethod
    def iter_locations(self) -> Iterator["StoreLocationMixinAPI"]:
        ...

    def iter(self) -> Iterator["StoreLocationMixinAPI"]:
        return self.iter_locations()

"""Location/file orchestration methods for the storage manager.

These methods are intentionally phrased in terms of `Location` handles and
manager-level routing. They should not expose backend-specific implementation
details or replica bookkeeping directly.
"""

from __future__ import annotations

import abc
from collections.abc import Iterator
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from LiuXin_alpha.metadata.api import MetadataContainerAPI
    from LiuXin_alpha.storage.api.location_api import StoreLocationMixinAPI
    from LiuXin_alpha.storage.storage_types import StoreRef


class StoreFileOrchestrationAPI(abc.ABC):
    """Manager methods for choosing a store and returning Locations."""

    @abc.abstractmethod
    def store_bytes(
        self,
        file_bytes: bytes,
        metadata: "MetadataContainerAPI | None" = None,
        *,
        preferred_store: "StoreRef | None" = None,
    ) -> "StoreLocationMixinAPI":
        ...

    @abc.abstractmethod
    def locate_file(
        self,
        file_url: str | None = None,
        metadata: "MetadataContainerAPI | None" = None,
        *,
        preferred_store: "StoreRef | None" = None,
    ) -> "StoreLocationMixinAPI":
        ...

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
        metadata: "MetadataContainerAPI | None" = None,
        location: "StoreLocationMixinAPI | None" = None,
    ) -> bool:
        ...

    @abc.abstractmethod
    def iter_locations(self) -> Iterator["StoreLocationMixinAPI"]:
        ...

from __future__ import annotations

import abc
from typing import Iterator, Optional, TYPE_CHECKING

from LiuXin_alpha.metadata.api import MetadataContainerAPI
if TYPE_CHECKING:
    from LiuXin_alpha.storage.api import StoreAPI, SingleFileAPI, StoreLocationMixinAPI


class StorageManagerAPI(abc.ABC):
    """
    Contract for the storage manager/front-end.

    This is the user-facing storage API. It coordinates multiple stores and
    hides physical placement details from callers.
    """
    # Todo: Callbacks for when a new store is added? Event bus takes care of all that?
    @abc.abstractmethod
    def add_store(self, new_store: StoreAPI) -> None:
        """Register a store with the manager."""

    @abc.abstractmethod
    def remove_store(self, store_identifier: str) -> bool:
        """Remove one store by UUID/name/url."""

    @abc.abstractmethod
    def get_store(self, store_identifier: str) -> StoreAPI:
        """Resolve one store by UUID/name/url."""

    @abc.abstractmethod
    def iter_stores(self) -> Iterator[StoreAPI]:
        """Iterate all registered stores."""

    @abc.abstractmethod
    def add_file(
        self,
        file_bytes: bytes,
        metadata: Optional[MetadataContainerAPI] = None,
        *,
        preferred_store: Optional[str] = None,
    ) -> "SingleFileAPI":
        """
        Store a file in managed storage.

        The manager decides final placement unless `preferred_store` is supplied.
        """

    @abc.abstractmethod
    def retrieve_file(
        self,
        file_url: Optional[str] = None,
        *,
        metadata: Optional[MetadataContainerAPI] = None,
        preferred_store: Optional[str] = None,
    ) -> "SingleFileAPI":
        """Return a file handle/container for a stored file."""

    @abc.abstractmethod
    def retrieve_folder(
        self,
        folder_key: str,
        *,
        preferred_store: Optional[str] = None,
    ) -> "StoreLocationMixinAPI":
        """Return a virtual folder location for the requested folder key."""

    @abc.abstractmethod
    def delete_file(
        self,
        file_url: Optional[str] = None,
        *,
        metadata: Optional[MetadataContainerAPI] = None,
        file_container: Optional["SingleFileAPI"] = None,
    ) -> bool:
        """Delete a stored file by URL, metadata lookup, or existing file container."""

    @abc.abstractmethod
    def iter(self) -> Iterator["SingleFileAPI"]:
        """Iterate files visible to the manager."""

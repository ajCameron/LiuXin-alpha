"""Location/file orchestration methods for the storage manager.

These methods are intentionally phrased in terms of `Location` handles and
manager-level routing. They should not expose backend-specific implementation
details or replica bookkeeping directly.

Examples:
    Store bytes, then retrieve them through the returned location::

        location = manager.store_bytes(b"hello", preferred_store="main")
        assert manager.locate_file(location.file_url).read_bytes() == b"hello"
"""

from __future__ import annotations

import abc
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api.location_api import StoreLocationMixinAPI
    from LiuXin_alpha.storage.storage_types import StoreRef


class StoreFileOrchestrationAPI(abc.ABC):
    """Manager methods for choosing a store and returning Locations.

    Examples:
        Let the manager select its default store::

            location = manager.store_bytes(b"payload")
    """

    @abc.abstractmethod
    def store_bytes(
        self,
        file_bytes: bytes,
        metadata: Any = None,
        *,
        preferred_store: "StoreRef | None" = None,
    ) -> "StoreLocationMixinAPI":
        """Store bytes and return their backend-neutral location.

        Examples:
            Prefer a named store when placement matters::

                location = manager.store_bytes(
                    b"payload", metadata={"file_extension": "bin"},
                    preferred_store="archive",
                )
        """
        ...

    def add_file(
        self,
        file_bytes: bytes,
        metadata: "MetadataContainerAPI | None" = None,
        *,
        preferred_store: "StoreRef | None" = None,
    ) -> "StoreLocationMixinAPI":
        """Compatibility alias for :meth:`store_bytes`.

        Examples:
            Existing callers may continue to use ``add_file``::

                location = manager.add_file(b"payload")
        """
        return self.store_bytes(file_bytes=file_bytes, metadata=metadata, preferred_store=preferred_store)

    @abc.abstractmethod
    def locate_file(
        self,
        file_url: str | None = None,
        metadata: Any = None,
        *,
        preferred_store: "StoreRef | None" = None,
    ) -> "StoreLocationMixinAPI":
        """Resolve a stored file to a readable location.

        Examples:
            Resolve by the URL returned from a previous write::

                found = manager.locate_file(file_url=location.file_url)
        """
        ...

    def retrieve_file(
        self,
        file_url: str | None = None,
        metadata: "MetadataContainerAPI | None" = None,
        *,
        preferred_store: "StoreRef | None" = None,
    ) -> "StoreLocationMixinAPI":
        """Compatibility alias for :meth:`locate_file`.

        Examples:
            Retrieve a stored payload and read it as bytes::

                payload = manager.retrieve_file(file_url=url).read_bytes()
        """
        return self.locate_file(file_url=file_url, metadata=metadata, preferred_store=preferred_store)

    @abc.abstractmethod
    def locate_folder(
        self,
        folder_key: str,
        *,
        preferred_store: "StoreRef | None" = None,
    ) -> "StoreLocationMixinAPI":
        """Resolve a virtual folder key through a selected store.

        Examples:
            Address an import folder in the default store::

                folder = manager.locate_folder("incoming/2026")
        """
        ...

    @abc.abstractmethod
    def delete_location(
        self,
        file_url: str | None = None,
        metadata: Any = None,
        location: "StoreLocationMixinAPI | None" = None,
    ) -> bool:
        """Delete one location selected by URL, metadata, or handle.

        Examples:
            Delete the exact handle returned by ``store_bytes``::

                deleted = manager.delete_location(location=location)
        """
        ...

    def delete_file(
        self,
        file_url: str | None = None,
        metadata: "MetadataContainerAPI | None" = None,
        file_container: "StoreLocationMixinAPI | None" = None,
    ) -> bool:
        """Compatibility alias for :meth:`delete_location`.

        Examples:
            Older callers can pass their location as ``file_container``::

                deleted = manager.delete_file(file_container=location)
        """
        return self.delete_location(file_url=file_url, metadata=metadata, location=file_container)

    @abc.abstractmethod
    def iter_locations(self) -> Iterator["StoreLocationMixinAPI"]:
        """Iterate over locations visible through every registered store.

        Examples:
            Collect URLs for an inventory report::

                urls = [location.file_url for location in manager.iter_locations()]
        """
        ...

    def iter(self) -> Iterator["StoreLocationMixinAPI"]:
        """Compatibility alias for :meth:`iter_locations`.

        Examples:
            Iterate directly over all known locations::

                locations = list(manager.iter())
        """
        return self.iter_locations()

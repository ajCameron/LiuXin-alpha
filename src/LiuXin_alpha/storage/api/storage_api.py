"""
API contracts for stores and the top-level storage manager.

The storage design is intentionally split:
- `StoreAPI` models one concrete store (disk, remote HTTP, tape, etc.).
- `StorageAPI` models the manager/front-end that orchestrates many stores.
"""

from __future__ import annotations

import abc
import dataclasses
import pprint

from typing import TYPE_CHECKING, Any, Iterator, Optional

from LiuXin_alpha.metadata.api import MetadataContainerAPI
from LiuXin_alpha.utils.logging.api import EventLogAPI

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api.file_api import SingleFileAPI
    from LiuXin_alpha.storage.api.location_api import StoreLocationMixinAPI


@dataclasses.dataclass
class StoreCheckStatus:
    """
    Outcome of store self-check probes.
    """

    # Store identity/marker verification (where applicable).
    store_marker_file: bool = False
    # Read-path health.
    read: bool = False
    # Write-path health.
    write: bool = False
    # Additional backend-specific checks.
    sundry: bool = False

    @property
    def all_ok(self) -> bool:
        """True when all core checks passed."""
        return self.store_marker_file and self.read and self.write and self.sundry


@dataclasses.dataclass
class StoreStatus:
    """
    Snapshot status for a store.

    Concrete store plugins can enrich this via `details`.
    """

    # Store identity.
    name: str
    uuid: str
    url: str

    # Capacity/accounting.
    file_count: Optional[int] = None
    store_free_space: Optional[int] = None

    # Health.
    check_status: StoreCheckStatus = dataclasses.field(default_factory=StoreCheckStatus)
    checked: bool = False
    good: bool | str = True

    # Observability.
    event_log: Optional[EventLogAPI] = None
    details: dict[str, Any] = dataclasses.field(default_factory=dict)

    @property
    def online(self) -> bool:
        """Best-effort online signal from health probes."""
        return bool(self.checked or self.check_status.read or self.check_status.write)


class StoreAPI(abc.ABC):
    """
    Contract for one physical/logical store.

    A store is responsible for low-level storage operations. Policy decisions
    (replication, store choice, cache strategy) belong in `StorageAPI`.
    """

    _url: str
    _name: str
    _uuid: Optional[str]

    def __init__(self, url: str, name: Optional[str] = None, uuid: Optional[str] = None) -> None:
        self.set_url(url)
        self._name = name if name is not None else self.url_to_name(url)
        self._uuid = uuid

    @abc.abstractmethod
    def url_to_name(self, url: str) -> str:
        """Generate a stable human-friendly name from a store URL."""

    @abc.abstractmethod
    def startup(self) -> Optional[StoreStatus]:
        """
        Bring the store online.

        Stores may return a status snapshot when startup is complete.
        """

    @abc.abstractmethod
    def self_test(self) -> StoreStatus:
        """Run store health checks and return a status snapshot."""

    @abc.abstractmethod
    def status(self) -> StoreStatus:
        """Return current store status."""

    @abc.abstractmethod
    def file_exists(self, file_url: str) -> bool:
        """Check whether a specific file URL exists inside this store."""

    @abc.abstractmethod
    def get_file(self, file_url: str) -> "SingleFileAPI":
        """Return a file container for the given file URL."""

    @property
    def url(self) -> str:
        """Store root URL."""
        return self._url

    @url.setter
    def url(self, url: str) -> None:
        raise AttributeError("Cannot directly set the url of a store.")

    def set_url(self, new_url: str) -> None:
        """Set store URL (intended for controlled migration/admin paths)."""
        self._url = new_url

    @property
    def name(self) -> str:
        """Human-readable store name."""
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        raise AttributeError("Cannot directly set the name of a store.")

    @property
    def uuid(self) -> Optional[str]:
        """Store UUID."""
        return self._uuid

    @uuid.setter
    def uuid(self, uuid: str) -> None:
        raise AttributeError("Cannot directly set the uuid of a store.")

    @property
    def online(self) -> bool:
        """Best-effort online indicator."""
        try:
            return self.status().online
        except Exception:
            return False

    @property
    def checked(self) -> bool:
        """Whether health checks have been run and passed policy gates."""
        try:
            return bool(self.status().checked)
        except Exception:
            return False

    def status_str(self) -> str:
        """Pretty-printed status snapshot for logs/CLI."""
        return pprint.pformat(self.status())

    def location(self, *tokens: str) -> "StoreLocationMixinAPI":
        """
        Resolve a store-relative location.

        Stores with path/location support should override this method.
        """
        raise NotImplementedError("This store does not expose Location objects.")

    def add_file(
        self,
        file_bytes: bytes,
        *,
        metadata: Optional[MetadataContainerAPI] = None,
    ) -> "SingleFileAPI":
        """Store file bytes inside this store."""
        raise PermissionError("This store does not support writing files.")

    def delete_file(self, file_url: str) -> bool:
        """Delete one file from this store."""
        raise PermissionError("This store does not support file deletion.")

    def true_files(self) -> Iterator["SingleFileAPI"]:
        """
        Iterate files physically present in this store.

        Default implementation is empty and should be overridden by stores
        that can enumerate files cheaply.
        """
        return iter(())

    def iter(self) -> Iterator["SingleFileAPI"]:
        """Alias for `true_files()` for older calling sites."""
        return self.true_files()


class StorageAPI(abc.ABC):
    """
    Contract for the storage manager/front-end.

    This is the user-facing storage API. It coordinates multiple stores and
    hides physical placement details from callers.
    """

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
        metadata: Optional[MetadataContainerAPI] = None,
        *,
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
        metadata: Optional[MetadataContainerAPI] = None,
        file_container: Optional["SingleFileAPI"] = None,
    ) -> bool:
        """Delete a stored file by URL, metadata lookup, or existing file container."""

    @abc.abstractmethod
    def iter(self) -> Iterator["SingleFileAPI"]:
        """Iterate files visible to the manager."""


# `StorageManagerAPI` is the intent-revealing name used in the docs.
StorageManagerAPI = StorageAPI

__all__ = [
    "StoreAPI",
    "StoreCheckStatus",
    "StoreStatus",
    "StorageAPI",
    "StorageManagerAPI",
]

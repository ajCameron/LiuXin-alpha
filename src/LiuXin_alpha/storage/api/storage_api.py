"""
API contracts for stores.

The storage design is intentionally split:
- `StoreAPI` models one concrete store (disk, remote HTTP, tape, etc.).
- `StorageManagerAPI` models the manager/front-end that orchestrates many stores.
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


@dataclasses.dataclass(slots=True)
class StoreSpec:
    """Declarative description of one configured store."""

    store_id: Optional[int]
    store_uuid: Optional[str]
    store_name: str
    store_kind: str
    store_url: str

    store_access_protocol: Optional[str] = None
    store_root_uri: Optional[str] = None

    store_failure_domain: Optional[str] = None
    store_region: Optional[str] = None
    store_tags: tuple[str, ...] = ()

    store_default_replication_policy_id: Optional[int] = None
    store_default_backup_policy_id: Optional[int] = None

    store_supports_active_replica_mode: bool = True
    store_supports_backup_replica_mode: bool = True
    store_supports_archive_replica_mode: bool = True

    store_is_read_only: bool = False
    store_supports_folders: bool = True
    store_policy_json: Optional[str] = None
    store_scratch: Optional[str] = None


@dataclasses.dataclass(slots=True)
class StoreCheckStatus:
    """Outcome of store self-check probes."""

    store_marker_file: bool = False
    read: bool = False
    write: bool = False
    update: bool = False
    sundry: bool = False

    @property
    def all_ok(self) -> bool:
        return self.store_marker_file and self.read and self.update and self.write and self.sundry


@dataclasses.dataclass(slots=True)
class StoreStatus:
    """Snapshot status for a store."""

    name: str
    uuid: Optional[str]
    url: str

    file_count: Optional[int] = None
    store_free_space: Optional[int] = None

    check_status: StoreCheckStatus = dataclasses.field(default_factory=StoreCheckStatus)
    checked: bool = False
    good: bool | str = True

    event_log: Optional[EventLogAPI] = None
    details: dict[str, Any] = dataclasses.field(default_factory=dict)

    @property
    def online(self) -> bool:
        return bool(self.checked or self.check_status.read or self.check_status.write)


class StoreAPI(abc.ABC):
    """
    Contract for one physical/logical store.

    Stores speak in terms of concrete replica objects identified by store-relative
    storage keys. They do not resolve bibliographic identity or replication policy.
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
        """Bring the store online."""

    @abc.abstractmethod
    def self_test(self) -> StoreStatus:
        """Run store health checks and return a status snapshot."""

    @abc.abstractmethod
    def status(self) -> StoreStatus:
        """Return current store status."""

    @abc.abstractmethod
    def replica_exists(self, storage_key: str) -> bool:
        """Check whether a specific replica storage key exists inside this store."""

    @abc.abstractmethod
    def get_replica(self, storage_key: str) -> "SingleFileAPI":
        """Return a file container for one concrete replica inside this store."""

    @property
    def url(self) -> str:
        return self._url

    @url.setter
    def url(self, url: str) -> None:
        raise AttributeError("Cannot directly set the url of a store.")

    def set_url(self, new_url: str) -> None:
        self._url = new_url

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        raise AttributeError("Cannot directly set the name of a store.")

    @property
    def uuid(self) -> Optional[str]:
        return self._uuid

    @uuid.setter
    def uuid(self, uuid: str) -> None:
        raise AttributeError("Cannot directly set the uuid of a store.")

    @property
    def online(self) -> bool:
        try:
            return self.status().online
        except Exception:
            return False

    @property
    def checked(self) -> bool:
        try:
            return bool(self.status().checked)
        except Exception:
            return False

    def status_str(self) -> str:
        return pprint.pformat(self.status())

    def location(self, *tokens: str) -> "StoreLocationMixinAPI":
        raise NotImplementedError("This store does not expose Location objects.")

    def put_replica(
        self,
        file_bytes: bytes,
        *,
        storage_key: Optional[str] = None,
        metadata: Optional[MetadataContainerAPI] = None,
        add_sidecar_opf: bool = False,
    ) -> "SingleFileAPI":
        raise PermissionError("This store does not support writing replicas.")

    def update_replica(self, storage_key: str, file_bytes: bytes) -> bool:
        raise PermissionError("This store does not support replica updates.")

    def delete_replica(self, storage_key: str) -> bool:
        raise PermissionError("This store does not support replica deletion.")

    def iter_replicas(self) -> Iterator["SingleFileAPI"]:
        return iter(())

    def iter(self) -> Iterator["SingleFileAPI"]:
        return self.iter_replicas()

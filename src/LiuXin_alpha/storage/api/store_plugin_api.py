"""Raw storage plugin API.

A store plugin talks to one physical medium or remote endpoint. It should know
nothing about database rows, item graphs, replica policy, or orchestration. If
it starts learning those things, the boundary has gone bad.
"""

from __future__ import annotations

import abc
import pprint
from collections.abc import Iterator
from typing import TYPE_CHECKING, Optional

from LiuXin_alpha.storage.api.location_api import StoreLocationMixinAPI
from LiuXin_alpha.storage.single_file import SingleFileStatus

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api.info_containers_api import StoreStatus


class StorePluginAPI(abc.ABC):
    """Contract for one raw storage plugin bound to one root location.

    This is the reusable bit that ingest, repair, or export code should be able
    to call without dragging in the whole managed-storage stack.
    """

    _url: str
    _name: str
    _uuid: Optional[str]

    def __init__(
        self,
        *,
        url: str,
        name: Optional[str] = None,
        uuid: Optional[str] = None,
    ) -> None:
        self.set_url(url)
        self._name = name if name is not None else self.url_to_name(url)
        self._uuid = uuid

    @property
    def plugin_kind(self) -> str:
        return type(self).__name__

    @abc.abstractmethod
    def url_to_name(self, url: str) -> str:
        ...

    @property
    @abc.abstractmethod
    def root_path(self):
        ...

    @abc.abstractmethod
    def startup(self) -> "StoreStatus":
        ...

    @abc.abstractmethod
    def self_test(self) -> "StoreStatus":
        ...

    @abc.abstractmethod
    def status(self) -> "StoreStatus":
        ...

    def close(self) -> None:
        return None

    @property
    def url(self) -> str:
        return self._url

    @url.setter
    def url(self, url: str) -> None:
        raise AttributeError("Cannot directly set the url of a store plugin.")

    def set_url(self, new_url: str) -> None:
        self._url = str(new_url)

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        raise AttributeError("Cannot directly set the name of a store plugin.")

    @property
    def uuid(self) -> Optional[str]:
        return self._uuid

    @property
    def store_uuid(self) -> Optional[str]:
        return self._uuid

    @uuid.setter
    def uuid(self, uuid: str) -> None:
        raise AttributeError("Cannot directly set the uuid of a store plugin.")

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

    @abc.abstractmethod
    def location(self, *tokens: str) -> StoreLocationMixinAPI:
        ...

    @abc.abstractmethod
    def locate(self, file_identifier: str | StoreLocationMixinAPI) -> StoreLocationMixinAPI:
        ...

    @abc.abstractmethod
    def exists(self, file_identifier: str | StoreLocationMixinAPI) -> bool:
        ...

    @abc.abstractmethod
    def file_size(self, file_identifier: str | StoreLocationMixinAPI) -> int | None:
        ...

    @abc.abstractmethod
    def stat(self, file_identifier: str | StoreLocationMixinAPI) -> SingleFileStatus:
        ...

    @abc.abstractmethod
    def iter_locations(self) -> Iterator[StoreLocationMixinAPI]:
        ...

    @abc.abstractmethod
    def write_bytes(
        self,
        file_bytes: bytes,
        *,
        metadata=None,
        location: str | None = None,
    ) -> StoreLocationMixinAPI:
        ...

    def copy_within_plugin(
        self,
        src_location: str | StoreLocationMixinAPI,
        dst_location: str | StoreLocationMixinAPI,
    ) -> StoreLocationMixinAPI:
        raise PermissionError("This store plugin does not support in-plugin copies.")

    @abc.abstractmethod
    def delete(self, file_identifier: str | StoreLocationMixinAPI) -> bool:
        ...

    def update_bytes(
        self,
        file_identifier: str | StoreLocationMixinAPI,
        file_bytes: bytes,
        *,
        append: bool = False,
    ) -> bool:
        raise PermissionError("This store plugin does not support in-place updates.")

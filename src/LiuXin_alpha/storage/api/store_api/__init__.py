"""Compatibility import surface for store API contracts."""

from __future__ import annotations

import abc
import pprint
from typing import Optional, Iterator, TYPE_CHECKING

from LiuXin_alpha.storage.api.store_api.storage_backend_api import StoreBackendAPI

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database import DatabaseAPI
    from LiuXin_alpha.storage.api import StoreStatus
    from LiuXin_alpha.storage.api.location_api import StoreLocationMixinAPI


class StoreAPI(StoreBackendAPI):
    """Contract for one physical/logical store."""

    _url: str
    _name: str
    _uuid: Optional[str]
    _db: "DatabaseAPI | None"

    def __init__(
        self,
        url: str,
        db: "DatabaseAPI | None" = None,
        name: Optional[str] = None,
        uuid: Optional[str] = None,
    ) -> None:
        self.set_url(url)
        self._name = name if name is not None else self.url_to_name(url)
        self._uuid = uuid
        self._db = db

    @abc.abstractmethod
    def url_to_name(self, url: str) -> str:
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
    def db(self) -> "DatabaseAPI | None":
        return self._db

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

    def iter_replicas(self) -> Iterator["StoreLocationMixinAPI"]:
        return self.true_files()

    def iter(self) -> Iterator["StoreLocationMixinAPI"]:
        return self.true_files()

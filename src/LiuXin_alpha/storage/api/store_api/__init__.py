"""
Compatibility import surface for store API contracts.

Note - a "replica" is a replica of a digital asset in a store.
It's a file - or thing which can act like a file.
"""

from __future__ import annotations

import abc
import pprint
from typing import Optional, Iterator, TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database import DatabaseAPI
    from LiuXin_alpha.storage.api import StoreStatus, SingleFileAPI
    from LiuXin_alpha.storage.api.store_api.storage_backend_api import StoreBackendAPI
    from LiuXin_alpha.storage.api.store_api.store_db_api import StoreDBAPI


class StoreAPI(StoreBackendAPI, StoreDBAPI):
    """
    Contract for one physical/logical store.

    Stores speak in terms of concrete replica objects identified by store-relative
    storage keys. They do not resolve bibliographic identity or replication policy.

    Stores are intended for stand alone and StorageManager driver operation.
    As such, they are composed of two mixins.
    One to handle the raw file access.
    The other to deal with database interface.
    """

    _url: str
    _name: str
    _uuid: Optional[str]

    _db: "DatabaseAPI"

    def __init__(
            self,
            url: str,
            db: "DatabaseAPI",
            name: Optional[str] = None,
            store_uuid: Optional[str] = None,

    ) -> None:
        """
        Startup the store.

        Optionally attaching it to a db.

        :param url:
        :param db:
        :param name:
        :param store_uuid:
        """

        self.set_url(url)
        self._name = name if name is not None else self.url_to_name(url)
        self._uuid = store_uuid

        self._db = db

    @abc.abstractmethod
    def url_to_name(self, url: str) -> str:
        """
        Generate a stable human-friendly name from a store URL.

        :param url: Url to resolve - either this store, or a file
        """

    @abc.abstractmethod
    def startup(self) -> "StoreStatus":
        """
        Bring the store online.
        """

    @abc.abstractmethod
    def self_test(self) -> "StoreStatus":
        """
        Run store health checks and return a status snapshot.
        """

    @abc.abstractmethod
    def status(self) -> "StoreStatus":
        """
        Return current store status.
        """

    @abc.abstractmethod
    def file_url_exists(self, file_url: str) -> bool:
        """
        Check whether a specific file url actually exists at the given url.

        :param file_url:
        :return:
        """

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

    def iter_replicas(self) -> Iterator["SingleFileAPI"]:
        return iter(())

    def iter(self) -> Iterator["SingleFileAPI"]:
        return self.iter_replicas()

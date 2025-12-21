
"""
Contains API elements for the actual storage classes.
"""


from __future__ import annotations

import abc
import dataclasses
import pprint
from typing import Optional, Iterator

from LiuXin_alpha.storage.api.file_api import SingleFileAPI
from LiuXin_alpha.utils.logging.api import EventLogAPI
from LiuXin_alpha.metadata.api import MetadataContainerAPI



@dataclasses.dataclass
class StorageBackendCheckStatus:
    """
    How did the store do when we were running self checks?
    """
    store_marker_file: bool

    read: bool
    write: bool

    sundry: bool


@dataclasses.dataclass
class StorageBackendStatus:
    """
    What's happening with the given store?

    Specific plugins may well include far more information as to the status of the store
    (For example, conceivably a disc store could include full SMART data).
    This is a minimum status report - not a maximum.
    """
    # - Store properties
    name: str           # - Human readable name for the store (should be unique)
    uuid: str           # - UUID for the store (definitely unique)

    # - Store status
    file_count: Optional[int]    # - How many LiuXin files the store _thinks_ it has
                                 # - this can be quite an expensive operation - so can be None if not needed
    store_free_space: int   # - Free space available for LiuXin use
    # - NOTE - This should be a MINIMUM not a MAXIMUM
    # -Some stores are under compression - so it's hard to know the TRUE total size

    check_status: StorageBackendCheckStatus  # - Result of checks being carried out on the storage backend.

    checked: bool           # - Has the store passed self checks?

    url: str                # - url to the store

    good: str              # - Are we worried about this store?

    event_log: EventLogAPI      # - query the status of this bit of the system


class StorageBackendAPI(abc.ABC):
    """
    Represents a file and metadata store on the system.

    Every store backend plugins should inherit from this class.
    """
    _url: str
    _name: str
    _uuid: Optional[str]

    def __init__(self, url: str, name: Optional[str] = None, uuid: Optional[str] = None) -> None:
        """
        Initialize the store.

        :param url:
        """
        self.set_url(url)
        self._name = name if name is not None else self.url_to_name(url)
        self._uuid = uuid

    @abc.abstractmethod
    def url_to_name(self, url: str) -> str:
        """
        Generate a name from a URL.

        :param url:
        :return:
        """

    @abc.abstractmethod
    def startup(self) -> StorageBackendStatus:
        """
        Preform store startup and report the status.

        :return:
        """

    @abc.abstractmethod
    def self_test(self) -> StorageBackendStatus:
        """
        Preform tests on the store.

        :return:
        """

    @property
    def url(self) -> str:
        """
        Return the url of the store.
        :return:
        """
        return self._url

    @url.setter
    def url(self, url: str) -> None:
        """
        Cannot directly set the url of a store.

        :param url:
        :return:
        """
        raise AttributeError("Cannot directly set the url of a store.")

    def set_url(self, new_url: str) -> None:
        """
        Set the URL for the backend store.

        :param new_url:
        :return:
        """
        self._url = new_url

    @property
    def name(self) -> str:
        """
        Human-readable name for this store.

        :return:
        """
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        """
        Cannot directly set the name of a store.

        :param name:
        :return:
        """
        raise AttributeError("Cannot directly set the name of a store.")

    @property
    def uuid(self) -> str:
        """
        The UUID for the given store.

        :return:
        """
        return self._uuid

    @uuid.setter
    def uuid(self, uuid: str) -> None:
        """
        Cannot directly set the uuid of a store.

        :param uuid:
        :return:
        """
        raise AttributeError("Cannot directly set the uuid of a store.")

    @property
    def online(self) -> bool:
        """
        Is the store online or not?

        :return:
        """

    @property
    def checked(self) -> bool:
        """
        Have self checks been run on the store?

        A store (probably) had to be online to be checked.
        :return:
        """

    @abc.abstractmethod
    def status(self) -> StorageBackendStatus:
        """
        The current status of the store.

        Returns a dict of the status of the store.
        How you display this information is up to you.
        :return:
        """

    def status_str(self) -> str:
        """
        A string rep of the stores' status - defaults to just the status dict.

        :return:
        """
        return pprint.pformat(self.status())

    def file_exists(self, file_url: str) -> bool:
        """
        Does a given file actually exist in the store?

        :param file_url:
        :return:
        """

    def true_files(self) -> Iterator[SingleFileAPI]:
        """
        Represents files ACTUALLY in the store.

        It's often useful to have an accounting for the files ACTUALLY present - provide it.
        :return:
        """


class StorageAPI(abc.ABC):
    """
    Provides management and frontend for actually working with the stores.

    There is probably only going to be one implementation of this class.
    But presenting an API is good practice.
    """

    @abc.abstractmethod
    def add_storage_backend(self, new_store: StorageBackendAPI) -> None:
        """
        Manually add a new storage backend to the system.

        :param new_store:
        :return:
        """

    @abc.abstractmethod
    def add_file(self,
                 file_bytes: bytes,
                 metadata: Optional[MetadataContainerAPI] = None) -> bool:
        """
        Add a file to storage.

        :param file_bytes:
        :param metadata: Some stores are metadata aware.
                         This means that they, in some way, store the file and the metadata together
                         This can be
                         - some meaningful file name
                         - directly stored as a json file
                         It's up to the store. Separation of concerns and all!
        :return:
        """

    @abc.abstractmethod
    def retrieve_file(self,
                      file_url: Optional[str],
                      metadata: Optional[MetadataContainerAPI]) -> SingleFileAPI:
        """
        Retrieve and return a file in the form of a container providing the SingleFileAPI.

        :param file_url:
        :param metadata:
        :return:
        """

    @abc.abstractmethod
    def delete_file(self,
                    file_url: Optional[str] = None,
                    metadata: Optional[MetadataContainerAPI] = None,
                    file_container: Optional[SingleFileAPI] = None) -> bool:
        """
        Delete a file in a store.

        :param file_url:
        :param metadata:
        :param file_container:
        :return:
        """

    @abc.abstractmethod
    def iter(self) -> Iterator[SingleFileAPI]:
        """
        Iterate over all files in storage.

        :return:
        """
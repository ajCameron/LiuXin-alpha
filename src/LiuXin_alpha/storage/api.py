
"""
Contains the fundamental APIs for the storage class.
"""

# Todo: Rules about what can be stored in which store?

from typing import Optional

import abc
import dataclasses
import pprint

from src.LiuXin_alpha.utils.event_log.api import EventLogAPI


@dataclasses.dataclass
class SingleFileStatus:
    """
    Contains the status of a file.

    This contains no metadata at all.
    Only information on the file itself is stored and presented.
    What this information means can be backend dependent.
    """
    uuid: str      # - If you want to do anything with a folder store, you need the file uuid
    size: int      # - File size in bytes
    hash: str      # - Hash of the file itself
    url: str       # - Some form of resource ULR to get at the file

    last_checked: str   # - When did we last KNOW we had the file?


class SingleFile(abc.ABC):
    """
    Container representing a single file in a single store.
    """
    status: SingleFileStatus          # - Status for the file on the system

    store: str                      # - Which store is the file in?
    file_url: str                   # - url to this instance of the file

    binary: Optional[bytes] = None      # - Binary bits of the file

    loaded: bool = False        # - Has the file actually been loaded into this dataclass?

    @property
    def uuid(self) -> str:
        """
        Return the uuid for the file stored in the status.

        :return:
        """
        return self.status.uuid

    @property
    def size(self) -> int:
        """
        Return the size for the individual file.

        :return:
        """
        return self.status.size

    @property
    def hash(self) -> str:
        """
        Return the hash of the file.
        :return:
        """
        return self.status.hash

    @property
    def url(self) -> str:
        """
        Return the URL of the file.

        :return:
        """
        return self.status.url


@dataclasses.dataclass
class FileStatus:
    """
    Status for a file on the system - includes LiuXin wide metadata
    """

    copies: str         # - Number of copies the system has access to?
    protected: bool     # - Does the system consider the file to be protected?


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

    def __init__(self, url: str) -> None:
        """
        Initialize the store.

        :param url:
        """
        self.set_url(url)

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

    @abc.abstractmethod
    @property
    def url(self) -> None:
        """
        Return the url of the store.
        :return:
        """

    @abc.abstractmethod
    def set_url(self, new_url: str) -> None:
        """
        Set the URL for the backend store.

        :param new_url:
        :return:
        """

    @abc.abstractmethod
    @property
    def name(self) -> str:
        """
        Human-readable name for this store.

        :return:
        """

    @abc.abstractmethod
    @property
    def uuid(self) -> str:
        """
        The UUID for the given store.

        :return:
        """

    @abc.abstractmethod
    @property
    def online(self) -> bool:
        """
        Is the store online or not?

        :return:
        """

    @abc.abstractmethod
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


class StorageAPI(abc.ABC):
    """
    Provides management and frontend for actually working with the stores.
    """

    @abc.abstractmethod
    def add_storage_backend(self, new_store: StorageBackendAPI) -> None:
        """
        Manually add a new storage backend to the system.

        :param new_store:
        :return:
        """

    @abc.abstractmethod
    def add_file(self, file_bytes: bytes):
        """
        Add a file to the store.

        :param file_bytes:
        :return:
        """


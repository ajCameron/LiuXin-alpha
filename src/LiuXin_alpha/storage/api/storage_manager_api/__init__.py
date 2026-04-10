
"""
API for the StorageManager front end.

As a rule, absent very specific circumstances, you should use this instead of raw stores directly.
"""

from __future__ import annotations

import abc
from typing import Iterator, Optional, TYPE_CHECKING, Union

from LiuXin_alpha.metadata.api import MetadataContainerAPI

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database import DatabaseAPI
    from LiuXin_alpha.storage.api import StoreAPI, SingleFileAPI, StoreLocationMixinAPI

    from LiuXin_alpha.storage.storage_types import StoreID, FileID

    from LiuXin_alpha.storage.api.info_containers_api import ReplicationCluster


class StorageManagerAPI(abc.ABC):
    """
    Contract for the storage manager/front-end.

    This is the user-facing storage API.
    It coordinates multiple stores and hides physical placement details from callers.
    """
    db: "DatabaseAPI"

    def __init__(self, db: "DatabaseAPI") -> None:
        """
        Startup the store off a database.

        :param db:
        """
        self.db = db


    # --------------------

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
        If Metadata is supplied, then the manager will attempt to use the storage policy to store the file correctly.

        :param file_bytes:
        :param metadata:
        :param preferred_store:

        :return:
        """

    @abc.abstractmethod
    def update_file(
        self,
        file_url: Union[str, FileID],
        file_bytes: bytes,
        *,
        update_replicant_pool: bool = False) -> bool:
        """
        Write an updated set of bytes out to a file.

        LiuXin is intended to preserve data at most costs.
        As such, the response to a file changing is not, necessarily, "copy that change to all backups".
        That means we've lost the backups.

        :param file_url: URL or the target FileID.
        :param file_bytes:
        :param update_replicant_pool:

        :return:
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
        file_url: Union[str, FileID] = None,
        *,
        metadata: Optional[MetadataContainerAPI] = None,
        remove_sidecar: bool = True,
        file_container: Optional["SingleFileAPI"] = None,
    ) -> bool:
        """
        Delete a stored file by URL, metadata lookup, or existing file container.

        This is for deleting a single file by id.
        :param file_url:
        :param metadata:
        :param remove_sidecar:
        :param file_container:
        :return:
        """

    @abc.abstractmethod
    def delete_replication_cluster(
        self,
        file_url: Union[str, FileID] = None,
        *,
        metadata: Optional[MetadataContainerAPI] = None,
        remove_sidecar: bool = True,
        file_container: Optional["SingleFileAPI"] = None,
    ) -> bool:
        """
        Delete a stored file by URL, metadata lookup, or existing file container.

        This is for deleting a single file by id.
        :param file_url:
        :param metadata:
        :param remove_sidecar:
        :param file_container:
        :return:
        """

    @abc.abstractmethod
    def purge_files(self, file_hash: str) -> bool:
        """
        Remove every file with the given hash from the system (or mark for delete when we can't).

        :param file_hash:
        :return:
        """

    @abc.abstractmethod
    def iter_files(self) -> Iterator["SingleFileAPI"]:
        """
        Iter the files available to this manager.

        This will, probably, be a very slow operatoin.
        :return:
        """

    @abc.abstractmethod
    def iter_replication_clusters(self) -> Iterator["ReplicationCluster"]:
        """
        Iter all the replication clusters available to this manager.

        :return:
        """

    # ------------------
    # - EXTERNAL CHANGES

    # Note external changes to the database
    @abc.abstractmethod
    def mark_file_as_deleted(
        self,
        file_url: Union[str, FileID],
    ) -> None:
        """
        Note that the file has been externally deleted.

        :param file_url:
        :return:
        """

    @abc.abstractmethod
    def mark_file_as_changed(self, file_url: Union[str, FileID]) -> None:
        """
        Note that the file has been externally changed.

        This can trigger a number of events - including replication and backup.
        :param file_url:
        :return:
        """

    @abc.abstractmethod
    def mark_file_as_added(
        self,
        file_url: str) -> bool:
        """
        A file has been added to a monitored store.

        :param file_url:
        :return:
        """

    # ------------------



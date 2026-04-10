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
    from LiuXin_alpha.storage.storage_types import StoreID, DigitalAssetID
    from LiuXin_alpha.storage.api.info_containers_api import DigitalAssetReplicationCluster


class StorageManagerAPI(abc.ABC):
    """
    Contract for the storage manager/front-end.

    This is the user-facing storage API.
    It coordinates multiple stores and hides physical placement details from callers.
    """
    db: "DatabaseAPI"

    def __init__(self, db: "DatabaseAPI") -> None:
        self.db = db

    @abc.abstractmethod
    def add_file(
        self,
        file_bytes: bytes,
        metadata: Optional[MetadataContainerAPI] = None,
        *,
        preferred_store: Optional[str] = None,
    ) -> "SingleFileAPI":
        """
        Store bytes in managed storage.

        For now the storage manager still exposes a file-oriented verb here, but
        the durable managed identity created by this operation is a digital asset.
        """

    @abc.abstractmethod
    def update_file(
        self,
        digital_asset_identifier: Union[str, "DigitalAssetID"],
        file_bytes: bytes,
        *,
        update_replicant_pool: bool = False) -> bool:
        """
        Write an updated set of bytes out to one managed digital asset.
        """

    @abc.abstractmethod
    def retrieve_file(
        self,
        digital_asset_locator: Optional[str] = None,
        *,
        metadata: Optional[MetadataContainerAPI] = None,
        preferred_store: Optional[str] = None,
    ) -> "SingleFileAPI":
        """Return a file handle/container for a stored digital asset payload."""

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
        digital_asset_identifier: Union[str, "DigitalAssetID", None] = None,
        *,
        metadata: Optional[MetadataContainerAPI] = None,
        remove_sidecar: bool = True,
        file_container: Optional["SingleFileAPI"] = None,
    ) -> bool:
        """
        Delete one managed digital asset by identifier, metadata lookup, or existing file container.
        """

    @abc.abstractmethod
    def delete_replication_cluster(
        self,
        digital_asset_identifier: Union[str, "DigitalAssetID", None] = None,
        *,
        metadata: Optional[MetadataContainerAPI] = None,
        remove_sidecar: bool = True,
        file_container: Optional["SingleFileAPI"] = None,
    ) -> bool:
        """
        Delete every managed digital asset in the resolved replication cluster.
        """

    @abc.abstractmethod
    def purge_files(self, digital_asset_hash: str) -> bool:
        """
        Remove every managed digital asset with the given content hash from the system.
        """

    @abc.abstractmethod
    def iter_files(self) -> Iterator["SingleFileAPI"]:
        """
        Iterate the file payloads available to this manager.

        This remains file-oriented for now even though the durable managed identity
        is a digital asset.
        """

    @abc.abstractmethod
    def iter_replication_clusters(self) -> Iterator["DigitalAssetReplicationCluster"]:
        """Iterate all managed digital asset replication clusters available to this manager."""

    @abc.abstractmethod
    def mark_digital_asset_as_deleted(
        self,
        digital_asset_identifier: Union[str, "DigitalAssetID"],
    ) -> None:
        """Note that a managed digital asset has been externally deleted."""

    @abc.abstractmethod
    def mark_digital_asset_as_changed(self, digital_asset_identifier: Union[str, "DigitalAssetID"]) -> None:
        """Note that a managed digital asset has been externally changed."""

    @abc.abstractmethod
    def mark_digital_asset_as_added(
        self,
        digital_asset_locator: str) -> bool:
        """Note that a managed digital asset has been externally added to a monitored store."""

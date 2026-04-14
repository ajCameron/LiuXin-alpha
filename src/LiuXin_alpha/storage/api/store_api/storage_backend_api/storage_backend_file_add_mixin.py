from __future__ import annotations

import abc
from typing import Optional, TYPE_CHECKING

from LiuXin_alpha.storage.api import SingleFileAPI

if TYPE_CHECKING:
    from LiuXin_alpha.metadata.api import MetadataContainerAPI



class StoreBackendAddFilesAPI(abc.ABC):
    """
    Adding files to a storage backend.
    """

    def add_file(
        self,
        file_bytes: bytes,
        *,
        metadata = None,
        url: Optional[str] = None) -> "SingleFileAPI":
        """
        Write a file out to the store.

        :param file_bytes:
        :param metadata:
        :param url:

        :return:
        """
        raise PermissionError("This store does not support writing files.")

    def put_replica(
        self,
        file_bytes: bytes,
        *,
        storage_key: Optional[str] = None,
        metadata: Optional[MetadataContainerAPI] = None,
        add_sidecar_opf: bool = False,
    ) -> "SingleFileAPI":
        """
        Write a file/replica out to the store.

        :param file_bytes:
        :param storage_key:
        :param metadata:
        :param add_sidecar_opf:
        :return:
        """
        raise PermissionError("This store does not support writing replicas.")

    def dupe_file_in_store(self, src_file_url: str, dst_file_url: str) -> "SingleFileAPI":
        """
        Create a duplicate of a file in a store and return the new file.

        :param src_file_url:
        :param dst_file_url:
        :return:
        """
        raise PermissionError("This store does not support writing.")

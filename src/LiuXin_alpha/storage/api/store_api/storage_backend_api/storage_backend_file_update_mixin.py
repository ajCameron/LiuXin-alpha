from __future__ import annotations

import abc
from typing import Union

from LiuXin_alpha.storage.api import StoreLocationMixinAPI, SingleFileAPI


class StoreBackendUpdateFilesAPI(abc.ABC):
    """
    Update files within a store.

    Use with care - this can cause data destruction.
    """

    def update_file(
            self, storage_key: str,
            file_bytes: bytes,
            append: bool = False) -> bool:
        """
        Update a file within a store.

        :param storage_key: The location of the file to update.
        :param file_bytes:
        :param append:
        :return:
        """
        raise PermissionError("This store does not support file updates.")


    def update_replica(
            self,
            storage_key: str,
            file_bytes: bytes) -> bool:
        """
        Update a replica within the store.

        :param storage_key:
        :param file_bytes:
        :return:
        """
        raise PermissionError("This store does not support replica updates.")

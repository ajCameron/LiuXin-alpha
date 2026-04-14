from __future__ import annotations

import abc
from typing import Union

from LiuXin_alpha.storage.api import StoreLocationMixinAPI


class StoreBackendDeleteFiles(abc.ABC):
    """
    Delete a file from the store.
    """

    def delete_file(self, file_url: Union[str, "StoreLocationMixinAPI"]) -> bool:
        """
        Delete a file from the store.

        :param file_url:
        :return:
        """


    def delete_replica(self, storage_key: str) -> bool:
        """
        Delete a replica from the store.

        :param storage_key:
        :return:
        """
        raise PermissionError("This store does not support replica deletion.")

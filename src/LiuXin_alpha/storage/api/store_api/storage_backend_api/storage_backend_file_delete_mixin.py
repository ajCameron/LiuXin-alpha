"""Compatibility methods for deleting files from a raw store.

Examples:
    Delete a writable backend location::

        removed = backend.delete_file(location)
"""

from __future__ import annotations

import abc
from typing import Union

from LiuXin_alpha.storage.api import StoreLocationMixinAPI


class StoreBackendDeleteFiles(abc.ABC):
    """
    Delete a file from the store.

    Examples:
        Delete by URL or by an existing location handle::

            removed = backend.delete_file(location)
    """

    def delete_file(self, file_url: Union[str, "StoreLocationMixinAPI"]) -> bool:
        """
        Delete a file from the store.

        :param file_url:
        :return:

        Examples:
            Remove one backend-local file::

                removed = backend.delete_file("authors/book.epub")
        """


    def delete_replica(self, storage_key: str) -> bool:
        """
        Delete a replica from the store.

        :param storage_key:
        :return:

        Examples:
            Delete a replica by its durable storage key::

                removed = backend.delete_replica("authors/book.epub")
        """
        raise PermissionError("This store does not support replica deletion.")

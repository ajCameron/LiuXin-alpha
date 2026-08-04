"""Compatibility methods for reading files from a raw store.

Examples:
    Resolve a legacy backend URL to a location handle::

        location = backend.get_file("authors/book.epub")
"""

from __future__ import annotations

import abc
from typing import Union

from LiuXin_alpha.storage.api.location_api import StoreLocationMixinAPI


class StoreBackendReadFilesAPI(abc.ABC):
    """Read concrete files/replicas from one store.

    Examples:
        Read bytes through the returned backend-neutral location::

            payload = backend.get_file("book.epub").read_bytes()
    """

    @abc.abstractmethod
    def get_file(
        self,
        file_url: Union[str, StoreLocationMixinAPI],
    ) -> StoreLocationMixinAPI:
        """Return a concrete location handle for one file in this store.

        Examples:
            Resolve an existing location without copying its bytes::

                location = backend.get_file("authors/book.epub")
        """

    def get_url(self, file_url: str) -> StoreLocationMixinAPI:
        """Compatibility alias for resolving a file URL.

        Examples:
            Resolve a URL used by older callers::

                location = backend.get_url("authors/book.epub")
        """
        return self.get_file(file_url)

    def get_replica(self, replica_url: str) -> StoreLocationMixinAPI:
        """Compatibility alias for resolving a replica URL.

        Examples:
            Resolve a stored replica::

                location = backend.get_replica("authors/book.epub")
        """
        return self.get_file(replica_url)

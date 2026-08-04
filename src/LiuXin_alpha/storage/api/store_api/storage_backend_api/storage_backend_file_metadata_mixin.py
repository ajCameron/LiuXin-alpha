"""Compatibility methods for inspecting raw-store metadata.

Examples:
    Build an inventory from location handles::

        locations = list(backend.true_files())
"""

from __future__ import annotations

import abc
from typing import Union, Optional, Iterator

from LiuXin_alpha.storage.api.location_api import StoreLocationMixinAPI
from LiuXin_alpha.storage.single_file import SingleFileStatus


class StoreBackendMetadataAPI(abc.ABC):
    """Get metadata about files in the store and the store itself.

    Examples:
        Check a file before reading it::

            if backend.file_exists("book.epub"):
                size = backend.file_size("book.epub")
    """

    @property
    @abc.abstractmethod
    def root_path(self):
        """Return the root path/location of the store.

        Examples:
            Display the physical endpoint::

                root = backend.root_path
        """

    @abc.abstractmethod
    def location(self, *tokens: str) -> StoreLocationMixinAPI:
        """Construct the location for an entry in the store.

        Examples:
            Address a portable nested key::

                location = backend.location("authors", "book.epub")
        """

    @abc.abstractmethod
    def file_exists(self, file_url: Union[str, StoreLocationMixinAPI]) -> bool:
        """Return whether a file exists at the selected location.

        Examples:
            Check by storage key::

                present = backend.file_exists("authors/book.epub")
        """
        ...

    @abc.abstractmethod
    def file_size(self, file_url: Union[str, StoreLocationMixinAPI]) -> Optional[int]:
        """Return a file's byte size when available.

        Examples:
            Query the size through a location handle::

                size = backend.file_size(location)
        """
        ...

    @abc.abstractmethod
    def get_file_status(self, file_url: Union[str, StoreLocationMixinAPI]) -> SingleFileStatus:
        """Return current backend metadata for one file.

        Examples:
            Refresh cached hash and size data::

                status = backend.get_file_status(location)
        """
        ...

    @abc.abstractmethod
    def true_files(self) -> Iterator[StoreLocationMixinAPI]:
        """Iterate over concrete file locations, excluding virtual folders.

        Examples:
            Inventory every stored file::

                files = list(backend.true_files())
        """
        ...

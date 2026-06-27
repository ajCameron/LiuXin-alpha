from __future__ import annotations

import abc
from typing import Union, Optional, Iterator

from LiuXin_alpha.storage.api.location_api import StoreLocationMixinAPI
from LiuXin_alpha.storage.single_file import SingleFileStatus


class StoreBackendMetadataAPI(abc.ABC):
    """Get metadata about files in the store and the store itself."""

    @property
    @abc.abstractmethod
    def root_path(self):
        """Return the root path/location of the store."""

    @abc.abstractmethod
    def location(self, *tokens: str) -> StoreLocationMixinAPI:
        """Construct the location for an entry in the store."""

    @abc.abstractmethod
    def file_exists(self, file_url: Union[str, StoreLocationMixinAPI]) -> bool:
        ...

    @abc.abstractmethod
    def file_size(self, file_url: Union[str, StoreLocationMixinAPI]) -> Optional[int]:
        ...

    @abc.abstractmethod
    def get_file_status(self, file_url: Union[str, StoreLocationMixinAPI]) -> SingleFileStatus:
        ...

    @abc.abstractmethod
    def true_files(self) -> Iterator[StoreLocationMixinAPI]:
        ...

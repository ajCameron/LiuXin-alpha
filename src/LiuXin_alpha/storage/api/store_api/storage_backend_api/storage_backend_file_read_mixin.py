from __future__ import annotations

import abc
from typing import Union

from LiuXin_alpha.storage.api.location_api import StoreLocationMixinAPI


class StoreBackendReadFilesAPI(abc.ABC):
    """Read concrete files/replicas from one store."""

    @abc.abstractmethod
    def get_file(
        self,
        file_url: Union[str, StoreLocationMixinAPI],
    ) -> StoreLocationMixinAPI:
        """Return a concrete location handle for one file in this store."""

    def get_url(self, file_url: str) -> StoreLocationMixinAPI:
        return self.get_file(file_url)

    def get_replica(self, replica_url: str) -> StoreLocationMixinAPI:
        return self.get_file(replica_url)

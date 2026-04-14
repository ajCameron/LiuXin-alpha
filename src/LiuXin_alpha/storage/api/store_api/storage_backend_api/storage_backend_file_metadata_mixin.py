from __future__ import annotations

import abc
from typing import Union, Optional, Iterator

from LiuXin_alpha.storage.api import StoreLocationMixinAPI, SingleFileAPI
from LiuXin_alpha.storage.single_file import SingleFileStatus


class StoreBackendMetadataAPI(abc.ABC):
    """
    Get metadata about files in the store and the store itself.
    """
    @property
    @abc.abstractmethod
    def root_path(self) -> "StoreLocationMixinAPI":
        """
        Return the root path of the store.

        :return:
        """

    @abc.abstractmethod
    def location(self, *tokens: str) -> "StoreLocationMixinAPI":
        """
        Construct the location for an entry in the store.

        :param tokens:
        :return:
        """

    @abc.abstractmethod
    def file_exists(self, file_url: Union[str, "StoreLocationMixinAPI"]) -> bool:
        """
        Check that the file exists.

        :param file_url:
        :return:
        """

    @abc.abstractmethod
    def file_size(self, file_url: Union[str, "StoreLocationMixinAPI"]) -> Optional[int]:
        """
        Directly check the file size.

        :param file_url:
        :return:
        """

    @abc.abstractmethod
    def get_file_status(self, file_url: Union[str, "StoreLocationMixinAPI"]) -> "SingleFileStatus":
        """
        Get the file status.

        :param file_url:
        :return:
        """


    @abc.abstractmethod
    def true_files(self) -> Iterator["SingleFileAPI"]:
        """
        Iterate over the actual files which exist in this store.

        :return:
        """

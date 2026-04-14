from __future__ import annotations

import abc
from typing import Union

from LiuXin_alpha.storage.api import StoreLocationMixinAPI, SingleFileAPI


class StoreBackendReadFilesAPI(abc.ABC):

    @abc.abstractmethod
    def get_file(
            self,
            file_url: Union[str, "StoreLocationMixinAPI"]) -> "SingleFileAPI":
        """
        Get the file in the form of a single file API.

        :param file_url:
        :return:
        """

    @abc.abstractmethod
    def get_url(self, file_url: str) -> "SingleFileAPI":
        """
        Return a single file from a URL.

        :param file_url:
        :return:
        """

    @abc.abstractmethod
    def get_file_content(
            self,
            file_url: Union[str, "StoreLocationMixinAPI"]) -> bytes:
        """
        Get the contents of the file in the form of bytes.

        :param file_url:
        :return:
        """

"""Configured read-only SquashFS archive Store."""

from __future__ import annotations

import pathlib

from typing import Optional
from uuid import UUID, uuid4

from LiuXin_alpha.storage.api import (
    DriverBackedStoreAPI,
    Location,
    StoreConfiguration,
)
from LiuXin_alpha.storage.drivers.squashfs import (
    SquashfsObjectAddress,
    SquashfsStorageDriver,
)
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name


class SquashfsReadOnlyStorageBackend(
    DriverBackedStoreAPI[SquashfsObjectAddress]
):
    """One immutable archive exposed through opaque internal paths."""

    store_kind = "squashfs_readonly"

    def __init__(
        self,
        url: str,
        name: Optional[str] = None,
        uuid: str | UUID | None = None,
        *,
        unsquashfs_exe: str = "unsquashfs",
        timeout_s: float = 60.0,
    ) -> None:
        store_uuid = uuid4() if uuid is None else (
            uuid if isinstance(uuid, UUID) else UUID(uuid)
        )
        self.__driver = SquashfsStorageDriver(
            url,
            address_space_uuid=store_uuid,
            unsquashfs_exe=unsquashfs_exe,
            timeout_s=timeout_s,
        )
        self._configuration = StoreConfiguration(
            store_uuid=store_uuid,
            store_name=name or self.url_to_name(str(self.__driver.archive_path)),
            store_kind=self.store_kind,
            store_root_uri=self.__driver.root_uri,
            store_url=self.__driver.root_uri,
            store_access_protocol="squashfs",
            read_only=True,
            supports_folders=True,
        )

    @property
    def configuration(self) -> StoreConfiguration:
        return self._configuration

    @property
    def _driver(self) -> SquashfsStorageDriver:
        return self.__driver

    @property
    def driver(self) -> SquashfsStorageDriver:
        return self.__driver

    @property
    def archive_path(self) -> pathlib.Path:
        return self.__driver.archive_path

    @property
    def db_path(self) -> pathlib.Path:
        return self.archive_path

    @property
    def root_path(self) -> pathlib.Path:
        return self.archive_path

    @staticmethod
    def url_to_name(url: str) -> str:
        return safe_path_to_name(url)

    def locate(self, identifier: str | Location) -> Location:
        """Accept an internal key or legacy ``archive-path/internal`` form."""

        if isinstance(identifier, Location):
            return self.require_location(identifier)
        text = str(identifier)
        legacy_prefix = str(self.archive_path) + "/"
        if text.startswith(legacy_prefix):
            text = text[len(legacy_prefix) :]
        return super().locate(text)

    def self_test(self):
        return self.probe()


__all__ = ["SquashfsReadOnlyStorageBackend"]

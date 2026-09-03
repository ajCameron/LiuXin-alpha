"""Configured Store facade for the transactional filesystem driver."""

from __future__ import annotations

import os

from pathlib import Path
from urllib.parse import unquote, urlparse
from uuid import UUID, uuid4

from LiuXin_alpha.storage.api import (
    DriverBackedStoreAPI,
    StoreConfiguration,
    StoreStatus,
)
from LiuXin_alpha.storage.drivers.filesystem import (
    FilesystemObjectAddress,
    FilesystemStorageDriver,
)


class FilesystemStore(DriverBackedStoreAPI[FilesystemObjectAddress]):
    """One configured local directory with staged, atomic publication."""

    store_kind = "filesystem"

    def __init__(
        self,
        url: str | os.PathLike[str],
        name: str | None = None,
        uuid: str | UUID | None = None,
        *,
        read_only: bool = False,
        create_root: bool = True,
        allocation_prefix: str = "objects",
        configuration: StoreConfiguration | None = None,
    ) -> None:
        root = _filesystem_path(url)
        store_uuid = (
            configuration.store_uuid
            if configuration is not None
            else _store_uuid(uuid)
        )
        self._configuration = configuration or StoreConfiguration(
            store_uuid=store_uuid,
            store_name=name or root.name or "filesystem",
            store_kind=self.store_kind,
            store_root_uri=root.resolve(strict=False).as_uri(),
            read_only=read_only,
        )
        self.__driver = FilesystemStorageDriver(
            root,
            address_space_uuid=self._configuration.store_uuid,
            read_only=self._configuration.read_only,
            create_root=create_root,
            allocation_prefix=allocation_prefix,
        )

    @property
    def configuration(self) -> StoreConfiguration:
        return self._configuration

    @property
    def _driver(self) -> FilesystemStorageDriver:
        return self.__driver

    @property
    def driver(self) -> FilesystemStorageDriver:
        """Expose the reusable driver for import and diagnostic workflows."""

        return self.__driver

    @property
    def root_path(self) -> Path:
        return self.__driver.root_path

    @classmethod
    def from_configuration(
        cls,
        configuration: StoreConfiguration,
    ) -> FilesystemStore:
        return cls(
            configuration.store_root_uri,
            configuration=configuration,
            create_root=not configuration.read_only,
        )

    def self_test(self) -> StoreStatus:
        """Actively probe this Store using the standard lifecycle contract."""

        return self.probe()

    @staticmethod
    def url_to_name(url: str) -> str:
        return _filesystem_path(url).name or "filesystem"


def _store_uuid(value: str | UUID | None) -> UUID:
    if value is None:
        return uuid4()
    return value if isinstance(value, UUID) else UUID(value)


def _filesystem_path(value: str | os.PathLike[str]) -> Path:
    if isinstance(value, os.PathLike):
        return Path(value).expanduser()
    parsed = urlparse(value)
    if parsed.scheme == "file":
        if parsed.netloc not in ("", "localhost"):
            raise ValueError("file Store URLs must refer to the local host.")
        return Path(unquote(parsed.path)).expanduser()
    if parsed.scheme:
        raise ValueError(f"filesystem Store requires a path or file URI: {value!r}")
    return Path(value).expanduser()


__all__ = ["FilesystemStore"]

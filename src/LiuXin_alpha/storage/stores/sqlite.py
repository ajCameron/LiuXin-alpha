"""Configured Store facade for the transactional SQLite BLOB driver."""

from __future__ import annotations

import dataclasses
import os

from pathlib import Path
from urllib.parse import unquote, urlparse
from uuid import UUID, uuid4

from LiuXin_alpha.storage.api import (
    DriverBackedStoreAPI,
    IngestObjectDelivery,
    IngestSourceCapabilities,
    StoreConfiguration,
)
from LiuXin_alpha.storage.drivers.sqlite import SQLiteObjectAddress, SQLiteStorageDriver


class SQLiteStore(DriverBackedStoreAPI[SQLiteObjectAddress]):
    """One durable SQLite BLOB container exposed as a configured Store."""

    store_kind = "sqlite"

    def __init__(
        self,
        url: str | os.PathLike[str],
        name: str | None = None,
        uuid: str | UUID | None = None,
    ) -> None:
        path = _sqlite_path(url)
        store_uuid = uuid4() if uuid is None else (
            uuid if isinstance(uuid, UUID) else UUID(uuid)
        )
        self._configuration = StoreConfiguration(
            store_uuid,
            name or path.stem or "sqlite",
            self.store_kind,
            path.resolve(strict=False).as_uri(),
            supports_folders=False,
        )
        self.__driver = SQLiteStorageDriver(
            path,
            address_space_uuid=store_uuid,
        )
        self.__driver.startup()

    @property
    def configuration(self) -> StoreConfiguration:
        return self._configuration

    @property
    def _driver(self) -> SQLiteStorageDriver:
        return self.__driver

    @property
    def driver(self) -> SQLiteStorageDriver:
        return self.__driver

    @property
    def db_path(self) -> Path:
        return self.__driver.db_path

    @property
    def root_path(self) -> Path:
        return self.db_path

    @property
    def ingest_capabilities(self) -> IngestSourceCapabilities:
        """Advertise SQLite's buffered reads and authoritative SHA-256.

        Example:
            >>> store.ingest_capabilities.object_delivery  # doctest: +SKIP
            <IngestObjectDelivery.MEMORY_BUFFERED: 'memory_buffered'>

        :return: SQLite-specific source-ingest capability profile.
        """

        return dataclasses.replace(
            super().ingest_capabilities,
            object_delivery=IngestObjectDelivery.MEMORY_BUFFERED,
            authoritative_digest_algorithms=("sha256",),
        )


def _sqlite_path(value: str | os.PathLike[str]) -> Path:
    if isinstance(value, os.PathLike):
        return Path(value).expanduser()
    parsed = urlparse(value)
    if parsed.scheme == "file":
        return Path(unquote(parsed.path)).expanduser()
    if parsed.scheme:
        raise ValueError("SQLite Store requires a path or file URI.")
    return Path(value).expanduser()


__all__ = ["SQLiteStore"]

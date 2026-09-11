"""
Create configured Stores backed by transactional SQLite BLOB containers.

SQLiteStore starts its raw driver during construction. The facade keeps Store
identity separate from raw object addresses and advertises SQLite-specific
buffering and checksum behavior to ingest callers.
"""

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
    """
    Expose one SQLite BLOB container as a configured Store.

    Construction immediately starts the owned driver and may create or open the database. Reads are
    advertised as memory-buffered, with authoritative SHA-256 available through the source-ingest
    capability profile.

    Example:
        >>> store = SQLiteStore(database_path)  # doctest: +SKIP
        >>> store.close()  # doctest: +SKIP
    """

    store_kind = "sqlite"

    def __init__(
        self,
        url: str | os.PathLike[str],
        name: str | None = None,
        uuid: str | UUID | None = None,
    ) -> None:
        """
        Resolve the container path, create configuration, and start the SQLite driver.

        The generated Store has no folder support. Driver startup failures propagate from
        construction, which can already have opened or created the database.

        Example:
            >>> store = SQLiteStore("/srv/library/objects.sqlite", name="objects")  # doctest: +SKIP


        :param url: Local database path or file URI; the helper uses the decoded URI path.
        :param name: Display name; None or empty uses the path stem, then "sqlite" if empty.
        :param uuid: UUID or parseable UUID text to retain, or None to generate one.
        :return: None after the raw container driver has started successfully.
        """
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
        """
        Return the configuration created for the SQLite container.

        Example:
            >>> store.configuration.supports_folders  # doctest: +SKIP
            False


        :return: Retained configuration containing the Store UUID and resolved file URI.
        """
        return self._configuration

    @property
    def _driver(self) -> SQLiteStorageDriver:
        """
        Supply the started SQLite driver to inherited Store operations.

        Example:
            >>> store._driver is store.driver  # doctest: +SKIP
            True


        :return: The same owned driver exposed by the public driver property.
        """
        return self.__driver

    @property
    def driver(self) -> SQLiteStorageDriver:
        """
        Expose the raw SQLite container driver without creating another connection owner.

        Example:
            >>> driver = store.driver  # doctest: +SKIP


        :return: Existing raw driver; callers share its lifecycle with this Store.
        """
        return self.__driver

    @property
    def db_path(self) -> Path:
        """
        Return the expanded, resolved SQLite container filename held by the driver.

        Example:
            >>> store.db_path.is_absolute()  # doctest: +SKIP
            True


        :return: Absolute database Path; the property performs no new I/O.
        """
        return self.__driver.db_path

    @property
    def root_path(self) -> Path:
        """
        Provide the database filename through the common root_path convenience name.

        Example:
            >>> store.root_path == store.db_path  # doctest: +SKIP
            True


        :return: The db_path value, not a directory containing object files.
        """
        return self.db_path

    @property
    def ingest_capabilities(self) -> IngestSourceCapabilities:
        """
        Advertise buffered object delivery and authoritative SHA-256 for SQLite ingest.

        Other source capabilities, including range and version behavior, are retained from the
        driver-backed profile rather than independently inferred here.

        Example:
            >>> store.ingest_capabilities.object_delivery  # doctest: +SKIP
            <IngestObjectDelivery.MEMORY_BUFFERED: 'memory_buffered'>


        :return: Inherited profile with MEMORY_BUFFERED delivery and the sha256 algorithm tuple.
        """

        return dataclasses.replace(
            super().ingest_capabilities,
            object_delivery=IngestObjectDelivery.MEMORY_BUFFERED,
            authoritative_digest_algorithms=("sha256",),
        )


def _sqlite_path(value: str | os.PathLike[str]) -> Path:
    """
    Expand a local path or the decoded path component of a file URI.

    PathLike inputs bypass URI parsing. Non-file schemes raise ValueError. Unlike the filesystem
    Store parser, this helper does not validate a file URI authority; the authority, query, and
    fragment are ignored.

    Example:
        >>> _sqlite_path("file:///srv/my%20books.sqlite").name
        'my books.sqlite'


    :param value: Path-like value or path/file-URI text selecting the database filename.
    :return: Expanded Path without resolution, creation, or connection attempts.
    """
    if isinstance(value, os.PathLike):
        return Path(value).expanduser()
    parsed = urlparse(value)
    if parsed.scheme == "file":
        return Path(unquote(parsed.path)).expanduser()
    if parsed.scheme:
        raise ValueError("SQLite Store requires a path or file URI.")
    return Path(value).expanduser()


__all__ = ["SQLiteStore"]

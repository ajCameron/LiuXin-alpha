"""
Configured Store facades for local ZIP, TAR, RAR, and 7z archive drivers.
"""

from __future__ import annotations

import pathlib

from collections.abc import Iterable
from uuid import UUID, uuid4

from LiuXin_alpha.storage.api import (
    DriverBackedStoreAPI,
    Location,
    StoreConfiguration,
)
from LiuXin_alpha.storage.drivers.archive_common import (
    ArchiveObjectAddress,
    DEFAULT_MAX_ARCHIVE_DEPTH,
    DEFAULT_MAX_ARCHIVE_INVENTORY_ENTRIES,
)
from LiuXin_alpha.storage.drivers.rar import (
    DEFAULT_MAX_RAR_COMPRESSION_RATIO,
    DEFAULT_MAX_RAR_MEMBER_BYTES,
    DEFAULT_MAX_RAR_PATH_BYTES,
    DEFAULT_MAX_RAR_TOTAL_UNCOMPRESSED_BYTES,
    DEFAULT_RAR_EXTRACT_TIMEOUT_S,
    RarStorageDriver,
)
from LiuXin_alpha.storage.drivers.sevenzip import (
    DEFAULT_MAX_SEVENZIP_COMPRESSION_RATIO,
    DEFAULT_MAX_SEVENZIP_HEADER_BYTES,
    DEFAULT_MAX_SEVENZIP_MEMBER_BYTES,
    DEFAULT_MAX_SEVENZIP_PATH_BYTES,
    DEFAULT_MAX_SEVENZIP_TOTAL_UNCOMPRESSED_BYTES,
    SevenZipStorageDriver,
)
from LiuXin_alpha.storage.drivers.tar import (
    DEFAULT_MAX_TAR_COMPRESSION_RATIO,
    DEFAULT_MAX_TAR_MEMBER_BYTES,
    DEFAULT_MAX_TAR_METADATA_BYTES,
    DEFAULT_MAX_TAR_SINGLE_METADATA_RECORD_BYTES,
    DEFAULT_MAX_TAR_TOTAL_UNCOMPRESSED_BYTES,
    TarStorageDriver,
    WritableTarStorageDriver,
)
from LiuXin_alpha.storage.drivers.zip import (
    DEFAULT_MAX_ZIP_CENTRAL_DIRECTORY_BYTES,
    DEFAULT_MAX_ZIP_COMPRESSION_RATIO,
    DEFAULT_MAX_ZIP_MEMBER_BYTES,
    DEFAULT_MAX_ZIP_TOTAL_UNCOMPRESSED_BYTES,
    WritableZipStorageDriver,
    ZipStorageDriver,
)
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name


class _ConfiguredArchiveStore(DriverBackedStoreAPI[ArchiveObjectAddress]):
    """
    Bind one local archive driver to durable Store identity and policy.

    Example:
        >>> store.archive_path  # doctest: +SKIP
    """

    def __init__(
        self,
        driver,
        *,
        store_kind: str,
        access_protocol: str,
        name: str | None,
        configuration: StoreConfiguration | None,
        read_only: bool,
        backend_options: Iterable[tuple[str, object]],
    ) -> None:
        """
        Bind a configured archive driver to one durable Store facade.

        Example:
            >>> store = _ConfiguredArchiveStore(driver, store_kind="zip_readonly", access_protocol="zip", name=None, configuration=None, read_only=True, backend_options=())  # doctest: +SKIP


        :param driver:
        :param store_kind:
        :param access_protocol:
        :param name:
        :param configuration:
        :param read_only:
        :param backend_options:
        :return:
        """

        self.__driver = driver
        self._configuration = configuration or StoreConfiguration(
            store_uuid=driver.object_address_checker.address_space_uuid,
            store_name=name or safe_path_to_name(str(driver.archive_path)),
            store_kind=store_kind,
            store_root_uri=driver.root_uri,
            store_url=driver.root_uri,
            store_access_protocol=access_protocol,
            read_only=read_only,
            supports_folders=True,
            backend_options=tuple(backend_options),
        )

    @property
    def configuration(self) -> StoreConfiguration:
        """
        Return the durable Store configuration used to build this facade.

        Example:
            >>> store.configuration.store_kind  # doctest: +SKIP
            'zip_readonly'


        :return:
        """

        return self._configuration

    @property
    def _driver(self):
        """
        Supply the archive driver to the generic Store bridge.

        Example:
            >>> store._driver is store.driver  # doctest: +SKIP
            True


        :return:
        """

        return self.__driver

    @property
    def driver(self):
        """
        Return the reusable archive driver for diagnostics.

        Example:
            >>> store.driver  # doctest: +SKIP


        :return:
        """

        return self.__driver

    @property
    def archive_path(self) -> pathlib.Path:
        """
        Return the resolved local archive path.

        Example:
            >>> store.archive_path  # doctest: +SKIP


        :return:
        """

        return self.__driver.archive_path

    @property
    def image_path(self) -> pathlib.Path:
        """
        Return the archive path through the image-backed Store alias.

        Example:
            >>> store.image_path == store.archive_path  # doctest: +SKIP
            True


        :return:
        """

        return self.archive_path

    @property
    def db_path(self) -> pathlib.Path:
        """
        Return the archive through the legacy single-file Store alias.

        Example:
            >>> store.db_path == store.archive_path  # doctest: +SKIP
            True


        :return:
        """

        return self.archive_path

    @property
    def root_path(self) -> pathlib.Path:
        """
        Return the archive through the legacy root-path alias.

        Example:
            >>> store.root_path == store.archive_path  # doctest: +SKIP
            True


        :return:
        """

        return self.archive_path

    def locate(self, identifier: str | Location) -> Location:
        """
        Resolve a member key, including the legacy archive-path prefix form.

        Example:
            >>> store.locate("books/novel.epub").key  # doctest: +SKIP
            'books/novel.epub'


        :param identifier:
        :return:
        """

        if isinstance(identifier, Location):
            return self.require_location(identifier)
        text = str(identifier)
        legacy_prefix = str(self.archive_path) + "/"
        if text.startswith(legacy_prefix):
            text = text[len(legacy_prefix) :]
        return super().locate(text)

    def self_test(self):
        """
        Run the Store probe used by older plugin consumers.

        Example:
            >>> store.self_test().available  # doctest: +SKIP
            True


        :return:
        """

        return self.probe()


def _store_uuid(
    uuid: str | UUID | None,
    configuration: StoreConfiguration | None,
) -> UUID:
    """
    Reconcile an explicit UUID with a durable configuration identity.

    Example:
        >>> value = _store_uuid(UUID(int=1), None)
        >>> value.int
        1


    :param uuid:
    :param configuration:
    :return:
    """

    if configuration is not None:
        configured = configuration.store_uuid
        if uuid is not None and UUID(str(uuid)) != configured:
            raise ValueError("configuration and explicit uuid identify different Stores.")
        return configured
    return uuid4() if uuid is None else uuid if isinstance(uuid, UUID) else UUID(uuid)


class ZipReadOnlyStorageBackend(_ConfiguredArchiveStore):
    """
    Expose one ZIP archive as an immutable Store.

    Example:
        >>> store = ZipReadOnlyStorageBackend("books.zip")  # doctest: +SKIP
    """

    store_kind = "zip_readonly"

    def __init__(
        self,
        url: str,
        name: str | None = None,
        uuid: str | UUID | None = None,
        *,
        configuration: StoreConfiguration | None = None,
        max_inventory_entries: int = DEFAULT_MAX_ARCHIVE_INVENTORY_ENTRIES,
        max_member_bytes: int = DEFAULT_MAX_ZIP_MEMBER_BYTES,
        max_depth: int = DEFAULT_MAX_ARCHIVE_DEPTH,
        max_total_uncompressed_bytes: int = DEFAULT_MAX_ZIP_TOTAL_UNCOMPRESSED_BYTES,
        max_compression_ratio: float = DEFAULT_MAX_ZIP_COMPRESSION_RATIO,
        max_central_directory_bytes: int = DEFAULT_MAX_ZIP_CENTRAL_DIRECTORY_BYTES,
    ) -> None:
        """
        Configure an immutable ZIP Store with durable safety limits.

        Example:
            >>> store = ZipReadOnlyStorageBackend("books.zip")  # doctest: +SKIP


        :param url:
        :param name:
        :param uuid:
        :param configuration:
        :param max_inventory_entries:
        :param max_member_bytes:
        :param max_depth:
        :param max_total_uncompressed_bytes:
        :param max_compression_ratio:
        :param max_central_directory_bytes:
        :return:
        """

        driver = ZipStorageDriver(
            url,
            address_space_uuid=_store_uuid(uuid, configuration),
            max_inventory_entries=max_inventory_entries,
            max_member_bytes=max_member_bytes,
            max_depth=max_depth,
            max_total_uncompressed_bytes=max_total_uncompressed_bytes,
            max_compression_ratio=max_compression_ratio,
            max_central_directory_bytes=max_central_directory_bytes,
        )
        super().__init__(
            driver,
            store_kind=self.store_kind,
            access_protocol="zip",
            name=name,
            configuration=configuration,
            read_only=True,
            backend_options=(
                ("max_inventory_entries", int(max_inventory_entries)),
                ("max_member_bytes", int(max_member_bytes)),
                ("max_depth", int(max_depth)),
                ("max_total_uncompressed_bytes", int(max_total_uncompressed_bytes)),
                ("max_compression_ratio", float(max_compression_ratio)),
                ("max_central_directory_bytes", int(max_central_directory_bytes)),
            ),
        )


class ZipWritableStorageBackend(_ConfiguredArchiveStore):
    """
    Expose one atomically rebuilt ZIP archive as a writable Store.

    Example:
        >>> store = ZipWritableStorageBackend("books.zip")  # doctest: +SKIP
    """

    store_kind = "zip_writable"

    def __init__(
        self,
        url: str,
        name: str | None = None,
        uuid: str | UUID | None = None,
        *,
        configuration: StoreConfiguration | None = None,
        create_archive: bool | None = None,
        compression: str = "deflated",
        compresslevel: int | None = None,
        deterministic: bool = False,
        allow_lossy_rebuild: bool = False,
        allocation_prefix: str = "objects",
        max_inventory_entries: int = DEFAULT_MAX_ARCHIVE_INVENTORY_ENTRIES,
        max_member_bytes: int = DEFAULT_MAX_ZIP_MEMBER_BYTES,
        max_depth: int = DEFAULT_MAX_ARCHIVE_DEPTH,
        max_total_uncompressed_bytes: int = DEFAULT_MAX_ZIP_TOTAL_UNCOMPRESSED_BYTES,
        max_compression_ratio: float = DEFAULT_MAX_ZIP_COMPRESSION_RATIO,
        max_central_directory_bytes: int = DEFAULT_MAX_ZIP_CENTRAL_DIRECTORY_BYTES,
    ) -> None:
        """
        Configure an atomically rebuilt ZIP Store and persist its policy.

        Example:
            >>> store = ZipWritableStorageBackend("books.zip")  # doctest: +SKIP


        :param url:
        :param name:
        :param uuid:
        :param configuration:
        :param create_archive:
        :param compression:
        :param compresslevel:
        :param deterministic:
        :param allow_lossy_rebuild:
        :param allocation_prefix:
        :param max_inventory_entries:
        :param max_member_bytes:
        :param max_depth:
        :param max_total_uncompressed_bytes:
        :param max_compression_ratio:
        :param max_central_directory_bytes:
        :return:
        """

        effective_create = (
            configuration is None or not configuration.read_only
            if create_archive is None
            else bool(create_archive)
        )
        driver = WritableZipStorageDriver(
            url,
            address_space_uuid=_store_uuid(uuid, configuration),
            create_archive=effective_create,
            compression=compression,
            compresslevel=compresslevel,
            deterministic=deterministic,
            allow_lossy_rebuild=allow_lossy_rebuild,
            allocation_prefix=allocation_prefix,
            max_inventory_entries=max_inventory_entries,
            max_member_bytes=max_member_bytes,
            max_depth=max_depth,
            max_total_uncompressed_bytes=max_total_uncompressed_bytes,
            max_compression_ratio=max_compression_ratio,
            max_central_directory_bytes=max_central_directory_bytes,
        )
        options: list[tuple[str, object]] = [
            ("create_archive", effective_create),
            ("compression", str(compression)),
            ("deterministic", bool(deterministic)),
            ("allow_lossy_rebuild", bool(allow_lossy_rebuild)),
            ("allocation_prefix", str(allocation_prefix)),
            ("max_inventory_entries", int(max_inventory_entries)),
            ("max_member_bytes", int(max_member_bytes)),
            ("max_depth", int(max_depth)),
            ("max_total_uncompressed_bytes", int(max_total_uncompressed_bytes)),
            ("max_compression_ratio", float(max_compression_ratio)),
            ("max_central_directory_bytes", int(max_central_directory_bytes)),
        ]
        if compresslevel is not None:
            options.append(("compresslevel", int(compresslevel)))
        super().__init__(
            driver,
            store_kind=self.store_kind,
            access_protocol="zip-write",
            name=name,
            configuration=configuration,
            read_only=False,
            backend_options=options,
        )


class TarReadOnlyStorageBackend(_ConfiguredArchiveStore):
    """
    Expose one compressed or uncompressed TAR archive as an immutable Store.

    Example:
        >>> store = TarReadOnlyStorageBackend("books.tar.gz")  # doctest: +SKIP
    """

    store_kind = "tar_readonly"

    def __init__(
        self,
        url: str,
        name: str | None = None,
        uuid: str | UUID | None = None,
        *,
        configuration: StoreConfiguration | None = None,
        max_inventory_entries: int = DEFAULT_MAX_ARCHIVE_INVENTORY_ENTRIES,
        max_member_bytes: int = DEFAULT_MAX_TAR_MEMBER_BYTES,
        max_depth: int = DEFAULT_MAX_ARCHIVE_DEPTH,
        max_total_uncompressed_bytes: int = DEFAULT_MAX_TAR_TOTAL_UNCOMPRESSED_BYTES,
        max_compression_ratio: float = DEFAULT_MAX_TAR_COMPRESSION_RATIO,
        max_metadata_bytes: int = DEFAULT_MAX_TAR_METADATA_BYTES,
        max_single_metadata_record_bytes: int = DEFAULT_MAX_TAR_SINGLE_METADATA_RECORD_BYTES,
    ) -> None:
        """
        Configure an immutable TAR Store with durable safety limits.

        Example:
            >>> store = TarReadOnlyStorageBackend("books.tar.gz")  # doctest: +SKIP


        :param url:
        :param name:
        :param uuid:
        :param configuration:
        :param max_inventory_entries:
        :param max_member_bytes:
        :param max_depth:
        :return:
        """

        driver = TarStorageDriver(
            url,
            address_space_uuid=_store_uuid(uuid, configuration),
            max_inventory_entries=max_inventory_entries,
            max_member_bytes=max_member_bytes,
            max_depth=max_depth,
            max_total_uncompressed_bytes=max_total_uncompressed_bytes,
            max_compression_ratio=max_compression_ratio,
            max_metadata_bytes=max_metadata_bytes,
            max_single_metadata_record_bytes=max_single_metadata_record_bytes,
        )
        super().__init__(
            driver,
            store_kind=self.store_kind,
            access_protocol="tar",
            name=name,
            configuration=configuration,
            read_only=True,
            backend_options=(
                ("max_inventory_entries", int(max_inventory_entries)),
                ("max_member_bytes", int(max_member_bytes)),
                ("max_depth", int(max_depth)),
                ("max_total_uncompressed_bytes", int(max_total_uncompressed_bytes)),
                ("max_compression_ratio", float(max_compression_ratio)),
                ("max_metadata_bytes", int(max_metadata_bytes)),
                ("max_single_metadata_record_bytes", int(max_single_metadata_record_bytes)),
            ),
        )


class TarWritableStorageBackend(_ConfiguredArchiveStore):
    """
    Expose one atomically rebuilt TAR archive as a writable Store.

    Example:
        >>> store = TarWritableStorageBackend("books.tar.xz")  # doctest: +SKIP
    """

    store_kind = "tar_writable"

    def __init__(
        self,
        url: str,
        name: str | None = None,
        uuid: str | UUID | None = None,
        *,
        configuration: StoreConfiguration | None = None,
        create_archive: bool | None = None,
        compression: str | None = None,
        deterministic: bool = False,
        allow_lossy_rebuild: bool = False,
        allocation_prefix: str = "objects",
        max_inventory_entries: int = DEFAULT_MAX_ARCHIVE_INVENTORY_ENTRIES,
        max_member_bytes: int = DEFAULT_MAX_TAR_MEMBER_BYTES,
        max_depth: int = DEFAULT_MAX_ARCHIVE_DEPTH,
        max_total_uncompressed_bytes: int = DEFAULT_MAX_TAR_TOTAL_UNCOMPRESSED_BYTES,
        max_compression_ratio: float = DEFAULT_MAX_TAR_COMPRESSION_RATIO,
        max_metadata_bytes: int = DEFAULT_MAX_TAR_METADATA_BYTES,
        max_single_metadata_record_bytes: int = DEFAULT_MAX_TAR_SINGLE_METADATA_RECORD_BYTES,
    ) -> None:
        """
        Configure an atomically rebuilt TAR Store and persist its policy.

        Example:
            >>> store = TarWritableStorageBackend("books.tar.xz")  # doctest: +SKIP


        :param url:
        :param name:
        :param uuid:
        :param configuration:
        :param create_archive:
        :param compression:
        :param deterministic:
        :param allow_lossy_rebuild:
        :param allocation_prefix:
        :param max_inventory_entries:
        :param max_member_bytes:
        :param max_depth:
        :return:
        """

        selected_compression = _tar_compression_for_path(url) if compression is None else str(compression)
        effective_create = (
            configuration is None or not configuration.read_only
            if create_archive is None
            else bool(create_archive)
        )
        driver = WritableTarStorageDriver(
            url,
            address_space_uuid=_store_uuid(uuid, configuration),
            create_archive=effective_create,
            compression=selected_compression,
            deterministic=deterministic,
            allow_lossy_rebuild=allow_lossy_rebuild,
            allocation_prefix=allocation_prefix,
            max_inventory_entries=max_inventory_entries,
            max_member_bytes=max_member_bytes,
            max_depth=max_depth,
            max_total_uncompressed_bytes=max_total_uncompressed_bytes,
            max_compression_ratio=max_compression_ratio,
            max_metadata_bytes=max_metadata_bytes,
            max_single_metadata_record_bytes=max_single_metadata_record_bytes,
        )
        super().__init__(
            driver,
            store_kind=self.store_kind,
            access_protocol="tar-write",
            name=name,
            configuration=configuration,
            read_only=False,
            backend_options=(
                ("create_archive", effective_create),
                ("compression", selected_compression),
                ("deterministic", bool(deterministic)),
                ("allow_lossy_rebuild", bool(allow_lossy_rebuild)),
                ("allocation_prefix", str(allocation_prefix)),
                ("max_inventory_entries", int(max_inventory_entries)),
                ("max_member_bytes", int(max_member_bytes)),
                ("max_depth", int(max_depth)),
                ("max_total_uncompressed_bytes", int(max_total_uncompressed_bytes)),
                ("max_compression_ratio", float(max_compression_ratio)),
                ("max_metadata_bytes", int(max_metadata_bytes)),
                (
                    "max_single_metadata_record_bytes",
                    int(max_single_metadata_record_bytes),
                ),
            ),
        )


class RarReadOnlyStorageBackend(_ConfiguredArchiveStore):
    """
    Expose one RAR 3/4/5 archive as a read-only Store.

    Example:
        >>> store = RarReadOnlyStorageBackend("books.rar")  # doctest: +SKIP
    """

    store_kind = "rar_readonly"

    def __init__(
        self,
        url: str,
        name: str | None = None,
        uuid: str | UUID | None = None,
        *,
        configuration: StoreConfiguration | None = None,
        extractor_exe: str | None = None,
        extract_timeout_s: float = DEFAULT_RAR_EXTRACT_TIMEOUT_S,
        max_inventory_entries: int = DEFAULT_MAX_ARCHIVE_INVENTORY_ENTRIES,
        max_member_bytes: int = DEFAULT_MAX_RAR_MEMBER_BYTES,
        max_depth: int = DEFAULT_MAX_ARCHIVE_DEPTH,
        max_total_uncompressed_bytes: int = DEFAULT_MAX_RAR_TOTAL_UNCOMPRESSED_BYTES,
        max_compression_ratio: float = DEFAULT_MAX_RAR_COMPRESSION_RATIO,
        max_path_bytes: int = DEFAULT_MAX_RAR_PATH_BYTES,
    ) -> None:
        """
        Configure a RAR Store with durable extractor and safety policy.

        Example:
            >>> store = RarReadOnlyStorageBackend("books.rar")  # doctest: +SKIP


        :param url:
        :param name:
        :param uuid:
        :param configuration:
        :param extractor_exe:
        :param extract_timeout_s:
        :param max_inventory_entries:
        :param max_member_bytes:
        :param max_depth:
        :return:
        """

        driver = RarStorageDriver(
            url,
            address_space_uuid=_store_uuid(uuid, configuration),
            extractor_exe=extractor_exe,
            extract_timeout_s=extract_timeout_s,
            max_inventory_entries=max_inventory_entries,
            max_member_bytes=max_member_bytes,
            max_depth=max_depth,
            max_total_uncompressed_bytes=max_total_uncompressed_bytes,
            max_compression_ratio=max_compression_ratio,
            max_path_bytes=max_path_bytes,
        )
        options: list[tuple[str, object]] = [
            ("extract_timeout_s", float(extract_timeout_s)),
            ("max_inventory_entries", int(max_inventory_entries)),
            ("max_member_bytes", int(max_member_bytes)),
            ("max_depth", int(max_depth)),
            ("max_total_uncompressed_bytes", int(max_total_uncompressed_bytes)),
            ("max_compression_ratio", float(max_compression_ratio)),
            ("max_path_bytes", int(max_path_bytes)),
        ]
        if extractor_exe is not None:
            options.append(("extractor_exe", str(extractor_exe)))
        super().__init__(
            driver,
            store_kind=self.store_kind,
            access_protocol="rar",
            name=name,
            configuration=configuration,
            read_only=True,
            backend_options=options,
        )


class SevenZipReadOnlyStorageBackend(_ConfiguredArchiveStore):
    """
    Expose one 7z archive as an immutable Store.

    Example:
        >>> store = SevenZipReadOnlyStorageBackend("books.7z")  # doctest: +SKIP
    """

    store_kind = "sevenzip_readonly"

    def __init__(
        self,
        url: str,
        name: str | None = None,
        uuid: str | UUID | None = None,
        *,
        configuration: StoreConfiguration | None = None,
        max_inventory_entries: int = DEFAULT_MAX_ARCHIVE_INVENTORY_ENTRIES,
        max_member_bytes: int = DEFAULT_MAX_SEVENZIP_MEMBER_BYTES,
        max_depth: int = DEFAULT_MAX_ARCHIVE_DEPTH,
        max_total_uncompressed_bytes: int = DEFAULT_MAX_SEVENZIP_TOTAL_UNCOMPRESSED_BYTES,
        max_compression_ratio: float = DEFAULT_MAX_SEVENZIP_COMPRESSION_RATIO,
        max_header_bytes: int = DEFAULT_MAX_SEVENZIP_HEADER_BYTES,
        max_path_bytes: int = DEFAULT_MAX_SEVENZIP_PATH_BYTES,
    ) -> None:
        """
        Configure a 7z Store with durable parser safety policy.

        Example:
            >>> store = SevenZipReadOnlyStorageBackend("books.7z")  # doctest: +SKIP


        :param url:
        :param name:
        :param uuid:
        :param configuration:
        :param max_inventory_entries:
        :param max_member_bytes:
        :param max_depth:
        :return:
        """

        driver = SevenZipStorageDriver(
            url,
            address_space_uuid=_store_uuid(uuid, configuration),
            max_inventory_entries=max_inventory_entries,
            max_member_bytes=max_member_bytes,
            max_depth=max_depth,
            max_total_uncompressed_bytes=max_total_uncompressed_bytes,
            max_compression_ratio=max_compression_ratio,
            max_header_bytes=max_header_bytes,
            max_path_bytes=max_path_bytes,
        )
        super().__init__(
            driver,
            store_kind=self.store_kind,
            access_protocol="7z",
            name=name,
            configuration=configuration,
            read_only=True,
            backend_options=(
                ("max_inventory_entries", int(max_inventory_entries)),
                ("max_member_bytes", int(max_member_bytes)),
                ("max_depth", int(max_depth)),
                ("max_total_uncompressed_bytes", int(max_total_uncompressed_bytes)),
                ("max_compression_ratio", float(max_compression_ratio)),
                ("max_header_bytes", int(max_header_bytes)),
                ("max_path_bytes", int(max_path_bytes)),
            ),
        )


def _tar_compression_for_path(value: str) -> str:
    """
    Infer the conventional TAR compression from a filename suffix.

    Example:
        >>> _tar_compression_for_path("library.tar.gz")
        'gz'


    :param value:
    :return:
    """

    lowered = str(value).lower()
    if lowered.endswith((".tar.gz", ".tgz")):
        return "gz"
    if lowered.endswith((".tar.bz2", ".tbz", ".tbz2")):
        return "bz2"
    if lowered.endswith((".tar.xz", ".txz")):
        return "xz"
    return "none"


__all__ = [
    "RarReadOnlyStorageBackend",
    "SevenZipReadOnlyStorageBackend",
    "TarReadOnlyStorageBackend",
    "TarWritableStorageBackend",
    "ZipReadOnlyStorageBackend",
    "ZipWritableStorageBackend",
]

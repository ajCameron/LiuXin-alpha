"""
Bind ZIP, TAR, RAR, and 7z drivers to durable Store identity and compatibility APIs.

Concrete constructors select drivers and record options for newly created Store
configuration. Existing configuration is retained; explicit UUID conflicts are
rejected, while other runtime arguments are not automatically reconciled with it.
Member Locations remain logical archive keys, with a legacy local-path prefix
accepted by the shared facade rather than an extracted directory tree.
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
    Bind an archive driver to Store identity, location handling, and durable policy.

    The generic driver-backed bridge implements file operations and policy masking. A supplied
    configuration is retained as-is rather than reconciled with the runtime driver's path or
    options. Concrete constructors separately reconcile explicit UUIDs before constructing their
    drivers.

    Example:
        >>> store.archive_path == store.root_path  # doctest: +SKIP
        True
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
        Retain the runtime driver and use an existing configuration or build a new one.

        New configuration derives UUID/root URI from the driver, supports hierarchical member keys,
        and snapshots backend options. When configuration is supplied, this method does not consume
        backend_options or validate other construction arguments against it.

        Example:
            >>> store = _ConfiguredArchiveStore(driver, store_kind="zip_readonly", access_protocol="zip", name=None, configuration=None, read_only=True, backend_options=())  # doctest: +SKIP


        :param driver: Configured raw archive driver retained by reference for file operations.
        :param store_kind: Registry backend kind used only when constructing configuration.
        :param access_protocol: Archive access-protocol label used for new configuration.
        :param name: Display-name override for new configuration; false values derive a name from the archive path.
        :param configuration: Existing durable configuration retained by identity, or None to construct one.
        :param read_only: Policy for new configuration; an existing configuration supplies its own policy.
        :param backend_options: Option pairs converted to a tuple only when new configuration is needed.
        :return: None after retaining driver and configuration.
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
        Return the retained durable configuration without reconciling live driver state.

        Example:
            >>> store.configuration.store_kind  # doctest: +SKIP


        :return: Existing or constructor-created StoreConfiguration object, not a refreshed copy.
        """

        return self._configuration

    @property
    def _driver(self):
        """
        Supply the retained raw driver to inherited driver-backed Store operations.

        Example:
            >>> store._driver is store.driver  # doctest: +SKIP
            True


        :return: The same configured archive driver exposed by driver.
        """

        return self.__driver

    @property
    def driver(self):
        """
        Expose the raw archive driver for direct operations and diagnostics.

        Direct use follows driver capabilities; callers bypassing the Store bridge must account for
        Store-level policy themselves.

        Example:
            >>> store.driver.archive_path == store.archive_path  # doctest: +SKIP
            True


        :return: Retained raw driver object.
        """

        return self.__driver

    @property
    def archive_path(self) -> pathlib.Path:
        """
        Return the runtime driver's resolved container path without probing it.

        Example:
            >>> store.archive_path.is_absolute()  # doctest: +SKIP
            True


        :return: Local archive Path from the driver, which may differ from a separately supplied configuration root.
        """

        return self.__driver.archive_path

    @property
    def image_path(self) -> pathlib.Path:
        """
        Expose the container path through the legacy image-Store spelling.

        Example:
            >>> store.image_path == store.archive_path  # doctest: +SKIP
            True


        :return: The archive path, without creating or locating a separate image.
        """

        return self.archive_path

    @property
    def db_path(self) -> pathlib.Path:
        """
        Expose the container path through the legacy single-file database-Store spelling.

        Example:
            >>> store.db_path == store.archive_path  # doctest: +SKIP
            True


        :return: The archive path; this alias does not imply a database inside the container.
        """

        return self.archive_path

    @property
    def root_path(self) -> pathlib.Path:
        """
        Expose the container filename as the Store's legacy root path.

        Example:
            >>> store.root_path == store.archive_path  # doctest: +SKIP
            True


        :return: The archive path, not an extracted member directory.
        """

        return self.archive_path

    def locate(self, identifier: str | Location) -> Location:
        """
        Resolve an owned Location or member key, accepting the legacy archive-path prefix.

        An existing Location is checked for Store ownership. Text beginning with the exact resolved
        archive path plus slash loses that prefix before inherited key validation. This is not
        file-URI decoding or a member-existence check.

        Example:
            >>> store.locate("books/novel.epub").key  # doctest: +SKIP
            'books/novel.epub'


        :param identifier: Owned Location, relative member key, or resolved archive-path/member spelling.
        :return: Owned Store Location corresponding to the validated member address.
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
        Delegate the legacy self-test entry point to the Store probe.

        Probe effects and failures depend on the configured archive backend; this is not a separate
        test suite.

        Example:
            >>> status = store.self_test()  # doctest: +SKIP


        :return: Current Store probe result; probe exceptions propagate.
        """

        return self.probe()


def _store_uuid(
    uuid: str | UUID | None,
    configuration: StoreConfiguration | None,
) -> UUID:
    """
    Select durable Store identity and reject a conflicting explicit UUID.

    Configuration takes precedence after matching any explicit UUID. Without configuration, retain
    an existing UUID, parse text, or generate a random UUID when absent. Parsing failures propagate.

    Example:
        >>> _store_uuid(UUID(int=1), None).int
        1


    :param uuid: Explicit identity as UUID/text, or None to use configured/generated identity.
    :param configuration: Optional configuration whose Store UUID must agree with any explicit value.
    :return: Configured, parsed, retained, or newly generated UUID.
    """

    if configuration is not None:
        configured = configuration.store_uuid
        if uuid is not None and UUID(str(uuid)) != configured:
            raise ValueError("configuration and explicit uuid identify different Stores.")
        return configured
    return uuid4() if uuid is None else uuid if isinstance(uuid, UUID) else UUID(uuid)


class ZipReadOnlyStorageBackend(_ConfiguredArchiveStore):
    """
    Expose an existing ZIP regular-file projection through the configured Store API.

    The facade does not make the underlying file immutable. Its raw driver supplies bounded
    indexing, range reads, and archive-wide conditional versions.

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
        Configure a read-only ZIP driver and retain or construct durable Store policy.

        Construction requires an existing regular path but does not parse the archive. Numeric ZIP
        policies are validated by the driver and captured in newly created backend options.

        Example:
            >>> store = ZipReadOnlyStorageBackend("books.zip")  # doctest: +SKIP


        :param url: Local container filename passed directly to the raw driver; despite the name, this constructor does not decode a file URI.
        :param name: Display name for new configuration, or None/empty to derive it from the resolved path.
        :param uuid: Explicit Store UUID or text, required to match configuration when both are supplied; otherwise generated when absent.
        :param configuration: Durable configuration retained as-is, or None to construct it from these arguments; other runtime arguments are not reconciled with it.
        :param max_inventory_entries: Maximum indexed entry count, including explicit directories, forwarded to driver validation.
        :param max_member_bytes: Maximum declared uncompressed bytes per regular member, additionally bounded by the total-byte limit.
        :param max_depth: Maximum number of components in a relative member key.
        :param max_total_uncompressed_bytes: Maximum sum of declared regular-member uncompressed sizes.
        :param max_compression_ratio: Finite ratio of at least one, applied by the format driver to its available compressed-size evidence.
        :param max_central_directory_bytes: Maximum declared central-directory bytes, checked by ZIP preflight before full inventory allocation.
        :return: None after constructing the raw driver and binding Store configuration.
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
    Expose ZIP member writes and deletes through whole-container rebuilds.

    The raw writer normalizes metadata and checks inspected loss policy. A supplied read-only Store
    configuration masks facade mutations even though the underlying driver supports writes.

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
        Configure a ZIP rebuild driver, optionally creating a missing container.

        The driver validates compression first but can create parent directories and an empty
        archive before later prefix/limit validation fails. Newly built options capture these
        arguments; supplied configuration remains unchanged, including its read-only policy.

        Example:
            >>> store = ZipWritableStorageBackend("books.zip")  # doctest: +SKIP


        :param url: Local container filename passed directly to the raw driver; despite the name, this constructor does not decode a file URI.
        :param name: Display name for new configuration, or None/empty to derive it from the resolved path.
        :param uuid: Explicit Store UUID or text, required to match configuration when both are supplied; otherwise generated when absent.
        :param configuration: Durable configuration retained as-is, or None to construct it from these arguments; other runtime arguments are not reconciled with it.
        :param create_archive: Whether to create a missing container; None permits creation unless a supplied configuration is read-only.
        :param compression: Stored, deflated, bzip2, or lzma; the driver strips/lowercases it, while new durable options retain the supplied text.
        :param compresslevel: Optional level: -1 through 9 for deflated, 1 through 9 for bzip2; stored/lzma require None. Omitted from new options when None.
        :param deterministic: Whether to normalize writer timestamps for repeatable output within the same implementation/toolchain.
        :param allow_lossy_rebuild: Whether inspected metadata-loss reasons may be normalized; this does not permit unsafe member kinds or bypass indexing bounds.
        :param allocation_prefix: Relative key prefix for driver-suggested member addresses, without reserving them.
        :param max_inventory_entries: Maximum indexed entry count, including explicit directories, forwarded to driver validation.
        :param max_member_bytes: Maximum declared uncompressed bytes per regular member, additionally bounded by the total-byte limit.
        :param max_depth: Maximum number of components in a relative member key.
        :param max_total_uncompressed_bytes: Maximum sum of declared regular-member uncompressed sizes.
        :param max_compression_ratio: Finite ratio of at least one, applied by the format driver to its available compressed-size evidence.
        :param max_central_directory_bytes: Maximum declared central-directory bytes, checked by ZIP preflight before full inventory allocation.
        :return: None after constructing the raw driver and binding Store configuration.
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
    Expose regular members of a local TAR, including supported compressed containers.

    The driver bounds declared expansion, parser reads, and stream position. Names remain member
    keys rather than extracted filesystem paths.

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
        Configure bounded TAR reading and retain or construct Store identity and policy.

        The existing regular path and limits are checked by the driver before lazy indexing.
        Stream-position and allocation bounds also apply to parser work, not only exposed file
        payloads.

        Example:
            >>> store = TarReadOnlyStorageBackend("books.tar.gz")  # doctest: +SKIP


        :param url: Local container filename passed directly to the raw driver; despite the name, this constructor does not decode a file URI.
        :param name: Display name for new configuration, or None/empty to derive it from the resolved path.
        :param uuid: Explicit Store UUID or text, required to match configuration when both are supplied; otherwise generated when absent.
        :param configuration: Durable configuration retained as-is, or None to construct it from these arguments; other runtime arguments are not reconciled with it.
        :param max_inventory_entries: Maximum indexed entry count, including explicit directories, forwarded to driver validation.
        :param max_member_bytes: Maximum declared uncompressed bytes per regular member, additionally bounded by the total-byte limit.
        :param max_depth: Maximum number of components in a relative member key.
        :param max_total_uncompressed_bytes: Maximum sum of declared regular-member uncompressed sizes.
        :param max_compression_ratio: Finite ratio of at least one bounding declared regular-member totals against the container byte size.
        :param max_metadata_bytes: Additional decompressed-stream byte allowance above max_total_uncompressed_bytes for TAR headers, padding, and metadata; not an independent metadata sum.
        :param max_single_metadata_record_bytes: Maximum individual parser read allocation in bytes, also applying when the TAR wrapper reads payload data.
        :return: None after constructing the raw driver and binding Store configuration.
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
    Expose whole-container TAR rebuilds with explicit compression and normalization policy.

    Filename suffixes choose compression only when no override is supplied. Generic Store policy can
    mask the raw writer, and allowing metadata loss does not allow symbolic or hard-link members.

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
        Select TAR compression, configure the rebuild driver, and retain durable policy.

        An absent container may be created before later prefix or numeric validation. Compression
        selection uses only the filename when compression is None; supplied durable configuration is
        retained rather than rewritten to match runtime options.

        Example:
            >>> store = TarWritableStorageBackend("books.tar.xz")  # doctest: +SKIP


        :param url: Local container filename passed directly to the raw driver; despite the name, this constructor does not decode a file URI.
        :param name: Display name for new configuration, or None/empty to derive it from the resolved path.
        :param uuid: Explicit Store UUID or text, required to match configuration when both are supplied; otherwise generated when absent.
        :param configuration: Durable configuration retained as-is, or None to construct it from these arguments; other runtime arguments are not reconciled with it.
        :param create_archive: Whether to create a missing container; None permits creation unless a supplied configuration is read-only.
        :param compression: none, gz, bz2, or xz, normalized by the driver; None guesses from the filename suffix.
        :param deterministic: Whether to normalize writer timestamps for repeatable output within the same implementation/toolchain.
        :param allow_lossy_rebuild: Whether inspected metadata-loss reasons may be normalized; this does not permit unsafe member kinds or bypass indexing bounds.
        :param allocation_prefix: Relative key prefix for driver-suggested member addresses, without reserving them.
        :param max_inventory_entries: Maximum indexed entry count, including explicit directories, forwarded to driver validation.
        :param max_member_bytes: Maximum declared uncompressed bytes per regular member, additionally bounded by the total-byte limit.
        :param max_depth: Maximum number of components in a relative member key.
        :param max_total_uncompressed_bytes: Maximum sum of declared regular-member uncompressed sizes.
        :param max_compression_ratio: Finite ratio of at least one bounding declared regular-member totals against the container byte size.
        :param max_metadata_bytes: Additional decompressed-stream byte allowance above max_total_uncompressed_bytes for TAR headers, padding, and metadata; not an independent metadata sum.
        :param max_single_metadata_record_bytes: Maximum individual parser read allocation in bytes, also applying when the TAR wrapper reads payload data.
        :return: None after constructing the raw driver and binding Store configuration.
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
    Expose a bounded local RAR projection with optional external compressed-member extraction.

    Stored members can be read without an extractor. Compressed members use the driver's staged
    extraction/CRC checks, and RAR5 requires the modern optional parser. No member tree is extracted
    by this facade.

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
        Configure RAR parsing, extraction limits, and durable Store options.

        The raw driver defers archive parsing and tool discovery until operations need them.
        Extractor timeout is passed to process waiting, not enforced as an end-to-end deadline for
        indexing, pipe cleanup, and reads.

        Example:
            >>> store = RarReadOnlyStorageBackend("books.rar")  # doctest: +SKIP


        :param url: Local container filename passed directly to the raw driver; despite the name, this constructor does not decode a file URI.
        :param name: Display name for new configuration, or None/empty to derive it from the resolved path.
        :param uuid: Explicit Store UUID or text, required to match configuration when both are supplied; otherwise generated when absent.
        :param configuration: Durable configuration retained as-is, or None to construct it from these arguments; other runtime arguments are not reconciled with it.
        :param extractor_exe: Optional executable name/path preferred by the driver; None uses its discovery policy and omits the durable override.
        :param extract_timeout_s: Positive seconds supplied to the extraction process wait; other extraction and cleanup work can add elapsed time.
        :param max_inventory_entries: Maximum indexed entry count, including explicit directories, forwarded to driver validation.
        :param max_member_bytes: Maximum declared uncompressed bytes per regular member, additionally bounded by the total-byte limit.
        :param max_depth: Maximum number of components in a relative member key.
        :param max_total_uncompressed_bytes: Maximum sum of declared regular-member uncompressed sizes.
        :param max_compression_ratio: Finite ratio of at least one bounding positive member sizes against packed sizes and total regular bytes against container size.
        :param max_path_bytes: Maximum UTF-8/surrogateescape bytes for the entire canonical member key.
        :return: None after constructing the raw driver and binding Store configuration.
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
    Expose bounded 7z member reads through the optional py7zr implementation.

    The driver owns parsing and temporary member extraction without a filesystem member tree. Header
    and expansion checks apply to observations obtained from the archive library.

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
        Configure 7z limits and retain or construct durable Store configuration.

        Construction validates the existing regular path and driver limits. The optional library,
        password state, header size, and member inventory are checked when the driver indexes the
        archive.

        Example:
            >>> store = SevenZipReadOnlyStorageBackend("books.7z")  # doctest: +SKIP


        :param url: Local container filename passed directly to the raw driver; despite the name, this constructor does not decode a file URI.
        :param name: Display name for new configuration, or None/empty to derive it from the resolved path.
        :param uuid: Explicit Store UUID or text, required to match configuration when both are supplied; otherwise generated when absent.
        :param configuration: Durable configuration retained as-is, or None to construct it from these arguments; other runtime arguments are not reconciled with it.
        :param max_inventory_entries: Maximum indexed entry count, including explicit directories, forwarded to driver validation.
        :param max_member_bytes: Maximum declared uncompressed bytes per regular member, additionally bounded by the total-byte limit.
        :param max_depth: Maximum number of components in a relative member key.
        :param max_total_uncompressed_bytes: Maximum sum of declared regular-member uncompressed sizes.
        :param max_compression_ratio: Finite ratio of at least one bounding known member compressed-size evidence and total regular bytes against container size.
        :param max_header_bytes: Maximum reported archive header bytes, checked after py7zr opens the archive; not a pre-allocation parser limit.
        :param max_path_bytes: Maximum UTF-8/surrogateescape bytes for the entire canonical member key.
        :return: None after constructing the raw driver and binding Store configuration.
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
    Guess TAR compression from a case-insensitive filename suffix.

    Recognize .tar.gz/.tgz, .tar.bz2/.tbz/.tbz2, and .tar.xz/.txz. Other names select uncompressed
    output. Text is lowercased without trimming; existing bytes are not inspected.

    Example:
        >>> [_tar_compression_for_path(name) for name in ("BOOKS.TGZ", "books.tbz2", "books.txz", "books.tar")]
        ['gz', 'bz2', 'xz', 'none']


    :param value: Filename or path text used to choose a default writer compression.
    :return: gz, bz2, xz, or none, accepted by the TAR writer.
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

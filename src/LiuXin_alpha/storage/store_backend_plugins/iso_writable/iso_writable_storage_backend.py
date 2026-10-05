"""
Adapt the ISO image writer to durable Store configuration and opaque locations.

The facade preserves supplied read-only policy and exposes compatibility image
path properties. Raw-driver options come from constructor arguments, and optional
empty-image creation depends on configuration only when no explicit flag is given.
"""

from __future__ import annotations

import pathlib

from uuid import UUID, uuid4

from LiuXin_alpha.storage.api import (
    DriverBackedStoreAPI,
    Location,
    StoreConfiguration,
)
from LiuXin_alpha.storage.drivers.iso import (
    DEFAULT_MAX_ISO_DEPTH,
    DEFAULT_MAX_ISO_DIRECTORY_BYTES,
    DEFAULT_MAX_ISO_INVENTORY_ENTRIES,
    DEFAULT_MAX_ISO_LOGICAL_EXPANSION_RATIO,
    DEFAULT_MAX_ISO_PATH_BYTES,
    DEFAULT_MAX_ISO_SUSP_BYTES,
    DEFAULT_MAX_ISO_TOTAL_UNCOMPRESSED_BYTES,
    DEFAULT_MAX_ISO_UDF_MEMBER_BYTES,
    IsoObjectAddress,
)
from LiuXin_alpha.storage.drivers.iso_writer import (
    DEFAULT_ISO_VOLUME_ID,
    WritableIsoStorageDriver,
)
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name


class IsoWritableStorageBackend(DriverBackedStoreAPI[IsoObjectAddress]):
    """
    Expose normalized whole-image ISO mutation through configured Store locations.

    One raw writer supplies staging, candidate validation, and publication. Supplied read-only
    configuration restricts the Store facade, while the exposed raw driver retains its own
    operations. Replacement is atomic at the pathname boundary; later synchronization or refresh
    failures can occur after the new image is visible.

    Example:
        >>> store = IsoWritableStorageBackend(path, deterministic=True)  # doctest: +SKIP
        >>> stored = store.store_bytes(b"book", location="books/book.epub")  # doctest: +SKIP
    """

    store_kind = "iso_writable"

    def __init__(
        self,
        url: str,
        name: str | None = None,
        uuid: str | UUID | None = None,
        *,
        configuration: StoreConfiguration | None = None,
        create_image: bool | None = None,
        volume_id: str = DEFAULT_ISO_VOLUME_ID,
        include_joliet: bool = True,
        deterministic: bool = False,
        allow_lossy_rebuild: bool = False,
        allocation_prefix: str = "objects",
        max_inventory_entries: int = DEFAULT_MAX_ISO_INVENTORY_ENTRIES,
        max_directory_bytes: int = DEFAULT_MAX_ISO_DIRECTORY_BYTES,
        max_depth: int = DEFAULT_MAX_ISO_DEPTH,
        max_susp_bytes: int = DEFAULT_MAX_ISO_SUSP_BYTES,
        max_udf_member_bytes: int = DEFAULT_MAX_ISO_UDF_MEMBER_BYTES,
        max_total_uncompressed_bytes: int = DEFAULT_MAX_ISO_TOTAL_UNCOMPRESSED_BYTES,
        max_logical_expansion_ratio: float = DEFAULT_MAX_ISO_LOGICAL_EXPANSION_RATIO,
        max_path_bytes: int = DEFAULT_MAX_ISO_PATH_BYTES,
    ) -> None:
        """
        Bind a raw ISO writer and retain or construct its durable Store configuration.

        Supplied configuration provides UUID/name and is retained unchanged. An explicit UUID must
        parse and agree. Runtime path/options still come from constructor arguments rather than
        replaying configuration fields. create_image=None permits creation unless the supplied
        configuration is read-only; an explicit flag overrides that default. Without configuration,
        UUID/name are parsed or generated and normalized output policy is recorded with writable ISO
        defaults. Construction may create an empty image.

        Example:
            >>> store = IsoWritableStorageBackend(path, volume_id="BOOKS", create_image=False)  # doctest: +SKIP


        :param url: Local image pathname; URI decoding belongs to registry construction.
        :param name: Optional generated-configuration name; ignored with supplied configuration.
        :param uuid: Optional UUID object/text; must agree with supplied configuration or is generated when both are absent.
        :param configuration: Durable configuration retained as-is, or None to create writable defaults.
        :param create_image: Explicit absent-image creation flag, or None for the configuration-dependent default.
        :param volume_id: Output volume identifier normalized by the raw writer.
        :param include_joliet: Whether to emit Joliet when all names fit.
        :param deterministic: Whether record timestamps use fixed fallbacks.
        :param allow_lossy_rebuild: Whether detected unpreserved features may be discarded.
        :param allocation_prefix: Canonical writable allocation prefix leaving member-depth headroom.
        :param max_inventory_entries: Positive parser all-entry cap and regular-source preflight cap.
        :param max_directory_bytes: Positive per-directory reader byte limit.
        :param max_depth: Positive path-component and traversal policy.
        :param max_susp_bytes: Positive per-record SUSP continuation budget.
        :param max_udf_member_bytes: Positive member-byte bound also used for direct ISO and writes.
        :param max_total_uncompressed_bytes: Positive aggregate regular-member byte bound.
        :param max_logical_expansion_ratio: Finite logical-to-physical byte ratio bound, at least one.
        :param max_path_bytes: Positive whole-key encoded byte bound.
        :return: None after binding the writer and configuration, with optional image creation and propagated validation errors.
        """

        if configuration is not None:
            store_uuid = configuration.store_uuid
            if uuid is not None and UUID(str(uuid)) != store_uuid:
                raise ValueError(
                    "configuration and explicit uuid identify different Stores."
                )
            effective_name = configuration.store_name
        else:
            store_uuid = uuid4() if uuid is None else (
                uuid if isinstance(uuid, UUID) else UUID(uuid)
            )
            effective_name = name
        if create_image is None:
            effective_create_image = (
                configuration is None or not configuration.read_only
            )
        else:
            effective_create_image = bool(create_image)
        self.__driver = WritableIsoStorageDriver(
            url,
            address_space_uuid=store_uuid,
            create_image=effective_create_image,
            volume_id=volume_id,
            include_joliet=include_joliet,
            deterministic=deterministic,
            allow_lossy_rebuild=allow_lossy_rebuild,
            allocation_prefix=allocation_prefix,
            max_inventory_entries=max_inventory_entries,
            max_directory_bytes=max_directory_bytes,
            max_depth=max_depth,
            max_susp_bytes=max_susp_bytes,
            max_udf_member_bytes=max_udf_member_bytes,
            max_total_uncompressed_bytes=max_total_uncompressed_bytes,
            max_logical_expansion_ratio=max_logical_expansion_ratio,
            max_path_bytes=max_path_bytes,
        )
        self._configuration = configuration or StoreConfiguration(
            store_uuid=store_uuid,
            store_name=(
                effective_name
                or self.url_to_name(str(self.__driver.image_path))
            ),
            store_kind=self.store_kind,
            store_root_uri=self.__driver.root_uri,
            store_url=self.__driver.root_uri,
            store_access_protocol="iso-write",
            read_only=False,
            supports_folders=True,
            backend_options=(
                ("create_image", effective_create_image),
                ("volume_id", self.__driver.volume_id),
                ("include_joliet", bool(include_joliet)),
                ("deterministic", bool(deterministic)),
                ("allow_lossy_rebuild", bool(allow_lossy_rebuild)),
                ("allocation_prefix", str(allocation_prefix)),
                ("max_inventory_entries", int(max_inventory_entries)),
                ("max_directory_bytes", int(max_directory_bytes)),
                ("max_depth", int(max_depth)),
                ("max_susp_bytes", int(max_susp_bytes)),
                ("max_udf_member_bytes", int(max_udf_member_bytes)),
                (
                    "max_total_uncompressed_bytes",
                    int(max_total_uncompressed_bytes),
                ),
                (
                    "max_logical_expansion_ratio",
                    float(max_logical_expansion_ratio),
                ),
                ("max_path_bytes", int(max_path_bytes)),
            ),
        )

    @property
    def configuration(self) -> StoreConfiguration:
        """
        Return the retained durable configuration without reconciling it with live driver policy.

        Example:
            >>> store.configuration.store_uuid == store.driver.object_address_checker.address_space_uuid  # doctest: +SKIP
            True


        :return: Supplied configuration object or the configuration created during construction.
        """

        return self._configuration

    @property
    def _driver(self) -> WritableIsoStorageDriver:
        """
        Expose the retained raw writer to inherited driver-backed Store operations.

        Example:
            >>> store._driver is store.driver  # doctest: +SKIP
            True


        :return: Same WritableIsoStorageDriver instance bound at construction.
        """

        return self.__driver

    @property
    def driver(self) -> WritableIsoStorageDriver:
        """
        Expose the same raw writer for diagnostics and driver-level operations.

        No additional driver or ownership boundary is created; callers must use its address space.

        Example:
            >>> store.driver.image_path == store.image_path  # doctest: +SKIP
            True


        :return: Retained WritableIsoStorageDriver shared with the Store adapter.
        """

        return self.__driver

    @property
    def image_path(self) -> pathlib.Path:
        """
        Return the driver's resolved image pathname without another filesystem check.

        Example:
            >>> store.image_path.is_absolute()  # doctest: +SKIP
            True


        :return: Path of the configured image file.
        """

        return self.__driver.image_path

    @property
    def db_path(self) -> pathlib.Path:
        """
        Provide the image pathname under the legacy db_path property.

        This alias does not identify a database.

        Example:
            >>> store.db_path == store.image_path  # doctest: +SKIP
            True


        :return: Same Path returned by image_path.
        """

        return self.image_path

    @property
    def root_path(self) -> pathlib.Path:
        """
        Provide the image pathname under the legacy root_path property.

        This alias does not identify an extracted directory.

        Example:
            >>> store.root_path == store.image_path  # doctest: +SKIP
            True


        :return: Same Path returned by image_path.
        """

        return self.image_path

    @staticmethod
    def url_to_name(url: str) -> str:
        """
        Derive a display name using the shared path-name helper's default sanitizing and hash
        policy.

        This is text conversion only; it neither resolves the path nor inspects the image.

        Example:
            >>> IsoWritableStorageBackend.url_to_name("/srv/library.iso").startswith("root__srv__library.iso-")
            True


        :param url: Path text passed unchanged to safe_path_to_name.
        :return: Generated path-derived name string.
        """

        return safe_path_to_name(url)

    def locate(self, identifier: str | Location) -> Location:
        """
        Check an existing Location or adapt a legacy image-path prefix before normal Store parsing.

        Only the exact resolved image pathname followed by slash is stripped. Other text is
        delegated unchanged; this performs address conversion, not member existence checks.

        Example:
            >>> store.locate(str(store.image_path) + "/books/novel.epub").key  # doctest: +SKIP
            'books/novel.epub'


        :param identifier: Owned Location, internal member key, or exact image-path/member compatibility text.
        :return: Validated Store Location for the selected member key.
        """

        if isinstance(identifier, Location):
            return self.require_location(identifier)
        text = str(identifier)
        legacy_prefix = str(self.image_path) + "/"
        if text.startswith(legacy_prefix):
            text = text[len(legacy_prefix) :]
        return super().locate(text)

    def self_test(self):
        """
        Run the current Store probe through the legacy health-check name.

        Example:
            >>> store.self_test().available  # doctest: +SKIP
            True


        :return: Probe status; indexing/dependency failures propagate.
        """

        return self.probe()


__all__ = ["IsoWritableStorageBackend"]

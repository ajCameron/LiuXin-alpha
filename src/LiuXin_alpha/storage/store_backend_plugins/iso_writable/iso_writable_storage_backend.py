"""
Configured read/write ISO image Store.
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
    Expose one atomically rebuilt ISO through ordinary read/write Store calls.

    Every committed mutation produces and validates a complete candidate image
    before replacing the published ISO. Readers never observe a partial image.

    Example:
        >>> store = IsoWritableStorageBackend("library.iso")  # doctest: +SKIP
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
        Configure one mutable image and its bounded reader/writer policy.

        Example:
            >>> IsoWritableStorageBackend("library.iso", volume_id="BOOKS")  # doctest: +SKIP


        :param url:
        :param name:
        :param uuid:
        :param configuration:
        :param create_image:
        :param volume_id:
        :param include_joliet:
        :param deterministic:
        :param allow_lossy_rebuild:
        :param allocation_prefix:
        :param max_inventory_entries:
        :param max_directory_bytes:
        :param max_depth:
        :param max_susp_bytes:
        :param max_udf_member_bytes:
        :param max_total_uncompressed_bytes:
        :param max_logical_expansion_ratio:
        :param max_path_bytes:
        :return:
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
        Return this Store's durable configuration.

        Example:
            >>> store.configuration.store_kind  # doctest: +SKIP
            'iso_writable'


        :return:
        """

        return self._configuration

    @property
    def _driver(self) -> WritableIsoStorageDriver:
        """
        Return the private driver used by the Store adapter.

        Example:
            >>> isinstance(store._driver, WritableIsoStorageDriver)  # doctest: +SKIP
            True


        :return:
        """

        return self.__driver

    @property
    def driver(self) -> WritableIsoStorageDriver:
        """
        Return the writable ISO driver for diagnostics and advanced callers.

        Example:
            >>> store.driver.capabilities.create  # doctest: +SKIP
            True


        :return:
        """

        return self.__driver

    @property
    def image_path(self) -> pathlib.Path:
        """
        Return the resolved path of the published ISO image.

        Example:
            >>> store.image_path  # doctest: +SKIP
            PosixPath('/srv/archive/library.iso')


        :return:
        """

        return self.__driver.image_path

    @property
    def db_path(self) -> pathlib.Path:
        """
        Return the image path for legacy path-backed Store callers.

        Example:
            >>> store.db_path == store.image_path  # doctest: +SKIP
            True


        :return:
        """

        return self.image_path

    @property
    def root_path(self) -> pathlib.Path:
        """
        Return the image path for legacy path-backed Store callers.

        Example:
            >>> store.root_path == store.image_path  # doctest: +SKIP
            True


        :return:
        """

        return self.image_path

    @staticmethod
    def url_to_name(url: str) -> str:
        """
        Derive a readable Store name from an image path.

        Example:
            >>> IsoWritableStorageBackend.url_to_name("/srv/library.iso")  # doctest: +SKIP


        :param url:
        :return:
        """

        return safe_path_to_name(url)

    def locate(self, identifier: str | Location) -> Location:
        """
        Accept an internal key or legacy ``image-path/internal`` form.

        Example:
            >>> store.locate("books/novel.epub").key  # doctest: +SKIP
            'books/novel.epub'


        :param identifier:
        :return:
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
        Probe read and publication access through the legacy health-check name.

        Example:
            >>> store.self_test().writable  # doctest: +SKIP
            True


        :return:
        """

        return self.probe()


__all__ = ["IsoWritableStorageBackend"]

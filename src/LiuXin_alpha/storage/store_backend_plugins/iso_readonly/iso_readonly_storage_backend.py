"""
Configured read-only ISO image Store.
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
    IsoStorageDriver,
)
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name


class IsoReadOnlyStorageBackend(DriverBackedStoreAPI[IsoObjectAddress]):
    """
    Expose one immutable ISO image through opaque internal paths.

    Example:
        >>> store = IsoReadOnlyStorageBackend("library.iso")  # doctest: +SKIP
    """

    store_kind = "iso_readonly"

    def __init__(
        self,
        url: str,
        name: str | None = None,
        uuid: str | UUID | None = None,
        *,
        max_inventory_entries: int = DEFAULT_MAX_ISO_INVENTORY_ENTRIES,
        max_directory_bytes: int = DEFAULT_MAX_ISO_DIRECTORY_BYTES,
        max_depth: int = DEFAULT_MAX_ISO_DEPTH,
        max_susp_bytes: int = DEFAULT_MAX_ISO_SUSP_BYTES,
        max_udf_member_bytes: int = DEFAULT_MAX_ISO_UDF_MEMBER_BYTES,
        max_total_uncompressed_bytes: int = DEFAULT_MAX_ISO_TOTAL_UNCOMPRESSED_BYTES,
        max_logical_expansion_ratio: float = DEFAULT_MAX_ISO_LOGICAL_EXPANSION_RATIO,
        max_path_bytes: int = DEFAULT_MAX_ISO_PATH_BYTES,
        enable_udf: bool = True,
        configuration: StoreConfiguration | None = None,
    ) -> None:
        """
        Configure one image and its bounded parser limits.

        Example:
            >>> IsoReadOnlyStorageBackend("library.iso", name="Archive")  # doctest: +SKIP


        :param url:
        :param name:
        :param uuid:
        :param max_inventory_entries:
        :param max_directory_bytes:
        :param max_depth:
        :param max_susp_bytes:
        :param max_udf_member_bytes:
        :param enable_udf:
        :return:
        """

        store_uuid = configuration.store_uuid if configuration is not None else (
            uuid4() if uuid is None else (
                uuid if isinstance(uuid, UUID) else UUID(uuid)
            )
        )
        self.__driver = IsoStorageDriver(
            url,
            address_space_uuid=store_uuid,
            max_inventory_entries=max_inventory_entries,
            max_directory_bytes=max_directory_bytes,
            max_depth=max_depth,
            max_susp_bytes=max_susp_bytes,
            max_udf_member_bytes=max_udf_member_bytes,
            max_total_uncompressed_bytes=max_total_uncompressed_bytes,
            max_logical_expansion_ratio=max_logical_expansion_ratio,
            max_path_bytes=max_path_bytes,
            enable_udf=enable_udf,
        )
        self._configuration = configuration or StoreConfiguration(
            store_uuid=store_uuid,
            store_name=name or self.url_to_name(str(self.__driver.image_path)),
            store_kind=self.store_kind,
            store_root_uri=self.__driver.root_uri,
            store_url=self.__driver.root_uri,
            store_access_protocol="iso",
            read_only=True,
            supports_folders=True,
            backend_options=(
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
                ("enable_udf", bool(enable_udf)),
            ),
        )

    @property
    def configuration(self) -> StoreConfiguration:
        """
        Return this Store's durable configuration.

        Example:
            >>> store.configuration.store_kind  # doctest: +SKIP
            'iso_readonly'


        :return:
        """

        return self._configuration

    @property
    def _driver(self) -> IsoStorageDriver:
        """
        Return the private driver used by the Store adapter.

        Example:
            >>> isinstance(store._driver, IsoStorageDriver)  # doctest: +SKIP
            True


        :return:
        """

        return self.__driver

    @property
    def driver(self) -> IsoStorageDriver:
        """
        Return the reusable ISO driver for diagnostics and advanced callers.

        Example:
            >>> store.driver.image_path  # doctest: +SKIP
            PosixPath('/srv/archive/library.iso')


        :return:
        """

        return self.__driver

    @property
    def image_path(self) -> pathlib.Path:
        """
        Return the resolved path of the ISO image.

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
            >>> IsoReadOnlyStorageBackend.url_to_name("/srv/library.iso")  # doctest: +SKIP


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
        Probe the image through the legacy Store health-check name.

        Example:
            >>> store.self_test().available  # doctest: +SKIP
            True


        :return:
        """

        return self.probe()


__all__ = ["IsoReadOnlyStorageBackend"]

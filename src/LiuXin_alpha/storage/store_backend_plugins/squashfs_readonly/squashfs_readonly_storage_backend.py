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
    DEFAULT_MAX_SQUASHFS_COMPRESSION_RATIO,
    DEFAULT_MAX_SQUASHFS_HEADER_BYTES,
    DEFAULT_MAX_SQUASHFS_MEMBER_BYTES,
    DEFAULT_MAX_SQUASHFS_PATH_BYTES,
    DEFAULT_MAX_SQUASHFS_TOTAL_UNCOMPRESSED_BYTES,
    SquashfsObjectAddress,
    SquashfsStorageDriver,
)
from LiuXin_alpha.storage.drivers.archive_common import (
    DEFAULT_MAX_ARCHIVE_DEPTH,
    DEFAULT_MAX_ARCHIVE_INVENTORY_ENTRIES,
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
        max_inventory_entries: int = DEFAULT_MAX_ARCHIVE_INVENTORY_ENTRIES,
        max_member_bytes: int = DEFAULT_MAX_SQUASHFS_MEMBER_BYTES,
        max_total_uncompressed_bytes: int = DEFAULT_MAX_SQUASHFS_TOTAL_UNCOMPRESSED_BYTES,
        max_compression_ratio: float = DEFAULT_MAX_SQUASHFS_COMPRESSION_RATIO,
        max_header_bytes: int = DEFAULT_MAX_SQUASHFS_HEADER_BYTES,
        max_depth: int = DEFAULT_MAX_ARCHIVE_DEPTH,
        max_path_bytes: int = DEFAULT_MAX_SQUASHFS_PATH_BYTES,
        configuration: StoreConfiguration | None = None,
    ) -> None:
        store_uuid = configuration.store_uuid if configuration is not None else (
            uuid4() if uuid is None else (
                uuid if isinstance(uuid, UUID) else UUID(uuid)
            )
        )
        self.__driver = SquashfsStorageDriver(
            url,
            address_space_uuid=store_uuid,
            unsquashfs_exe=unsquashfs_exe,
            timeout_s=timeout_s,
            max_inventory_entries=max_inventory_entries,
            max_member_bytes=max_member_bytes,
            max_total_uncompressed_bytes=max_total_uncompressed_bytes,
            max_compression_ratio=max_compression_ratio,
            max_header_bytes=max_header_bytes,
            max_depth=max_depth,
            max_path_bytes=max_path_bytes,
        )
        self._configuration = configuration or StoreConfiguration(
            store_uuid=store_uuid,
            store_name=name or self.url_to_name(str(self.__driver.archive_path)),
            store_kind=self.store_kind,
            store_root_uri=self.__driver.root_uri,
            store_url=self.__driver.root_uri,
            store_access_protocol="squashfs",
            read_only=True,
            supports_folders=True,
            backend_options=(
                ("unsquashfs_exe", str(unsquashfs_exe)),
                ("timeout_s", float(timeout_s)),
                ("max_inventory_entries", int(max_inventory_entries)),
                ("max_member_bytes", int(max_member_bytes)),
                (
                    "max_total_uncompressed_bytes",
                    int(max_total_uncompressed_bytes),
                ),
                ("max_compression_ratio", float(max_compression_ratio)),
                ("max_header_bytes", int(max_header_bytes)),
                ("max_depth", int(max_depth)),
                ("max_path_bytes", int(max_path_bytes)),
            ),
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

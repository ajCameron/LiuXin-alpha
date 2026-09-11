"""
Adapt a raw SquashFS reader to configured Store locations and legacy path aliases.

Configuration supplies Store identity and facade policy; constructor arguments
independently configure the local image, external tool, and reader limits.
"""

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
    """
    Expose one local SquashFS image through configured Store locations.

    The raw driver owns inventory and spooled reads; this adapter supplies Store identity,
    configuration, and legacy pathname lookup. A supplied configuration is retained even when its
    recorded URL/options differ from the constructor arguments used by the driver.

    Example:
        >>> store = SquashfsReadOnlyStorageBackend("library.sqsh")  # doctest: +SKIP
        >>> store.read_file("books/a.epub")  # doctest: +SKIP
    """

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
        """
        Create the raw reader and retain or synthesize its Store configuration.

        A supplied configuration selects the UUID and ignores the separate uuid/name arguments
        without an agreement check. Runtime path/tool/limits always come from the constructor
        arguments. Otherwise configuration records the resolved file URI, read-only policy, folders,
        and options. Construction checks the local file and policy but does not index it.

        Example:
            >>> store = SquashfsReadOnlyStorageBackend("library.sqsh", name="Archive")  # doctest: +SKIP


        :param url: Local image pathname, despite the legacy parameter name.
        :param name: Optional nonempty display name when configuration is omitted.
        :param uuid: UUID or UUID string when configuration is omitted; None generates an identity.
        :param unsquashfs_exe: Executable name or path used for inventory and extraction.
        :param timeout_s: Positive wait timeout in seconds; cleanup can exceed it.
        :param max_inventory_entries: Positive ceiling on non-root inventory entries, including directories.
        :param max_member_bytes: Positive uncompressed-member byte ceiling, also limited by the total budget.
        :param max_total_uncompressed_bytes: Positive ceiling on summed declared regular-member sizes.
        :param max_compression_ratio: Finite aggregate declared-size/image-size ratio ceiling, at least one.
        :param max_header_bytes: Positive pseudo-header byte ceiling; reader chunks may temporarily exceed it.
        :param max_depth: Positive maximum parsed member-key component count.
        :param max_path_bytes: Positive maximum byte length of a complete UTF-8 surrogateescape member key.
        :param configuration: Existing Store configuration to retain, or None to construct one.
        :return: None after binding the reader and configuration.
        """
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
        """
        Expose the retained Store configuration without reconciling it with runtime arguments.

        Example:
            >>> store.configuration.read_only  # doctest: +SKIP
            True


        :return: The same StoreConfiguration instance on each access.
        """
        return self._configuration

    @property
    def _driver(self) -> SquashfsStorageDriver:
        """
        Supply the raw SquashFS reader to the inherited driver-backed Store operations.

        Example:
            >>> store._driver is store.driver  # doctest: +SKIP
            True


        :return: The retained SquashfsStorageDriver.
        """
        return self.__driver

    @property
    def driver(self) -> SquashfsStorageDriver:
        """
        Expose the same raw reader used by the Store facade.

        Example:
            >>> store.driver.archive_path == store.archive_path  # doctest: +SKIP
            True


        :return: The retained driver, whose methods accept driver addresses rather than Store locations.
        """
        return self.__driver

    @property
    def archive_path(self) -> pathlib.Path:
        """
        Expose the resolved image pathname held by the raw reader.

        Example:
            >>> store.archive_path.name  # doctest: +SKIP
            'library.sqsh'


        :return: Configured image Path without a new existence check.
        """
        return self.__driver.archive_path

    @property
    def db_path(self) -> pathlib.Path:
        """
        Provide the legacy database-path alias for this archive image.

        Example:
            >>> store.db_path == store.archive_path  # doctest: +SKIP
            True


        :return: The same Path value as archive_path; it denotes the image.
        """
        return self.archive_path

    @property
    def root_path(self) -> pathlib.Path:
        """
        Provide the legacy root-path alias for the archive image.

        Example:
            >>> store.root_path == store.archive_path  # doctest: +SKIP
            True


        :return: Image Path rather than a filesystem directory containing extracted members.
        """
        return self.archive_path

    @staticmethod
    def url_to_name(url: str) -> str:
        """
        Derive a display name using the shared path-to-name sanitizer.

        Example:
            >>> SquashfsReadOnlyStorageBackend.url_to_name("library.sqsh") == safe_path_to_name("library.sqsh")
            True


        :param url: Path-like display text passed directly to safe_path_to_name.
        :return: Sanitized display-name string; no archive is opened.
        """
        return safe_path_to_name(url)

    def locate(self, identifier: str | Location) -> Location:
        """
        Validate an existing Location or parse a member key after removing an exact legacy
        image-path prefix.

        Only the resolved archive pathname followed by a slash is stripped. Remaining text is
        handled by the normal Store/driver location parser; ownership is required for an existing
        Location.

        Example:
            >>> store.locate(str(store.archive_path) + "/books/a.epub").key  # doctest: +SKIP
            'books/a.epub'


        :param identifier: Store Location, internal key, or legacy resolved-image-path/internal-key text.
        :return: Owned Location; no member existence check is implied.
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
        Run the ordinary Store probe through the compatibility self-test entry point.

        Example:
            >>> store.self_test().available  # doctest: +SKIP
            True


        :return: Effective StoreStatus from probe; this inventories metadata without reading every payload.
        """
        return self.probe()


__all__ = ["SquashfsReadOnlyStorageBackend"]

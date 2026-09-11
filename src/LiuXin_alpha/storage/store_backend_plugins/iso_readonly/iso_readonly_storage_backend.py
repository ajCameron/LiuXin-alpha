"""
Bind the ISO reader to configured Store locations and compatibility path helpers.

The adapter owns one raw reader, preserves supplied configuration, and exposes
legacy aliases without extracting the image. Runtime parser options come from
constructor arguments; an existing configuration is not automatically replayed
into those arguments. Registry construction performs its own URI/option mapping.
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
    Expose a local ISO image through configured Store locations and a reusable raw reader.

    The driver selects the readable namespace and enforces read-only operations. Construction binds
    configuration without indexing. Compatibility path properties refer to the image file, not an
    extracted directory or database.

    Example:
        >>> store = IsoReadOnlyStorageBackend(path, name="Disc archive")  # doctest: +SKIP
        >>> store.startup().available  # doctest: +SKIP
        True
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
        Bind a raw ISO reader and retain or construct its durable Store configuration.

        A supplied configuration provides the UUID and is retained as-is; explicit uuid/name and
        configuration path/options are not reconciled with runtime arguments. Without configuration,
        uuid is parsed or generated, a false name gets a path-derived name, and driver
        limits/options are recorded with read-only ISO defaults. The raw driver checks the existing
        file and policy but does not parse it during construction.

        Example:
            >>> store = IsoReadOnlyStorageBackend(path, name="Archive", enable_udf=False)  # doctest: +SKIP


        :param url: Local image pathname; URI decoding belongs to registry construction, not this parameter.
        :param name: Optional Store name used only when creating configuration; a false value selects a generated name.
        :param uuid: UUID object/text or None for a new UUID, ignored when configuration is supplied.
        :param max_inventory_entries: Positive all-entry cap passed to the raw parser.
        :param max_directory_bytes: Positive maximum bytes loaded per direct-parser directory.
        :param max_depth: Positive traversal/key-component policy.
        :param max_susp_bytes: Positive per-record Rock Ridge continuation-byte budget.
        :param max_udf_member_bytes: Positive member-byte ceiling also applied to direct ISO files.
        :param max_total_uncompressed_bytes: Positive maximum indexed logical regular-file bytes.
        :param max_logical_expansion_ratio: Finite maximum indexed logical bytes per physical image byte, at least one.
        :param max_path_bytes: Positive whole-key UTF-8/surrogatepass byte limit.
        :param enable_udf: Whether namespace selection may invoke optional pycdlib UDF support.
        :param configuration: Existing durable configuration whose UUID wins and whose fields are retained unchanged, or None.
        :return: None after binding the driver and configuration; path/policy errors propagate.
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
        Return the retained durable configuration without reconciling it with live driver policy.

        Example:
            >>> store.configuration.store_uuid == store.driver.object_address_checker.address_space_uuid  # doctest: +SKIP
            True


        :return: Supplied configuration object or the configuration created during construction.
        """

        return self._configuration

    @property
    def _driver(self) -> IsoStorageDriver:
        """
        Expose the retained raw reader to inherited driver-backed Store operations.

        Example:
            >>> store._driver is store.driver  # doctest: +SKIP
            True


        :return: Same IsoStorageDriver instance bound at construction.
        """

        return self.__driver

    @property
    def driver(self) -> IsoStorageDriver:
        """
        Expose the same raw reader for diagnostics and driver-level operations.

        No additional driver or ownership boundary is created; callers must use its address space.

        Example:
            >>> store.driver.image_path == store.image_path  # doctest: +SKIP
            True


        :return: Retained IsoStorageDriver shared with the Store adapter.
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
            >>> IsoReadOnlyStorageBackend.url_to_name("/srv/library.iso").startswith("root__srv__library.iso-")
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


__all__ = ["IsoReadOnlyStorageBackend"]

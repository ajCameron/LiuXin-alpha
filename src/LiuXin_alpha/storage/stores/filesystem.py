"""
Bind a local filesystem driver to configured Store identity and read-only policy.

The facade owns one driver and inherits Location routing, file operations, and
lifecycle translation from DriverBackedStoreAPI. Path conversion is separate
from driver startup; constructing the facade alone does not create the root.
"""

from __future__ import annotations

import os

from pathlib import Path
from urllib.parse import unquote, urlparse
from uuid import UUID, uuid4

from LiuXin_alpha.storage.api import (
    DriverBackedStoreAPI,
    StoreConfiguration,
    StoreStatus,
)
from LiuXin_alpha.storage.drivers.filesystem import (
    FilesystemObjectAddress,
    FilesystemStorageDriver,
)


class FilesystemStore(DriverBackedStoreAPI[FilesystemObjectAddress]):
    """
    Represent one local directory with a stable Store identity and raw filesystem driver.

    Construction resolves the root and builds the driver; startup and probing belong to the
    inherited lifecycle. A supplied configuration is retained, with its UUID and read-only policy
    applied to the driver. The separate url argument still selects the physical root and is not
    checked against that configuration's URI.

    Example:
        >>> store = FilesystemStore(root, read_only=True)  # doctest: +SKIP
        >>> store.startup().available  # doctest: +SKIP
    """

    store_kind = "filesystem"

    def __init__(
        self,
        url: str | os.PathLike[str],
        name: str | None = None,
        uuid: str | UUID | None = None,
        *,
        read_only: bool = False,
        create_root: bool = True,
        allocation_prefix: str = "objects",
        configuration: StoreConfiguration | None = None,
    ) -> None:
        """
        Resolve the directory and bind a filesystem driver to the selected configuration.

        This constructor does not create or probe the root. Without configuration, derive the name
        and canonical file URI from the path and generate a UUID when absent. With configuration,
        name, uuid, and read_only arguments do not replace its values; create_root and
        allocation_prefix still configure the raw driver.

        Example:
            >>> store = FilesystemStore("/srv/books", create_root=False)  # doctest: +SKIP


        :param url: Local path or file URI selecting the physical directory, even with configuration.
        :param name: Generated configuration name; None or an empty string uses the root basename.
        :param uuid: UUID or UUID text for generated configuration; None creates a new identity.
        :param read_only: Read-only policy for generated configuration; supplied configuration takes precedence.
        :param create_root: Whether driver startup may create a missing root directory.
        :param allocation_prefix: Relative key prefix used by the raw driver when allocating object addresses.
        :param configuration: Existing configuration to retain, or None to derive one from the other arguments.
        :return: None after storing configuration and the unstarted driver.
        """
        root = _filesystem_path(url)
        store_uuid = (
            configuration.store_uuid
            if configuration is not None
            else _store_uuid(uuid)
        )
        self._configuration = configuration or StoreConfiguration(
            store_uuid=store_uuid,
            store_name=name or root.name or "filesystem",
            store_kind=self.store_kind,
            store_root_uri=root.resolve(strict=False).as_uri(),
            read_only=read_only,
        )
        self.__driver = FilesystemStorageDriver(
            root,
            address_space_uuid=self._configuration.store_uuid,
            read_only=self._configuration.read_only,
            create_root=create_root,
            allocation_prefix=allocation_prefix,
        )

    @property
    def configuration(self) -> StoreConfiguration:
        """
        Return the retained configuration without probing or copying it.

        Example:
            >>> store.configuration.store_kind  # doctest: +SKIP
            'filesystem'


        :return: The configuration object selected at construction.
        """
        return self._configuration

    @property
    def _driver(self) -> FilesystemStorageDriver:
        """
        Supply the owned filesystem driver to inherited Store routing methods.

        Example:
            >>> store._driver is store.driver  # doctest: +SKIP
            True


        :return: The raw driver whose address-space UUID matches this Store.
        """
        return self.__driver

    @property
    def driver(self) -> FilesystemStorageDriver:
        """
        Expose the raw driver for import and diagnostic workflows.

        The returned object is shared with the facade. Callers using it directly operate at the raw
        address boundary rather than through Store Location translation.

        Example:
            >>> driver = store.driver  # doctest: +SKIP


        :return: The existing filesystem driver, not a new driver or ownership transfer.
        """

        return self.__driver

    @property
    def root_path(self) -> Path:
        """
        Return the expanded, resolved directory path retained by the driver.

        Example:
            >>> store.root_path.is_absolute()  # doctest: +SKIP
            True


        :return: Absolute root Path; access does not test whether it exists.
        """
        return self.__driver.root_path

    @classmethod
    def from_configuration(
        cls,
        configuration: StoreConfiguration,
    ) -> FilesystemStore:
        """
        Construct a Store using the configuration URI as its physical root.

        Missing-root creation is enabled only for a writable configuration. Other driver options use
        constructor defaults; backend_options are not reconstructed here.

        Example:
            >>> restored = FilesystemStore.from_configuration(configuration)  # doctest: +SKIP


        :param configuration: Configuration supplying the root URI, UUID, name, and read-only policy.
        :return: An unstarted Store retaining the supplied configuration object.
        """
        return cls(
            configuration.store_root_uri,
            configuration=configuration,
            create_root=not configuration.read_only,
        )

    def self_test(self) -> StoreStatus:
        """
        Actively probe this Store through the inherited lifecycle and status translation.

        Example:
            >>> status = store.self_test()  # doctest: +SKIP


        :return: The current probe result; operational errors follow the driver lifecycle contract.
        """

        return self.probe()

    @staticmethod
    def url_to_name(url: str) -> str:
        """
        Derive a display name from a supported local path or file URI.

        Example:
            >>> FilesystemStore.url_to_name("file:///srv/my%20books")
            'my books'


        :param url: Local path or local-host file URI to parse without creating the directory.
        :return: Path basename, or "filesystem" when the path has no basename.
        """
        return _filesystem_path(url).name or "filesystem"


def _store_uuid(value: str | UUID | None) -> UUID:
    """
    Preserve a UUID, parse UUID text, or generate a fresh identity for None.

    Example:
        >>> value = UUID(int=1)
        >>> _store_uuid(value) is value
        True


    :param value: UUID instance, canonical/parseable UUID text, or None for a random UUID.
    :return: Selected UUID; invalid text raises the UUID constructor error.
    """
    if value is None:
        return uuid4()
    return value if isinstance(value, UUID) else UUID(value)


def _filesystem_path(value: str | os.PathLike[str]) -> Path:
    """
    Convert a path or local file URI to an expanded Path without resolving it.

    PathLike inputs bypass URI parsing. File URI authorities must be empty or exactly localhost;
    other schemes and remote authorities raise ValueError. File URI paths are percent-decoded, while
    query and fragment components are not used.

    Example:
        >>> _filesystem_path("file:///srv/my%20books").name
        'my books'


    :param value: Filesystem path or local file URI; a leading home marker is expanded.
    :return: Expanded Path, without creation, existence checks, or absolute-path resolution.
    """
    if isinstance(value, os.PathLike):
        return Path(value).expanduser()
    parsed = urlparse(value)
    if parsed.scheme == "file":
        if parsed.netloc not in ("", "localhost"):
            raise ValueError("file Store URLs must refer to the local host.")
        return Path(unquote(parsed.path)).expanduser()
    if parsed.scheme:
        raise ValueError(f"filesystem Store requires a path or file URI: {value!r}")
    return Path(value).expanduser()


__all__ = ["FilesystemStore"]

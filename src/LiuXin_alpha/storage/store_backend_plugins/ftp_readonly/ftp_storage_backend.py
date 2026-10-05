"""
Configure a read-only FTP/FTPS Store over the raw driver and shared Store adapters.

FtpDriverOptions remains an exact alias of FtpDriverOptions. The Store retains
runtime connection options, snapshots durable non-callback configuration, and adapts
full FTP URLs to opaque Locations. Its ingest profile records completed spool delivery
and inspection metadata rather than leaving those qualities at the base defaults.
"""

from __future__ import annotations

import dataclasses

from typing import Optional
from uuid import UUID, uuid4

from LiuXin_alpha.storage.api import (
    DriverBackedStoreAPI,
    IngestMetadataAvailability,
    IngestObjectDelivery,
    IngestSourceCapabilities,
    Location,
    StoreConfiguration,
)
from LiuXin_alpha.storage.drivers.ftp import (
    FtpDriverOptions,
    FtpObjectAddress,
    FtpStorageDriver,
)
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name




class FtpReadOnlyStorageBackend(DriverBackedStoreAPI[FtpObjectAddress]):
    """
    Expose one FTP-family root through configured read-only Store operations.

    The driver retains runtime login credentials and mutable options. Configuration captures the
    normalized root without user information and snapshots option values except client_factory.
    Reads are completed into a spool before return; inspection supplies metadata for advanced
    ingest. Construction does not connect.

    Example:
        >>> store = FtpReadOnlyStorageBackend("ftp://example.test/books/", name="Archive")
        >>> store.configuration.read_only, store.root_path
        (True, '/books')
    """

    store_kind = "ftp_readonly"

    def __init__(
        self,
        url: str,
        *,
        name: Optional[str] = None,
        uuid: str | UUID | None = None,
        options: FtpDriverOptions | None = None,
    ) -> None:
        """
        Create Store identity, configure its raw driver, and capture durable option values.

        Omitted UUID creates a new identity; UUID text is parsed. The driver normalizes
        endpoint/root data and retains credentials for login. Configuration uses its public root
        URI, and a missing/falsey name is generated from that URI. The option object is shared with
        the driver, while configuration records its field values at this time and excludes the
        client-factory callback.

        Example:
            >>> store = FtpReadOnlyStorageBackend("ftp://user:pass@example.test/books/")
            >>> store.configuration.store_root_uri
            'ftp://example.test/books/'


        :param url: FTP/FTPS root passed to the driver, optionally including runtime login credentials.
        :param name: Truthy display name override, or None/empty text for a generated name from the normalized root URI.
        :param uuid: Existing UUID or UUID text, or None to create a fresh Store identity.
        :param options: Truthy mutable FTP option record shared with the driver, otherwise new default options.
        :return: None after retaining the unstarted driver and its Store configuration snapshot.
        """
        store_uuid = uuid4() if uuid is None else (
            uuid if isinstance(uuid, UUID) else UUID(uuid)
        )
        self.options = options or FtpDriverOptions()
        self.__driver = FtpStorageDriver(
            url,
            address_space_uuid=store_uuid,
            options=self.options,
        )
        self._configuration = StoreConfiguration(
            store_uuid=store_uuid,
            store_name=name or self.url_to_name(self.__driver.root_uri),
            store_kind=self.store_kind,
            store_root_uri=self.__driver.root_uri,
            store_url=self.__driver.root_uri,
            store_access_protocol=(
                "ftps" if self.__driver.root_uri.startswith("ftps:") else "ftp"
            ),
            read_only=True,
            supports_folders=True,
            backend_options=tuple(
                (field.name, getattr(self.options, field.name))
                for field in dataclasses.fields(self.options)
                if field.name != "client_factory"
            ),
        )

    @property
    def configuration(self) -> StoreConfiguration:
        """
        Return the retained identity/root/options snapshot without refreshing driver settings or
        status.

        Example:
            >>> store = FtpReadOnlyStorageBackend("ftp://example.test/books/")
            >>> store.configuration.store_kind
            'ftp_readonly'


        :return: StoreConfiguration created during construction; later option mutations are not reflected automatically.
        """
        return self._configuration

    @property
    def _driver(self) -> FtpStorageDriver:
        """
        Supply the owned FTP driver to inherited Store adapters.

        Example:
            >>> store = FtpReadOnlyStorageBackend("ftp://example.test/books/")
            >>> store._driver is store.driver
            True


        :return: The existing raw driver; access neither starts it nor transfers ownership.
        """
        return self.__driver

    @property
    def driver(self) -> FtpStorageDriver:
        """
        Expose the retained raw FTP driver for transport-level configuration and diagnostics.

        Example:
            >>> store = FtpReadOnlyStorageBackend("ftp://example.test/books/")
            >>> store.driver.root_uri
            'ftp://example.test/books/'


        :return: Shared FtpStorageDriver instance used by this Store.
        """
        return self.__driver

    @property
    def root_path(self) -> str:
        """
        Return the decoded remote POSIX root used after client login.

        Example:
            >>> FtpReadOnlyStorageBackend("ftp://example.test/books/").root_path
            '/books'


        :return: Remote absolute path from the driver, rather than its rendered root URI or a local Path.
        """
        return self.__driver.ftp_root_path

    @property
    def ingest_capabilities(self) -> IngestSourceCapabilities:
        """
        Describe completed spool delivery and inspection metadata on the inherited ingest profile.

        DISK_SPOOLED names the retrieval strategy; small spools can remain in memory below their
        rollover threshold. INSPECTION reflects metadata obtainable through stat. Other fields
        retain the base profile derived from driver capabilities, including unguarded reads and no
        stable-range or cursor resume guarantee.

        Example:
            >>> store = FtpReadOnlyStorageBackend("ftp://example.test/books/")
            >>> store.ingest_capabilities.object_delivery is IngestObjectDelivery.DISK_SPOOLED
            True


        :return: New IngestSourceCapabilities with delivery and metadata availability replaced on the inherited record.
        """

        return dataclasses.replace(
            super().ingest_capabilities,
            object_delivery=IngestObjectDelivery.DISK_SPOOLED,
            metadata_availability=IngestMetadataAvailability.INSPECTION,
        )

    @staticmethod
    def url_to_name(url: str) -> str:
        """
        Generate a bounded ASCII label using the shared path-name helper defaults.

        The helper sanitizes path-like components and appends a short hash; it does not parse URL
        credentials or redact arbitrary input. Store construction calls this method with the
        driver's normalized public root URI.

        Example:
            >>> label = FtpReadOnlyStorageBackend.url_to_name("ftp://example.test/books/")
            >>> label.isascii() and len(label) <= 120 and "/" not in label
            True


        :param url: Text treated as a path-like naming input; callers needing credential-free labels must supply a suitable value.
        :return: Name produced by safe_path_to_name with its default sanitization, length, and hash settings.
        """
        return safe_path_to_name(url)

    def locate(self, identifier: str | Location) -> Location:
        """
        Resolve an owned Location, a relative key, or an FTP-family URL to this Store's Location.

        Existing Locations receive ownership checking. Text beginning with ftp:// or ftps://,
        ignoring case, is routed through driver URI parsing; user information there does not replace
        configured login credentials. Other inputs use inherited key parsing. Resolution does not
        stat or retrieve the object.

        Example:
            >>> store = FtpReadOnlyStorageBackend("ftp://example.test/books/")
            >>> store.locate("ftp://other:login@example.test/books/a.epub").key
            'a.epub'


        :param identifier: Owned Location, persisted relative key, or absolute FTP-family URI belonging to this endpoint/root.
        :return: Opaque Location bound to this Store identity, without an existence guarantee.
        """

        if isinstance(identifier, Location):
            return self.require_location(identifier)
        if str(identifier).lower().startswith(("ftp://", "ftps://")):
            return self._location(self.__driver.object_address_from_uri(identifier))
        return super().locate(identifier)

    def self_test(self):
        """
        Run the inherited Store probe, which delegates to the raw driver active endpoint check.

        Example:
            >>> status = store.self_test()  # doctest: +SKIP
            >>> status.writable  # doctest: +SKIP
            False


        :return: Translated StoreStatus from the active probe; connection and listing I/O may occur.
        """
        return self.probe()


__all__ = [
    "FtpReadOnlyStorageBackend",
]

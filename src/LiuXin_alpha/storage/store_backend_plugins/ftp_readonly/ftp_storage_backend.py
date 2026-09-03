"""Configured read-only FTP/FTPS Store."""

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


FtpBackendOptions = FtpDriverOptions


class FtpReadOnlyStorageBackend(DriverBackedStoreAPI[FtpObjectAddress]):
    """One credential-safe configured Store over an FTP-family root."""

    store_kind = "ftp_readonly"

    def __init__(
        self,
        url: str,
        *,
        name: Optional[str] = None,
        uuid: str | UUID | None = None,
        options: FtpBackendOptions | None = None,
    ) -> None:
        store_uuid = uuid4() if uuid is None else (
            uuid if isinstance(uuid, UUID) else UUID(uuid)
        )
        self.options = options or FtpBackendOptions()
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
        return self._configuration

    @property
    def _driver(self) -> FtpStorageDriver:
        return self.__driver

    @property
    def driver(self) -> FtpStorageDriver:
        return self.__driver

    @property
    def root_path(self) -> str:
        return self.__driver.ftp_root_path

    @property
    def ingest_capabilities(self) -> IngestSourceCapabilities:
        """Advertise FTP's disk-spooled reads and inspection metadata.

        Example:
            >>> store.ingest_capabilities.object_delivery  # doctest: +SKIP
            <IngestObjectDelivery.DISK_SPOOLED: 'disk_spooled'>

        :return: FTP-specific source-ingest capability profile.
        """

        return dataclasses.replace(
            super().ingest_capabilities,
            object_delivery=IngestObjectDelivery.DISK_SPOOLED,
            metadata_availability=IngestMetadataAvailability.INSPECTION,
        )

    @staticmethod
    def url_to_name(url: str) -> str:
        return safe_path_to_name(url)

    def locate(self, identifier: str | Location) -> Location:
        """Accept a persisted key, owned Location, or endpoint-owned FTP URI."""

        if isinstance(identifier, Location):
            return self.require_location(identifier)
        if str(identifier).lower().startswith(("ftp://", "ftps://")):
            return self._location(self.__driver.object_address_from_uri(identifier))
        return super().locate(identifier)

    def self_test(self):
        return self.probe()


__all__ = ["FtpBackendOptions", "FtpReadOnlyStorageBackend"]

"""Configured read-only HTTP Store facade."""

from __future__ import annotations

from collections.abc import Mapping
from uuid import UUID, uuid4

from LiuXin_alpha.storage.api import DriverBackedStoreAPI, StoreConfiguration
from LiuXin_alpha.storage.drivers.http import (
    DEFAULT_MAX_HTTP_INVENTORY_ENTRIES,
    HttpInventoryProvider,
    HttpObjectAddress,
    HttpRequestOpener,
    HttpStorageDriver,
)


class HttpReadOnlyStore(DriverBackedStoreAPI[HttpObjectAddress]):
    """One configured HTTP root with optional discovered inventory."""

    store_kind = "http_readonly"

    def __init__(
        self,
        url: str,
        *,
        name: str | None = None,
        uuid: str | UUID | None = None,
        store_kind: str | None = None,
        inventory_provider: HttpInventoryProvider | None = None,
        request_opener: HttpRequestOpener | None = None,
        probe=None,
        timeout_s: float | None = 30.0,
        headers: Mapping[str, str] | None = None,
        max_requests_per_hour: float | None = None,
        max_inventory_entries: int | None = DEFAULT_MAX_HTTP_INVENTORY_ENTRIES,
    ) -> None:
        store_uuid = uuid4() if uuid is None else (
            uuid if isinstance(uuid, UUID) else UUID(uuid)
        )
        kind = store_kind or self.store_kind
        self._configuration = StoreConfiguration(
            store_uuid=store_uuid,
            store_name=name or self.url_to_name(url),
            store_kind=kind,
            store_root_uri=url,
            store_url=url,
            store_access_protocol="https" if url.lower().startswith("https:") else "http",
            read_only=True,
            supports_folders=True,
            backend_options=(
                ("timeout_s", timeout_s),
                ("max_requests_per_hour", max_requests_per_hour),
                ("max_inventory_entries", max_inventory_entries),
            ),
        )
        self.__driver = HttpStorageDriver(
            url,
            address_space_uuid=store_uuid,
            inventory_provider=inventory_provider,
            request_opener=request_opener,
            probe=probe,
            timeout_s=timeout_s,
            headers=headers,
            max_requests_per_hour=max_requests_per_hour,
            max_inventory_entries=max_inventory_entries,
        )

    @property
    def configuration(self) -> StoreConfiguration:
        return self._configuration

    @property
    def _driver(self) -> HttpStorageDriver:
        return self.__driver

    @property
    def driver(self) -> HttpStorageDriver:
        return self.__driver

    @property
    def root_path(self) -> str:
        return self.configuration.store_root_uri

    def self_test(self):
        return self.probe()

    @staticmethod
    def url_to_name(url: str) -> str:
        parsed = url.rstrip("/").rsplit("/", 1)[-1]
        return parsed or "HTTP Store"


__all__ = ["HttpReadOnlyStore"]

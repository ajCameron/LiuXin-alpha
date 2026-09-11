"""
Configure a read-only HTTP Store over an injectable raw HTTP transport.

The facade supplies Store identity, the original root URL, and durable numeric
options. The driver owns address validation, network requests, optional inventory,
and probing. Headers and callback objects remain runtime dependencies.
"""

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
    """
    Expose a configured HTTP root through the read-only driver-backed Store API.

    Optional callbacks supply inventory, request transport, and probing. Configuration retains the
    supplied URL and numeric options, while headers and callbacks remain runtime driver inputs.
    Construction does not perform an HTTP probe.

    Example:
        >>> store = HttpReadOnlyStore("https://example.invalid/books/")
        >>> store.configuration.read_only
        True
    """

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
        """
        Create configuration and a raw HTTP driver for one URL root.

        The driver validates and normalizes its own root and limits. Configuration keeps the
        original URL, so its root spelling need not equal the driver's canonical URI. Neither
        headers nor injected callbacks are serialized into backend_options.

        Example:
            >>> store = HttpReadOnlyStore("https://example.invalid/books/", timeout_s=10)
            >>> store.root_path
            'https://example.invalid/books/'


        :param url: HTTP or HTTPS root supplied to the driver and retained verbatim in configuration.
        :param name: Display name; None or empty uses the final slash-separated URL text.
        :param uuid: Existing UUID or UUID text, or None to generate the Store identity.
        :param store_kind: Truthy configured kind override, or the class http_readonly kind.
        :param inventory_provider: Optional zero-argument provider yielding absolute object URLs under the configured root.
        :param request_opener: Optional transport callable receiving a Request and timeout in seconds.
        :param probe: Optional zero-argument callback returning None, used instead of the default root HEAD request.
        :param timeout_s: Per-request timeout in seconds, or None as accepted by the driver transport.
        :param headers: Optional request headers copied into the driver; not persisted in Store configuration.
        :param max_requests_per_hour: Positive request-rate budget per hour; None, nonpositive, or unparseable values disable it.
        :param max_inventory_entries: Positive inventory-entry bound, or None to disable that bound.
        :return: None after retaining configuration and the unstarted HTTP driver.
        """
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
        """
        Return the immutable configuration snapshot created for this HTTP root.

        Example:
            >>> store.configuration.read_only  # doctest: +SKIP
            True


        :return: The retained Store configuration, without refreshing endpoint status.
        """
        return self._configuration

    @property
    def _driver(self) -> HttpStorageDriver:
        """
        Supply the HTTP driver used by inherited Location and file adapters.

        Example:
            >>> store._driver is store.driver  # doctest: +SKIP
            True


        :return: The existing raw HTTP driver in this Store address space.
        """
        return self.__driver

    @property
    def driver(self) -> HttpStorageDriver:
        """
        Expose the shared raw HTTP driver for transport and diagnostic access.

        Example:
            >>> driver = store.driver  # doctest: +SKIP


        :return: The owned driver; the property neither starts it nor transfers ownership.
        """
        return self.__driver

    @property
    def root_path(self) -> str:
        """
        Return the original configured URL without driver normalization or redaction.

        Example:
            >>> HttpReadOnlyStore("https://example.invalid/books").root_path
            'https://example.invalid/books'


        :return: Configuration store_root_uri text, not a local filesystem Path.
        """
        return self.configuration.store_root_uri

    def self_test(self):
        """
        Run the inherited active probe using the configured driver transport or callback.

        Example:
            >>> status = store.self_test()  # doctest: +SKIP


        :return: Translated StoreStatus from the probe; this can perform network I/O.
        """
        return self.probe()

    @staticmethod
    def url_to_name(url: str) -> str:
        """
        Select the last slash-separated URL component as a display name.

        This is a lexical operation, not URL parsing or credential redaction. Query text remains
        part of the name when it appears in the selected component.

        Example:
            >>> HttpReadOnlyStore.url_to_name("https://example.invalid/books/")
            'books'


        :param url: URL text whose trailing slashes are removed before selecting the last component.
        :return: Last component, or "HTTP Store" if that component is empty.
        """
        parsed = url.rstrip("/").rsplit("/", 1)[-1]
        return parsed or "HTTP Store"


__all__ = ["HttpReadOnlyStore"]

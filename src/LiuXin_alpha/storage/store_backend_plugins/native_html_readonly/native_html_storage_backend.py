"""
Compose native HTML discovery with a configured HTTP Store for remote byte access.

The HTTP facade owns Store lifecycle and file operations; the discovery source
supplies a cached partial inventory. Construction configures both layers without
fetching pages. Discovery acceptance does not establish a complete remote listing
or prove that each accepted address contains readable ebook bytes.
"""

from __future__ import annotations

import dataclasses
import urllib.request

from typing import Optional

from LiuXin_alpha.ingest.sources.native_html import (
    NATIVE_HTML_MAX_REQUESTS_PER_HOUR_DEFAULT,
    NATIVE_HTML_MAX_REQUESTS_PER_HOUR_PREF_KEY,
    NativeHtmlBackendOptions,
    NativeHtmlDiscoverySource,
    _FetchResult,
    get_default_crawler_http_requests_per_hour,
    get_default_native_html_requests_per_hour,
)
from LiuXin_alpha.storage.api import StorageUnavailable
from LiuXin_alpha.storage.stores.http import HttpReadOnlyStore
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name


class NativeHtmlReadOnlyStorageBackend(
    HttpReadOnlyStore,
    NativeHtmlDiscoverySource,
):
    """
    Expose discovered URLs through Store locations, metadata, and read-only byte I/O.

    Store lifecycle/file methods resolve through the HTTP facade before the
    discovery base; direct crawl helpers retain native-source semantics. Crawling
    and byte reads have distinct rate schedulers. Mutable crawler options remain
    referenced, while HTTP timeout/rate/header settings are captured at construction.

    Example:
        >>> store = NativeHtmlReadOnlyStorageBackend('https://example.test/books/', options=NativeHtmlBackendOptions(max_http_requests_per_hour=0))
        >>> store.configuration.read_only
        True
    """

    store_kind = "native_html_readonly"

    def __init__(
        self,
        url: str,
        *,
        name: Optional[str] = None,
        uuid: Optional[str] = None,
        options: NativeHtmlBackendOptions | None = None,
    ) -> None:
        """
        Configure the native crawler, HTTP driver, Store identity, and option metadata.

        Missing options use the shared modern preference resolver. Supplied
        options are retained, not deep-copied. Replace the facade's backend_options
        with dataclass fields from those options; contained mutable values remain
        references. No startup, probe, catalogue registration, or fetch runs here.

        Example:
            >>> store = NativeHtmlReadOnlyStorageBackend('https://example.test/', name='Remote books')
            >>> store.configuration.store_name
            'Remote books'


        :param url: HTTP(S) root normalized by discovery and checked by the HTTP driver.
        :param name: Display name, or falsey to derive a sanitized name from the URL.
        :param uuid: Store UUID text, or None to generate a new identity in the facade.
        :param options: Mutable crawler controls, or None for preference-backed defaults.
        :return: None after creating the configured but unstarted Store.
        :raises ValueError: Root, UUID, or delegated option/driver validation rejects input.
        """
        if options is None:
            options = NativeHtmlBackendOptions(
                max_http_requests_per_hour=(
                    get_default_crawler_http_requests_per_hour()
                ),
            )
        NativeHtmlDiscoverySource.__init__(self, url=url, options=options)
        HttpReadOnlyStore.__init__(
            self,
            self.url,
            name=name,
            uuid=uuid,
            store_kind=self.store_kind,
            inventory_provider=lambda: self.discover_urls(force=False),
            request_opener=self._open_http_request,
            probe=self._probe_http_storage,
            timeout_s=options.timeout_s,
            headers={"User-Agent": options.user_agent} if options.user_agent else None,
            max_requests_per_hour=options.max_http_requests_per_hour,
        )
        self._configuration = dataclasses.replace(
            self._configuration,
            backend_options=tuple(
                (field.name, getattr(options, field.name))
                for field in dataclasses.fields(options)
            ),
        )

    @staticmethod
    def url_to_name(url: str) -> str:
        """
        Derive a sanitized display name using the shared path naming helper.

        Treat the URL as path text rather than parse its authority or query.
        The helper appends a short input hash; uniqueness is not guaranteed.

        Example:
            >>> NativeHtmlReadOnlyStorageBackend.url_to_name('https://example.test/') == safe_path_to_name('https://example.test/')
            True


        :param url: Text supplied unchanged to safe_path_to_name with its defaults.
        :return: Generated filename-style display name, without network activity.
        """
        return safe_path_to_name(url)

    def _probe_http_storage(self) -> None:
        """
        Fetch the crawl root and reject unsuccessful or out-of-scope final responses.

        This callback does not check robots, require HTML, reject truncation, or
        populate the crawl cache. The enclosing HTTP driver's probe can separately
        enumerate inventory afterward. Fetch errors propagate with their original
        types; an unusable returned record raises StorageUnavailable here.

        Example:
            >>> store._probe_http_storage()  # doctest: +SKIP


        :return: None when the root response satisfies the native usability predicate.
        :raises StorageUnavailable: Returned status/final URL fails that predicate.
        """
        result = self._fetch_url(self.url)
        if not self._usable_fetch_result(result):
            raise StorageUnavailable(
                "HTTP crawl root returned an unsuccessful or scope-escaping "
                f"response (status {result.status}, final URL {result.final_url!r}): "
                f"{self.url}"
            )

    @staticmethod
    def _open_http_request(
        request: urllib.request.Request,
        timeout_s: float | None,
    ):
        """
        Open a driver-prepared request with urllib and the supplied timeout.

        The driver owns response cleanup, rate scheduling, and Store policy.
        This seam adds no discovery/robots check or exception translation.

        Example:
            >>> with NativeHtmlReadOnlyStorageBackend._open_http_request(request, 30.0) as response:  # doctest: +SKIP
            ...     payload = response.read()


        :param request: Prepared urllib request passed directly to urlopen.
        :param timeout_s: Socket timeout seconds, or None without an explicit timeout.
        :return: Open response object for the driver to consume and close.
        """
        return urllib.request.urlopen(request, timeout=timeout_s)


__all__ = [
    "NATIVE_HTML_MAX_REQUESTS_PER_HOUR_DEFAULT",
    "NATIVE_HTML_MAX_REQUESTS_PER_HOUR_PREF_KEY",
    "NativeHtmlBackendOptions",
    "NativeHtmlReadOnlyStorageBackend",
    "_FetchResult",
    "get_default_crawler_http_requests_per_hour",
    "get_default_native_html_requests_per_hour",
]

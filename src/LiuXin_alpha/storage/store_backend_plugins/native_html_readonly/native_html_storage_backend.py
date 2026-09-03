"""Read-only Store combining native HTML discovery with HTTP byte access."""

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
    """A partial-inventory HTTP Store discovered by the native crawler."""

    store_kind = "native_html_readonly"

    def __init__(
        self,
        url: str,
        *,
        name: Optional[str] = None,
        uuid: Optional[str] = None,
        options: NativeHtmlBackendOptions | None = None,
    ) -> None:
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
        return safe_path_to_name(url)

    def _probe_http_storage(self) -> None:
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

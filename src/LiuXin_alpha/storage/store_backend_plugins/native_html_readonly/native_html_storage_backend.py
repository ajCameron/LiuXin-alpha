"""Read-only HTTP store wrapper over the native HTML discovery source."""

from __future__ import annotations

from typing import Iterator, Optional

from ...api import StoreAPI, StoreCheckStatus, StoreStatus
from LiuXin_alpha.ingest.sources.native_html import (
    NATIVE_HTML_MAX_REQUESTS_PER_HOUR_DEFAULT,
    NATIVE_HTML_MAX_REQUESTS_PER_HOUR_PREF_KEY,
    NativeHtmlBackendOptions,
    NativeHtmlDiscoverySource,
    _FetchResult,
    get_default_crawler_http_requests_per_hour,
    get_default_native_html_requests_per_hour,
)
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name

from .native_html_single_file import NativeHtmlReadOnlySingleFile


class NativeHtmlReadOnlyStorageBackend(StoreAPI, NativeHtmlDiscoverySource):
    """Read-only store facade for native HTML-discovered remote file URLs."""

    def __init__(
        self,
        url: str,
        *,
        name: Optional[str] = None,
        uuid: Optional[str] = None,
        options: NativeHtmlBackendOptions | None = None,
    ) -> None:
        StoreAPI.__init__(self, url=url, name=name, uuid=uuid)
        if options is None:
            options = NativeHtmlBackendOptions(
                max_http_requests_per_hour=get_default_crawler_http_requests_per_hour(),
            )
        NativeHtmlDiscoverySource.__init__(self, url=self.url, options=options)

    def url_to_name(self, url: str) -> str:
        return safe_path_to_name(url)

    def startup(self) -> None:
        NativeHtmlDiscoverySource.startup(self)

    def file_exists(self, file_url: str) -> bool:
        return NativeHtmlDiscoverySource.file_exists(self, file_url)

    def discover_urls(self, **kwargs) -> list[str]:  # noqa: ANN003 - compatibility shim
        return NativeHtmlDiscoverySource.discover_urls(self, **kwargs)

    def crawl_urls(self, **kwargs) -> list[str]:  # noqa: ANN003 - compatibility shim
        return NativeHtmlDiscoverySource.crawl_urls(self, **kwargs)

    def self_test(self) -> StoreStatus:
        cs = StoreCheckStatus()
        cs.store_marker_file = True
        cs.read = False
        cs.write = False
        cs.sundry = False
        good = "unknown"
        discovered = None if self._crawl_cache_urls is None else len(self._crawl_cache_urls)
        try:
            fetched = self._fetch_url(self.url)
            if int(fetched.status) < 400:
                cs.read = True
                cs.sundry = True
                good = "ok (read-only)"
            else:
                good = "unhealthy"
        except Exception as exc:
            self._event_log.put("self_test failed: {!r}".format(exc))
            good = "unhealthy"

        return StoreStatus(
            name=self.name,
            uuid=self.uuid or self.name,
            file_count=discovered,
            store_free_space=0,
            check_status=cs,
            checked=bool(cs.read),
            url=self.url,
            good=good,
            event_log=self._event_log,
            details={
                "crawler": "native_html",
                "max_http_requests_per_hour": self._normalized_requests_per_hour(),
                "timeout_s": self.options.timeout_s,
                "recurse": bool(self.options.recurse),
                "max_depth": self.options.max_depth,
            },
        )

    def status(self) -> StoreStatus:
        return self.self_test()

    def get_file(self, file_url: str) -> NativeHtmlReadOnlySingleFile:
        return NativeHtmlReadOnlySingleFile(file_url=file_url, store=self)

    def true_files(self) -> Iterator[NativeHtmlReadOnlySingleFile]:
        for url in list(self._crawl_cache_urls or ()):
            yield self.get_file(url)


__all__ = [
    "NATIVE_HTML_MAX_REQUESTS_PER_HOUR_DEFAULT",
    "NATIVE_HTML_MAX_REQUESTS_PER_HOUR_PREF_KEY",
    "NativeHtmlBackendOptions",
    "NativeHtmlReadOnlyStorageBackend",
    "_FetchResult",
    "get_default_crawler_http_requests_per_hour",
    "get_default_native_html_requests_per_hour",
]

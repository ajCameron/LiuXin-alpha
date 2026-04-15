"""Read-only HTTP store wrapper over the wget HTML discovery source."""

from __future__ import annotations

from typing import Iterator, Optional

from ...api import StorePluginAPI, StoreCheckStatus, StoreStatus, StoreLocationMixinAPI
from LiuXin_alpha.ingest.sources.wget_html import (
    WGET_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT,
    WGET_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY,
    WgetBackendOptions,
    WgetHtmlDiscoverySource,
    get_default_crawler_http_requests_per_hour,
    get_default_wget_http_requests_per_hour,
)
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name

from .wget_html_location import WgetHtmlReadOnlyStoreLocation
from .wget_utils import run_wget


class WgetHtmlReadOnlyStorageBackend(StorePluginAPI, WgetHtmlDiscoverySource):
    location_cls = WgetHtmlReadOnlyStoreLocation
    """Read-only store facade for wget-discovered remote file URLs."""

    def __init__(
        self,
        url: str,
        *,
        name: Optional[str] = None,
        uuid: Optional[str] = None,
        options: WgetBackendOptions | None = None,
    ) -> None:
        StorePluginAPI.__init__(self, url=url, name=name, uuid=uuid)
        if options is None:
            options = WgetBackendOptions(
                max_http_requests_per_hour=get_default_crawler_http_requests_per_hour(),
            )
        WgetHtmlDiscoverySource.__init__(self, url=self.url, options=options)

    def url_to_name(self, url: str) -> str:
        return safe_path_to_name(url)

    def _run_wget(self, args, **kwargs):  # noqa: ANN001 - passthrough for testability
        return run_wget(args, **kwargs)

    def startup(self) -> None:
        WgetHtmlDiscoverySource.startup(self)

    def file_exists(self, file_url: str) -> bool:
        return WgetHtmlDiscoverySource.file_exists(self, file_url)

    def discover_urls(self, **kwargs) -> list[str]:  # noqa: ANN003 - compatibility shim
        return WgetHtmlDiscoverySource.discover_urls(self, **kwargs)

    def crawl_urls(self, **kwargs) -> list[str]:  # noqa: ANN003 - compatibility shim
        return WgetHtmlDiscoverySource.crawl_urls(self, **kwargs)

    def self_test(self) -> StoreStatus:
        cs = StoreCheckStatus()
        cs.store_marker_file = True
        cs.read = False
        cs.write = False
        cs.sundry = False
        good = "unknown"

        try:
            urls = self.discover_urls(force=True)
            cs.read = True
            cs.sundry = True
            good = "ok (read-only)"
            discovered = len(urls)
        except Exception as exc:
            self._event_log.put("self_test failed: {!r}".format(exc))
            discovered = 0
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
                "crawler": "wget",
                "max_http_requests_per_hour": self._normalized_requests_per_hour(),
                "timeout_s": self.options.timeout_s,
            },
        )

    @property
    def root_path(self) -> str:
        return self.url

    def location(self, *tokens: str) -> WgetHtmlReadOnlyStoreLocation:
        return self.location_cls(*tokens, store=self)

    def _location_from_url(self, file_url: str | StoreLocationMixinAPI) -> WgetHtmlReadOnlyStoreLocation:
        if isinstance(file_url, StoreLocationMixinAPI):
            if file_url.store is self:
                return file_url
            file_url = file_url.file_url
        url = str(file_url).strip()
        base = self.url.rstrip("/") + "/"
        if url.startswith(base):
            rel = url[len(base):]
            return self.location(*[part for part in rel.split("/") if part])
        return self.location(*[part for part in url.split("/") if part])

    def status(self) -> StoreStatus:
        return self.self_test()

    def file_size(self, file_url: str | StoreLocationMixinAPI) -> int | None:
        if self.file_exists(file_url):
            return 0
        return None

    def get_file_status(self, file_url: str | StoreLocationMixinAPI):
        from LiuXin_alpha.storage.single_file import SingleFileStatus
        url = self._location_from_url(file_url).as_store_key()
        return SingleFileStatus(
            url=url,
            check_exists_function=lambda _url: bool(self.file_exists(url)),
            check_size_function=lambda _url: 0,
            check_hash_function=lambda _url: "",
        )

    def get_file(self, file_url: str | StoreLocationMixinAPI) -> WgetHtmlReadOnlyStoreLocation:
        return self._location_from_url(file_url)

    def add_file(self, *args, **kwargs):
        raise PermissionError("Wget HTML backend is read-only")

    def delete_file(self, *args, **kwargs):
        raise PermissionError("Wget HTML backend is read-only")

    def true_files(self) -> Iterator[WgetHtmlReadOnlyStoreLocation]:
        for url in self.discover_urls(force=False):
            yield self.get_file(url)

    def iter(self) -> Iterator[WgetHtmlReadOnlyStoreLocation]:
        return self.true_files()


__all__ = [
    "WgetBackendOptions",
    "WgetHtmlReadOnlyStorageBackend",
    "WGET_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT",
    "WGET_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY",
    "get_default_crawler_http_requests_per_hour",
    "get_default_wget_http_requests_per_hour",
    "run_wget",
]

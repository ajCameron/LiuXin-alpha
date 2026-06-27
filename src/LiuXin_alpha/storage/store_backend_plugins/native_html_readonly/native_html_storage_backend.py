"""Read-only HTML-discovery storage plugin using the native crawler.

This is a raw storage plugin over a remote HTML crawl root. It can discover and
name concrete remote file URLs, but it does not manage items, replicas,
policies, or database state.
"""

from __future__ import annotations

from typing import Optional

from ...api import StorePluginAPI, StoreCheckStatus, StoreStatus, StoreLocationMixinAPI
from LiuXin_alpha.ingest.sources.native_html import (
    NATIVE_HTML_MAX_REQUESTS_PER_HOUR_DEFAULT,
    NATIVE_HTML_MAX_REQUESTS_PER_HOUR_PREF_KEY,
    NativeHtmlBackendOptions,
    NativeHtmlDiscoverySource,
    _FetchResult,
    get_default_crawler_http_requests_per_hour,
    get_default_native_html_requests_per_hour,
)
from LiuXin_alpha.storage.single_file import SingleFileStatus
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name

from .native_html_location import NativeHtmlReadOnlyStoreLocation


class NativeHtmlReadOnlyStorageBackend(StorePluginAPI, NativeHtmlDiscoverySource):
    """Read-only plugin for native-HTML-discovered remote file URLs."""

    location_cls = NativeHtmlReadOnlyStoreLocation

    def __init__(
        self,
        url: str,
        *,
        name: Optional[str] = None,
        uuid: Optional[str] = None,
        options: NativeHtmlBackendOptions | None = None,
    ) -> None:
        StorePluginAPI.__init__(self, url=url, name=name, uuid=uuid)
        if options is None:
            options = NativeHtmlBackendOptions(
                max_http_requests_per_hour=get_default_crawler_http_requests_per_hour(),
            )
        NativeHtmlDiscoverySource.__init__(self, url=self.url, options=options)

    def url_to_name(self, url: str) -> str:
        return safe_path_to_name(url)

    def startup(self) -> StoreStatus:
        NativeHtmlDiscoverySource.startup(self)
        return self.self_test()

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

    @property
    def root_path(self) -> str:
        return self.url

    def location(self, *tokens: str) -> NativeHtmlReadOnlyStoreLocation:
        return self.location_cls(*tokens, store=self)

    def _location_from_identifier(
        self,
        file_identifier: str | StoreLocationMixinAPI,
    ) -> NativeHtmlReadOnlyStoreLocation:
        if isinstance(file_identifier, StoreLocationMixinAPI):
            if file_identifier.store is self:
                return file_identifier
            file_identifier = file_identifier.file_url
        url = str(file_identifier).strip()
        base = self.url.rstrip("/") + "/"
        if url.startswith(base):
            rel = url[len(base):]
            return self.location(*[part for part in rel.split("/") if part])
        return self.location(*[part for part in url.split("/") if part])

    def status(self) -> StoreStatus:
        return self.self_test()

    def locate(self, file_identifier: str | StoreLocationMixinAPI) -> NativeHtmlReadOnlyStoreLocation:
        return self._location_from_identifier(file_identifier)

    def exists(self, file_identifier: str | StoreLocationMixinAPI) -> bool:
        try:
            url = self._location_from_identifier(file_identifier).as_store_key()
        except Exception:
            return False
        return self.file_exists(url)

    def file_size(self, file_identifier: str | StoreLocationMixinAPI) -> int | None:
        if self.exists(file_identifier):
            return 0
        return None

    def stat(self, file_identifier: str | StoreLocationMixinAPI) -> SingleFileStatus:
        location = self._location_from_identifier(file_identifier)
        canonical_url = location.file_url

        def _exists(_url: str) -> bool:
            return self.exists(location)

        def _size(_url: str) -> int:
            return 0

        def _hash(_url: str) -> str:
            return ""

        return SingleFileStatus(
            url=canonical_url,
            size=0,
            file_hash="",
            check_exists_function=_exists,
            check_size_function=_size,
            check_hash_function=_hash,
        )

    def iter_locations(self):
        for url in self.discover_urls(force=False):
            yield self.locate(url)

    def write_bytes(
        self,
        file_bytes: bytes,
        *,
        metadata=None,
        location: str | None = None,
    ) -> NativeHtmlReadOnlyStoreLocation:
        raise PermissionError("NativeHtmlReadOnlyStorageBackend is read-only.")

    def copy_within_plugin(
        self,
        src_location: str | StoreLocationMixinAPI,
        dst_location: str | StoreLocationMixinAPI,
    ) -> NativeHtmlReadOnlyStoreLocation:
        raise PermissionError("NativeHtmlReadOnlyStorageBackend is read-only.")

    def delete(self, file_identifier: str | StoreLocationMixinAPI) -> bool:
        raise PermissionError("NativeHtmlReadOnlyStorageBackend is read-only.")

    def update_bytes(
        self,
        file_identifier: str | StoreLocationMixinAPI,
        file_bytes: bytes,
        *,
        append: bool = False,
    ) -> bool:
        raise PermissionError("NativeHtmlReadOnlyStorageBackend is read-only.")


__all__ = [
    "NATIVE_HTML_MAX_REQUESTS_PER_HOUR_DEFAULT",
    "NATIVE_HTML_MAX_REQUESTS_PER_HOUR_PREF_KEY",
    "NativeHtmlBackendOptions",
    "NativeHtmlReadOnlyStorageBackend",
    "_FetchResult",
    "get_default_crawler_http_requests_per_hour",
    "get_default_native_html_requests_per_hour",
]

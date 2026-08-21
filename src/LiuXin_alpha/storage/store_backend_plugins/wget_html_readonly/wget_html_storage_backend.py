"""Read-only Store combining wget discovery with HTTP byte access."""

from __future__ import annotations

import dataclasses
import urllib.request

from typing import Optional

from LiuXin_alpha.ingest.sources.wget_html import (
    WGET_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT,
    WGET_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY,
    WgetBackendOptions,
    WgetHtmlDiscoverySource,
    get_default_crawler_http_requests_per_hour,
    get_default_wget_http_requests_per_hour,
)
from LiuXin_alpha.storage.stores.http import HttpReadOnlyStore
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name

from .wget_utils import run_wget


class WgetHtmlReadOnlyStorageBackend(HttpReadOnlyStore, WgetHtmlDiscoverySource):
    """A partial-inventory HTTP Store discovered by ``wget --spider``."""

    store_kind = "wget_html_readonly"

    def __init__(
        self,
        url: str,
        *,
        name: Optional[str] = None,
        uuid: Optional[str] = None,
        options: WgetBackendOptions | None = None,
    ) -> None:
        if options is None:
            options = WgetBackendOptions(
                max_http_requests_per_hour=(
                    get_default_crawler_http_requests_per_hour()
                ),
            )
        WgetHtmlDiscoverySource.__init__(self, url=url, options=options)
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
                if field.name != "env"
            ),
        )

    @staticmethod
    def url_to_name(url: str) -> str:
        return safe_path_to_name(url)

    def _run_wget(self, args, **kwargs):  # noqa: ANN001 - testable subprocess seam
        return run_wget(args, **kwargs)

    def _probe_http_storage(self) -> None:
        WgetHtmlDiscoverySource.startup(self)
        self.discover_urls(force=True)

    @staticmethod
    def _open_http_request(
        request: urllib.request.Request,
        timeout_s: float | None,
    ):
        return urllib.request.urlopen(request, timeout=timeout_s)


__all__ = [
    "WgetBackendOptions",
    "WgetHtmlReadOnlyStorageBackend",
    "WGET_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT",
    "WGET_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY",
    "get_default_crawler_http_requests_per_hour",
    "get_default_wget_http_requests_per_hour",
    "run_wget",
]

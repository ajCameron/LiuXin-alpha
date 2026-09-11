"""
Compose wget spider discovery with an HTTP Store for separate remote byte access.

Wget provides candidate inventory; urllib-backed Store operations read bytes.
Construction does not execute either transport. The module-level runner import
is a replaceable subprocess seam used by discovery and startup probes.
"""

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
    """
    Expose wget's partial URL inventory through read-only HTTP Store operations.

    The HTTP facade takes precedence for Store lifecycle/file methods; crawl
    helpers retain source caching and callback behavior. Wget's generated waits
    and the byte driver's rate scheduler are independent. Changing source options
    later does not update the driver's captured timeout/rate/headers.

    Example:
        >>> store = WgetHtmlReadOnlyStorageBackend('https://example.test/books/', options=WgetBackendOptions(max_http_requests_per_hour=0))
        >>> store.configuration.read_only
        True
    """

    store_kind = "wget_html_readonly"

    def __init__(
        self,
        url: str,
        *,
        name: Optional[str] = None,
        uuid: Optional[str] = None,
        options: WgetBackendOptions | None = None,
    ) -> None:
        """
        Configure wget discovery, HTTP byte access, Store identity, and option metadata.

        Missing options use the shared modern preference resolver. Retain supplied
        options by reference and replace the facade's backend_options with their
        dataclass fields except env. Omission of env is not general credential
        scrubbing: wget_args/user_agent can still contain caller-supplied secrets.
        No executable lookup, startup, crawl, or catalogue registration runs here.

        Example:
            >>> store = WgetHtmlReadOnlyStorageBackend('https://example.test/', name='Remote books')
            >>> store.configuration.store_name
            'Remote books'


        :param url: HTTP(S) root normalized by discovery and checked by the byte driver.
        :param name: Display name, or falsey to derive one from the URL text.
        :param uuid: Store UUID text, or None for a new facade-generated identity.
        :param options: Mutable wget controls, or None for preference-backed defaults.
        :return: None after composing an unstarted configured Store.
        :raises ValueError: Root, UUID, or delegated option/driver validation fails.
        """
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
        """
        Generate a sanitized display name from URL text through the shared path helper.

        URL components are not interpreted separately. The helper's short input
        hash reduces collisions without establishing unique Store identity.

        Example:
            >>> WgetHtmlReadOnlyStorageBackend.url_to_name('https://example.test/') == safe_path_to_name('https://example.test/')
            True


        :param url: Text passed unchanged to safe_path_to_name with default options.
        :return: Filename-style display name; no remote lookup is performed.
        """
        return safe_path_to_name(url)

    def _run_wget(self, args, **kwargs):  # noqa: ANN001 - testable subprocess seam
        """
        Delegate process execution through this module's replaceable runner import.

        Example:
            >>> result = store._run_wget(['--version'], timeout_s=15)  # doctest: +SKIP


        :param args: Invocation tokens forwarded as the runner's positional argument.
        :param kwargs: Runner keyword options passed without modification.
        :return: Runner result; subprocess/policy/callback errors propagate.
        """
        return run_wget(args, **kwargs)

    def _probe_http_storage(self) -> None:
        """
        Check wget version execution and force a fresh discovery attempt.

        Ignore the returned URL count: successful empty discovery satisfies this
        callback. Version startup uses the source's fixed startup options, while
        discovery uses configured crawl settings. Errors propagate without undoing
        previous cache state; no ebook bytes are fetched through the HTTP driver.

        Example:
            >>> store._probe_http_storage()  # doctest: +SKIP


        :return: None after version execution and forced discovery return normally.
        """
        WgetHtmlDiscoverySource.startup(self)
        self.discover_urls(force=True)

    @staticmethod
    def _open_http_request(
        request: urllib.request.Request,
        timeout_s: float | None,
    ):
        """
        Open an HTTP-driver request directly through urllib with the supplied timeout.

        Wget arguments/environment and crawl callbacks do not participate in this
        byte-access seam. The driver owns response handling and cleanup.

        Example:
            >>> with WgetHtmlReadOnlyStorageBackend._open_http_request(request, 30.0) as response:  # doctest: +SKIP
            ...     payload = response.read()


        :param request: Prepared urllib request forwarded to urlopen.
        :param timeout_s: Socket timeout seconds, or None without an explicit timeout.
        :return: Open response; urllib failures propagate without wrapping here.
        """
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

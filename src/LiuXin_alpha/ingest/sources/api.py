from __future__ import annotations

import abc

from typing import Callable


LogLineCallback = Callable[[str], None]
DiscoveredUrlCallback = Callable[[str], None]
ObservedUrlCallback = Callable[[dict[str, object]], None]


class DiscoverySourceAPI(abc.ABC):
    """Minimal contract for remote discovery engines.

    A discovery source walks some remote surface and yields candidate URLs.
    It is intentionally narrower than `StoreAPI`: discovery decides what URLs
    are seen, while storage backends decide how specific files are addressed or
    opened.
    """

    _url: str

    def __init__(self, url: str) -> None:
        self._url = str(url)

    @property
    def url(self) -> str:
        return self._url

    @abc.abstractmethod
    def startup(self) -> None:
        """Validate that the discovery engine is usable."""

    @abc.abstractmethod
    def discover_urls(
        self,
        *,
        force: bool = False,
        log_line_callback: LogLineCallback | None = None,
        discovered_url_callback: DiscoveredUrlCallback | None = None,
        observed_url_callback: ObservedUrlCallback | None = None,
    ) -> list[str]:
        """Return candidate URLs accepted by this discovery source."""

"""
Define an abstract URL-discovery contract and its optional callback shapes.

Discovery inventories candidate addresses; it does not provide Store byte I/O
or catalogue registration. Concrete engines define cache, scope, callback-error,
and resource-limit policies. This module imposes no network or persistence work.
"""

from __future__ import annotations

import abc

from typing import Callable


LogLineCallback = Callable[[str], None]
DiscoveredUrlCallback = Callable[[str], None]
ObservedUrlCallback = Callable[[dict[str, object]], None]


class DiscoverySourceAPI(abc.ABC):
    """
    Require startup and URL discovery from a concrete remote-inventory engine.

    The base stores only str(url), without validating scheme, scope, or reachability.
    It cannot be instantiated until abstract methods are implemented. Discovery
    is narrower than StoreAPI: acceptance does not prove a readable ebook or a
    complete inventory of the remote site.

    Example:
        >>> source.discover_urls(force=False)  # doctest: +SKIP
    """

    _url: str

    def __init__(self, url: str) -> None:
        """
        Retain the discovery root as text without normalization or probing.

        Example:
            >>> super().__init__(url='https://example.test/books/')  # doctest: +SKIP


        :param url: Root selector converted using str; validation belongs to the concrete source.
        :return: None after assigning the private URL field.
        """
        self._url = str(url)

    @property
    def url(self) -> str:
        """
        Expose the stored discovery root without recomputing or validating it.

        Example:
            >>> source.url  # doctest: +SKIP
            'https://example.test/books/'


        :return: Root text recorded by the base initializer.
        """
        return self._url

    @abc.abstractmethod
    def startup(self) -> None:
        """
        Require a concrete readiness check appropriate to the engine.

        Implementations may probe a URL or only an executable; success does not
        have a universal meaning of complete or safe discovery. The base has no
        implementation or automatic call from discover_urls.

        Example:
            >>> source.startup()  # doctest: +SKIP


        :return: None on successful concrete startup validation.
        """

    @abc.abstractmethod
    def discover_urls(
        self,
        *,
        force: bool = False,
        log_line_callback: LogLineCallback | None = None,
        discovered_url_callback: DiscoveredUrlCallback | None = None,
        observed_url_callback: ObservedUrlCallback | None = None,
    ) -> list[str]:
        """
        Require discovery of accepted candidate URLs with optional progress consumers.

        Concrete implementations determine caching, callback ordering, and
        exception handling. A callback receives observations, not an authorization
        token or a transactional promise about later persistence.

        Example:
            >>> urls = source.discover_urls(force=True)  # doctest: +SKIP


        :param force: Request fresh discovery instead of any implementation cache.
        :param log_line_callback: Optional textual diagnostic consumer.
        :param discovered_url_callback: Optional consumer of each accepted URL.
        :param observed_url_callback: Optional consumer of acceptance/rejection detail mappings.
        :return: List of accepted candidate URL strings under the concrete engine's policy.
        """

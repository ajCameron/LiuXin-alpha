"""Driver-owned key resolution and allocation facade."""

from __future__ import annotations

import abc

from LiuXin_alpha.storage.api2.errors import StoreUnsupportedOperation
from LiuXin_alpha.storage.api2.models import Digest
from LiuXin_alpha.storage.api2.store_driver_api.models import DriverKey, DriverKeyInput


class StoreDriverKeyAPI(abc.ABC):
    """Resolve opaque object keys relative to one configured driver endpoint.

    This carries forward the useful legacy ``root_path``, ``location`` and
    ``locate`` concepts without exposing backend-specific path objects above
    the driver boundary.

    Example:
        >>> def nested_key(driver: StoreDriverKeyAPI) -> DriverKey:
        ...     return driver.join_key("authors", "book.epub")
    """

    @property
    def driver_kind(self) -> str:
        """Return the concrete driver class name as its default kind.

        Example:
            >>> kind = driver.driver_kind  # doctest: +SKIP
        """
        return type(self).__name__

    @property
    @abc.abstractmethod
    def root_uri(self) -> str:
        """Return the endpoint URI to which driver keys are relative.

        Example:
            >>> root_uri = driver.root_uri  # doctest: +SKIP
        """
        ...

    def suggest_store_name(self) -> str:
        """Suggest a display name when configuration does not provide one.

        Drivers may override this legacy ``url_to_name`` replacement when the
        endpoint has a useful backend-specific label.  The neutral fallback is
        the driver kind and does not parse the endpoint URI.

        Example:
            >>> suggested_name = driver.suggest_store_name()  # doctest: +SKIP
        """
        return self.driver_kind

    @abc.abstractmethod
    def resolve_key(self, identifier: DriverKeyInput) -> DriverKey:
        """Canonicalize a driver key or endpoint-owned URI.

        Example:
            >>> key = driver.resolve_key("authors/book.epub")  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def join_key(self, *tokens: str) -> DriverKey:
        """Construct a canonical key using driver-specific joining rules.

        Example:
            >>> key = driver.join_key("authors", "book.epub")  # doctest: +SKIP
        """
        ...

    def allocate_key(
        self,
        *,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        name_hint: str | None = None,
    ) -> DriverKey:
        """Allocate a safe driver-selected key when inherently supported.

        Writable drivers that advertise ``key_allocation`` override this
        method.  Content-addressed drivers commonly derive the key from
        ``expected_digest``; other drivers may use a collision-resistant id.

        Example:
            >>> key = driver.allocate_key(  # doctest: +SKIP
            ...     expected_size=4, name_hint="book.epub",
            ... )
        """
        raise StoreUnsupportedOperation(
            f"{self.driver_kind} does not support driver-selected keys."
        )


__all__ = ["StoreDriverKeyAPI"]

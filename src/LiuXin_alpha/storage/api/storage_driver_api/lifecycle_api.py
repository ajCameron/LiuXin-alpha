"""
Store-neutral driver connection lifecycle and health facade.
"""

from __future__ import annotations

import abc

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api.storage_driver_api.models import DriverStatus


class StorageDriverLifecycleAPI(abc.ABC):
    """
    Start, probe, inspect, and close one configured storage endpoint.

    Example:
        >>> def healthy(driver: StorageDriverLifecycleAPI) -> bool:
        ...     return driver.probe().available
    """

    @abc.abstractmethod
    def startup(self) -> "DriverStatus":
        """
        Idempotently connect or initialize the driver and return its status.

        Construction configures a driver but need not connect it. Calling
        ``startup`` repeatedly must not leak or duplicate backend resources.

        Example:
            >>> status = driver.startup()  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def probe(self) -> "DriverStatus":
        """Actively test backend access and return a fresh status snapshot.

        Ordinary offline conditions return ``DriverStatus(available=False)``.
        Invalid configuration, authentication, permission, and unexpected
        backend failures remain typed exceptions rather than being flattened
        into an unavailable status.

        Example:
            >>> status = driver.probe()  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def status(self) -> "DriverStatus":
        """Return current dynamic status without suppressing failures.

        Example:
            >>> status = driver.status()  # doctest: +SKIP
        """
        ...

    @property
    def available(self) -> bool:
        """Return current availability without suppressing failures.

        Example:
            >>> available = driver.available  # doctest: +SKIP
        """
        return self.status().available

    @property
    def writable(self) -> bool:
        """Return whether the driver's current state permits writes.

        Example:
            >>> writable = driver.writable  # doctest: +SKIP
        """
        return self.status().writable

    def close(self) -> None:
        """Release backend resources; repeated closure should be safe.

        Example:
            >>> driver.close()  # doctest: +SKIP
        """
        return None


__all__ = ["StorageDriverLifecycleAPI"]

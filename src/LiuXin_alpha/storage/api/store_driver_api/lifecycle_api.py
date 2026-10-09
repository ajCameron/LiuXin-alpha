"""
Specify startup, health observation, and closure for a configured raw driver.

The abstract methods define lifecycle behavior; convenience properties forward
current status without suppressing failures. The base close method is a no-op,
so resource-owning implementations must override it.

Example:
    >>> status = driver.probe()  # doctest: +SKIP
"""

from __future__ import annotations

import abc

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api.store_driver_api.models import DriverStatus


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

        Construction configures a driver but need not connect it. Calling ``startup`` repeatedly
        must not leak or duplicate backend resources.

        Example:
            >>> status = driver.startup()  # doctest: +SKIP


        :return: Operational DriverStatus after idempotent initialization; implementation failures remain visible.
        """
        ...

    @abc.abstractmethod
    def probe(self) -> "DriverStatus":
        """
        Actively test backend access and return a fresh status snapshot.

        Ordinary offline conditions return ``DriverStatus(available=False)``. Invalid configuration,
        authentication, permission, and unexpected backend failures remain typed exceptions rather
        than being flattened into an unavailable status.

        Example:
            >>> status = driver.probe()  # doctest: +SKIP


        :return: Fresh health/status snapshot; ordinary offline state can be unavailable while other failures raise.
        """
        ...

    @abc.abstractmethod
    def status(self) -> "DriverStatus":
        """
        Return current dynamic status without suppressing failures.

        Example:
            >>> status = driver.status()  # doctest: +SKIP


        :return: Implementation-defined current DriverStatus observation, without generic exception suppression.
        """
        ...

    @property
    def available(self) -> bool:
        """
        Return current availability without suppressing failures.

        The property does not independently probe; it uses the concrete status method.

        Example:
            >>> available = driver.available  # doctest: +SKIP


        :return: available from a direct status() call; any status failure propagates.
        """
        return self.status().available

    @property
    def writable(self) -> bool:
        """
        Return whether the driver's current state permits writes.

        This property forwards the flag without deriving it from other status fields.

        Example:
            >>> writable = driver.writable  # doctest: +SKIP


        :return: writable from a direct status() call, without an additional capability or availability check.
        """
        return self.status().writable

    def close(self) -> None:
        """
        Release backend resources; repeated closure should be safe.

        The default returns None, matching ordinary context-manager cleanup semantics. Shutdown
        observations remain available through status/probe rather than a second close-result type.
        Subclasses owning connections, clients, or staged resources supply their own idempotent
        release behavior.

        Example:
            >>> driver.close()  # doctest: +SKIP


        :return: None; this base implementation performs no cleanup.
        """
        return None


__all__ = ["StorageDriverLifecycleAPI"]

"""Store-driver connection lifecycle and health facade."""

from __future__ import annotations

import abc

from LiuXin_alpha.storage.api2.models import StoreStatus


class StoreDriverLifecycleAPI(abc.ABC):
    """Start, probe, inspect, and close one backend driver instance.

    Unlike the legacy ``online`` helper, status failures are not converted to
    ``False``; typed connection and permission errors remain visible.

    Example:
        >>> def healthy(driver: StoreDriverLifecycleAPI) -> bool:
        ...     return driver.probe().available
    """

    @abc.abstractmethod
    def startup(self) -> StoreStatus:
        """Connect or initialize the driver and return its resulting status.

        Example:
            >>> status = driver.startup()  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def probe(self) -> StoreStatus:
        """Actively test backend access and return a fresh status snapshot.

        This is the precise replacement for the legacy ``self_test`` method.

        Example:
            >>> status = driver.probe()  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def status(self) -> StoreStatus:
        """Return the driver's current dynamic status without hiding errors.

        Example:
            >>> status = driver.status()  # doctest: +SKIP
        """
        ...

    @property
    def available(self) -> bool:
        """Return the current availability flag without suppressing failures.

        Example:
            >>> available = driver.available  # doctest: +SKIP
        """
        return self.status().available

    @property
    def writable(self) -> bool:
        """Return whether current driver status permits writes.

        Example:
            >>> writable = driver.writable  # doctest: +SKIP
        """
        return self.status().writable

    def close(self) -> None:
        """Release backend resources; the default implementation is a no-op.

        Overrides should make repeated closure safe.

        Example:
            >>> driver.close()  # doctest: +SKIP
        """
        return None


__all__ = ["StoreDriverLifecycleAPI"]

"""
Configured-store lifecycle and dynamic status facade.
"""

from __future__ import annotations

import abc

from LiuXin_alpha.storage.api.models import StoreStatus


class StoreLifecycleAPI(abc.ABC):
    """
    Start, probe, inspect, and close one configured store.

    Lifecycle belongs to the configured store wrapper.  Backend connection
    details remain below this boundary and durable registry persistence remains
    in the storage manager.

    Example:
        >>> def check_writable(store: StoreLifecycleAPI) -> bool:
        ...     return store.status(refresh=True).writable
    """

    @abc.abstractmethod
    def startup(self) -> StoreStatus:
        """
        Start the store and return its resulting operational status.

        Example:
            >>> status = store.startup()  # doctest: +SKIP


        :return:
        """
        ...

    @abc.abstractmethod
    def probe(self) -> StoreStatus:
        """
        Actively check the configured store and return fresh status.

        Example:
            >>> status = store.probe()  # doctest: +SKIP


        :return:
        """
        ...

    @abc.abstractmethod
    def status(self, *, refresh: bool = False) -> StoreStatus:
        """
        Return cached status, optionally probing before returning it.

        Example:
            >>> status = store.status(refresh=True)  # doctest: +SKIP


        :param refresh:
        :return:
        """
        ...

    @property
    def available(self) -> bool:
        """
        Return current availability without concealing status failures.

        Example:
            >>> available = store.available  # doctest: +SKIP


        :return:
        """
        return self.status().available

    @property
    def writable(self) -> bool:
        """
        Return whether current store status permits writes.

        Example:
            >>> writable = store.writable  # doctest: +SKIP


        :return:
        """
        return self.status().writable

    @abc.abstractmethod
    def close(self) -> None:
        """
        Release resources held for this configured store.

        Repeated closure should be safe for callers performing cleanup.

        Example:
            >>> store.close()  # doctest: +SKIP


        :return:
        """
        ...


__all__ = ["StoreLifecycleAPI"]

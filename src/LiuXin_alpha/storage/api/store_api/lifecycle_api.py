"""
Define configured-Store startup, probing, cached status, and resource release.

Concrete wrappers combine endpoint state with Store configuration. These abstract
operations do not persist registry state or implement a universal probe policy.
The available/writable properties return the corresponding status fields and
leave status failures visible.

Example:
    >>> status = store.status(refresh=True)  # doctest: +SKIP
"""

from __future__ import annotations

import abc

from LiuXin_alpha.storage.api.models import StoreStatus


class StoreLifecycleAPI(abc.ABC):
    """
    Start, probe, inspect, and close one configured store.

    Lifecycle belongs to the configured store wrapper.  Backend connection details remain below this
    boundary and durable registry persistence remains in the storage manager.

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


        :return: Resulting StoreStatus after the implementation attempts startup; callers must inspect its state.
        """
        ...

    @abc.abstractmethod
    def probe(self) -> StoreStatus:
        """
        Actively check the configured store and return fresh status.

        Example:
            >>> status = store.probe()  # doctest: +SKIP


        :return: Fresh operational status after checking this configured endpoint.
        """
        ...

    @abc.abstractmethod
    def status(self, *, refresh: bool = False) -> StoreStatus:
        """
        Return cached status, optionally probing before returning it.

        Example:
            >>> status = store.status(refresh=True)  # doctest: +SKIP


        :param refresh: Whether to request a fresh probe instead of returning cached status.
        :return: Current StoreStatus under the implementation's cached/refresh policy.
        """
        ...

    @property
    def available(self) -> bool:
        """
        Return current availability without concealing status failures.

        This property adds no probe call or fallback value of its own.

        Example:
            >>> available = store.available  # doctest: +SKIP


        :return: The available field returned by status() with its default refresh policy.
        """
        return self.status().available

    @property
    def writable(self) -> bool:
        """
        Return whether current store status permits writes.

        Concrete status production owns the combination of endpoint state and configured read-only
        policy.

        Example:
            >>> writable = store.writable  # doctest: +SKIP


        :return: The writable field returned by status(), without an additional availability or capability test here.
        """
        return self.status().writable

    @abc.abstractmethod
    def close(self) -> None:
        """
        Release resources held for this configured store.

        Repeated closure should be safe for callers performing cleanup.

        Example:
            >>> store.close()  # doctest: +SKIP


        :return: None after implementation-owned cleanup; concrete cleanup failures may propagate.
        """
        ...


__all__ = ["StoreLifecycleAPI"]

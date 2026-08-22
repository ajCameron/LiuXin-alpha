"""Aggregate operational health contract for storage managers."""

from __future__ import annotations

import abc

from LiuXin_alpha.storage.api.storage_manager_api.models.operational import (
    StorageOperationalStatus,
)


class StorageOperationalStatusAPI(abc.ABC):
    """Expose actionable health without leaking repository rows.

    Example:
        >>> isinstance(manager, StorageOperationalStatusAPI)  # doctest: +SKIP
        True
    """

    @abc.abstractmethod
    def get_operational_status(
        self,
        *,
        refresh_stores: bool = False,
    ) -> StorageOperationalStatus:
        """Inspect Store reachability, journal, Replica, and policy state.

        Example:
            >>> status = manager.get_operational_status()  # doctest: +SKIP
            >>> status.healthy  # doctest: +SKIP
            True
        """

        ...


__all__ = ["StorageOperationalStatusAPI"]

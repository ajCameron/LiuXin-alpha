"""Store reconciliation facade above raw enumeration."""

import abc

from LiuXin_alpha.storage.api2.models import StoreRef
from LiuXin_alpha.storage.api2.storage_manager_api.models import ReconciliationReport


class StorageReconciliationAPI(abc.ABC):
    """Compare raw store inventory with catalogued replica metadata.

    Dry-run reconciliation is the safe default and reports intended repairs
    without applying them.

    Example:
        >>> def preview(manager: StorageReconciliationAPI) -> ReconciliationReport:
        ...     return manager.reconcile_store("archive", dry_run=True)
    """

    @abc.abstractmethod
    def reconcile_store(
        self, store_ref: StoreRef, *, dry_run: bool = True,
        verify_digests: bool = False,
    ) -> ReconciliationReport:
        """Compare one store's concrete files with catalogue expectations.

        Example:
            >>> report = manager.reconcile_store(  # doctest: +SKIP
            ...     "archive", dry_run=True, verify_digests=True,
            ... )
        """
        ...


__all__ = ["StorageReconciliationAPI"]

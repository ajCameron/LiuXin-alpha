"""Store reconciliation planning and application facade."""

import abc

from LiuXin_alpha.storage.api.models import StoreRef
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    ReconciliationPlan,
    ReconciliationReport,
)


class StorageReconciliationAPI(abc.ABC):
    """Compare Store inventory with Replica claims and apply reviewed changes.

    Planning is non-mutating. Applying a plan is explicit and can reject stale
    repository state rather than hiding mutation behind a ``dry_run`` flag.

    Example:
        >>> plan = manager.plan_reconciliation(store_uuid)  # doctest: +SKIP
        >>> report = manager.apply_reconciliation(plan)  # doctest: +SKIP
    """

    @abc.abstractmethod
    def plan_reconciliation(
        self,
        store_ref: StoreRef,
        *,
        verify_digests: bool = False,
    ) -> ReconciliationPlan:
        """Create a non-mutating comparison with declared completeness.

        Example:
            >>> plan = manager.plan_reconciliation(  # doctest: +SKIP
            ...     store_uuid, verify_digests=True,
            ... )
        """
        ...

    @abc.abstractmethod
    def apply_reconciliation(
        self,
        plan: ReconciliationPlan,
    ) -> ReconciliationReport:
        """Apply one current plan or raise ``ReconciliationPlanStale``.

        Example:
            >>> report = manager.apply_reconciliation(plan)  # doctest: +SKIP
        """
        ...


__all__ = ["StorageReconciliationAPI"]

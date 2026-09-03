"""
Store reconciliation planning and application facade.
"""

import abc

from LiuXin_alpha.storage.api.models import StoreUUID
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    StoreReconciliationPlan,
    StoreReconciliationReport,
)


class StorageReconciliationAPI(abc.ABC):
    """
    Compare Store inventory with Replica claims and apply reviewed changes.

    Planning is non-mutating. Applying a plan is explicit and can reject stale
    repository state rather than hiding mutation behind a ``dry_run`` flag.

    Example:
        >>> plan = manager.plan_reconciliation(store_uuid)  # doctest: +SKIP
        >>> report = manager.apply_reconciliation(plan)  # doctest: +SKIP
    """

    @abc.abstractmethod
    def plan_reconciliation(
        self,
        store_ref: StoreUUID,
        *,
        verify_digests: bool = False,
    ) -> StoreReconciliationPlan:
        """
        Create a non-mutating comparison with declared completeness.

        Example:
            >>> plan = manager.plan_reconciliation(  # doctest: +SKIP
            ...     store_uuid, verify_digests=True,
            ... )


        :param store_ref:
        :param verify_digests:
        :return:
        """
        ...

    @abc.abstractmethod
    def apply_reconciliation(
        self,
        plan: StoreReconciliationPlan,
    ) -> StoreReconciliationReport:
        """
        Apply one current plan or raise ``StoreReconciliationPlanStale``.

        Example:
            >>> report = manager.apply_reconciliation(plan)  # doctest: +SKIP


        :param plan:
        :return:
        """
        ...


__all__ = ["StorageReconciliationAPI"]

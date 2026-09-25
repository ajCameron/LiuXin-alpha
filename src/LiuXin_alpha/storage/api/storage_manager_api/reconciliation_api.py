"""
Define explicit comparison and application of Store inventory against Replica claims.

Planning returns classifications without updating manager observations. Application
is a separate metadata mutation; completeness and digest-verification intent remain
evidence to review alongside the implementation's revision and failure boundaries.
"""

import abc

from LiuXin_alpha.storage.api.models import StoreUUID
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    StoreReconciliationPlan,
    StoreReconciliationReport,
)


# Todo: What is reconciliation? I think I know, but some clarity would be good.
class StorageReconciliationAPI(abc.ABC):
    """
    Separate inventory comparison from applying classified Replica observations.

    Planning does not update manager records. Applying a plan changes metadata explicitly, without
    repairing bytes or adopting unexpected objects. The composed manager guards a global Replica
    generation; it does not promise a Store snapshot or independent Store-generation check.

    Example:
        >>> plan = manager.plan_reconciliation(store_uuid)  # doctest: +SKIP
        >>> report = manager.apply_reconciliation(plan)  # doctest: +SKIP
    """

    @abc.abstractmethod
    def plan_reconciliation(
        self,
        # Todo: If this is a StoreUUID ... call it that?
        store_ref: StoreUUID,
        *,
        verify_digests: bool = False,
    ) -> StoreReconciliationPlan:
        """
        Compare nondeleted claims with inventory and return classifications plus completeness
        evidence.

        Complete enumeration can establish missing locations without probing each absent claim.
        Partial or unavailable enumeration requires individual inspection. Digest checking is
        optional and can use an authoritative Store digest instead of rereading bytes. The composed
        manager combines separately captured observations and records a repository revision after
        inspection, rather than acquiring one atomic snapshot.

        Example:
            >>> plan = manager.plan_reconciliation(store_uuid, verify_digests=True)  # doctest: +SKIP

        :param store_ref: Registered Store whose inventory and nondeleted Replica claims should be compared.
        :param verify_digests: Whether individual inspections should compare digest evidence in addition to size; absent complete-inventory claims are not read.

        :return: Plan containing counts, missing/corrupt/unavailable IDs, unexpected locations, revision evidence, and diagnostics; unexpected lookup failures propagate.
        """
        ...

    # Todo: Not clear what this plan is intended to do...
    @abc.abstractmethod
    def apply_reconciliation(
        self,
        plan: StoreReconciliationPlan,
    ) -> StoreReconciliationReport:
        """
        Apply a revision-compatible plan's missing, corrupt, and unavailable classifications to
        metadata.

        The composed manager checks the global Replica revision and each listed record's Store
        membership, then replaces observations. It does not require a conclusive plan, reprobe
        bytes, promote matched claims to VERIFIED, or act on unexpected objects. Lists are processed
        in classification order without deduplication; transaction rollback depends on the
        repository implementation.

        Example:
            >>> report = manager.apply_reconciliation(plan)  # doctest: +SKIP


        :param plan: Reviewed classifications with repository revision and Store identity; the producer owns their accuracy.
        :return: Application report retaining the plan and updated IDs; stale revision or wrong-Store membership raises StoreReconciliationPlanStale.
        """
        ...


__all__ = ["StorageReconciliationAPI"]

"""
Aggregate health and durable ingest recovery operations.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import override
from uuid import UUID

import LiuXin_alpha.storage.api as api
from LiuXin_alpha.storage.storage_manager.mixins._state import _StorageManagerState


class StorageOperationalStatusMixin(_StorageManagerState):
    """
    Aggregate storage health into operator-facing issues and actions.

    Status combines Store availability, durable ingest journals, Replica
    observations, and policy assessments without mutating manager metadata.
    ``refresh_stores=True`` may ask Store plugins for fresh status.  Durable
    recovery and retry operations are no-ops or errors here and are overridden
    by the database-backed application manager.
    """

    @override
    def get_operational_status(
        self,
        *,
        refresh_stores: bool = False,
    ) -> api.StorageOperationalStatus:
        """
        Return an attributable, actionable snapshot of storage health.


        :param refresh_stores:
        :return:
        """

        store_statuses = tuple(self.iter_store_statuses(refresh=refresh_stores))
        issues: list[api.StorageOperationalIssue] = []
        actions: list[api.StorageRecoveryAction] = []
        for found_issues, found_actions in (
            self._store_operational_findings(store_statuses),
            self._ingest_operational_findings(),
            self._replica_operational_findings(),
            self._policy_operational_findings(),
        ):
            issues.extend(found_issues)
            actions.extend(found_actions)
        issues.extend(self._deferred_recovery_issues())

        return api.StorageOperationalStatus(
            checked_at=datetime.now(UTC),
            store_statuses=store_statuses,
            issues=tuple(issues),
            recovery_actions=tuple(dict.fromkeys(actions)),
        )

    def _store_operational_findings(
        self,
        store_statuses: tuple[api.StoreStatusObservation, ...],
    ) -> tuple[
        list[api.StorageOperationalIssue],
        list[api.StorageRecoveryAction],
    ]:
        """
        Translate unavailable Stores and plugin warnings into findings.


        :param store_statuses:
        :return:
        """

        issues: list[api.StorageOperationalIssue] = []
        actions: list[api.StorageRecoveryAction] = []
        for observation in store_statuses:
            issues.extend(
                api.StorageOperationalIssue(
                    "store_warning",
                    api.StorageOperationalSeverity.WARNING,
                    warning,
                    store_ref=observation.store_ref,
                )
                for warning in observation.status.warnings
            )
            if observation.status.available:
                continue
            message = (
                observation.status.message
                or f"Store {observation.store_ref} is unavailable."
            )
            issues.append(
                api.StorageOperationalIssue(
                    "store_unavailable",
                    api.StorageOperationalSeverity.WARNING,
                    message,
                    store_ref=observation.store_ref,
                )
            )
            actions.append(
                api.StorageRecoveryAction(
                    "reload_stores",
                    "Reload the Store after its endpoint becomes available.",
                    store_ref=observation.store_ref,
                )
            )
        return issues, actions

    def _ingest_operational_findings(
        self,
    ) -> tuple[
        list[api.StorageOperationalIssue],
        list[api.StorageRecoveryAction],
    ]:
        """
        Translate unfinished durable journal rows into recovery guidance.


        :return:
        """

        issues: list[api.StorageOperationalIssue] = []
        actions: list[api.StorageRecoveryAction] = []
        for journal in self._ingest_journal_statuses():
            state = str(journal.get("state") or "unknown")
            if state == "committed":
                continue
            operation_id = journal.get("operation_id")
            if not isinstance(operation_id, UUID):
                operation_id = None
            last_error = journal.get("last_error")
            if state == "failed":
                message = f"Ingest {operation_id} failed"
                if last_error:
                    message += f": {last_error}"
                issues.append(
                    api.StorageOperationalIssue(
                        "ingest_failed",
                        api.StorageOperationalSeverity.ERROR,
                        message,
                        operation_id=operation_id,
                    )
                )
                actions.append(
                    api.StorageRecoveryAction(
                        "retry_ingest",
                        "Retry with the same operation UUID after correcting the failure.",
                        operation_id=operation_id,
                    )
                )
                continue
            issues.append(
                api.StorageOperationalIssue(
                    "ingest_pending",
                    api.StorageOperationalSeverity.WARNING,
                    f"Ingest {operation_id} remains in journal state {state!r}.",
                    operation_id=operation_id,
                )
            )
            actions.append(
                api.StorageRecoveryAction(
                    "recover_pending_ingests",
                    "Run pending-ingest recovery after required Stores are online.",
                    operation_id=operation_id,
                )
            )
        return issues, actions

    def _replica_operational_findings(
        self,
    ) -> tuple[
        list[api.StorageOperationalIssue],
        list[api.StorageRecoveryAction],
    ]:
        """
        Report Replica observations that cannot currently satisfy reads.


        :return:
        """

        issues: list[api.StorageOperationalIssue] = []
        actions: list[api.StorageRecoveryAction] = []
        unhealthy_states = {
            api.ReplicaState.MISSING,
            api.ReplicaState.UNAVAILABLE,
            api.ReplicaState.CORRUPT,
        }
        for replica in self.iter_replica_records():
            if replica.state not in unhealthy_states:
                continue
            corrupt = replica.state is api.ReplicaState.CORRUPT
            issues.append(
                api.StorageOperationalIssue(
                    "replica_corrupt" if corrupt else "replica_unavailable",
                    (
                        api.StorageOperationalSeverity.ERROR
                        if corrupt or replica.state is api.ReplicaState.MISSING
                        else api.StorageOperationalSeverity.WARNING
                    ),
                    f"Replica {replica.replica_id} for Digital Asset {replica.digital_asset_id} is {replica.state.value}.",
                    digital_asset_id=replica.digital_asset_id,
                    replica_id=replica.replica_id,
                    store_ref=replica.location.store_ref,
                )
            )
            actions.append(
                api.StorageRecoveryAction(
                    "replicate_digital_asset",
                    "Create and verify another Replica from a healthy source.",
                    digital_asset_id=replica.digital_asset_id,
                    replica_id=replica.replica_id,
                    store_ref=replica.location.store_ref,
                )
            )
        return issues, actions

    def _policy_operational_findings(
        self,
    ) -> tuple[
        list[api.StorageOperationalIssue],
        list[api.StorageRecoveryAction],
    ]:
        """
        Report Assets whose replication or backup policy is unsatisfied.


        :return:
        """

        issues: list[api.StorageOperationalIssue] = []
        actions: list[api.StorageRecoveryAction] = []
        for asset in self.iter_digital_asset_records():
            try:
                assessment = self.assess_digital_asset(asset.digital_asset_id)
            except Exception as error:
                issues.append(
                    api.StorageOperationalIssue(
                        "policy_assessment_failed",
                        api.StorageOperationalSeverity.ERROR,
                        f"Could not assess Digital Asset {asset.digital_asset_id}: {str(error) or type(error).__name__}",
                        digital_asset_id=asset.digital_asset_id,
                    )
                )
                continue
            for code, satisfied, action, reason in (
                (
                    "replication_policy_violation",
                    assessment.replication_satisfied,
                    "plan_replication",
                    "Plan or execute additional live Replica placement.",
                ),
                (
                    "backup_policy_violation",
                    assessment.backup_satisfied,
                    "plan_backup",
                    "Plan or execute an additional backup/archive Replica.",
                ),
            ):
                if satisfied:
                    continue
                issues.append(
                    api.StorageOperationalIssue(
                        code,
                        (
                            api.StorageOperationalSeverity.ERROR
                            if assessment.unavailable
                            else api.StorageOperationalSeverity.WARNING
                        ),
                        f"Digital Asset {asset.digital_asset_id} does not meet its {code.replace('_', ' ')}.",
                        digital_asset_id=asset.digital_asset_id,
                    )
                )
                actions.append(
                    api.StorageRecoveryAction(
                        action,
                        reason,
                        digital_asset_id=asset.digital_asset_id,
                    )
                )
        return issues, actions

    def _deferred_recovery_issues(self) -> list[api.StorageOperationalIssue]:
        """
        Expose startup recovery failures retained by the application manager.


        :return:
        """

        return [
            api.StorageOperationalIssue(
                "ingest_recovery_deferred",
                api.StorageOperationalSeverity.WARNING,
                str(message),
            )
            for message in tuple(getattr(self, "ingest_recovery_issues", ()))
        ]

    def list_ingest_operations(self) -> tuple[Mapping[str, object], ...]:
        """
        Return operator-safe durable ingest summaries when available.


        :return:
        """

        return self._ingest_journal_statuses()

    def recover_pending_ingests(
        self,
        operation_id: UUID | None = None,
    ) -> tuple[str, ...]:
        """
        Recover durable publication gaps; transient managers have none.


        :param operation_id:
        :return:
        """

        del operation_id
        return ()

    def retry_ingest_operation(
        self,
        operation_id: UUID,
    ) -> api.DigitalAssetIngestResult:
        """
        Retry one durable ingest when its original source is replayable.


        :param operation_id:
        :return:
        """

        del operation_id
        raise api.StoragePreconditionFailed(
            "transient storage managers have no durable ingest journal."
        )


__all__ = ["StorageOperationalStatusMixin"]

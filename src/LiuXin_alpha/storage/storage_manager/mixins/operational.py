"""
Aggregate manager observations into issues and suggested recovery actions.

The default recovery methods model a transient manager without a durable journal.
Application overrides own publication recovery and retry; inspection remains
separate from those actions.
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
    Aggregate Store, journal, Replica, policy, and deferred-recovery findings from shared manager
    hooks. Inspection does not itself repair publications or replace Replica observations, though
    delegated Store/profile hooks may perform fresh reads. This base has no durable recovery: its
    recover method returns empty and retry raises; the application manager overrides those
    operations.

    Example:
        >>> status = manager.get_operational_status()  # doctest: +SKIP
    """

    @override
    def get_operational_status(
        self,
        *,
        refresh_stores: bool = False,
    ) -> api.StorageOperationalStatus:
        """
        Materialize attributable Store status, then collect Store, ingest, Replica, and policy
        findings in that order and append deferred-recovery issues. Exact-equal hashable recovery
        actions are deduplicated in first-seen order; issues are retained. checked_at is UTC time
        sampled after collection.

        Separate reads do not form one atomic snapshot. Helper/iteration errors propagate except
        where an individual helper explicitly translates them; this method applies no recovery or
        verification itself.

        Example:
            >>> status = manager.get_operational_status(refresh_stores=True)  # doctest: +SKIP


        :param refresh_stores: Flag forwarded to Store status enumeration.
        :return: New aggregate status with ordered issues and deduplicated suggested actions.
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

        status = api.StorageOperationalStatus(
            checked_at=datetime.now(UTC),
            store_statuses=store_statuses,
            issues=tuple(issues),
            recovery_actions=tuple(dict.fromkeys(actions)),
        )
        with self._lock:
            self._operational_status_history.append(status)
        return status

    def get_operational_status_history(
        self,
        *,
        limit: int | None = None,
    ) -> tuple[api.StorageOperationalStatus, ...]:
        """Return bounded process-local observations in chronological order.

        The manager retains at most the newest 100 snapshots created by
        ``get_operational_status``. Reading does not refresh Stores, append a snapshot, or expose the
        mutable deque. A zero limit returns no values; negative, boolean, or noninteger limits are
        rejected.

        :param limit: Optional maximum number of newest retained observations.
        :return: Immutable oldest-to-newest snapshot tuple.
        """
        if limit is not None:
            if isinstance(limit, bool) or not isinstance(limit, int):
                raise TypeError("operational status history limit must be an integer or None.")
            if limit < 0:
                raise ValueError("operational status history limit must not be negative.")
        with self._lock:
            history = tuple(self._operational_status_history)
        if limit is None:
            return history
        if limit == 0:
            return ()
        return history[-limit:]

    def _store_operational_findings(
        self,
        store_statuses: tuple[api.StoreStatusObservation, ...],
    ) -> tuple[
        list[api.StorageOperationalIssue],
        list[api.StorageRecoveryAction],
    ]:
        """
        Turn every plugin warning into an attributed warning issue, then add an unavailable issue
        and reload suggestion for each false availability flag. Message fallback applies only to
        false message values. Warning/issue construction failures propagate; writability alone
        produces no issue here.

        Example:
            >>> issues, actions = manager._store_operational_findings(())  # doctest: +SKIP


        :param store_statuses: Ordered observations already collected from configured Stores.
        :return: Pair of mutable issue/action lists in observation order.
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
                    recoverability=api.StorageOperationalRecoverability.UNKNOWN,
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
                    recoverability=api.StorageOperationalRecoverability.RETRYABLE,
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
        Read journal summaries, skip committed state, and classify failed entries as errors with
        retry suggestions. Other states become pending warnings with recovery suggestions.
        Missing/false state becomes unknown; operation attribution is retained only for UUID
        instances. Getter, mapping, and string-conversion failures are not generally suppressed.

        Example:
            >>> issues, actions = manager._ingest_operational_findings()  # doctest: +SKIP


        :return: Pair of journal issue/action lists in supplied journal order.
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
                        recoverability=api.StorageOperationalRecoverability.RETRYABLE,
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
                    recoverability=api.StorageOperationalRecoverability.AUTOMATIC,
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
        Inspect stored Replica state without reading or verifying bytes. MISSING and CORRUPT produce
        error severity; UNAVAILABLE produces warning severity. Corruption has its own issue code,
        while missing/unavailable share replica_unavailable. Each receives a replication suggestion;
        other states are ignored.

        Example:
            >>> issues, actions = manager._replica_operational_findings()  # doctest: +SKIP


        :return: Pair of attributed Replica issue/action lists in repository iteration order.
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
                    recoverability=api.StorageOperationalRecoverability.MANUAL,
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
        Assess each Asset and report unsatisfied replication/backup policy with planning
        suggestions. An Exception from the assessment call becomes a policy_assessment_failed error
        and skips that Asset. Unsatisfied policies are errors when the assessment says unavailable,
        otherwise warnings. Asset iteration and later assessment-property errors are outside that
        catch.

        Example:
            >>> issues, actions = manager._policy_operational_findings()  # doctest: +SKIP


        :return: Pair of policy issue/action lists; failed assessments contribute an issue without a suggested action.
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
                        recoverability=api.StorageOperationalRecoverability.RETRYABLE,
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
                        recoverability=api.StorageOperationalRecoverability.MANUAL,
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
        Snapshot an optional ingest_recovery_issues attribute and stringify every message into a
        warning. Missing attributes yield no warnings. No messages are cleared and no recovery runs;
        iteration/string/blank-message validation errors propagate.

        Example:
            >>> issues = manager._deferred_recovery_issues()  # doctest: +SKIP


        :return: New list of deferred-ingest recovery warning issues.
        """

        return [
            api.StorageOperationalIssue(
                "ingest_recovery_deferred",
                api.StorageOperationalSeverity.WARNING,
                str(message),
                recoverability=api.StorageOperationalRecoverability.AUTOMATIC,
            )
            for message in tuple(getattr(self, "ingest_recovery_issues", ()))
        ]

    def list_ingest_operations(self) -> tuple[Mapping[str, object], ...]:
        """
        Return the journal-status hook result unchanged. The transient hook supplies an empty tuple;
        application overrides can expose durable summaries. This wrapper adds no filtering, copying,
        or redaction.

        Example:
            >>> operations = manager.list_ingest_operations()  # doctest: +SKIP


        :return: Tuple of mappings returned by _ingest_journal_statuses.
        """

        return self._ingest_journal_statuses()

    def recover_pending_ingests(
        self,
        operation_id: UUID | None = None,
    ) -> tuple[str, ...]:
        """
        Return an empty result because this base implements no durable journal recovery. The
        operation UUID is ignored without validation; no issue state is cleared or publication
        inspected here.

        Example:
            >>> messages = manager.recover_pending_ingests()  # doctest: +SKIP


        :param operation_id: Unused optional UUID accepted for interface conformance.
        :return: Empty tuple for every call to this base implementation.
        """

        del operation_id
        return ()

    def retry_ingest_operation(
        self,
        operation_id: UUID,
    ) -> api.DigitalAssetIngestResult:
        """
        Reject retry because this base has no durable ingest journal. The supplied UUID is discarded
        without lookup or replay; the application manager can override this behavior.

        Example:
            >>> result = manager.retry_ingest_operation(operation_id)  # doctest: +SKIP


        :param operation_id: Unused UUID accepted for interface conformance.
        :return: Never returns; raises StoragePreconditionFailed explaining the absent durable journal.
        """

        del operation_id
        raise api.StoragePreconditionFailed(
            "transient storage managers have no durable ingest journal."
        )


__all__ = ["StorageOperationalStatusMixin"]

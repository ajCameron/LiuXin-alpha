"""
Exercise policy-value defaults, selected validation rules, and assessment/plan fields.

These cases construct public domain values without registering policies or touching
Store bytes. They distinguish declared flags and retained identifiers from actual
publication, retention, verification, or planning behavior.
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from LiuXin_alpha.storage.api import (
    BackupPolicy,
    BackupPolicyID,
    BackupPolicyRecord,
    DigitalAssetBackupPlan,
    DigitalAssetID,
    DigitalAssetLossAction,
    DigitalAssetReplicationPlan,
    ReplicaID,
    ReplicaMode,
    ReplicaSeparationDimension,
    ReplicationPolicy,
    ReplicationPolicyID,
    ReplicationPolicyRecord,
    ResolvedStoragePolicies,
    StoragePolicyAssessment,
)


def test_policy_records_validate_identity_payload_revision_and_resolution_source() -> None:
    """Keep passive records safe without coupling construction to persistence writes."""
    replication = ReplicationPolicyRecord(
        ReplicationPolicyID(1),
        ReplicationPolicy(),
        revision="r1",
    )
    backup = BackupPolicyRecord(
        BackupPolicyID(2),
        BackupPolicy(),
        revision="r2",
    )
    resolved = ResolvedStoragePolicies(
        replication.policy,
        backup.policy,
        "digital_asset",
        "manager_default",
    )

    assert resolved.replication is replication.policy
    assert resolved.backup_source == "manager_default"
    with pytest.raises(ValueError, match="must be positive"):
        ReplicationPolicyRecord(ReplicationPolicyID(0), ReplicationPolicy())
    with pytest.raises(TypeError, match="BackupPolicy"):
        BackupPolicyRecord(BackupPolicyID(1), ReplicationPolicy())  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="revision"):
        BackupPolicyRecord(BackupPolicyID(1), BackupPolicy(), revision=" ")
    with pytest.raises(ValueError, match="replication_source"):
        ResolvedStoragePolicies(
            ReplicationPolicy(),
            BackupPolicy(),
            " ",
            "manager_default",
        )


def test_replication_policy_defaults_are_explicit_and_safe() -> None:
    """
    Check the default target fallback, separation, synchronous count, flags, mode, and loss action.

    These assertions inspect a constructed value; they do not demonstrate publication durability,
    automatic healing, or policy execution.

    Example:
        >>> test_replication_policy_defaults_are_explicit_and_safe()


    :return: None after the stated policy-value assertions pass.
    """
    policy = ReplicationPolicy(name="two_copies", min_copies=2)

    assert policy.effective_target_copies == 2
    assert policy.distinct_by == (ReplicaSeparationDimension.STORE,)
    assert policy.synchronous_write_copies == 1
    assert policy.auto_heal is True
    assert policy.mode is ReplicaMode.ACTIVE
    assert policy.loss_action is DigitalAssetLossAction.REQUIRE_COPY


def test_replication_policy_validates_zero_copy_and_durability_constraints() -> None:
    """
    Exercise zero-copy consent, target ordering, synchronous-count bounds, and nonempty separation
    validation.

    An explicit ACCEPT_LOSS policy with zero synchronous copies is accepted at a zero target.
    Rejected combinations are checked through their ValueError diagnostics, without storing or
    deleting any bytes.

    Example:
        >>> test_replication_policy_validates_zero_copy_and_durability_constraints()


    :return: None after the stated policy-value assertions pass.
    """
    with pytest.raises(ValueError, match="zero-copy"):
        ReplicationPolicy(min_copies=0)
    assert ReplicationPolicy(
        min_copies=0,
        synchronous_write_copies=0,
        loss_action=DigitalAssetLossAction.ACCEPT_LOSS,
    ).effective_target_copies == 0
    with pytest.raises(ValueError, match="copy target"):
        ReplicationPolicy(min_copies=2, target_copies=1)
    with pytest.raises(ValueError, match="synchronous"):
        ReplicationPolicy(
            min_copies=2,
            target_copies=2,
            synchronous_write_copies=3,
        )
    with pytest.raises(ValueError, match="distinct_by"):
        ReplicationPolicy(distinct_by=())


def test_backup_policy_modes_counts_and_retention_are_validated() -> None:
    """
    Check archive-mode defaults and rejection of invalid mode, target, and zero-copy retention-lock
    combinations.

    Verification flags and target fallback are inspected as declarations. The test does not execute
    a backup or prove enforcement of retention by a planner or deletion workflow.

    Example:
        >>> test_backup_policy_modes_counts_and_retention_are_validated()


    :return: None after the stated policy-value assertions pass.
    """
    policy = BackupPolicy(
        name="deep_archive",
        min_copies=2,
        mode=ReplicaMode.ARCHIVE,
        retention_locked=True,
    )

    assert policy.effective_target_copies == 2
    assert policy.verify_after_write is True
    assert policy.periodic_verification is True
    with pytest.raises(ValueError, match="backup or archive"):
        BackupPolicy(mode=ReplicaMode.ACTIVE)
    with pytest.raises(ValueError, match="copy target"):
        BackupPolicy(min_copies=2, target_copies=1)
    with pytest.raises(ValueError, match="retention locked"):
        BackupPolicy(min_copies=0, target_copies=0, retention_locked=True)


def test_policy_assessment_and_plans_keep_typed_asset_replica_and_store_ids() -> None:
    """
    Retain representative Asset/Replica IDs, Store UUIDs, and diagnostics in assessment and plan
    tuples.

    The test also rejects a target-satisfied flag with an unsatisfied minimum. NewType IDs remain
    ordinary runtime integers; these assertions do not establish broader identity/type validation or
    execute the proposed work.

    Example:
        >>> test_policy_assessment_and_plans_keep_typed_asset_replica_and_store_ids()


    :return: None after the stated policy-value assertions pass.
    """
    asset_id = DigitalAssetID(7)
    replica_id = ReplicaID(12)
    store_ref = uuid4()
    assessment = StoragePolicyAssessment(
        digital_asset_id=asset_id,
        policy_name="two_copies",
        mode=ReplicaMode.ACTIVE,
        present_replica_ids=(replica_id,),
        healthy_replica_ids=(replica_id,),
        meets_minimum=False,
        meets_target=False,
        errors=("missing second copy",),
    )
    replication = DigitalAssetReplicationPlan(
        digital_asset_id=asset_id,
        destination_store_refs=(store_ref,),
        replica_ids_to_verify=(replica_id,),
    )
    backup = DigitalAssetBackupPlan(
        digital_asset_id=asset_id,
        destination_store_refs=(store_ref,),
        source_replica_ids=(replica_id,),
    )

    assert assessment.errors == ("missing second copy",)
    assert replication.destination_store_refs == (store_ref,)
    assert replication.implementable and replication.has_work
    assert backup.source_replica_ids == (replica_id,)
    assert backup.implementable and backup.has_work
    blocked = DigitalAssetReplicationPlan(
        digital_asset_id=asset_id,
        warnings=("no destination",),
        blocking_reasons=("no destination",),
    )
    assert not blocked.implementable and not blocked.has_work
    with pytest.raises(ValueError, match="both verified and removed"):
        DigitalAssetReplicationPlan(
            digital_asset_id=asset_id,
            replica_ids_to_verify=(replica_id,),
            replica_ids_to_remove=(replica_id,),
        )
    with pytest.raises(ValueError, match="meeting a target"):
        StoragePolicyAssessment(
            digital_asset_id=asset_id,
            policy_name="invalid",
            mode=ReplicaMode.ACTIVE,
            meets_minimum=False,
            meets_target=True,
        )

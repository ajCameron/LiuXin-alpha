from __future__ import annotations

from uuid import uuid4

import pytest

from LiuXin_alpha.storage.api import (
    BackupPolicy,
    DigitalAssetBackupPlan,
    DigitalAssetID,
    DigitalAssetLossAction,
    DigitalAssetReplicationPlan,
    ReplicaID,
    ReplicaMode,
    ReplicaSeparationDimension,
    ReplicationPolicy,
    StoragePolicyAssessment,
)


def test_replication_policy_defaults_are_explicit_and_safe() -> None:
    policy = ReplicationPolicy(name="two_copies", min_copies=2)

    assert policy.effective_target_copies == 2
    assert policy.distinct_by == (ReplicaSeparationDimension.STORE,)
    assert policy.synchronous_write_copies == 1
    assert policy.auto_heal is True
    assert policy.mode is ReplicaMode.ACTIVE
    assert policy.loss_action is DigitalAssetLossAction.REQUIRE_COPY


def test_replication_policy_validates_zero_copy_and_durability_constraints() -> None:
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
    assert backup.source_replica_ids == (replica_id,)
    with pytest.raises(ValueError, match="meeting a target"):
        StoragePolicyAssessment(
            digital_asset_id=asset_id,
            policy_name="invalid",
            mode=ReplicaMode.ACTIVE,
            meets_minimum=False,
            meets_target=True,
        )

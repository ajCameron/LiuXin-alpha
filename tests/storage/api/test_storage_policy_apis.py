from __future__ import annotations

import pytest

from LiuXin_alpha.storage.api import (
    BackupPolicy,
    DistinctBy,
    ReplicationMode,
    ReplicationPlan,
    ReplicationPolicy,
    ReplicationStatus,
)


def test_replication_policy_defaults_are_reasonable() -> None:
    policy = ReplicationPolicy(name="two_copies_min", min_copies=2)

    assert policy.name == "two_copies_min"
    assert policy.min_copies == 2
    assert policy.target_copies is None
    assert policy.effective_target_copies == 2
    assert policy.distinct_by == (DistinctBy.STORE,)
    assert policy.max_copies_per_bucket == 1
    assert policy.synchronous_write_copies == 1
    assert policy.auto_heal is True
    assert policy.mode == ReplicationMode.ACTIVE


def test_replication_policy_validation_rejects_bad_counts() -> None:
    with pytest.raises(ValueError):
        ReplicationPolicy(min_copies=0)

    with pytest.raises(ValueError):
        ReplicationPolicy(min_copies=2, target_copies=1)

    with pytest.raises(ValueError):
        ReplicationPolicy(min_copies=2, target_copies=2, synchronous_write_copies=3)

    with pytest.raises(ValueError):
        ReplicationPolicy(distinct_by=())


def test_backup_policy_defaults_and_validation() -> None:
    policy = BackupPolicy(name="deep_archive", min_backup_copies=2)

    assert policy.name == "deep_archive"
    assert policy.effective_target_backup_copies == 2
    assert policy.verify_after_write is True
    assert policy.periodic_verification is True
    assert policy.mode == ReplicationMode.BACKUP

    with pytest.raises(ValueError):
        BackupPolicy(min_backup_copies=0)

    with pytest.raises(ValueError):
        BackupPolicy(min_backup_copies=2, target_backup_copies=1)

    with pytest.raises(ValueError):
        BackupPolicy(distinct_by=())


def test_replication_status_and_plan_are_smoke_usable() -> None:
    status = ReplicationStatus(
        digital_asset_identifier="dummy://file",
        policy_name="two_copies_min",
        present_store_identifiers=("store-a",),
        healthy_store_identifiers=("store-a",),
        copy_count=1,
        healthy_copy_count=1,
        meets_minimum=False,
        meets_target=False,
        errors=("missing second copy",),
    )
    plan = ReplicationPlan(
        digital_asset_identifier="dummy://file",
        policy_name="two_copies_min",
        stores_to_add=("store-b",),
        stores_to_verify=("store-a",),
        warnings=("only one eligible store currently healthy",),
    )

    assert status.copy_count == 1
    assert status.errors == ("missing second copy",)
    assert plan.stores_to_add == ("store-b",)
    assert plan.stores_to_verify == ("store-a",)

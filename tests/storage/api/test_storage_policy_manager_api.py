"""Exercise manager policy defaults and atomic single-reference assignment conveniences."""

from __future__ import annotations

import pytest

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.storage_manager import TransientStorageManager


def _declared_asset(manager: TransientStorageManager) -> api.DigitalAssetRecord:
    """
    Register a small metadata-only Asset for policy-assignment tests.

    The digest is declaration evidence rather than a hash recomputed by this fixture. No Store or
    Replica is required because policy-reference mutation operates on catalogue state.

    :param manager: Transient manager that receives the declaration.
    :return: Newly registered Asset record with no explicit policy references.
    """

    return manager.declare_digital_asset(
        api.DigitalAssetDeclaration(
            3,
            (api.Digest("sha256", "fixture-digest"),),
        )
    )


def test_manager_exposes_configured_default_policy_values() -> None:
    """Return the exact frozen manager defaults without registering policy records."""

    replication = api.ReplicationPolicy(name="manager-replication")
    backup = api.BackupPolicy(name="manager-backup")
    manager = TransientStorageManager(
        default_replication_policy=replication,
        default_backup_policy=backup,
    )

    assert manager.default_replication_policy is replication
    assert manager.default_backup_policy is backup
    assert tuple(manager.iter_replication_policy_records()) == ()
    assert tuple(manager.iter_backup_policy_records()) == ()


def test_single_policy_setters_preserve_the_other_assignment() -> None:
    """Update and clear each policy reference without changing its independently assigned peer."""

    manager = TransientStorageManager()
    asset = _declared_asset(manager)
    replication = manager.create_replication_policy(
        api.ReplicationPolicy(name="explicit-replication")
    )
    backup = manager.create_backup_policy(api.BackupPolicy(name="explicit-backup"))

    assigned = manager.set_digital_asset_policies(
        asset.digital_asset_id,
        replication_policy_id=replication.replication_policy_id,
        backup_policy_id=backup.backup_policy_id,
        if_revision=asset.revision,
    )
    replication_cleared = manager.set_digital_asset_replication_policy(
        asset.digital_asset_id,
        None,
        if_revision=assigned.revision,
    )

    assert replication_cleared.replication_policy_id is None
    assert replication_cleared.backup_policy_id == backup.backup_policy_id

    replication_restored = manager.set_digital_asset_replication_policy(
        asset.digital_asset_id,
        replication.replication_policy_id,
        if_revision=replication_cleared.revision,
    )
    backup_cleared = manager.set_digital_asset_backup_policy(
        asset.digital_asset_id,
        None,
        if_revision=replication_restored.revision,
    )

    assert backup_cleared.replication_policy_id == replication.replication_policy_id
    assert backup_cleared.backup_policy_id is None


def test_single_policy_setter_honours_revision_without_partial_change() -> None:
    """Reject a stale single-reference update and retain both current assignments."""

    manager = TransientStorageManager()
    asset = _declared_asset(manager)
    backup = manager.create_backup_policy(api.BackupPolicy(name="backup"))
    assigned = manager.set_digital_asset_backup_policy(
        asset.digital_asset_id,
        backup.backup_policy_id,
        if_revision=asset.revision,
    )

    with pytest.raises(api.StoragePreconditionFailed, match="revision"):
        manager.set_digital_asset_replication_policy(
            asset.digital_asset_id,
            None,
            if_revision=asset.revision,
        )

    assert manager.get_digital_asset_record(asset.digital_asset_id) == assigned


def test_policy_assignment_conveniences_accept_records_and_clear_independently() -> None:
    """Normalize Asset/policy records while retaining the independently assigned peer policy."""

    manager = TransientStorageManager()
    asset = _declared_asset(manager)
    replication = manager.define_replication_policy("replication", copies=1)
    backup = manager.define_backup_policy("backup", copies=1)

    with_replication = manager.assign_replication_policy(
        asset,
        replication,
        if_revision=asset.revision,
    )
    with_both = manager.assign_backup_policy(
        with_replication,
        backup,
        if_revision=with_replication.revision,
    )
    without_replication = manager.assign_replication_policy(
        with_both,
        None,
        if_revision=with_both.revision,
    )

    assert without_replication.replication_policy_id is None
    assert without_replication.backup_policy_id == backup.backup_policy_id

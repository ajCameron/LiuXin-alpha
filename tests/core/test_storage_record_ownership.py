"""Keep public storage admin calls inside the Asset/Replica mutation owner."""

import base64
from types import SimpleNamespace

import pytest

from LiuXin_alpha.core.errors import CoreHandlerError
from LiuXin_alpha.core.factory import create_core


@pytest.fixture
def owned_core(tmp_path):
    root = tmp_path / "store"
    root.mkdir()
    with create_core(
        database_path=tmp_path / "library.sqlite", create=True, enable_maintenance=False
    ) as runtime:
        store = runtime.command(
            "storage.store.save",
            {
                "store": {
                    "store_name": "record-owner",
                    "store_kind": "filesystem",
                    "store_access_protocol": "file",
                    "store_root_uri": str(root),
                    "store_is_read_only": 0,
                    "store_online_status": "online",
                    "store_operational_role": "live",
                }
            },
        )["store"]
        runtime.command(
            "storage.refresh",
            {"startup_on_add": True, "clear_existing": True, "strict": True},
        )
        runtime.command("storage.default.set", {"store": store["store_name"]})
        stored = runtime.command(
            "storage.file.put",
            {
                "content_base64": base64.b64encode(b"owned storage record").decode(
                    "ascii"
                ),
                "original_name": "owned.txt",
            },
        )
        yield runtime, store, stored


@pytest.mark.parametrize("resource", ["asset", "replica"])
@pytest.mark.parametrize("action", ["create", "update", "delete"])
def test_raw_resource_routes_cannot_bypass_storage_records(
    owned_core, resource, action
):
    runtime, store, stored = owned_core
    asset_id = stored["asset"]["digital_asset_id"]
    replica_id = stored["replica_id"]
    manager = runtime.library.storage
    before_asset = manager.get_digital_asset_record(asset_id)
    before_replica = manager.get_replica_record(replica_id)
    values = (
        {"name": "unchecked asset"}
        if resource == "asset"
        else {
            "digital_asset_id": asset_id,
            "store_id": store["store_id"],
            "storage_key": "unverified.txt",
            "presence_status": "present",
        }
    )
    with pytest.raises(CoreHandlerError) as refused:
        runtime.command(
            f"storage.resource.{action}",
            {
                "resource": resource,
                "id": asset_id if resource == "asset" else replica_id,
                "values": values,
            },
        )
    assert refused.value.code == "storage_resource_read_only"
    assert manager.get_digital_asset_record(asset_id) == before_asset
    assert manager.get_replica_record(replica_id) == before_replica


def test_policy_assignment_updates_scalars_envelope_and_revision(owned_core):
    runtime, _store, stored = owned_core
    asset_id = stored["asset"]["digital_asset_id"]
    manager = runtime.library.storage
    before = manager.get_digital_asset_record(asset_id)
    policy = runtime.command(
        "storage.resource.create",
        {
            "resource": "replication-policy",
            "values": {"name": "owned policy", "min_copies": 1, "target_copies": 1},
        },
    )
    receipt = runtime.command(
        "storage.asset.policies.set",
        {
            "asset_id": asset_id,
            "replication_policy_id": policy["id"],
            "if_revision": before.revision,
        },
    )
    after = manager.get_digital_asset_record(asset_id)
    assert after.replication_policy_id == policy["id"]
    assert after.backup_policy_id == before.backup_policy_id
    assert after.revision != before.revision
    assert receipt["revision"] == after.revision
    row = runtime.library.db.macros.get_row("digital_assets", asset_id)
    assert row["digital_asset_replication_policy_id"] == policy["id"]
    with pytest.raises(CoreHandlerError, match="revision precondition failed"):
        runtime.command(
            "storage.asset.policies.set",
            {
                "asset_id": asset_id,
                "replication_policy_id": None,
                "if_revision": before.revision,
            },
        )
    assert manager.get_digital_asset_record(asset_id) == after
    runtime.command(
        "storage.asset.policies.set",
        {"asset_id": asset_id, "replication_policy_id": None},
    )
    assert manager.get_digital_asset_record(asset_id).replication_policy_id is None


def test_policy_owner_failure_rolls_back_all_record_forms(owned_core, monkeypatch):
    runtime, _store, stored = owned_core
    manager = runtime.library.storage
    asset_id = stored["asset"]["digital_asset_id"]
    before = manager.get_digital_asset_record(asset_id)
    row_before = dict(runtime.library.db.macros.get_row("digital_assets", asset_id))

    def fail_validation():
        raise ValueError("late owner validation")

    monkeypatch.setattr(manager, "_validate_all_recreation_policies", fail_validation)
    with pytest.raises(CoreHandlerError, match="late owner validation"):
        runtime.command(
            "storage.asset.policies.set",
            {
                "asset_id": asset_id,
                "replication_policy_id": None,
                "backup_policy_id": None,
            },
        )
    assert manager.get_digital_asset_record(asset_id) == before
    assert (
        dict(runtime.library.db.macros.get_row("digital_assets", asset_id))
        == row_before
    )


def test_policy_commit_receipt_survives_core_cache_refresh_failure(
    owned_core, monkeypatch
):
    runtime, _store, stored = owned_core
    manager = runtime.library.storage
    asset_id = stored["asset"]["digital_asset_id"]
    before = manager.get_digital_asset_record(asset_id)

    def fail_refresh():
        raise RuntimeError("cache refresh failed")

    monkeypatch.setattr(
        runtime.services, "_cache", SimpleNamespace(reload_data=fail_refresh)
    )
    with pytest.raises(CoreHandlerError) as failed:
        runtime.command(
            "storage.asset.policies.set",
            {
                "asset_id": asset_id,
                "replication_policy_id": None,
                "if_revision": before.revision,
            },
        )
    after = manager.get_digital_asset_record(asset_id)
    assert after.revision != before.revision
    assert failed.value.code == "cache_reconciliation_failed"
    assert failed.value.details["canonical_write_committed"] is True
    assert failed.value.details["receipt"]["revision"] == after.revision

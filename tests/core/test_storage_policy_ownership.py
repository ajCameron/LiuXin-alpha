"""Policy admin routes must preserve storage's records, references and revisions."""

import dataclasses
from types import SimpleNamespace
from uuid import uuid4

import pytest

from LiuXin_alpha.core.errors import CoreHandlerError
from LiuXin_alpha.core.factory import create_core
from LiuXin_alpha.storage import api


@pytest.fixture(params=["SQLite", "SQLite_apsw"])
def policy_core(tmp_path, request):
    if request.param == "SQLite_apsw":
        pytest.importorskip("apsw")
    with create_core(
        database_path=tmp_path / "library.sqlite",
        db_type=request.param,
        create=True,
        enable_maintenance=False,
    ) as runtime:
        yield runtime


@pytest.fixture(params=["replication-policy", "backup-policy"])
def policy_kind(request):
    return request.param


def _fields(kind):
    return (
        ("replication", "min_copies", "target_copies")
        if kind == "replication-policy"
        else ("backup", "min_backup_copies", "target_backup_copies")
    )


def _owned_policy(runtime, kind):
    manager = runtime.library.storage
    family, _minimum, _target = _fields(kind)
    policy_type = api.ReplicationPolicy if family == "replication" else api.BackupPolicy
    record = getattr(manager, f"create_{family}_policy")(
        policy_type(
            name=f"policy-{uuid4().hex}",
            min_copies=1,
            target_copies=2,
            retention_priority=37,
        )
    )
    return record, getattr(record, f"{family}_policy_id")


def test_core_policy_creation_uses_domain_defaults_and_revision(
    policy_core, policy_kind
):
    runtime = policy_core
    family, minimum, target = _fields(policy_kind)
    receipt = runtime.command(
        "storage.resource.create",
        {
            "resource": policy_kind,
            "values": {"name": "owned", minimum: 1, target: 2},
        },
    )
    record = getattr(runtime.library.storage, f"get_{family}_policy_record")(
        receipt["id"]
    )
    assert record.revision.startswith("d-")
    assert receipt["revision"] == record.revision
    assert record.policy.min_copies == 1
    assert record.policy.target_copies == 2
    assert record.policy.auto_heal is True
    assert record.policy.required_store_tags == frozenset()
    if family == "replication":
        assert record.policy.synchronous_write_copies == 1


def test_policy_patch_preserves_envelope_and_checks_revision(policy_core, policy_kind):
    runtime = policy_core
    family, minimum, target = _fields(policy_kind)
    before, identifier = _owned_policy(runtime, policy_kind)
    get_record = getattr(runtime.library.storage, f"get_{family}_policy_record")
    receipt = runtime.command(
        "storage.resource.update",
        {
            "resource": policy_kind,
            "id": identifier,
            "if_revision": before.revision,
            "values": {"name": "renamed", "required_store_tags_json": '["safe"]'},
        },
    )
    after = get_record(identifier)
    assert after.policy.name == "renamed"
    assert after.policy.required_store_tags == frozenset({"safe"})
    assert after.policy.target_copies == before.policy.target_copies
    assert after.policy.mode == before.policy.mode
    assert after.policy.retention_priority == 37
    assert after.revision != before.revision
    assert receipt["revision"] == after.revision
    table = f"{family}_policies"
    row = dict(runtime.library.db.macros.get_row(table, identifier))
    assert row[f"{family}_policy_name"] == after.policy.name

    with pytest.raises(CoreHandlerError, match="revision precondition failed"):
        runtime.command(
            "storage.resource.update",
            {
                "resource": policy_kind,
                "id": identifier,
                "if_revision": before.revision,
                "values": {"name": "stale replacement"},
            },
        )
    with pytest.raises(CoreHandlerError, match="copy target"):
        runtime.command(
            "storage.resource.update",
            {
                "resource": policy_kind,
                "id": identifier,
                "values": {minimum: 3, target: 1},
            },
        )
    with pytest.raises(CoreHandlerError, match="field"):
        runtime.command(
            "storage.resource.update",
            {
                "resource": policy_kind,
                "id": identifier,
                "values": {"scratch": "{}"},
            },
        )
    assert get_record(identifier) == after
    assert dict(runtime.library.db.macros.get_row(table, identifier)) == row

    deleted = runtime.command(
        "storage.resource.delete", {"resource": policy_kind, "id": identifier}
    )
    assert deleted["deleted"] is True
    assert runtime.library.db.macros.get_row(table, identifier) is None


def test_assigned_policy_deletion_uses_owner_reference_guard(policy_core, policy_kind):
    runtime = policy_core
    family, _minimum, _target = _fields(policy_kind)
    before, identifier = _owned_policy(runtime, policy_kind)
    assignment = {f"{family}_policy_id": identifier}
    asset = runtime.library.storage.declare_digital_asset(
        api.DigitalAssetDeclaration(1, (api.Digest("sha256", "a" * 64),), **assignment)
    )
    with pytest.raises(CoreHandlerError, match="still assigned"):
        runtime.command(
            "storage.resource.delete", {"resource": policy_kind, "id": identifier}
        )
    assert (
        getattr(runtime.library.storage, f"get_{family}_policy_record")(identifier)
        == before
    )
    assert (
        runtime.library.storage.get_digital_asset_record(asset.digital_asset_id)
        == asset
    )


def test_policy_update_rolls_back_after_owner_validation_failure(
    policy_core, monkeypatch
):
    runtime = policy_core
    manager = runtime.library.storage
    before, identifier = _owned_policy(runtime, "replication-policy")
    row = dict(runtime.library.db.macros.get_row("replication_policies", identifier))

    def fail_validation():
        raise ValueError("late policy validation")

    monkeypatch.setattr(manager, "_validate_all_recreation_policies", fail_validation)
    with pytest.raises(CoreHandlerError, match="late policy validation"):
        runtime.command(
            "storage.resource.update",
            {
                "resource": "replication-policy",
                "id": identifier,
                "values": {"name": "must roll back"},
            },
        )
    assert manager.get_replication_policy_record(identifier) == before
    assert (
        dict(runtime.library.db.macros.get_row("replication_policies", identifier))
        == row
    )


def test_policy_patch_detects_an_intervening_write_without_client_revision(
    policy_core, monkeypatch
):
    runtime = policy_core
    manager = runtime.library.storage
    before, identifier = _owned_policy(runtime, "replication-policy")
    original_update = manager.update_replication_policy
    intervening = []

    def interleave(policy_id, policy, *, if_revision=None):
        other = original_update(
            policy_id,
            dataclasses.replace(before.policy, retention_priority=99),
            if_revision=before.revision,
        )
        intervening.append(other)
        return original_update(policy_id, policy, if_revision=if_revision)

    monkeypatch.setattr(manager, "update_replication_policy", interleave)
    with pytest.raises(CoreHandlerError, match="revision precondition failed"):
        runtime.command(
            "storage.resource.update",
            {
                "resource": "replication-policy",
                "id": identifier,
                "values": {"name": "stale partial update"},
            },
        )
    assert manager.get_replication_policy_record(identifier) == intervening[0]
    assert intervening[0].policy.name == before.policy.name


def test_policy_readback_failure_retains_committed_receipt(policy_core, monkeypatch):
    from LiuXin_alpha.core import storage_graph_api

    runtime = policy_core

    def fail_readback(*_args, **_kwargs):
        raise OSError("readback unavailable")

    monkeypatch.setattr(
        storage_graph_api,
        "_macros",
        lambda _runtime: SimpleNamespace(get_row=fail_readback),
    )
    with pytest.raises(CoreHandlerError) as failed:
        runtime.command(
            "storage.resource.create",
            {"resource": "replication-policy", "values": {"name": "committed"}},
        )
    assert failed.value.code == "cache_reconciliation_failed"
    assert failed.value.details["canonical_write_committed"] is True
    receipt = failed.value.details["receipt"]
    assert receipt["created"] is True
    record = runtime.library.storage.get_replication_policy_record(receipt["id"])
    assert record.policy.name == "committed"
    assert record.revision == receipt["revision"]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("min_copies", True),
        ("min_copies", 1.5),
        ("min_copies", "2"),
        ("target_copies", False),
        ("auto_heal", "false"),
        ("auto_heal", 2),
        ("auto_heal", None),
        ("distinct_by_json", '{"store": true}'),
        ("required_store_tags_json", "[3]"),
        ("preferred_store_tags", [None]),
        ("scratch", "{}"),
        ("name_norm", "generated"),
        ("replication_policy_id", 4),
    ],
)
def test_policy_wire_values_refuse_ambiguous_or_bookkeeping_input(field, value):
    from LiuXin_alpha.core.storage_policy_mutations import patch_policy

    with pytest.raises(ValueError, match="field"):
        patch_policy(api.ReplicationPolicy(), "replication-policy", {field: value})


def test_policy_wire_aliases_and_domain_fields_preserve_intent():
    from LiuXin_alpha.core.storage_policy_mutations import patch_policy

    policy = patch_policy(
        api.BackupPolicy(),
        "backup-policy",
        {
            "backup_policy_min_backup_copies": 2,
            "target_backup_copies": None,
            "retention_locked": 1,
            "verify_after_write": False,
            "required_store_tags_json": '["offline"]',
            "distinct_by": ["store"],
            "retention_priority": 71,
        },
    )
    assert policy.effective_target_copies == 2
    assert policy.retention_locked is True
    assert policy.verify_after_write is False
    assert policy.required_store_tags == frozenset({"offline"})
    assert policy.distinct_by == (api.ReplicaSeparationDimension.STORE,)
    assert policy.retention_priority == 71
    with pytest.raises(ValueError, match="Duplicate policy field"):
        patch_policy(
            policy,
            "backup-policy",
            {"min_copies": 1, "backup_policy_min_backup_copies": 2},
        )

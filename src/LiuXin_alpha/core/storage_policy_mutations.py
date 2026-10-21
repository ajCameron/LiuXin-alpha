"""Translate Core policy edits into the existing storage-owned mutation APIs.

Wire coercion is confined here. Policy constraints, reference protection,
transactions and revision allocation remain with storage. Partial updates use
the revision read for patch construction even when the client omits a token.
"""

from __future__ import annotations

import dataclasses
import json
from collections.abc import Mapping
from typing import Any

from LiuXin_alpha.storage.api.storage_manager_api import (
    BackupPolicy,
    BackupPolicyID,
    BackupPolicyRecord,
    DigitalAssetLossAction,
    ReplicaMode,
    ReplicaSeparationDimension,
    ReplicationPolicy,
    ReplicationPolicyID,
    ReplicationPolicyRecord,
    StorageManagerAPI,
)

Policy = ReplicationPolicy | BackupPolicy
PolicyRecord = ReplicationPolicyRecord | BackupPolicyRecord

_INTEGER_FIELDS = frozenset(
    {
        "min_copies",
        "target_copies",
        "max_copies_per_bucket",
        "synchronous_write_copies",
        "retention_priority",
    }
)
_BOOLEAN_FIELDS = frozenset(
    {
        "auto_heal",
        "verify_after_write",
        "periodic_verification",
        "retention_locked",
    }
)
_SEQUENCE_FIELDS = frozenset(
    {
        "distinct_by",
        "required_store_tags",
        "preferred_store_tags",
        "forbidden_store_tags",
    }
)
_ALIASES = {f"{name}_json": name for name in _SEQUENCE_FIELDS}


def _family(resource: str) -> str:
    if resource == "replication-policy":
        return "replication"
    if resource == "backup-policy":
        return "backup"
    raise ValueError(f"Unknown policy resource: {resource!r}")


def _sequence_value(field: str, value: Any) -> object:
    parsed = json.loads(value) if isinstance(value, str) else value
    if not isinstance(parsed, (list, tuple)) or not all(
        isinstance(item, str) for item in parsed
    ):
        raise ValueError(f"Policy field {field!r} requires an array of strings")
    if field == "distinct_by":
        return tuple(ReplicaSeparationDimension(item) for item in parsed)
    return frozenset(parsed)


def _wire_value(field: str, value: Any) -> object:
    if field == "target_copies" and value is None:
        return None
    if field in _INTEGER_FIELDS:
        if type(value) is not int:
            raise ValueError(f"Policy field {field!r} requires an integer")
        return value
    if field in _BOOLEAN_FIELDS:
        if type(value) is bool or (type(value) is int and value in (0, 1)):
            return bool(value)
        raise ValueError(f"Policy field {field!r} requires a boolean or 0/1")
    if field in _SEQUENCE_FIELDS:
        return _sequence_value(field, value)
    if not isinstance(value, str):
        raise ValueError(f"Policy field {field!r} requires text")
    if field == "mode":
        return ReplicaMode(value)
    if field == "loss_action":
        return DigitalAssetLossAction(value)
    return value


def patch_policy(policy: Policy, resource: str, values: Mapping[str, Any]) -> Policy:
    """Preserve omitted fields; reject SQL bookkeeping and ambiguous aliases."""
    family = _family(resource)
    aliases = dict(_ALIASES)
    if family == "backup":
        aliases.update(
            min_backup_copies="min_copies", target_backup_copies="target_copies"
        )
    allowed = {item.name for item in dataclasses.fields(policy)}
    changes: dict[str, Any] = {}
    for raw_name, value in values.items():
        if not isinstance(raw_name, str):
            raise ValueError("Policy field names must be strings")
        name = raw_name.strip().removeprefix(f"{family}_policy_")
        field = aliases.get(name, name)
        if field not in allowed:
            raise ValueError(f"Unsupported policy field: {raw_name!r}")
        if field in changes:
            raise ValueError(f"Duplicate policy field: {field!r}")
        changes[field] = _wire_value(field, value)
    return dataclasses.replace(policy, **changes)


def create_policy(
    manager: StorageManagerAPI, resource: str, values: Mapping[str, Any]
) -> PolicyRecord:
    """Use domain defaults and let storage allocate the record and revision."""
    default = (
        ReplicationPolicy() if _family(resource) == "replication" else BackupPolicy()
    )
    policy = patch_policy(default, resource, values)
    if isinstance(policy, ReplicationPolicy):
        return manager.create_replication_policy(policy)
    return manager.create_backup_policy(policy)


def update_policy(
    manager: StorageManagerAPI,
    resource: str,
    identifier: int,
    values: Mapping[str, Any],
    *,
    if_revision: object = None,
) -> PolicyRecord:
    """Fence a partial replacement against its source record and client token."""
    if if_revision is not None and (
        not isinstance(if_revision, str) or not if_revision.strip()
    ):
        raise ValueError("Policy if_revision must be a nonblank string or null")
    current: PolicyRecord
    if _family(resource) == "replication":
        current = manager.get_replication_policy_record(ReplicationPolicyID(identifier))
    else:
        current = manager.get_backup_policy_record(BackupPolicyID(identifier))
    policy = patch_policy(current.policy, resource, values)
    expected = current.revision if if_revision is None else if_revision
    if isinstance(policy, ReplicationPolicy):
        return manager.update_replication_policy(
            ReplicationPolicyID(identifier), policy, if_revision=expected
        )
    return manager.update_backup_policy(
        BackupPolicyID(identifier), policy, if_revision=expected
    )


def delete_policy(manager: StorageManagerAPI, resource: str, identifier: int) -> bool:
    """Delegate reference protection and metadata removal without touching bytes."""
    if _family(resource) == "replication":
        return manager.delete_replication_policy(ReplicationPolicyID(identifier))
    return manager.delete_backup_policy(BackupPolicyID(identifier))
